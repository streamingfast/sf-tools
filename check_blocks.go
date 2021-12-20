package sftools

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/jsonpb"
	"go.uber.org/zap"
)

var numberRegex = regexp.MustCompile(`(\d{10})`)

type PrintDetails uint8

const (
	PrintNothing PrintDetails = iota
	PrintStats
	PrintFull
)

func CheckMergedBlocks(
	ctx context.Context,
	logger *zap.Logger,
	storeURL string,
	fileBlockSize uint32,
	blockRange BlockRange,
	blockPrinter func(block *bstream.Block),
	printDetails PrintDetails,
) error {
	fmt.Printf("Checking block holes on %s\n", storeURL)

	var expected uint32
	var count int
	var baseNum32 uint32

	holeFound := false
	expected = RoundToBundleStartBlock(uint32(blockRange.Start), fileBlockSize)
	currentStartBlk := uint32(blockRange.Start)
	seenFilters := map[string]FilteringFilters{}

	blocksStore, err := dstore.NewDBinStore(storeURL)
	if err != nil {
		return err
	}

	walkPrefix := WalkBlockPrefix(blockRange, fileBlockSize)

	logger.Debug("walking merged blocks", zap.Stringer("block_range", blockRange), zap.String("walk_prefix", walkPrefix))
	err = blocksStore.Walk(ctx, walkPrefix, ".tmp", func(filename string) error {
		match := numberRegex.FindStringSubmatch(filename)
		if match == nil {
			return nil
		}

		logger.Debug("received merged blocks", zap.String("filename", filename))

		count++
		baseNum, _ := strconv.ParseUint(match[1], 10, 32)
		if baseNum+uint64(fileBlockSize)-1 < blockRange.Start {
			logger.Debug("base num lower then block range start, quitting", zap.Uint64("base_num", baseNum), zap.Uint64("starting_at", blockRange.Start))
			return nil
		}

		baseNum32 = uint32(baseNum)

		if printDetails != PrintNothing {
			newSeenFilters := validateBlockSegment(ctx, blocksStore, filename, fileBlockSize, blockRange, blockPrinter, printDetails)
			for key, filters := range newSeenFilters {
				seenFilters[key] = filters
			}
		}

		if baseNum32 != expected {
			// There is no previous valid block range if we are at the ever first seen file
			if count > 1 {
				fmt.Printf("✅ Range %s\n", BlockRange{uint64(currentStartBlk), uint64(RoundToBundleEndBlock(expected-fileBlockSize, fileBlockSize))})
			}

			// Otherwise, we do not follow last seen element (previous is `100 - 199` but we are `299 - 300`)
			missingRange := BlockRange{uint64(expected), uint64(RoundToBundleEndBlock(baseNum32-fileBlockSize, fileBlockSize))}
			fmt.Printf("❌ Range %s! (Missing, [%s])\n", missingRange, missingRange.ReprocRange())
			currentStartBlk = baseNum32

			holeFound = true
		}
		expected = baseNum32 + fileBlockSize

		if count%10000 == 0 {
			fmt.Printf("✅ Range %s\n", BlockRange{uint64(currentStartBlk), uint64(RoundToBundleEndBlock(baseNum32, fileBlockSize))})
			currentStartBlk = baseNum32 + fileBlockSize
		}

		if !blockRange.Unbounded() && RoundToBundleEndBlock(baseNum32, fileBlockSize) >= uint32(blockRange.Stop-1) {
			return errStopWalk
		}

		return nil
	})
	if err != nil && err != errStopWalk {
		return err
	}

	actualEndBlock := RoundToBundleEndBlock(baseNum32, fileBlockSize)
	if !blockRange.Unbounded() {
		actualEndBlock = uint32(blockRange.Stop)
	}

	fmt.Printf("✅ Range %s\n", BlockRange{uint64(currentStartBlk), uint64(actualEndBlock)})

	if len(seenFilters) > 0 {
		fmt.Println()
		fmt.Println("Seen filters")
		for _, filters := range seenFilters {
			fmt.Printf("- [Include %q, Exclude %q, System %q]\n", filters.Include, filters.Exclude, filters.System)
		}
		fmt.Println()
	}

	if holeFound {
		fmt.Printf("🆘 Holes found!\n")
	} else {
		fmt.Printf("🆗 No hole found\n")
	}

	return nil
}

func validateBlockSegment(
	ctx context.Context,
	store dstore.Store,
	segment string,
	fileBlockSize uint32,
	blockRange BlockRange,
	blockPrinter func(block *bstream.Block),
	printDetails PrintDetails,
) (seenFilters map[string]FilteringFilters) {
	reader, err := store.OpenObject(ctx, segment)
	if err != nil {
		fmt.Printf("❌ Unable to read blocks segment %s: %s\n", segment, err)
		return
	}
	defer reader.Close()

	fdb := forkable.NewForkDB() // FIXME this should be global to the checker

	readerFactory, err := bstream.GetBlockReaderFactory.New(reader)
	if err != nil {
		fmt.Printf("❌ Unable to read blocks segment %s: %s\n", segment, err)
		return
	}

	// FIXME: Need to track block continuity (100, 101, 102a, 102b, 103, ...) and report which one are missing
	seenBlockCount := 0
	for {

		block, err := readerFactory.Read()
		if block != nil {

			if !blockRange.Unbounded() {
				if block.Number >= blockRange.Stop {
					return
				}

				if block.Number < blockRange.Start {
					continue
				}
			}

			if !fdb.HasLIB() {
				//FIXME need a way to override this from Commandline when checking
				fdb.InitLIB(block.PreviousRef())
			}

			fdb.AddLink(block.AsRef(), block.PreviousRef(), nil)
			if fdb.ReversibleSegment(block.AsRef()) == nil {

				// TODO: this print should be under a 'check forkable' flag, and needs a counter to see if it "never advances"....
				// if it never advances, maybe find the hole ...
				fmt.Println("this block is not in the right chain")
			}
			seenBlockCount++

			if printDetails == PrintStats {
				blockPrinter(block)
			}

			if printDetails == PrintFull {
				out, err := jsonpb.MarshalIndentToString(block.ToNative().(proto.Message), "  ")
				if err != nil {
					fmt.Printf("❌ Unable to print full block %s: %s\n", block.AsRef(), err)
					continue
				}

				fmt.Println(out)
			}

			continue
		}

		if block == nil && err == io.EOF {
			if seenBlockCount < expectedBlockCount(segment, fileBlockSize) {
				fmt.Printf("❌ Segment %s contained only %d blocks, expected at least 100\n", segment, seenBlockCount)
			}

			return
		}

		if err != nil {
			fmt.Printf("❌ Unable to read all blocks from segment %s after reading %d blocks: %s\n", segment, seenBlockCount, err)
			return
		}
	}
}

func WalkBlockPrefix(blockRange BlockRange, fileBlockSize uint32) string {
	if blockRange.Unbounded() {
		return ""
	}

	startString := fmt.Sprintf("%010d", RoundToBundleStartBlock(uint32(blockRange.Start), fileBlockSize))
	endString := fmt.Sprintf("%010d", RoundToBundleEndBlock(uint32(blockRange.Stop-1), fileBlockSize)+1)

	offset := 0
	for i := 0; i < len(startString); i++ {
		if startString[i] != endString[i] {
			return string(startString[0:i])
		}

		offset++
	}

	// At this point, the two strings are equal, to return the string
	return startString
}

func expectedBlockCount(segment string, fileBlockSize uint32) int {
	if segment == "0000000000" {
		return int(fileBlockSize) - int(bstream.GetProtocolFirstStreamableBlock)
	}

	return int(fileBlockSize)
}

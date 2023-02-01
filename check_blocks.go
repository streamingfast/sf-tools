package sftools

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/remeh/sizedwaitgroup"
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
	MaxUint64 = ^uint64(0)
)

type jobInfo struct {
	jobId      uint64
	blockRange BlockRange
}

type logInfo struct {
	jobId   uint64
	message string
	isDone  bool
}

type jobContext struct {
	ctx           context.Context
	logger        *zap.Logger
	storeURL      string
	fileBlockSize uint32
	blockRange    BlockRange
	blockPrinter  func(block *bstream.Block)
	printDetails  PrintDetails
	isDone        bool
	logs          chan logInfo
}

func (jc *jobContext) worker(jobs <-chan jobInfo, results chan<- error, swg *sizedwaitgroup.SizedWaitGroup) {
	for j := range jobs {
		results <- CheckMergedBlocks(jc.ctx, jc.logger, jc.storeURL, jc.fileBlockSize, j.blockRange, jc.blockPrinter, jc.printDetails, j.jobId, jc.logs)
		swg.Done()
		if jc.isDone {
			break
		}
	}
}

func (jc *jobContext) findMinMaxBlock() (uint64, uint64, error) {
	var min = MaxUint64
	var max = uint64(0)

	blocksStore, err := dstore.NewDBinStore(jc.storeURL)
	if err != nil {
		return min, max, err
	}
	err = blocksStore.Walk(jc.ctx, "", func(filename string) error {
		match := numberRegex.FindStringSubmatch(filename)
		if match == nil {
			return nil
		}
		baseNum, _ := strconv.ParseUint(match[1], 10, 32)
		if baseNum < min {
			min = baseNum
		}

		baseNum += uint64(jc.fileBlockSize)
		if baseNum > max {
			max = baseNum + uint64(jc.fileBlockSize)
		}
		return nil
	})
	return min, max, err
}

func (jc *jobContext) messageLogger(logDone chan<- bool) {

	defer func() { logDone <- true }()

	messages := make(map[uint64]string)
	isDone := make(map[uint64]bool)
	var currentJobId uint64 = 0
	var maxJobId uint64 = 0

	for log := range jc.logs {
		isDone[log.jobId] = log.isDone
		if log.jobId > maxJobId {
			maxJobId = log.jobId
		}

		if log.jobId == currentJobId {
			if messages[currentJobId] != "" {
				fmt.Println(messages[currentJobId])
				delete(messages, currentJobId)
			}
			if log.isDone {
				delete(messages, currentJobId)
				delete(isDone, currentJobId)
				currentJobId++
				if currentJobId > maxJobId && jc.isDone {
					break
				}
				continue
			}
			fmt.Println(log.message)
		} else {
			messages[log.jobId] += log.message
		}
		if isDone[currentJobId] {
			delete(messages, currentJobId)
			delete(isDone, currentJobId)
			fmt.Println(messages[currentJobId])
			currentJobId++
		}
	}
}

func CheckMergedBlocksBatch(
	ctx context.Context,
	logger *zap.Logger,
	storeURL string,
	fileBlockSize uint32,
	blockRange BlockRange,
	blockPrinter func(block *bstream.Block),
	printDetails PrintDetails,
	batchSize int,
	workers int,
) error {

	jc := jobContext{
		ctx:           ctx,
		logger:        logger,
		storeURL:      storeURL,
		fileBlockSize: fileBlockSize,
		blockRange:    blockRange,
		blockPrinter:  blockPrinter,
		printDetails:  printDetails,
		isDone:        false,
	}

	// find unbounded ranges
	var err error
	start := blockRange.Start
	stop := blockRange.Stop
	if blockRange.Unbounded() {
		start, stop, err = jc.findMinMaxBlock()
		if err != nil {
			return err
		}
	}

	// calculate batchCount
	offset := start
	totalSize := stop - start
	batchCount := totalSize / uint64(batchSize)
	extraBatchSize := totalSize % uint64(batchSize)
	if extraBatchSize != 0 {
		batchCount++
	}

	// limit concurrency
	swg := sizedwaitgroup.New(int(workers))

	// create log channel and message logger
	jc.logs = make(chan logInfo, workers*2)
	logDone := make(chan bool, 1)
	go jc.messageLogger(logDone)

	// create channels and workers
	results := make(chan error, batchCount)
	jobs := make(chan jobInfo, workers)

	// create workers
	for w := 0; w < workers; w++ {
		go jc.worker(jobs, results, &swg)
	}

	// assign jobs to workers
	for j := uint64(0); j < batchCount; j++ {
		start := j*uint64(batchSize) + offset
		stop := (j+1)*uint64(batchSize) - 1 + offset
		if j == batchCount-1 && extraBatchSize != 0 {
			stop += -uint64(batchSize) + extraBatchSize
		}
		blockRange := BlockRange{start, stop}
		swg.Add()
		jobs <- jobInfo{jobId: j, blockRange: blockRange}
	}

	swg.Wait()
	jc.isDone = true
	<-logDone
	return nil
}

func CheckMergedBlocks(
	ctx context.Context,
	logger *zap.Logger,
	storeURL string,
	fileBlockSize uint32,
	blockRange BlockRange,
	blockPrinter func(block *bstream.Block),
	printDetails PrintDetails, jobId uint64,
	logs chan<- logInfo,
) error {
	var msg string

	msg = fmt.Sprintf("Checking block holes on %s\n", storeURL)
	logs <- logInfo{jobId, msg, false}

	defer func() { logs <- logInfo{jobId, "", true} }()

	var expected uint32
	var count int
	var baseNum32 uint32
	var highestBlockSeen uint64
	lowestBlockSeen := MaxUint64

	if blockRange.Start < bstream.GetProtocolFirstStreamableBlock {
		blockRange.Start = bstream.GetProtocolFirstStreamableBlock
	}

	if blockRange.Start > blockRange.Stop {
		return fmt.Errorf("invalid range: start %d is after stop %d", blockRange.Start, blockRange.Stop)
	}

	holeFound := false
	expected = RoundToBundleStartBlock(uint32(blockRange.Start), fileBlockSize)
	currentStartBlk := uint32(blockRange.Start)
	seenFilters := map[string]FilteringFilters{}

	blocksStore, err := dstore.NewDBinStore(storeURL)
	if err != nil {
		return err
	}

	walkPrefix := WalkBlockPrefix(blockRange, fileBlockSize)

	tfdb := &trackedForkDB{
		fdb: forkable.NewForkDB(),
	}

	logger.Debug("walking merged blocks", zap.Stringer("block_range", blockRange), zap.String("walk_prefix", walkPrefix))
	err = blocksStore.Walk(ctx, walkPrefix, func(filename string) error {
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

		if baseNum32 != expected {
			// There is no previous valid block range if we are at the ever first seen file
			if count > 1 {
				msg = fmt.Sprintf("✅ Range %s\n", BlockRange{uint64(currentStartBlk), uint64(RoundToBundleEndBlock(expected-fileBlockSize, fileBlockSize))})
				logs <- logInfo{jobId, msg, false}
			}

			// Otherwise, we do not follow last seen element (previous is `100 - 199` but we are `299 - 300`)
			missingRange := BlockRange{uint64(expected), uint64(RoundToBundleEndBlock(baseNum32-fileBlockSize, fileBlockSize))}
			msg = fmt.Sprintf("❌ Range %s! (Missing, [%s])\n", missingRange, missingRange.ReprocRange())
			logs <- logInfo{jobId, msg, false}
			currentStartBlk = baseNum32

			holeFound = true
		}
		expected = baseNum32 + fileBlockSize

		if printDetails != PrintNothing {
			newSeenFilters, lowestBlockSegment, highestBlockSegment := validateBlockSegment(ctx, blocksStore, filename, fileBlockSize, blockRange, blockPrinter, printDetails, tfdb, jobId, logs)
			for key, filters := range newSeenFilters {
				seenFilters[key] = filters
			}
			if lowestBlockSegment < lowestBlockSeen {
				lowestBlockSeen = lowestBlockSegment
			}
			if highestBlockSegment > highestBlockSeen {
				highestBlockSeen = highestBlockSegment
			}
		} else {
			if uint64(baseNum32) < lowestBlockSeen {
				lowestBlockSeen = uint64(baseNum32)
			}
			if uint64(baseNum32+fileBlockSize) > highestBlockSeen {
				highestBlockSeen = uint64(baseNum32 + fileBlockSize)
			}
		}

		if count%10000 == 0 {
			msg = fmt.Sprintf("✅ Range %s\n", BlockRange{uint64(currentStartBlk), uint64(RoundToBundleEndBlock(baseNum32, fileBlockSize))})
			logs <- logInfo{jobId, msg, false}
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

	logger.Debug("checking incomplete range",
		zap.Stringer("range", blockRange),
		zap.Bool("range_unbounded", blockRange.Unbounded()),
		zap.Uint64("lowest_block_seen", lowestBlockSeen),
		zap.Uint64("highest_block_seen", highestBlockSeen),
	)

	if blockRange.Bounded() &&
		(highestBlockSeen < (blockRange.Stop-1) ||
			(lowestBlockSeen > blockRange.Start && lowestBlockSeen > bstream.GetProtocolFirstStreamableBlock)) {
		msg = fmt.Sprintf("🔶 Incomplete range %s, started at block %s and stopped at block: %s\n", blockRange, PrettyBlockNum(lowestBlockSeen), PrettyBlockNum(highestBlockSeen))
		logs <- logInfo{jobId, msg, false}
	}

	if tfdb.lastLinkedBlock != nil && tfdb.lastLinkedBlock.Number < highestBlockSeen {
		msg = fmt.Sprintf("🔶 Range %s has issues with forks, last linkable block number: %d\n", BlockRange{uint64(currentStartBlk), highestBlockSeen}, tfdb.lastLinkedBlock.Number)
		logs <- logInfo{jobId, msg, false}
	} else {
		msg = fmt.Sprintf("✅ Range %s\n", BlockRange{uint64(currentStartBlk), uint64(highestBlockSeen)})
		logs <- logInfo{jobId, msg, false}
	}

	if len(seenFilters) > 0 {
		msg = "\nSeen filters"
		for _, filters := range seenFilters {
			msg += fmt.Sprintf("- [Include %q, Exclude %q, System %q]\n", filters.Include, filters.Exclude, filters.System)
		}
		msg += "\n"
		logs <- logInfo{jobId, msg, false}
	}

	if holeFound {
		msg = fmt.Sprintf("🆘 Holes found!\n")
		logs <- logInfo{jobId, msg, false}
	} else {
		msg = fmt.Sprintf("🆗 No hole found\n")
		logs <- logInfo{jobId, msg, false}
	}

	return nil
}

type trackedForkDB struct {
	fdb                    *forkable.ForkDB
	firstUnlinkableBlock   *bstream.Block
	lastLinkedBlock        *bstream.Block
	unlinkableSegmentCount int
}

func validateBlockSegment(
	ctx context.Context,
	store dstore.Store,
	segment string,
	fileBlockSize uint32,
	blockRange BlockRange,
	blockPrinter func(block *bstream.Block),
	printDetails PrintDetails,
	tfdb *trackedForkDB,
	jobId uint64,
	logs chan<- logInfo,
) (seenFilters map[string]FilteringFilters, lowestBlockSeen, highestBlockSeen uint64) {
	var msg string
	lowestBlockSeen = MaxUint64
	reader, err := store.OpenObject(ctx, segment)
	if err != nil {
		msg = fmt.Sprintf("❌ Unable to read blocks segment %s: %s\n", segment, err)
		logs <- logInfo{jobId, msg, false}
		return
	}
	defer reader.Close()

	readerFactory, err := bstream.GetBlockReaderFactory.New(reader)
	if err != nil {
		msg = fmt.Sprintf("❌ Unable to read blocks segment %s: %s\n", segment, err)
		logs <- logInfo{jobId, msg, false}
		return
	}

	// FIXME: Need to track block continuity (100, 101, 102a, 102b, 103, ...) and report which one are missing
	seenBlockCount := 0
	for {
		block, err := readerFactory.Read()
		if block != nil {
			if !blockRange.Unbounded() {
				if block.Number > blockRange.Stop {
					return
				}

				if block.Number < blockRange.Start {
					continue
				}
			}
			if block.Number < lowestBlockSeen {
				lowestBlockSeen = block.Number
			}
			if block.Number > highestBlockSeen {
				highestBlockSeen = block.Number
			}

			if !tfdb.fdb.HasLIB() {
				tfdb.fdb.InitLIB(block)
			}

			tfdb.fdb.AddLink(block.AsRef(), block.PreviousID(), nil)
			revSeg, _ := tfdb.fdb.ReversibleSegment(block.AsRef())
			if revSeg == nil {
				tfdb.unlinkableSegmentCount++
				if tfdb.firstUnlinkableBlock == nil {
					tfdb.firstUnlinkableBlock = block
				}

				if printDetails != PrintNothing {
					// TODO: this print should be under a 'check forkable' flag?
					msg = fmt.Sprintf("🔶 Block #%d is not linkable at this point\n", block.Num())
					logs <- logInfo{jobId, msg, false}
				}

				if tfdb.unlinkableSegmentCount > 99 && tfdb.unlinkableSegmentCount%100 == 0 {
					// TODO: this print should be under a 'check forkable' flag?
					msg = fmt.Sprintf("❌ Large gap of %d unlinkable blocks found in chain. Last linked block: %d, first Unlinkable block: %d. \n", tfdb.unlinkableSegmentCount, tfdb.lastLinkedBlock.Num(), tfdb.firstUnlinkableBlock.Num())
					logs <- logInfo{jobId, msg, false}
				}
			} else {
				tfdb.lastLinkedBlock = block
				tfdb.unlinkableSegmentCount = 0
				tfdb.firstUnlinkableBlock = nil
				tfdb.fdb.SetLIB(block, block.PreviousId, block.LibNum)
				if tfdb.fdb.HasLIB() {
					tfdb.fdb.PurgeBeforeLIB(0)
				}
			}
			seenBlockCount++

			if printDetails == PrintStats {
				blockPrinter(block)
			}

			if printDetails == PrintFull {
				out, err := jsonpb.MarshalIndentToString(block.ToProtocol().(proto.Message), "  ")
				if err != nil {
					msg = fmt.Sprintf("❌ Unable to print full block %s: %s\n", block.AsRef(), err)
					logs <- logInfo{jobId, msg, false}
					continue
				}
				logs <- logInfo{jobId, out, false}
			}

			continue
		}

		if block == nil && err == io.EOF {
			if seenBlockCount < expectedBlockCount(segment, fileBlockSize) {
				msg = fmt.Sprintf("🔶 Segment %s contained only %d blocks (< 100), this can happen on some chains\n", segment, seenBlockCount)
				logs <- logInfo{jobId, msg, false}
			}

			return
		}

		if err != nil {
			msg = fmt.Sprintf("❌ Unable to read all blocks from segment %s after reading %d blocks: %s\n", segment, seenBlockCount, err)
			logs <- logInfo{jobId, msg, false}
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

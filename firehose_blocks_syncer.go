package sftools

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/firehose/client"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

// FirehoseResponseDecoder will usually look like this (+ error handling):
/*
	block := &pbcodec.Block{} 									// chain-specific protobuf Block
	anypb.UnmarshalTo(in, in.block, proto.UnmarshalOptions{})
	return codec.BlockFromProto(block) 							// chain-specific bstream block converter
*/
type FirehoseResponseDecoder func(in *anypb.Any) (*bstream.Block, error)

func DownloadFirehoseBlocks(
	ctx context.Context,
	endpoint string,
	jwt string,
	insecure bool,
	plaintext bool,
	startBlock uint64,
	stopBlock uint64,
	destURL string,
	respDecoder FirehoseResponseDecoder,
	tweakBlock func(*bstream.Block) (*bstream.Block, error),
	logger *zap.Logger) error {

	var retryDelay = time.Second * 4

	firehoseClient, connClose, grpcCallOpts, err := client.NewFirehoseClient(endpoint, jwt, insecure, plaintext)
	if err != nil {
		return err
	}
	defer connClose()

	store, err := dstore.NewDBinStore(destURL)
	if err != nil {
		return err
	}
	mergeWriter := newMergedBlocksWriter(store, logger, tweakBlock)

	for {

		request := &pbfirehose.Request{
			StartBlockNum: int64(startBlock),
			StopBlockNum:  stopBlock,
			ForkSteps:     []pbfirehose.ForkStep{pbfirehose.ForkStep_STEP_IRREVERSIBLE},
		}

		stream, err := firehoseClient.Blocks(ctx, request, grpcCallOpts...)
		if err != nil {
			return fmt.Errorf("unable to start blocks stream: %w", err)
		}

		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return nil
				}

				logger.Error("Stream encountered a remote error, going to retry",
					zap.Duration("retry_delay", retryDelay),
					zap.Error(err),
				)
				<-time.After(retryDelay)
				break
			}

			blk, err := respDecoder(response.Block)
			if err != nil {
				return fmt.Errorf("error decoding response to bstream block: %w", err)
			}
			if err := mergeWriter.process(blk); err != nil {
				return fmt.Errorf("write to blockwriter: %w", err)
			}

		}

	}

}

func newMergedBlocksWriter(store dstore.Store, logger *zap.Logger, tweakBlock func(*bstream.Block) (*bstream.Block, error)) *mergedBlocksWriter {
	return &mergedBlocksWriter{
		store:         store,
		writerFactory: bstream.GetBlockWriterFactory,
		tweakBlock:    tweakBlock,
		logger:        logger,
	}
}

type mergedBlocksWriter struct {
	store         dstore.Store
	lowBlockNum   uint64
	blocks        []*bstream.Block
	writerFactory bstream.BlockWriterFactory
	logger        *zap.Logger
	tweakBlock    func(*bstream.Block) (*bstream.Block, error)
}

func (w *mergedBlocksWriter) process(blk *bstream.Block) error {

	if w.lowBlockNum == 0 { // initial block
		if blk.Number%100 == 0 || blk.Number == bstream.GetProtocolFirstStreamableBlock {
			w.lowBlockNum = lowBoundary(blk.Number)
			w.blocks = append(w.blocks, blk)
			return nil
		} else {
			return fmt.Errorf("received unexpected block %s (not a boundary, not the first streamable block %d)", blk, bstream.GetProtocolFirstStreamableBlock)
		}
	}

	if blk.Number == w.lowBlockNum+99 {
		w.blocks = append(w.blocks, blk)

		if err := w.writeBundle(); err != nil {
			return err
		}
		return nil
	}
	if w.tweakBlock != nil {
		b, err := w.tweakBlock(blk)
		if err != nil {
			return fmt.Errorf("tweaking block: %w", err)
		}
		blk = b
	}
	w.blocks = append(w.blocks, blk)

	return nil
}

func filename(num uint64) string {
	return fmt.Sprintf("%010d", num)
}

func (w *mergedBlocksWriter) writeBundle() error {
	file := filename(w.lowBlockNum)
	w.logger.Info("writing merged file to store (suffix: .dbin.zst)", zap.String("filename", file), zap.Uint64("lowBlockNum", w.lowBlockNum))

	if len(w.blocks) == 0 {
		return fmt.Errorf("no blocks to write to bundle")
	}
	writeDone := make(chan struct{})

	pr, pw := io.Pipe()
	defer func() {
		pw.Close()
		<-writeDone
	}()

	go func() {
		err := w.store.WriteObject(context.Background(), file, pr)
		if err != nil {
			w.logger.Error("writing to store", zap.Error(err))
		}
		w.lowBlockNum += 100
		w.blocks = nil
		close(writeDone)
	}()

	blockWriter, err := w.writerFactory.New(pw)
	if err != nil {
		return err
	}

	for _, blk := range w.blocks {
		if err := blockWriter.Write(blk); err != nil {
			return err
		}
	}

	return err
}

func lowBoundary(i uint64) uint64 {
	return i - (i % 100)
}

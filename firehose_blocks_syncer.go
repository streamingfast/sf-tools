package sftools

import (
	"context"
	"fmt"
	"io"

	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/firehose/client"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/streamingfast/dstore"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"

	"go.uber.org/zap"
)

// AnyPBDecoder will usually look like this (+ error handling):
/*
	block := &pbcodec.Block{} 								// chain-specific protobuf Block
	anypb.UnmarshalTo(in, block, proto.UnmarshalOptions{})
	return codec.BlockFromProto(block) 						// chain-specific bstream block converter
*/
type AnyPBDecoder func(in *anypb.Any) (*bstream.Block, error)

func DownloadFirehoseBlocks(
	ctx context.Context,
	endpoint string,
	jwt string,
	insecure bool,
	plaintext bool,
	startBlock uint64,
	stopBlock uint64,
	destURL string,
	anypbToBstreamBlock AnyPBDecoder,
	logger *zap.Logger) error {

	var retryDelay = time.Second * 4

	firehoseClient, grpcCallOpts, err := client.NewFirehoseClient(endpoint, jwt, insecure, plaintext)
	if err != nil {
		return err
	}

	store, err := dstore.NewDBinStore(destURL)
	if err != nil {
		return err
	}
	mergeWriter := newMergedBlocksWriter(store, logger)

	for {
		forkSteps := []pbfirehose.ForkStep{pbfirehose.ForkStep_STEP_IRREVERSIBLE}

		request := &pbfirehose.Request{
			StartBlockNum: int64(startBlock),
			StopBlockNum:  stopBlock,
			ForkSteps:     forkSteps,
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

			blk, err := anypbToBstreamBlock(response.Block)

			if err := mergeWriter.process(blk); err != nil {
				return fmt.Errorf("write to blockwriter: %w", err)
			}

		}

	}

}

func newMergedBlocksWriter(store dstore.Store, logger *zap.Logger) *mergedBlocksWriter {
	return &mergedBlocksWriter{
		store:         store,
		writerFactory: bstream.GetBlockWriterFactory,
		logger:        logger,
	}
}

type mergedBlocksWriter struct {
	store         dstore.Store
	lowBlockNum   uint64
	blocks        []*bstream.Block
	writerFactory bstream.BlockWriterFactory
	logger        *zap.Logger
}

func (w *mergedBlocksWriter) process(blk *bstream.Block) error {
	if blk.Number%100 == 0 && w.lowBlockNum == 0 {
		if w.lowBlockNum == 0 { // initial block
			w.lowBlockNum = blk.Number
			w.blocks = append(w.blocks, blk)
			return nil
		}
	}

	if blk.Number == w.lowBlockNum+99 {
		w.blocks = append(w.blocks, blk)

		if err := w.writeBundle(); err != nil {
			return err
		}
		return nil
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

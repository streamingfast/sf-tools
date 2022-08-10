package sftools

import (
	"context"
	"fmt"
	"io"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type mergedBlocksWriter struct {
	store        dstore.Store
	lowBlockNum  uint64
	stopBlockNum uint64

	blocks        []*bstream.Block
	writerFactory bstream.BlockWriterFactory
	logger        *zap.Logger

	tweakBlock func(*bstream.Block) (*bstream.Block, error)
}

func (w *mergedBlocksWriter) ProcessBlock(blk *bstream.Block, obj interface{}) error {
	if w.tweakBlock != nil {
		b, err := w.tweakBlock(blk)
		if err != nil {
			return fmt.Errorf("tweaking block: %w", err)
		}
		blk = b
	}

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

	if blk.Number > w.lowBlockNum+99 {
		if err := w.writeBundle(); err != nil {
			return err
		}
		w.blocks = append(w.blocks, blk)

		return nil
	}

	if w.stopBlockNum > w.lowBlockNum && blk.Number >= w.stopBlockNum {
		return io.EOF
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

	pr, pw := io.Pipe()

	go func() {
		var err error
		defer func() {
			pw.CloseWithError(err)
		}()

		blockWriter, err := w.writerFactory.New(pw)
		if err != nil {
			return
		}

		for _, blk := range w.blocks {
			err = blockWriter.Write(blk)
			if err != nil {
				return
			}
		}
	}()

	err := w.store.WriteObject(context.Background(), file, pr)
	if err != nil {
		w.logger.Error("writing to store", zap.Error(err))
	}

	w.lowBlockNum += 100
	w.blocks = nil

	return err
}

func lowBoundary(i uint64) uint64 {
	return i - (i % 100)
}

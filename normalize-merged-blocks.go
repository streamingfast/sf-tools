package sftools

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/streamingfast/logging"
	"go.uber.org/zap"

	"github.com/spf13/cobra"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/stream"
	"github.com/streamingfast/dstore"
)

var GetMergedBlocksNormalizer = func(zlog *zap.Logger, tracer logging.Tracer, tweakFunc func(block *bstream.Block) (*bstream.Block, error)) *cobra.Command {
	out := &cobra.Command{
		Use:   "normalize-merged-blocks <source> <destination> <start> <stop>",
		Short: "from a merged-blocks source, rewrite normalized blocks to a merged-blocks destination. normalized blocks are FINAL only, with fixed log ordinals",
		Args:  cobra.ExactArgs(4),
		RunE:  getMergedBlockNormalizer(zlog, tracer, tweakFunc),
	}

	return out
}

func getMergedBlockNormalizer(zlog *zap.Logger, tracer logging.Tracer, tweakFunc func(block *bstream.Block) (*bstream.Block, error)) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		source := args[0]
		sourceStore, err := dstore.NewDBinStore(source)
		if err != nil {
			return fmt.Errorf("reading source store: %w", err)
		}

		dest := args[1]
		destStore, err := dstore.NewDBinStore(dest)
		if err != nil {
			return fmt.Errorf("reading destination store: %w", err)
		}

		start, err := strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			return fmt.Errorf("parsing start block num: %w", err)
		}
		stop, err := strconv.ParseUint(args[3], 10, 64)
		if err != nil {
			return fmt.Errorf("parsing stop block num: %w", err)
		}

		writer := &mergedBlocksWriter{
			store:         destStore,
			lowBlockNum:   start,
			stopBlockNum:  stop,
			writerFactory: bstream.GetBlockWriterFactory,
			tweakBlock:    tweakFunc,
			logger:        zlog,
		}
		stream := stream.New(nil, sourceStore, nil, int64(start), writer, stream.WithFinalBlocksOnly())

		err = stream.Run(context.Background())
		if errors.Is(err, io.EOF) {
			zlog.Info("Complete!")
			return nil
		}
		return err
	}
}

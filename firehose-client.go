package sftools

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/streamingfast/firehose/client"
	"github.com/streamingfast/jsonpb"
	"github.com/streamingfast/logging"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

type TransformsSetter func(cmd *cobra.Command) ([]*anypb.Any, error)

// You should add your custom 'transforms' flags to this command in your init(), then parse them in transformsSetter
var GetFirehoseClientCmd = func(zlog *zap.Logger, tracer logging.Tracer, transformsSetter TransformsSetter) *cobra.Command {
	out := &cobra.Command{
		Use:   "firehose-client",
		Short: "print firehose block stream as JSON",
		Args:  cobra.ExactArgs(3),
		RunE:  getFirehoseClientE(zlog, tracer, transformsSetter),
	}
	out.Flags().StringP("api-token-env-var", "a", "FIREHOSE_API_TOKEN", "Look for a JWT in this environment variable to authenticate against endpoint")
	out.Flags().String("cursor", "", "Send this cursor with the request")
	out.Flags().BoolP("plaintext", "p", false, "Use plaintext connection to firehose")
	out.Flags().BoolP("insecure", "k", false, "Skip SSL certificate validation when connecting to firehose")
	out.Flags().Bool("print-cursor-only", false, "Skip block decoding, only print the step cursor (useful for performance testing)")
	out.Flags().Bool("final-blocks-only", false, "Only ask for final blocks")
	return out
}

func getFirehoseClientE(zlog *zap.Logger, tracer logging.Tracer, transformsSetter TransformsSetter) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		endpoint := args[0]
		start, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("parsing start block num: %w", err)
		}
		stop, err := strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			return fmt.Errorf("parsing stop block num: %w", err)
		}
		apiTokenEnvVar := mustGetString(cmd, "api-token-env-var")
		jwt := os.Getenv(apiTokenEnvVar)

		cursor := mustGetString(cmd, "cursor")

		plaintext := mustGetBool(cmd, "plaintext")
		insecure := mustGetBool(cmd, "insecure")

		printCursorOnly := mustGetBool(cmd, "print-cursor-only")
		finalBlocksOnly := mustGetBool(cmd, "final-blocks-only")

		firehoseClient, connClose, grpcCallOpts, err := client.NewFirehoseClient(endpoint, jwt, insecure, plaintext)
		if err != nil {
			return err
		}
		defer connClose()

		var transforms []*anypb.Any
		if transformsSetter != nil {
			transforms, err = transformsSetter(cmd)
			if err != nil {
				return err
			}
		}
		steps := []pbfirehose.ForkStep{pbfirehose.ForkStep_STEP_NEW, pbfirehose.ForkStep_STEP_UNDO}

		if finalBlocksOnly {
			steps = []pbfirehose.ForkStep{pbfirehose.ForkStep_STEP_IRREVERSIBLE}
		}

		request := &pbfirehose.Request{
			StartBlockNum: int64(start),
			StopBlockNum:  stop,
			Transforms:    transforms,
			ForkSteps:     steps,
			StartCursor:   cursor,
		}

		stream, err := firehoseClient.Blocks(ctx, request, grpcCallOpts...)
		if err != nil {
			return fmt.Errorf("unable to start blocks stream: %w", err)
		}

		meta, err := stream.Header()
		if err != nil {
			zlog.Warn("cannot read header")
		} else {
			if hosts := meta.Get("hostname"); len(hosts) != 0 {
				zlog = zlog.With(zap.String("remote_hostname", hosts[0]))
			}
		}
		zlog.Info("connected")

		type respChan struct {
			ch chan string
		}

		resps := make(chan *respChan, 10)
		allDone := make(chan bool)

		if !printCursorOnly {
			// print the responses linearly
			go func() {
				for resp := range resps {
					line := <-resp.ch
					fmt.Println(line)
				}
				close(allDone)
			}()
		}

		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("stream error while receiving: %w", err)
			}

			if printCursorOnly {
				fmt.Printf("%s - %s\n", response.Step.String(), response.Cursor)
				continue
			}

			resp := &respChan{
				ch: make(chan string),
			}
			resps <- resp

			// async process the response
			go func() {
				line, err := jsonpb.MarshalToString(response)
				if err != nil {
					zlog.Error("marshalling to string", zap.Error(err))
				}
				resp.ch <- line
			}()
		}
		if printCursorOnly {
			return nil
		}

		close(resps)
		<-allDone
		return nil
	}
}

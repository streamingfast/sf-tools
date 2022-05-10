package sftools

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/merger/tools"
	"go.uber.org/zap"
)

var GetMergeCmd = func(zlog *zap.Logger, tracer logging.Tracer) *cobra.Command {
	out := &cobra.Command{
		Use:   "merge <source> <dest> <start> <stop>",
		Short: "download blocks from firehose and save them to merged-blocks",
		Args:  cobra.ExactArgs(4),
		RunE:  getMergeE(zlog, tracer),
	}
	out.Flags().Bool("delete", false, "delete one-block-files after merge")
	out.Flags().Bool("force", false, "skips user confirmation")
	return out
}

func getMergeE(zlog *zap.Logger, tracer logging.Tracer) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		sourceURL := args[0]
		destURL := args[1]

		oneBlocksStore, err := dstore.NewDBinStore(sourceURL)
		if err != nil {
			return fmt.Errorf("setting up oneblock store: %w", err)
		}

		mergedBlocksStore, err := dstore.NewDBinStore(destURL)
		if err != nil {
			return fmt.Errorf("setting up merge store: %w", err)
		}

		start, err := strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			return fmt.Errorf("parsing start block num: %w", err)
		}
		stop, err := strconv.ParseUint(args[3], 10, 64)
		if err != nil {
			return fmt.Errorf("parsing stop block num: %w", err)
		}
		if start/100*100 != start {
			return fmt.Errorf("start must be on a boundary")
		}

		withDelete := mustGetBool(cmd, "delete")
		confirmer := askForConfirmation
		if mustGetBool(cmd, "force") {
			confirmer = nil
		}
		return tools.Merge(zlog, tracer, oneBlocksStore, mergedBlocksStore, start, stop, 100, confirmer, withDelete)
	}
}

// askForConfirmation asks the user for confirmation. A user must type in "yes" or "no" and
// then press enter. It has fuzzy matching, so "y", "Y", "yes", "YES", and "Yes" all count as
// confirmations. If the input is not recognized, it will ask again. The function does not return
// until it gets a valid response from the user.
func askForConfirmation(s string) bool {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%s [y/n]: ", s)

		response, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		response = strings.ToLower(strings.TrimSpace(response))

		if response == "y" || response == "yes" {
			return true
		} else if response == "n" || response == "no" {
			return false
		}
	}
}

func mustGetBool(cmd *cobra.Command, flagName string) bool {
	val, err := cmd.Flags().GetBool(flagName)
	if err != nil {
		panic(fmt.Sprintf("flags: couldn't find flag %q", flagName))
	}
	return val
}

package sftools

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"os/exec"
)

func CompareBlockFiles(blockAFilePath, blockBFilePath string, logger *zap.Logger) (bool, error) {

	logger.Info("comparing block files",
		zap.String("block_a_file_path", blockAFilePath),
		zap.String("block_b_file_path", blockBFilePath),
	)

	cntA, err := ioutil.ReadFile(blockAFilePath)
	if err != nil {
		return false, fmt.Errorf("unable to read block file %q: %w", blockAFilePath, err)
	}

	cntB, err := ioutil.ReadFile(blockBFilePath)
	if err != nil {
		return false, fmt.Errorf("unable to read block file %q: %w", blockBFilePath, err)
	}

	var blocksAJSONInterface, blocksBJSONInterface interface{}

	if err = json.Unmarshal(cntA, &blocksAJSONInterface); err != nil {
		return false, fmt.Errorf("unable to unmarshal block %q: %w", blockAFilePath, err)
	}

	if err = json.Unmarshal(cntB, &blocksBJSONInterface); err != nil {
		return false, fmt.Errorf("unable to unmarshal block %q: %w", blockBFilePath, err)
	}

	if assert.ObjectsAreEqualValues(blocksAJSONInterface, blocksBJSONInterface) {
		fmt.Println("Files are equal, all good")
		return true, nil
	}

	useBash := true
	command := fmt.Sprintf("diff -C 5 \"%s\" \"%s\" | less", blockAFilePath, blockBFilePath)
	if os.Getenv("DIFF_EDITOR") != "" {
		command = fmt.Sprintf("%s \"%s\" \"%s\"", os.Getenv("DIFF_EDITOR"), blockAFilePath, blockBFilePath)
	}

	showDiff, wasAnswered := AskConfirmation(`File %q and %q differs, do you want to see the difference now`, blockAFilePath, blockBFilePath)
	if wasAnswered && showDiff {
		diffCmd := exec.Command(command)
		if useBash {
			diffCmd = exec.Command("bash", "-c", command)
		}

		diffCmd.Stdout = os.Stdout
		diffCmd.Stderr = os.Stderr

		if err := diffCmd.Run(); err != nil {
			return false, fmt.Errorf("diff command failed to run properly")
		}

		fmt.Println("You can run the following command to see it manually later:")
	} else {
		fmt.Println("Not showing diff between files, run the following command to see it manually:")
	}

	fmt.Println()
	fmt.Printf("    %s\n", command)
	fmt.Println("")
	return false, nil
}

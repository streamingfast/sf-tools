package sftools

import (
	"fmt"
	"github.com/lithammer/dedent"
	"github.com/manifoldco/promptui"
	"golang.org/x/crypto/ssh/terminal"
	"os"
)

func AskConfirmation(label string, args ...interface{}) (answeredYes bool, wasAnswered bool) {
	if !terminal.IsTerminal(int(os.Stdout.Fd())) {
		wasAnswered = false
		return
	}

	prompt := promptui.Prompt{
		Label:     dedent.Dedent(fmt.Sprintf(label, args...)),
		IsConfirm: true,
	}

	_, err := prompt.Run()
	if err != nil {
		// zlog.Debug("unable to aks user to see diff right now, too bad", zap.Error(err))
		wasAnswered = false
		return
	}

	wasAnswered = true
	answeredYes = true

	return
}

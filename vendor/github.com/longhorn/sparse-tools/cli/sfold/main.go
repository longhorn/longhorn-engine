package sfold

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"time"

	"github.com/longhorn/sparse-tools/sparse"
)

const (
	FoldFileUpdateInterval = 1 * time.Second
)

type FoldFileCLI struct {
	err  error
	done bool
}

func (f *FoldFileCLI) UpdateFoldFileProgress(progress int, done bool, err error) {
	f.done = done
	f.err = err
}

func Main() {
	defaultNonVerboseLogLevel := log.DebugLevel // set if -verbose is false
	// Command line parsing
	verbose := flag.Bool("verbose", false, "verbose mode")
	flag.Usage = func() {
		const usage = "fold <Options> <SrcOrChildFile> <DstOrParentFile>"
		const examples = `
Examples:
  fold child.snapshot parent.snapshot`
		fmt.Fprintf(os.Stderr, "\nUsage of %s:\n", os.Args[0])
		fmt.Fprintln(os.Stderr, usage)
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, examples)
	}
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		cmdError("missing file paths")
	}
	if len(args) < 2 {
		cmdError("missing destination file path")
	}
	if len(args) > 2 {
		cmdError("too many arguments")
	}

	srcPath := args[0]
	dstPath := args[1]
	if *verbose {
		fmt.Fprintf(os.Stderr, "Folding %s to %s...\n", srcPath, dstPath)
	} else {
		log.SetLevel(defaultNonVerboseLogLevel)
	}

	ops := &FoldFileCLI{}
	err := sparse.FoldFile(srcPath, dstPath, ops)
	if err != nil {
		log.Errorf("error starting to fold file: %s to: %s, err:%v", srcPath, dstPath, err)
		os.Exit(1)
	}

	doneChan := make(chan struct{})
	go func() {
		for {
			if ops.done {
				break
			}
			time.Sleep(FoldFileUpdateInterval)
		}
		close(doneChan)
	}()
	<-doneChan
	if ops.err != nil {
		log.Errorf("failed to fold file: %s to: %s, err: %v", srcPath, dstPath, ops.err)
		os.Exit(1)
	}
}

func cmdError(msg string) {
	fmt.Fprintln(os.Stderr, "Error:", msg)
	flag.Usage()
	os.Exit(2)
}

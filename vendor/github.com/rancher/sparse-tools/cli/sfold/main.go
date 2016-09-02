package sfold

import (
	"flag"
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/rancher/sparse-tools/sparse"
)

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

	err := sparse.FoldFile(srcPath, dstPath)
	if err != nil {
		log.Errorf("failed to fold file: %s to: %s, err: %v", srcPath, dstPath, err)
		os.Exit(1)
	}
}

func cmdError(msg string) {
	fmt.Fprintln(os.Stderr, "Error:", msg)
	flag.Usage()
	os.Exit(2)
}

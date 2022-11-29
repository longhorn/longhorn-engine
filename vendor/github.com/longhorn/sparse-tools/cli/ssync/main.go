package ssync

import (
	"context"
	"flag"

	log "github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"
	"github.com/longhorn/sparse-tools/sparse/rest"
)

const (
	usage = `
Usage of Ssync is:
sync <Options> <SrcFile> [<DstFile>]
Examples:
sync -daemon filePath
sync -host remoteHostName filePath`
)

func Main() {
	// Command line parsing
	verbose := flag.Bool("verbose", false, "verbose mode")
	daemon := flag.Bool("daemon", false, "daemon mode (run on remote host)")
	port := flag.String("port", "5000", "optional daemon port")
	timeout := flag.Int("timeout", 120, "optional daemon/client timeout (seconds)")
	host := flag.String("host", "", "remote host of <DstFile> (requires running daemon)")
	directIO := flag.Bool("directIO", true, "optional client sync file using directIO")
	fastSync := flag.Bool("fastSync", true, "optional fast synchronization based on file's change time and checksum")

	flag.Parse()

	if *verbose {
		log.SetLevel(log.DebugLevel)
	}

	args := flag.Args()
	if *daemon {
		if len(args) < 1 {
			log.Error(usage)
			log.Fatal("missing file path")
		}
		dstPath := args[0]

		ops := &rest.SyncFileStub{}
		err := rest.Server(context.Background(), *port, dstPath, ops)
		if err != nil {
			log.WithError(err).Fatalf("Ssync server failed")
		}
	} else {
		if len(args) < 1 {
			log.Error(usage)
			log.Fatal("missing file path")
		}
		srcPath := args[0]
		log.Infof("Syncing %s to %s:%s...\n", srcPath, *host, *port)

		err := sparse.SyncFile(srcPath, *host+":"+*port, *timeout, *directIO, *fastSync)
		if err != nil {
			log.WithError(err).Fatalf("Ssync client failed")
		}
		log.Info("Ssync client: exit code 0")
	}
}

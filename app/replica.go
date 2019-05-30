package app

import (
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/docker/go-units"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-engine/replica"
	"github.com/longhorn/longhorn-engine/replica/rest"
	"github.com/longhorn/longhorn-engine/replica/rpc"
	"github.com/longhorn/longhorn-engine/util"
)

func ReplicaCmd() cli.Command {
	return cli.Command{
		Name:      "replica",
		UsageText: "longhorn controller DIRECTORY SIZE",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:9502",
			},
			cli.StringFlag{
				Name:  "backing-file",
				Usage: "qcow file or encapsulating directory to use as the base image of this disk",
			},
			cli.BoolTFlag{
				Name: "sync-agent",
			},
			cli.StringFlag{
				Name:  "size",
				Usage: "Volume size in bytes or human readable 42kb, 42mb, 42gb",
			},
			cli.StringFlag{
				Name:  "restore-from",
				Usage: "specify backup to be restored, must be used with --restore-name",
			},
			cli.StringFlag{
				Name:  "restore-name",
				Usage: "specify the snapshot name for restore, must be used with --restore-from",
			},
		},
		Action: func(c *cli.Context) {
			if err := startReplica(c); err != nil {
				logrus.Fatalf("Error running start replica command: %v", err)
			}
		},
	}
}

func startReplica(c *cli.Context) error {
	if c.NArg() != 1 {
		return errors.New("directory name is required")
	}

	dir := c.Args()[0]
	backingFile, err := openBackingFile(c.String("backing-file"))
	if err != nil {
		return err
	}

	s := replica.NewServer(dir, backingFile, 512)

	address := c.String("listen")

	size := c.String("size")
	restoreURL := c.String("restore-from")
	restoreName := c.String("restore-name")

	if restoreURL != "" && restoreName != "" {
		if err := s.Restore(restoreURL, restoreName); err != nil {
			return err
		}
	} else if size != "" {
		size, err := units.RAMInBytes(size)
		if err != nil {
			return err
		}

		if err := s.Create(size); err != nil {
			return err
		}
	}

	controlAddress, dataAddress, syncAddress, replicaServiceURL, err := util.ParseAddresses(address)
	if err != nil {
		return err
	}

	resp := make(chan error)

	go func() {
		server := rest.NewServer(s)
		router := http.Handler(rest.NewRouter(server))
		router = util.FilteredLoggingHandler(map[string]struct{}{
			"/ping":          {},
			"/v1":            {},
			"/v1/replicas/1": {},
		}, os.Stdout, router)
		logrus.Infof("Listening on control %s", controlAddress)
		err := http.ListenAndServe(controlAddress, router)
		logrus.Warnf("Replica rest server at %v is down: %v", controlAddress, err)
	}()

	go func() {
		listen, err := net.Listen("tcp", replicaServiceURL)
		if err != nil {
			logrus.Warnf("Failed to listen %v: %v", replicaServiceURL, err)
			resp <- err
			return
		}

		server := grpc.NewServer()
		rpc.RegisterReplicaServiceServer(server, rpc.NewReplicaServer(s))
		reflection.Register(server)

		logrus.Infof("Listening on gRPC Replica server %s", replicaServiceURL)
		err = server.Serve(listen)
		logrus.Warnf("gRPC Replica server at %v is down: %v", replicaServiceURL, err)
		resp <- err
	}()

	go func() {
		rpcServer := rpc.NewDataServer(dataAddress, s)
		logrus.Infof("Listening on data %s", dataAddress)
		err := rpcServer.ListenAndServe()
		logrus.Warnf("Replica rest server at %v is down: %v", dataAddress, err)
		resp <- err
	}()

	if c.Bool("sync-agent") {
		exe, err := exec.LookPath(os.Args[0])
		if err != nil {
			return err
		}

		exe, err = filepath.Abs(exe)
		if err != nil {
			return err
		}

		go func() {
			cmd := exec.Command(exe, "sync-agent", "--listen", syncAddress)
			cmd.SysProcAttr = &syscall.SysProcAttr{
				Pdeathsig: syscall.SIGKILL,
			}
			cmd.Dir = dir
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			logrus.Infof("Listening on sync agent %s", syncAddress)
			err := cmd.Run()
			logrus.Warnf("Replica sync agent at %v is down: %v", syncAddress, err)
			resp <- err
		}()
	}

	// empty shutdown hook for signal message
	addShutdown(func() {})

	return <-resp
}

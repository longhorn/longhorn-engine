package app

import (
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/docker/go-units"
	"github.com/gorilla/handlers"
	"github.com/rancher/longhorn/replica"
	"github.com/rancher/longhorn/replica/rest"
	"github.com/rancher/longhorn/replica/rpc"
	"github.com/rancher/longhorn/util"
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
				Usage: "qcow file to use as the base image of this disk",
			},
			cli.BoolTFlag{
				Name: "sync-agent",
			},
			cli.StringFlag{
				Name:  "size",
				Usage: "Volume size in bytes or human readable 42kb, 42mb, 42gb",
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
	if size != "" {
		size, err := units.RAMInBytes(size)
		if err != nil {
			return err
		}

		if err := s.Create(size); err != nil {
			return err
		}
	}

	controlAddress, dataAddress, syncAddress, err := util.ParseAddresses(address)
	if err != nil {
		return err
	}

	resp := make(chan error)

	go func() {
		server := rest.NewServer(s)
		handler := http.Handler(rest.NewRouter(server))
		loggingHandler := handlers.LoggingHandler(os.Stdout, handler)
		wrappedRouter := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			switch req.Method {
			case "GET":
				switch req.URL.Path {
				case "/ping", "/v1/replicas/1":
					// bypass http logging for frequent control plane verbs/URLs
					handler.ServeHTTP(rw, req)
					return
				}
			}
			loggingHandler.ServeHTTP(rw, req)
		})
		logrus.Infof("Listening on control %s", controlAddress)
		resp <- http.ListenAndServe(controlAddress, wrappedRouter)
	}()

	go func() {
		rpcServer := rpc.New(dataAddress, s)
		logrus.Infof("Listening on data %s", dataAddress)
		resp <- rpcServer.ListenAndServe()
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
			resp <- cmd.Run()
		}()
	}

	return <-resp
}

package main

import (
	"net/http"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rancher/longhorn/agent/controller"
	"github.com/rancher/longhorn/agent/controller/rest"
	replica "github.com/rancher/longhorn/agent/replica/rest"
	"github.com/rancher/longhorn/agent/status"
)

var (
	VERSION = "0.0.0"
)

func main() {
	app := cli.NewApp()
	app.Version = VERSION
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name: "controller",
		},
		cli.BoolFlag{
			Name: "replica",
		},
		cli.StringFlag{
			Name:  "listen, l",
			Value: ":8199",
		},
		cli.StringFlag{
			Name:  "log-level",
			Value: "debug",
		},
	}
	app.Action = func(c *cli.Context) {
		if err := runApp(c); err != nil {
			logrus.Fatal(err)
		}
	}
	app.Run(os.Args)
}

func runApp(context *cli.Context) error {
	logLevel := context.GlobalString("log-level")
	lvl, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return err
	}
	logrus.SetLevel(lvl)

	runController := context.GlobalBool("controller")
	runReplica := context.GlobalBool("replica")

	if runController {
		go runPing(context)
		go runControllerAPI(context)
		c := controller.New()
		defer c.Close()
		return c.Start()
	} else if runReplica {
		go runReplicaAPI(context)
		return runPing(context)
	}

	return nil
}

func runControllerAPI(context *cli.Context) {
	server := rest.NewServer()
	router := http.Handler(rest.NewRouter(server))

	router = handlers.LoggingHandler(os.Stdout, router)
	router = handlers.ProxyHeaders(router)
	listen := "0.0.0.0:80"
	logrus.Infof("Listening on %s", listen)
	err := http.ListenAndServe(listen, router)
	logrus.Fatalf("API returned with error: %v", err)
}

func runReplicaAPI(context *cli.Context) {
	router := http.Handler(replica.NewRouter())
	router = handlers.LoggingHandler(os.Stdout, router)
	router = handlers.ProxyHeaders(router)
	listen := "0.0.0.0:80"
	logrus.Infof("Listening on %s", listen)
	err := http.ListenAndServe(listen, router)
	logrus.Fatalf("API returned with error: %v", err)
}

func runPing(context *cli.Context) error {
	controller := status.NewControllerStatus()

	replica, err := status.NewReplicaStatus()
	if err != nil {
		return err
	}

	r := mux.NewRouter()
	r.Handle("/controller/status", controller)
	r.Handle("/replica/status", replica)
	http.Handle("/", r)

	listen := context.GlobalString("listen")
	logrus.Info("Listening for healthcheck requests on ", listen)
	return http.ListenAndServe(listen, nil)
}

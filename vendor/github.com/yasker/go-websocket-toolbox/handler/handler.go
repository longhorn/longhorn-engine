package handler

import (
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	"github.com/yasker/go-websocket-toolbox/broadcaster"
)

const (
	keepAlivePeriod = 5 * time.Second
	timeoutPeriod   = 3 * keepAlivePeriod

	writeWait = 10 * time.Second

	MessagePong            = "pong"
	WebSocketCommandPrefix = "wscmd_"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewStreamHandlerFunc(streamType string,
	eventProcessor func(event *broadcaster.Event, r *http.Request) (interface{}, error),
	b *broadcaster.Broadcaster, events ...string) func(w http.ResponseWriter, r *http.Request) error {

	return func(w http.ResponseWriter, r *http.Request) error {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return err
		}
		fields := logrus.Fields{
			"id":   strconv.Itoa(rand.Int()),
			"type": streamType,
		}
		logrus.WithFields(fields).Info("websocket: open")

		watcher := b.NewWatcher(events...)
		defer watcher.Close()

		done := make(chan struct{})

		timeoutTimer := time.NewTimer(timeoutPeriod)

		conn.SetPongHandler(func(data string) error {
			timeoutTimer.Reset(timeoutPeriod)
			return nil
		})

		responseChan := make(chan string, 10)
		sendKeepalive := false
		go func() {
			defer close(done)
			for {
				msgType, msg, err := conn.ReadMessage()
				if err != nil {
					logrus.WithFields(fields).Infof("close websocket: %v", err.Error())
					return
				}
				msgString := string(msg)
				if msgType == 1 && strings.HasPrefix(msgString, WebSocketCommandPrefix) {
					cmd := strings.TrimPrefix(msgString, WebSocketCommandPrefix)
					switch cmd {
					case "ping":
						responseChan <- MessagePong
					case "start_keepalive":
						sendKeepalive = true
					case "stop_keepalive":
						sendKeepalive = false
					}
				}
			}
		}()

		keepAliveTicker := time.NewTicker(keepAlivePeriod)
		for {
			select {
			case <-done:
				return nil
			case event := <-watcher.Events():
				data, err := eventProcessor(event, r)
				if err != nil {
					return err
				}
				conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err = conn.WriteJSON(data); err != nil {
					return err
				}
			case resp := <-responseChan:
				if err = conn.WriteJSON(resp); err != nil {
					return err
				}
			case <-keepAliveTicker.C:
				if err = conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeWait)); err != nil {
					return err
				}
				if sendKeepalive {
					if err := conn.WriteJSON(MessagePong); err != nil {
						return err
					}
				}
			case <-timeoutTimer.C:
				logrus.WithFields(fields).Info("websocket: no response for ping, close websocket due to timeout")
				return fmt.Errorf("websocket ping timeout")
			}
		}
	}
}

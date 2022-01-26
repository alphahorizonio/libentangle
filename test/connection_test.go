package test

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/alphahorizonio/libentangle/internal/logging"
	dataApi "github.com/alphahorizonio/libentangle/pkg/api/datachannels/v1"
	api "github.com/alphahorizonio/libentangle/pkg/api/websockets/v1"
	"github.com/alphahorizonio/libentangle/pkg/callbacks"
	"github.com/alphahorizonio/libentangle/pkg/handlers"
	"github.com/alphahorizonio/libentangle/pkg/networking"
	"github.com/alphahorizonio/libentangle/pkg/signaling"
	"github.com/pion/webrtc/v3"
	"nhooyr.io/websocket"
)

func TestConnection(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:9090")
	if err != nil {
		panic(err)
	}

	log.Printf("signaling server listening on %v", addr)

	communityManager := handlers.NewCommunitiesManager()

	l := logging.NewJSONLogger(2)

	signaler := signaling.NewSignalingServer(
		func(application api.Application, conn *websocket.Conn) error {
			return communityManager.HandleApplication(application, conn)
		},
		func(ready api.Ready, conn *websocket.Conn) error {
			return communityManager.HandleReady(ready, conn)
		},
		func(offer api.Offer) error {
			return communityManager.HandleOffer(offer)
		},
		func(answer api.Answer) error {
			return communityManager.HandleAnswer(answer)
		},
		func(candidate api.Candidate) error {
			return communityManager.HandleCandidate(candidate)
		},
		func(exited api.Exited) error {
			return communityManager.HandleExited(exited)
		},
		l,
	)

	handler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(rw, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true, // CORS
		})
		if err != nil {
			panic(err)
		}

		log.Println("client connected")

		go func() {
			signaler.HandleConn(*conn)
		}()
	})

	go http.ListenAndServe(addr.String(), handler)

	onOpen := make(chan struct{})
	manager := handlers.NewClientManager(func() {
		onOpen <- struct{}{}
	})

	connectionManager := networking.NewConnectionManager(manager)

	var file *os.File

	callback := callbacks.NewCallback(l)

	go connectionManager.Connect("localhost:9090", "test", callback.GetServerCallback(*connectionManager, file, "path/to/file.tar"), callback.GetDebugErrorCallback(), l)

	rmFile := networking.NewRemoteFile(*connectionManager)

	go connectionManager.Connect("localhost:9090", "test", callback.GetClientCallback(*rmFile), callback.GetDebugErrorCallback(), l)

	<-onOpen
}

func TestMessage(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:9091")
	if err != nil {
		panic(err)
	}

	log.Printf("signaling server listening on %v", addr)

	communityManager := handlers.NewCommunitiesManager()

	l := logging.NewJSONLogger(2)

	signaler := signaling.NewSignalingServer(
		func(application api.Application, conn *websocket.Conn) error {
			return communityManager.HandleApplication(application, conn)
		},
		func(ready api.Ready, conn *websocket.Conn) error {
			return communityManager.HandleReady(ready, conn)
		},
		func(offer api.Offer) error {
			return communityManager.HandleOffer(offer)
		},
		func(answer api.Answer) error {
			return communityManager.HandleAnswer(answer)
		},
		func(candidate api.Candidate) error {
			return communityManager.HandleCandidate(candidate)
		},
		func(exited api.Exited) error {
			return communityManager.HandleExited(exited)
		},
		l,
	)

	handler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(rw, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true, // CORS
		})
		if err != nil {
			panic(err)
		}

		log.Println("client connected")

		go func() {
			signaler.HandleConn(*conn)
		}()
	})

	go http.ListenAndServe(addr.String(), handler)

	onOpen := make(chan struct{})
	onOpenClient := make(chan struct{})
	finished := make(chan struct{})

	manager := handlers.NewClientManager(func() {
		onOpen <- struct{}{}
	})

	managerClient := handlers.NewClientManager(func() {
		onOpenClient <- struct{}{}
	})

	connectionManager := networking.NewConnectionManager(manager)
	connectionManagerClient := networking.NewConnectionManager(managerClient)

	callback := callbacks.NewCallback(l)

	go connectionManager.Connect("localhost:9091", "test", func(msg webrtc.DataChannelMessage) {
		var w dataApi.WrappedMessage

		if err := json.Unmarshal(msg.Data, &w); err != nil {
			t.Fail()
		}

		var v api.Message

		if err := json.Unmarshal(w.Payload, &v); err != nil {
			t.Fail()
		}

		switch v.Opcode {
		case dataApi.OpcodeOpen:
			finished <- struct{}{}
		default:
			t.Fail()
		}
	}, callback.GetDebugErrorCallback(), l)

	rmFile := networking.NewRemoteFile(*connectionManagerClient)

	go connectionManagerClient.Connect("localhost:9091", "test", callback.GetClientCallback(*rmFile), callback.GetDebugErrorCallback(), l)

	<-onOpen
	<-onOpenClient

	msg, err := json.Marshal(dataApi.NewOpenOp(true))
	if err != nil {
		panic(err)
	}

	connectionManagerClient.Write(msg)

	<-finished
}

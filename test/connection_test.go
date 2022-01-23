package test

import (
	"log"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	api "github.com/alphahorizonio/libentangle/pkg/api/websockets/v1"
	"github.com/alphahorizonio/libentangle/pkg/callbacks"
	"github.com/alphahorizonio/libentangle/pkg/handlers"
	"github.com/alphahorizonio/libentangle/pkg/networking"
	"github.com/alphahorizonio/libentangle/pkg/signaling"
	"nhooyr.io/websocket"
)

func TestConnection(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:9090")
	if err != nil {
		panic(err)
	}

	log.Printf("signaling server listening on %v", addr)

	communityManager := handlers.NewCommunitiesManager()

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

	callback := callbacks.NewCallback()

	var errChanServer chan error
	go connectionManager.Connect("localhost:9090", "test", callback.GetServerCallback(*connectionManager, file, "path/to/file.tar"), callback.GetDebugErrorCallback(), errChanServer)

	time.Sleep(50 * time.Millisecond)

	rmFile := networking.NewRemoteFile(*connectionManager)

	var errChanClient chan error
	go connectionManager.Connect("localhost:9090", "test", callback.GetClientCallback(*rmFile), callback.GetDebugErrorCallback(), errChanClient)

	<-onOpen
}

package signaling

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	api "github.com/alphahorizonio/libentangle/pkg/api/websockets/v1"
	"github.com/alphahorizonio/libentangle/pkg/config"
	"github.com/google/uuid"
	"github.com/pion/webrtc/v3"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type SignalingClient struct {
	onAcceptance   func(conn *websocket.Conn, uuid string) error
	onIntroduction func(conn *websocket.Conn, data []byte, uuid string, wg *sync.WaitGroup) error
	onOffer        func(conn *websocket.Conn, data []byte, wg *sync.WaitGroup, uuid string) error
	onAnswer       func(data []byte, wg *sync.WaitGroup) error
	onCandidate    func(data []byte) error
	onResignation  func() error
}

func NewSignalingClient(
	onAcceptance func(conn *websocket.Conn, uuid string) error,
	onIntroduction func(conn *websocket.Conn, data []byte, uuid string, wg *sync.WaitGroup) error,
	onOffer func(conn *websocket.Conn, data []byte, wg *sync.WaitGroup, uuid string) error,
	onAnswer func(data []byte, wg *sync.WaitGroup) error,
	onCandidate func(data []byte) error,
	onResignation func() error,
) *SignalingClient {
	return &SignalingClient{
		onAcceptance:   onAcceptance,
		onIntroduction: onIntroduction,
		onOffer:        onOffer,
		onAnswer:       onAnswer,
		onCandidate:    onCandidate,
		onResignation:  onResignation,
	}
}

func (s *SignalingClient) HandleConn(laddrKey string, communityKey string, f func(msg webrtc.DataChannelMessage)) {
	uuid := uuid.NewString()

	wsAddress := "ws://" + laddrKey
	conn, _, error := websocket.Dial(context.Background(), wsAddress, nil)
	if error != nil {
		log.Printf("Signaling server could not be reached on: %v", wsAddress)
		os.Exit(0)
	}
	defer conn.Close(websocket.StatusNormalClosure, "Closing websocket connection nominally")

	var wg sync.WaitGroup

	go func() {
		if err := wsjson.Write(context.Background(), conn, api.NewApplication(communityKey, uuid)); err != nil {
			panic(err)
		}

		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-c

			if err := wsjson.Write(context.Background(), conn, api.NewExited(uuid)); err != nil {
				panic(err)
			}

			os.Exit(0)
		}()

	}()

	go func() {
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				panic(err)
			}

			var v api.Message
			if err := json.Unmarshal(data, &v); err != nil {
				panic(err)
			}

			switch v.Opcode {
			case api.OpcodeAcceptance:
				s.onAcceptance(conn, uuid)
				break
			case api.OpcodeIntroduction:
				s.onIntroduction(conn, data, uuid, &wg)
				break
			case api.OpcodeOffer:
				s.onOffer(conn, data, &wg, uuid)
				break
			case api.OpcodeAnswer:
				s.onAnswer(data, &wg)
				break
			case api.OpcodeCandidate:
				s.onCandidate(data)
				break
			case api.OpcodeResignation:
				s.onResignation()
			}
		}
	}()
	<-config.ExitClient
	if err := wsjson.Write(context.Background(), conn, api.NewExited(uuid)); err != nil {
		panic(err)
	}
	return
}

package signaling

import (
	"context"
	"encoding/json"
	"log"

	api "github.com/alphahorizonio/libentangle/pkg/api/websockets/v1"
	"nhooyr.io/websocket"
)

// The signaling protocol is located at /docs/signaling-protocol.txt

type SignalingServer struct {
	onApplication func(application api.Application, conn *websocket.Conn) error
	onReady       func(ready api.Ready, conn *websocket.Conn) error
	onOffer       func(offer api.Offer) error
	onAnswer      func(answer api.Answer) error
	onCandidate   func(candidate api.Candidate) error
	onExited      func(exited api.Exited) error
}

func NewSignalingServer(
	onApplication func(application api.Application, conn *websocket.Conn) error,
	onReady func(ready api.Ready, conn *websocket.Conn) error,
	onOffer func(offer api.Offer) error,
	onAnswer func(answer api.Answer) error,
	onCandidate func(candidate api.Candidate) error,
	onExited func(exited api.Exited) error,
) *SignalingServer {
	return &SignalingServer{
		onApplication: onApplication,
		onReady:       onReady,
		onOffer:       onOffer,
		onAnswer:      onAnswer,
		onCandidate:   onCandidate,
		onExited:      onExited,
	}
}

func (s *SignalingServer) HandleConn(conn websocket.Conn) {

	go func() {
	loop:
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				continue
			}

			var v api.Message
			if err := json.Unmarshal(data, &v); err != nil {
				continue
			}

			log.Println(v)

			switch v.Opcode {
			case api.OpcodeApplication:
				var application api.Application
				if err := json.Unmarshal(data, &application); err != nil {
					continue
				}
				s.onApplication(application, &conn)
				break
			case api.OpcodeReady:
				var ready api.Ready
				if err := json.Unmarshal(data, &ready); err != nil {
					continue
				}
				s.onReady(ready, &conn)
				break
			case api.OpcodeOffer:
				var offer api.Offer
				if err := json.Unmarshal(data, &offer); err != nil {
					continue
				}
				s.onOffer(offer)
				break
			case api.OpcodeAnswer:
				var answer api.Answer
				if err := json.Unmarshal(data, &answer); err != nil {
					continue
				}
				s.onAnswer(answer)
				break
			case api.OpcodeCandidate:
				var candidate api.Candidate
				if err := json.Unmarshal(data, &candidate); err != nil {
					continue
				}
				s.onCandidate(candidate)
				break
			case api.OpcodeExited:
				var exited api.Exited
				if err := json.Unmarshal(data, &exited); err != nil {
					continue
				}
				s.onExited(exited)
				break loop
			default:
				continue
			}
		}
	}()
}

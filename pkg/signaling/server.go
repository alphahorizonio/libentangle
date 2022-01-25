package signaling

import (
	"context"
	"encoding/json"

	"github.com/JakWai01/sile-fystem/pkg/logging"
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

	log logging.StructuredLogger
}

func NewSignalingServer(
	onApplication func(application api.Application, conn *websocket.Conn) error,
	onReady func(ready api.Ready, conn *websocket.Conn) error,
	onOffer func(offer api.Offer) error,
	onAnswer func(answer api.Answer) error,
	onCandidate func(candidate api.Candidate) error,
	onExited func(exited api.Exited) error,

	log logging.StructuredLogger,
) *SignalingServer {
	return &SignalingServer{
		onApplication: onApplication,
		onReady:       onReady,
		onOffer:       onOffer,
		onAnswer:      onAnswer,
		onCandidate:   onCandidate,
		onExited:      onExited,
		log:           log,
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

			switch v.Opcode {
			case api.OpcodeApplication:
				var application api.Application
				if err := json.Unmarshal(data, &application); err != nil {
					continue
				}

				s.log.Trace("SignalingServer.HandleConn", map[string]interface{}{
					"operation": application.Opcode,
					"community": application.Community,
					"mac":       application.Mac,
				})

				s.onApplication(application, &conn)
				break
			case api.OpcodeReady:
				var ready api.Ready
				if err := json.Unmarshal(data, &ready); err != nil {
					continue
				}

				s.log.Trace("SignalingServer.HandleConn", map[string]interface{}{
					"operation": ready.Opcode,
					"mac":       ready.Mac,
				})

				s.onReady(ready, &conn)
				break
			case api.OpcodeOffer:
				var offer api.Offer
				if err := json.Unmarshal(data, &offer); err != nil {
					continue
				}

				s.log.Trace("SignalingServer.HandleConn", map[string]interface{}{
					"operation": offer.Opcode,
					"payload":   offer.Payload,
					"sender":    offer.SenderMac,
					"receiver":  offer.ReceiverMac,
				})

				s.onOffer(offer)
				break
			case api.OpcodeAnswer:
				var answer api.Answer
				if err := json.Unmarshal(data, &answer); err != nil {
					continue
				}

				s.log.Trace("SignalingServer.HandleConn", map[string]interface{}{
					"operation": answer.Opcode,
					"payload":   answer.Payload,
					"sender":    answer.SenderMac,
					"receiver":  answer.ReceiverMac,
				})

				s.onAnswer(answer)
				break
			case api.OpcodeCandidate:
				var candidate api.Candidate
				if err := json.Unmarshal(data, &candidate); err != nil {
					continue
				}

				s.log.Trace("SignalingServer.HandleConn", map[string]interface{}{
					"operation": candidate.Opcode,
					"payload":   candidate.Payload,
					"sender":    candidate.SenderMac,
					"receiver":  candidate.ReceiverMac,
				})

				s.onCandidate(candidate)
				break
			case api.OpcodeExited:
				var exited api.Exited
				if err := json.Unmarshal(data, &exited); err != nil {
					continue
				}

				s.log.Trace("SignalingServer.HandleConn", map[string]interface{}{
					"operation": exited.Opcode,
					"mac":       exited.Mac,
				})

				s.onExited(exited)
				break loop
			default:
				continue
			}
		}
	}()
}

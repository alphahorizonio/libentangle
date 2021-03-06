package signaling

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/JakWai01/sile-fystem/pkg/logging"
	api "github.com/alphahorizonio/libentangle/pkg/api/websockets/v1"
	"github.com/alphahorizonio/libentangle/pkg/config"
	"github.com/google/uuid"
	"github.com/pion/webrtc/v3"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type SignalingClient struct {
	onAcceptance   func(conn *websocket.Conn, uuid string) error
	onIntroduction func(conn *websocket.Conn, uuid string, wg *sync.WaitGroup, introduction api.Introduction) error
	onOffer        func(conn *websocket.Conn, wg *sync.WaitGroup, uuid string, offer api.Offer) error
	onAnswer       func(wg *sync.WaitGroup, answer api.Answer) error
	onCandidate    func(candidate api.Candidate) error
	onResignation  func() error

	log logging.StructuredLogger
}

func NewSignalingClient(
	onAcceptance func(conn *websocket.Conn, uuid string) error,
	onIntroduction func(conn *websocket.Conn, uuid string, wg *sync.WaitGroup, introduction api.Introduction) error,
	onOffer func(conn *websocket.Conn, wg *sync.WaitGroup, uuid string, offer api.Offer) error,
	onAnswer func(wg *sync.WaitGroup, answer api.Answer) error,
	onCandidate func(candidate api.Candidate) error,
	onResignation func() error,

	log logging.StructuredLogger,
) *SignalingClient {
	return &SignalingClient{
		onAcceptance:   onAcceptance,
		onIntroduction: onIntroduction,
		onOffer:        onOffer,
		onAnswer:       onAnswer,
		onCandidate:    onCandidate,
		onResignation:  onResignation,
		log:            log,
	}
}

func (s *SignalingClient) HandleConn(laddrKey string, communityKey string, f func(msg webrtc.DataChannelMessage)) error {
	uuid := uuid.NewString()
	wsAddress := "ws://" + laddrKey
	fatal := make(chan error)

	conn, _, err := websocket.Dial(context.Background(), wsAddress, nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "Closing websocket connection nominally")

	var wg sync.WaitGroup

	go func() {
		if err := wsjson.Write(context.Background(), conn, api.NewApplication(communityKey, uuid)); err != nil {
			fatal <- err
		}

		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-c

			if err := wsjson.Write(context.Background(), conn, api.NewExited(uuid)); err != nil {
				fatal <- err
			}

			os.Exit(0)
		}()

	}()

	go func() {
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				if err == io.EOF {
					continue
				} else {
					fatal <- err
				}
			}

			var v api.Message
			if err := json.Unmarshal(data, &v); err != nil {
				fatal <- err
			}

			switch v.Opcode {
			case api.OpcodeAcceptance:
				var acceptance api.Acceptance
				if err := json.Unmarshal(data, &acceptance); err != nil {
					fatal <- err
				}

				s.log.Trace("SignalingClient.HandleConn", map[string]interface{}{
					"operation": acceptance.Opcode,
				})

				s.onAcceptance(conn, uuid)
				break
			case api.OpcodeIntroduction:
				var introduction api.Introduction
				if err := json.Unmarshal(data, &introduction); err != nil {
					fatal <- err
				}

				s.log.Trace("SignalingClient.HandleConn", map[string]interface{}{
					"operation": introduction.Opcode,
					"mac":       introduction.Mac,
				})

				s.onIntroduction(conn, uuid, &wg, introduction)
				break
			case api.OpcodeOffer:
				var offer api.Offer
				if err := json.Unmarshal(data, &offer); err != nil {
					fatal <- err
				}

				s.log.Trace("SignalingClient.HandleConn", map[string]interface{}{
					"operation": offer.Opcode,
					"payload":   offer.Payload,
					"sender":    offer.SenderMac,
					"receiver":  offer.ReceiverMac,
				})

				s.onOffer(conn, &wg, uuid, offer)
				break
			case api.OpcodeAnswer:
				var answer api.Answer
				if err := json.Unmarshal(data, &answer); err != nil {
					fatal <- err
				}

				s.log.Trace("SignalingClient.HandleConn", map[string]interface{}{
					"operation": answer.Opcode,
					"payload":   answer.Payload,
					"sender":    answer.SenderMac,
					"receiver":  answer.ReceiverMac,
				})

				s.onAnswer(&wg, answer)
				break
			case api.OpcodeCandidate:
				var candidate api.Candidate
				if err := json.Unmarshal(data, &candidate); err != nil {
					fatal <- err
				}

				s.log.Trace("SignalingClient.HandleConn", map[string]interface{}{
					"operation": candidate.Opcode,
					"payload":   candidate.Payload,
					"sender":    candidate.SenderMac,
					"receiver":  candidate.ReceiverMac,
				})

				s.onCandidate(candidate)
				break
			case api.OpcodeResignation:
				var resignation api.Resignation
				if err := json.Unmarshal(data, &resignation); err != nil {
					fatal <- err
				}

				s.log.Trace("SignalingClient.HandleConn", map[string]interface{}{
					"operation": resignation.Opcode,
					"mac":       resignation.Mac,
				})

				s.onResignation()
			}
		}
	}()

	for {
		select {
		case err := <-fatal:
			return err
		case <-config.ExitClient:
			if err := wsjson.Write(context.Background(), conn, api.NewExited(uuid)); err != nil {
				return err
			}
			return nil

		}
	}
}

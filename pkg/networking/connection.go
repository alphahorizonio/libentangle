package networking

import (
	"sync"

	api "github.com/alphahorizonio/libentangle/pkg/api/websockets/v1"
	"github.com/alphahorizonio/libentangle/pkg/handlers"
	"github.com/alphahorizonio/libentangle/pkg/logging"
	"github.com/alphahorizonio/libentangle/pkg/signaling"
	"github.com/pion/webrtc/v3"
	"nhooyr.io/websocket"
)

type ConnectionManager struct {
	manager *handlers.ClientManager
}

func NewConnectionManager(manager *handlers.ClientManager) *ConnectionManager {
	return &ConnectionManager{
		manager: manager,
	}
}

func (m *ConnectionManager) Connect(signaler string, community string, f func(msg webrtc.DataChannelMessage), onError func(err error) interface{}, l logging.StructuredLogger) {
	client := signaling.NewSignalingClient(
		func(conn *websocket.Conn, uuid string) error {
			return m.manager.HandleAcceptance(conn, uuid)
		},
		func(conn *websocket.Conn, uuid string, wg *sync.WaitGroup, introduction api.Introduction) error {
			return m.manager.HandleIntroduction(conn, uuid, wg, f, introduction)
		},
		func(conn *websocket.Conn, wg *sync.WaitGroup, uuid string, offer api.Offer) error {
			return m.manager.HandleOffer(conn, wg, uuid, f, offer)
		},
		func(wg *sync.WaitGroup, answer api.Answer) error {
			return m.manager.HandleAnswer(wg, answer)
		},
		func(candidate api.Candidate) error {
			return m.manager.HandleCandidate(candidate)
		},
		func() error {
			return m.manager.HandleResignation()
		},
		onError,
		l,
	)

	go func() {
		go client.HandleConn(signaler, community, f)
	}()
}

func (m *ConnectionManager) Write(p []byte) (int, error) {
	if m.manager != nil {
		m.manager.SendMessage(p)
		return len(p), nil
	}
	return 0, &NoConnectionEstablished{}
}

func (m *ConnectionManager) WriteUnicast(p []byte, mac string) error {
	if m.manager != nil {
		m.manager.SendMessageUnicast(p, mac)
		return nil
	}
	return &NoConnectionEstablished{}
}

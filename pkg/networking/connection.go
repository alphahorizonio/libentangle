package networking

import (
	"sync"

	"github.com/alphahorizonio/libentangle/pkg/handlers"
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

func (m *ConnectionManager) Connect(signaler string, community string, f func(msg webrtc.DataChannelMessage), onError func(err error) interface{}) {
	client := signaling.NewSignalingClient(
		func(conn *websocket.Conn, uuid string) error {
			return m.manager.HandleAcceptance(conn, uuid)
		},
		func(conn *websocket.Conn, data []byte, uuid string, wg *sync.WaitGroup) error {
			return m.manager.HandleIntroduction(conn, data, uuid, wg, f)
		},
		func(conn *websocket.Conn, data []byte, wg *sync.WaitGroup, uuid string) error {
			return m.manager.HandleOffer(conn, data, wg, uuid, f)
		},
		func(data []byte, wg *sync.WaitGroup) error {
			return m.manager.HandleAnswer(data, wg)
		},
		func(data []byte) error {
			return m.manager.HandleCandidate(data)
		},
		func() error {
			return m.manager.HandleResignation()
		},
		onError,
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

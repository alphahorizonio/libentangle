package handlers

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	apiDataChannels "github.com/alphahorizonio/libentangle/pkg/api/datachannels/v1"
	api "github.com/alphahorizonio/libentangle/pkg/api/websockets/v1"
	"github.com/pion/webrtc/v3"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type ClientManager struct {
	lock sync.Mutex

	peers       map[string]*peer
	onConnected func()

	mac string
}

func NewClientManager(onConnected func()) *ClientManager {
	return &ClientManager{
		peers:       map[string]*peer{},
		onConnected: onConnected,
	}
}

type peer struct {
	connection *webrtc.PeerConnection
	channel    *webrtc.DataChannel
	candidates []webrtc.ICECandidateInit
}

func (m *ClientManager) HandleAcceptance(conn *websocket.Conn, uuid string) error {
	m.mac = uuid

	if err := wsjson.Write(context.Background(), conn, api.NewReady(uuid)); err != nil {
		return err
	}
	return nil
}

func (m *ClientManager) HandleIntroduction(conn *websocket.Conn, uuid string, wg *sync.WaitGroup, f func(msg webrtc.DataChannelMessage), introduction api.Introduction) error {
	wg.Add(1)

	peerConnection, err := m.createPeer(introduction.Mac, conn, uuid, f)
	if err != nil {
		return err
	}

	if err := m.createDataChannel(introduction.Mac, peerConnection, f); err != nil {
		return err
	}

	offer, err := peerConnection.CreateOffer(nil)
	if err != nil {
		return err
	}

	if err := peerConnection.SetLocalDescription(offer); err != nil {
		return err
	}

	data, err := json.Marshal(offer)
	if err != nil {
		return err
	}

	if err := wsjson.Write(context.Background(), conn, api.NewOffer(data, uuid, introduction.Mac)); err != nil {
		return err
	}
	return nil
}

func (m *ClientManager) HandleOffer(conn *websocket.Conn, wg *sync.WaitGroup, uuid string, f func(msg webrtc.DataChannelMessage), offer api.Offer) error {
	wg.Add(1)

	var offer_val webrtc.SessionDescription

	if err := json.Unmarshal([]byte(offer.Payload), &offer_val); err != nil {
		return err
	}

	peerConnection, err := m.createPeer(offer.SenderMac, conn, uuid, f)
	if err != nil {
		return err
	}

	if err := peerConnection.SetRemoteDescription(offer_val); err != nil {
		return err
	}

	answer_val, err := peerConnection.CreateAnswer(nil)
	if err != nil {
		return err
	}

	err = peerConnection.SetLocalDescription(answer_val)
	if err != nil {
		return err
	}

	data, err := json.Marshal(answer_val)
	if err != nil {
		return err
	}

	if err := wsjson.Write(context.Background(), conn, api.NewAnswer(data, offer.ReceiverMac, offer.SenderMac)); err != nil {
		return err
	}

	wg.Done()
	return nil
}

func (m *ClientManager) HandleAnswer(wg *sync.WaitGroup, answer api.Answer) error {
	var answer_val webrtc.SessionDescription

	if err := json.Unmarshal([]byte(answer.Payload), &answer_val); err != nil {
		return err
	}

	peerConnection, err := m.getPeerConnection(answer.SenderMac)
	if err != nil {
		return err
	}

	if err := peerConnection.SetRemoteDescription(answer_val); err != nil {
		return err
	}

	if len(m.peers[answer.SenderMac].candidates) > 0 {
		for _, candidate := range m.peers[answer.SenderMac].candidates {
			if err := peerConnection.AddICECandidate(candidate); err != nil {
				return err
			}
		}

		m.peers[answer.SenderMac].candidates = []webrtc.ICECandidateInit{}
	}

	wg.Done()
	return nil
}

func (m *ClientManager) HandleCandidate(candidate api.Candidate) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	peerConnection, err := m.getPeerConnection(candidate.SenderMac)
	if err != nil {
		return err
	}

	if peerConnection.RemoteDescription() != nil {
		if err := peerConnection.AddICECandidate(webrtc.ICECandidateInit{Candidate: string(candidate.Payload)}); err != nil {
			return err
		}
	}

	m.peers[candidate.SenderMac].candidates = append(m.peers[candidate.SenderMac].candidates, webrtc.ICECandidateInit{Candidate: string(candidate.Payload)})

	return nil
}

func (m *ClientManager) HandleResignation() error {
	return nil
}

func (m *ClientManager) createPeer(mac string, conn *websocket.Conn, uuid string, f func(msg webrtc.DataChannelMessage)) (*webrtc.PeerConnection, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	peerConnection, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	peerConnection.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Printf("Peer Connection State has changed: %s\n", s.String())
	})

	m.peers[mac] = &peer{
		connection: peerConnection,
		candidates: []webrtc.ICECandidateInit{},
	}

	peerConnection.OnICECandidate(func(i *webrtc.ICECandidate) {
		if i == nil {
			return
		} else {
			m.lock.Lock()
			defer func() {
				m.lock.Unlock()
			}()

			if err := wsjson.Write(context.Background(), conn, api.NewCandidate([]byte(i.ToJSON().Candidate), uuid, mac)); err != nil {
				panic(err)
			}
		}
	})

	peerConnection.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnOpen(func() {
			log.Println("sendChannel has opened")

			m.peers[mac].channel = dc

			m.onConnected()
		})
		dc.OnClose(func() {
			log.Println("sendChannel has closed")
		})
		dc.OnMessage(f)
	})

	return peerConnection, nil
}

func (m *ClientManager) createDataChannel(mac string, peerConnection *webrtc.PeerConnection, f func(msg webrtc.DataChannelMessage)) error {
	dc, err := peerConnection.CreateDataChannel("foo", nil)
	if err != nil {
		return err
	}
	dc.OnOpen(func() {
		log.Println("sendChannel has opened")

		m.peers[mac].channel = dc

		m.onConnected()
	})
	dc.OnClose(func() {
		log.Println("sendChannel has closed")
	})
	dc.OnMessage(f)

	return nil
}

func (m *ClientManager) getPeerConnection(mac string) (*webrtc.PeerConnection, error) {
	return m.peers[mac].connection, nil
}

func (m *ClientManager) SendMessage(msg []byte) error {
	wrappedMsg, err := json.Marshal(apiDataChannels.WrappedMessage{Mac: m.mac, Payload: msg})
	if err != nil {
		return err
	}

	for key := range m.peers {
		if key != m.mac {
			if err := m.peers[key].channel.Send(wrappedMsg); err != nil {
				return nil
			} else {
				return err
			}
		}
	}
	return nil
}

func (m *ClientManager) SendMessageUnicast(msg []byte, mac string) error {
	wrappedMsg, err := json.Marshal(apiDataChannels.WrappedMessage{Mac: m.mac, Payload: msg})
	if err != nil {
		return err
	}

	if err := m.peers[mac].channel.Send(wrappedMsg); err != nil {
		return nil
	} else {
		return err
	}
}

func refString(s string) *string {
	return &s
}

func refUint16(i uint16) *uint16 {
	return &i
}

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
		panic(err)
	}
	return nil
}

func (m *ClientManager) HandleIntroduction(conn *websocket.Conn, data []byte, uuid string, wg *sync.WaitGroup, f func(msg webrtc.DataChannelMessage)) error {
	var introduction api.Introduction
	if err := json.Unmarshal(data, &introduction); err != nil {
		panic(err)
	}

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
		panic(err)
	}

	if err := peerConnection.SetLocalDescription(offer); err != nil {
		panic(err)
	}

	data, err = json.Marshal(offer)
	if err != nil {
		panic(err)
	}

	if err := wsjson.Write(context.Background(), conn, api.NewOffer(data, uuid, introduction.Mac)); err != nil {
		panic(err)
	}
	return nil
}

func (m *ClientManager) HandleOffer(conn *websocket.Conn, data []byte, wg *sync.WaitGroup, uuid string, f func(msg webrtc.DataChannelMessage)) error {
	var offer api.Offer
	if err := json.Unmarshal(data, &offer); err != nil {
		panic(err)
	}

	wg.Add(1)

	var offer_val webrtc.SessionDescription

	if err := json.Unmarshal([]byte(offer.Payload), &offer_val); err != nil {
		panic(err)
	}

	peerConnection, err := m.createPeer(offer.SenderMac, conn, uuid, f)
	if err != nil {
		panic(err)
	}

	if err := peerConnection.SetRemoteDescription(offer_val); err != nil {
		panic(err)
	}

	answer_val, err := peerConnection.CreateAnswer(nil)
	if err != nil {
		panic(err)
	}

	err = peerConnection.SetLocalDescription(answer_val)
	if err != nil {
		panic(err)
	}

	data, err = json.Marshal(answer_val)
	if err != nil {
		panic(err)
	}

	if err := wsjson.Write(context.Background(), conn, api.NewAnswer(data, offer.ReceiverMac, offer.SenderMac)); err != nil {
		panic(err)
	}

	wg.Done()
	return nil
}

func (m *ClientManager) HandleAnswer(data []byte, wg *sync.WaitGroup) error {
	var answer api.Answer
	if err := json.Unmarshal(data, &answer); err != nil {
		panic(err)
	}

	var answer_val webrtc.SessionDescription

	if err := json.Unmarshal([]byte(answer.Payload), &answer_val); err != nil {
		panic(err)
	}

	peerConnection, err := m.getPeerConnection(answer.SenderMac)
	if err != nil {
		panic(err)
	}

	if err := peerConnection.SetRemoteDescription(answer_val); err != nil {
		panic(err)
	}

	if len(m.peers[answer.SenderMac].candidates) > 0 {
		for _, candidate := range m.peers[answer.SenderMac].candidates {
			if err := peerConnection.AddICECandidate(candidate); err != nil {
				panic(err)
			}
		}

		m.peers[answer.SenderMac].candidates = []webrtc.ICECandidateInit{}
	}

	wg.Done()
	return nil
}

func (m *ClientManager) HandleCandidate(data []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	log.Println("received Candidate")
	var candidate api.Candidate
	if err := json.Unmarshal(data, &candidate); err != nil {
		panic(err)
	}

	log.Println(string(candidate.Payload))

	peerConnection, err := m.getPeerConnection(candidate.SenderMac)
	if err != nil {
		panic(err)
	}

	if peerConnection.RemoteDescription() != nil {
		if err := peerConnection.AddICECandidate(webrtc.ICECandidateInit{Candidate: string(candidate.Payload)}); err != nil {
			panic(err)
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
		panic(err)
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

			log.Println("Candidate was generated!")
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
		panic(err)
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
		panic(err)
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
		panic(err)
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

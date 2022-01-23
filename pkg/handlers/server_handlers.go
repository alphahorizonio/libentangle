package handlers

import (
	"context"
	"errors"
	"sync"

	api "github.com/alphahorizonio/libentangle/pkg/api/websockets/v1"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type ServerManager struct {
	lock sync.Mutex

	communities map[string][]string
	macs        map[string]websocket.Conn
}

func NewCommunitiesManager() *ServerManager {
	return &ServerManager{
		communities: map[string][]string{},
		macs:        map[string]websocket.Conn{},
	}
}

func (m *ServerManager) HandleApplication(application api.Application, conn *websocket.Conn) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.macs[application.Mac]; ok {
		// Send rejection. That mac is already contained
		if err := wsjson.Write(context.Background(), conn, api.NewRejection()); err != nil {
			panic(err)
		}

		return nil
	}

	m.macs[application.Mac] = *conn

	// Check if community exists
	if _, ok := m.communities[application.Community]; ok {
		m.communities[application.Community] = append(m.communities[application.Community], application.Mac)

		if err := wsjson.Write(context.Background(), conn, api.NewAcceptance()); err != nil {
			panic(err)
		}

		return nil
	} else {
		// Community does not exist. Create commuity and insert mac
		m.communities[application.Community] = append(m.communities[application.Community], application.Mac)

		if err := wsjson.Write(context.Background(), conn, api.NewAcceptance()); err != nil {
			panic(err)
		}

		return nil
	}

}

func (m *ServerManager) HandleReady(ready api.Ready, conn *websocket.Conn) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	community, err := m.getCommunity(ready.Mac)
	if err != nil {
		panic(err)
	}

	// Broadcast the introduction to all connections, excluding our own
	for _, mac := range m.communities[community] {
		if mac != ready.Mac {
			receiver := m.macs[mac]

			// ensure that ready.Mac == m.communities[community][0]
			if err := wsjson.Write(context.Background(), &receiver, api.NewIntroduction(ready.Mac)); err != nil {
				panic(err)
			}
		} else {
			continue
		}
	}

	return nil
}

func (m *ServerManager) HandleOffer(offer api.Offer) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	receiver := m.macs[offer.ReceiverMac]

	if err := wsjson.Write(context.Background(), &receiver, offer); err != nil {
		panic(err)
	}

	return nil
}

func (m *ServerManager) HandleAnswer(answer api.Answer) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	receiver := m.macs[answer.ReceiverMac]

	if err := wsjson.Write(context.Background(), &receiver, answer); err != nil {
		panic(err)
	}

	return nil
}

func (m *ServerManager) HandleCandidate(candidate api.Candidate) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	receiver := m.macs[candidate.ReceiverMac]

	if err := wsjson.Write(context.Background(), &receiver, candidate); err != nil {
		panic(err)
	}

	return nil
}

func (m *ServerManager) HandleExited(exited api.Exited) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	community, err := m.getCommunity(exited.Mac)
	if err != nil {
		panic(err)
	}

	for _, mac := range m.communities[community] {
		if mac != exited.Mac {
			receiver := m.macs[mac]

			if err := wsjson.Write(context.Background(), &receiver, api.NewResignation(exited.Mac)); err != nil {
				panic(err)
			}
		} else {
			continue
		}
	}

	// Remove this peer from all maps
	delete(m.macs, exited.Mac)
	delete(m.macs, exited.Mac)

	// Remove member from community
	m.communities[community] = m.deleteCommunity(m.communities[community], exited.Mac)

	if len(m.communities[community]) == 0 {
		delete(m.communities, community)
	}

	return nil
}

func (m *ServerManager) getCommunity(mac string) (string, error) {
	for key, element := range m.communities {
		for i := 0; i < len(element); i++ {
			if element[i] == mac {
				return key, nil
			}
		}
	}

	return "", errors.New("This mac is not part of any community so far!")
}

func (m *ServerManager) deleteCommunity(s []string, str string) []string {
	var elementIndex int
	for index, element := range s {
		if element == str {
			elementIndex = index
		}
	}
	return append(s[:elementIndex], s[elementIndex+1:]...)
}

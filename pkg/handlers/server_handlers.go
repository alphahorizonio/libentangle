package handlers

import (
	"context"
	"errors"
	"sync"

	api "github.com/alphahorizonio/libentangle/pkg/api/websockets/v1"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type CommunitiesManager struct {
	lock sync.Mutex

	communities map[string][]string
	macs        map[string]websocket.Conn

	introducedPeers [][2]string
}

func NewCommunitiesManager() *CommunitiesManager {
	return &CommunitiesManager{
		communities: map[string][]string{},
		macs:        map[string]websocket.Conn{},
	}
}

func (m *CommunitiesManager) HandleApplication(application api.Application, conn *websocket.Conn) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.macs[application.Mac]; ok {
		// Send rejection. That mac is already contained
		if err := wsjson.Write(context.Background(), conn, api.NewRejection()); err != nil {
			return err
		}

		return nil
	}

	m.macs[application.Mac] = *conn

	// Check if community exists
	if _, ok := m.communities[application.Community]; ok {
		m.communities[application.Community] = append(m.communities[application.Community], application.Mac)

		if err := wsjson.Write(context.Background(), conn, api.NewAcceptance()); err != nil {
			return err
		}

		return nil
	} else {
		// Community does not exist. Create commuity and insert mac
		m.communities[application.Community] = append(m.communities[application.Community], application.Mac)

		if err := wsjson.Write(context.Background(), conn, api.NewAcceptance()); err != nil {
			return err
		}

		return nil
	}

}

func (m *CommunitiesManager) HandleReady(ready api.Ready, conn *websocket.Conn) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	community, err := m.getCommunity(ready.Mac)
	if err != nil {
		return err
	}

	// Broadcast the introduction to all connections, excluding our own
	for _, mac := range m.communities[community] {
		if mac != ready.Mac {
			receiver := m.macs[mac]

			if !m.introduced(ready.Mac, mac) {
				if err := wsjson.Write(context.Background(), &receiver, api.NewIntroduction(ready.Mac)); err != nil {
					return err
				}

				m.introduce(ready.Mac, mac)
			}
		} else {
			continue
		}
	}

	return nil
}

func (m *CommunitiesManager) HandleOffer(offer api.Offer) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	receiver := m.macs[offer.ReceiverMac]

	if err := wsjson.Write(context.Background(), &receiver, offer); err != nil {
		return err
	}

	return nil
}

func (m *CommunitiesManager) HandleAnswer(answer api.Answer) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	receiver := m.macs[answer.ReceiverMac]

	if err := wsjson.Write(context.Background(), &receiver, answer); err != nil {
		return err
	}

	return nil
}

func (m *CommunitiesManager) HandleCandidate(candidate api.Candidate) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	receiver := m.macs[candidate.ReceiverMac]

	if err := wsjson.Write(context.Background(), &receiver, candidate); err != nil {
		return err
	}

	return nil
}

func (m *CommunitiesManager) HandleExited(exited api.Exited) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	community, err := m.getCommunity(exited.Mac)
	if err != nil {
		return err
	}

	m.removeAssociatedPairs(exited.Mac)

	for _, mac := range m.communities[community] {
		if mac != exited.Mac {
			receiver := m.macs[mac]

			if err := wsjson.Write(context.Background(), &receiver, api.NewResignation(exited.Mac)); err != nil {
				return err
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

func (m *CommunitiesManager) getCommunity(mac string) (string, error) {
	for key, element := range m.communities {
		for i := 0; i < len(element); i++ {
			if element[i] == mac {
				return key, nil
			}
		}
	}

	return "", errors.New("This mac is not part of any community so far!")
}

func (m *CommunitiesManager) deleteCommunity(s []string, str string) []string {
	var elementIndex int
	for index, element := range s {
		if element == str {
			elementIndex = index
		}
	}
	return append(s[:elementIndex], s[elementIndex+1:]...)
}

func (m *CommunitiesManager) introduce(firstMac string, secondMac string) {
	m.introducedPeers = append(m.introducedPeers, [2]string{firstMac, secondMac})
}

func (m *CommunitiesManager) introduced(firstMac string, secondMac string) bool {
	for _, pair := range m.introducedPeers {
		if pair[0] == firstMac && pair[1] == secondMac {
			return true
		}
		if pair[0] == secondMac && pair[1] == firstMac {
			return true
		}
	}
	return false
}

func (m *CommunitiesManager) removeAssociatedPairs(mac string) {
	newSlice := make([][2]string, 0)

	for _, pair := range m.introducedPeers {
		if pair[0] == mac || pair[1] == mac {
			continue
		} else {
			newSlice = append(newSlice, pair)
		}
	}

	m.introducedPeers = newSlice
}

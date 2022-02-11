package networking

import (
	"encoding/json"
	"errors"
	"io"
	"sync"

	api "github.com/alphahorizonio/libentangle/pkg/api/datachannels/v1"
	"github.com/alphahorizonio/libentangle/pkg/config"
)

type RemoteFile struct {
	lock   sync.Mutex
	opened bool
	oplock sync.Mutex

	ReadCh  chan api.ReadOpResponse
	WriteCh chan api.WriteOpResponse
	SeekCh  chan api.SeekOpResponse
	CloseCh chan api.CloseOpResponse
	OpenCh  chan api.OpenOpResponse

	cm ConnectionManager
}

func NewRemoteFile(cm ConnectionManager) *RemoteFile {
	return &RemoteFile{
		cm:      cm,
		ReadCh:  make(chan api.ReadOpResponse),
		WriteCh: make(chan api.WriteOpResponse),
		SeekCh:  make(chan api.SeekOpResponse),
		CloseCh: make(chan api.CloseOpResponse),
		OpenCh:  make(chan api.OpenOpResponse),
	}
}

func (f *RemoteFile) Fd() uintptr {
	return 1
}

func (f *RemoteFile) Open(create bool) error {
	f.oplock.Lock()
	defer f.oplock.Unlock()

	if f.opened == true {
		return nil
	}

	f.lock.Lock()

	msg, err := json.Marshal(api.NewOpenOp(create))
	if err != nil {
		return err
	}

	f.cm.Write(msg)

	errorChan := make(chan string)
	go func() {
		err := <-f.OpenCh
		errorChan <- err.Error
	}()

	error2 := <-errorChan

	f.opened = true

	if error2 == "" {
		return nil
	}

	return errors.New(error2)
}

func (f *RemoteFile) Close() error {

	f.oplock.Lock()
	defer f.oplock.Unlock()

	msg, err := json.Marshal(api.NewCloseOp())
	if err != nil {
		return err
	}

	f.cm.Write(msg)

	response := <-f.CloseCh

	if f.opened != false {
		f.lock.Unlock()
	}
	f.opened = false
	return getError(response.Error)
}

func (f *RemoteFile) Read(n []byte) (int, error) {
	f.oplock.Lock()
	defer f.oplock.Unlock()
	msg, err := json.Marshal(api.NewReadOp(len(n)))
	if err != nil {
		return 0, err
	}

	f.cm.Write(msg)

	response := <-f.ReadCh

	copy(n, response.Bytes)

	return int(response.BytesRead), getError(response.Error)
}

func (f *RemoteFile) Write(n []byte) (int, error) {
	f.oplock.Lock()
	defer f.oplock.Unlock()
	msg, err := json.Marshal(api.NewWriteOp(n))
	if err != nil {
		return 0, err
	}

	f.cm.Write(msg)

	response := <-f.WriteCh

	return int(response.BytesRead), getError(response.Error)
}

func (f *RemoteFile) Seek(offset int64, whence int) (int64, error) {
	f.oplock.Lock()
	defer f.oplock.Unlock()
	msg, err := json.Marshal(api.NewSeekOp(offset, whence))
	if err != nil {
		return 0, err
	}

	f.cm.Write(msg)

	response := <-f.SeekCh

	return response.Offset, getError(response.Error)
}

func getError(err string) error {
	switch err {
	case config.NoneKey:
		return nil
	case "EOF":
		return io.EOF
	default:
		return errors.New(err)
	}
}

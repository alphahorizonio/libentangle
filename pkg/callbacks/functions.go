package callbacks

import (
	"encoding/json"
	"log"
	"os"

	api "github.com/alphahorizonio/libentangle/pkg/api/datachannels/v1"
	"github.com/alphahorizonio/libentangle/pkg/networking"
	"github.com/pion/webrtc/v3"
)

type Callback struct{}

func NewCallback() *Callback {
	return &Callback{}
}

func (c *Callback) GetServerCallback(cm networking.ConnectionManager, file *os.File, myFile string) func(msg webrtc.DataChannelMessage) {
	return func(msg webrtc.DataChannelMessage) {
		var err error

		// log.Println(string(msg.Data))

		var w api.WrappedMessage

		if err := json.Unmarshal(msg.Data, &w); err != nil {
			panic(err)
		}

		log.Println(string(w.Payload))

		var v api.Message

		if err := json.Unmarshal(w.Payload, &v); err != nil {
			panic(err)
		}

		switch v.Opcode {
		case api.OpcodeOpen:
			var openOp api.OpenOp
			if err := json.Unmarshal(w.Payload, &openOp); err != nil {
				panic(err)
			}
			log.Println(openOp.ReadOnly)
			if openOp.ReadOnly == true {
				file, err = os.Open(myFile)
				if err != nil {
					msg, err := json.Marshal(api.NewOpenOpResponse(err.Error()))
					if err != nil {
						panic(err)
					}

					cm.WriteUnicast(msg, w.Mac)
				} else {
					msg, err := json.Marshal(api.NewOpenOpResponse(""))
					if err != nil {
						panic(err)
					}

					cm.WriteUnicast(msg, w.Mac)
				}
			} else {
				file, err = os.OpenFile(myFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
				if err != nil {
					msg, err := json.Marshal(api.NewOpenOpResponse(err.Error()))
					if err != nil {
						panic(err)
					}

					cm.WriteUnicast(msg, w.Mac)
				} else {
					msg, err := json.Marshal(api.NewOpenOpResponse(""))
					if err != nil {
						panic(err)
					}

					cm.WriteUnicast(msg, w.Mac)
				}
			}

			break
		case api.OpcodeClose:
			var closeOp api.CloseOp
			if err := json.Unmarshal(w.Payload, &closeOp); err != nil {
				panic(err)
			}

			file.Close()

			msg, err := json.Marshal(api.NewCloseOpResponse(""))
			if err != nil {
				panic(err)
			}

			cm.WriteUnicast(msg, w.Mac)

			break
		case api.OpcodeRead:
			var readOp api.ReadOp
			if err := json.Unmarshal(w.Payload, &readOp); err != nil {
				panic(err)
			}

			buf := make([]byte, readOp.Length)

			n, err := file.Read(buf)
			if err != nil {
				msg, err := json.Marshal(api.NewReadOpResponse(buf, int64(n), err.Error()))
				if err != nil {
					panic(err)
				}

				cm.WriteUnicast(msg, w.Mac)
			} else {
				msg, err := json.Marshal(api.NewReadOpResponse(buf, int64(n), ""))
				if err != nil {
					panic(err)
				}

				cm.WriteUnicast(msg, w.Mac)
			}

			break
		case api.OpcodeWrite:
			var writeOp api.WriteOp
			if err := json.Unmarshal(w.Payload, &writeOp); err != nil {
				panic(err)
			}

			n, err := file.Write(writeOp.Payload)
			if err != nil {
				msg, err := json.Marshal(api.NewWriteOpResponse(int64(n), err.Error()))
				if err != nil {
					panic(err)
				}

				cm.WriteUnicast(msg, w.Mac)
			} else {
				msg, err := json.Marshal(api.NewWriteOpResponse(int64(n), ""))
				if err != nil {
					panic(err)
				}

				cm.WriteUnicast(msg, w.Mac)
			}

			break
		case api.OpcodeSeek:
			var seekOp api.SeekOp
			if err := json.Unmarshal(w.Payload, &seekOp); err != nil {
				panic(err)
			}

			offset, err := file.Seek(seekOp.Offset, seekOp.Whence)
			if err != nil {
				msg, err := json.Marshal(api.NewSeekOpResponse(offset, err.Error()))
				if err != nil {
					panic(err)
				}

				cm.WriteUnicast(msg, w.Mac)
			} else {
				msg, err := json.Marshal(api.NewSeekOpResponse(offset, ""))
				if err != nil {
					panic(err)
				}

				cm.WriteUnicast(msg, w.Mac)
			}

			break
		}
	}
}

func (c *Callback) GetClientCallback(rmFile networking.RemoteFile) func(msg webrtc.DataChannelMessage) {
	return func(msg webrtc.DataChannelMessage) {
		// log.Println(string(msg.Data))

		var w api.WrappedMessage

		if err := json.Unmarshal(msg.Data, &w); err != nil {
			panic(err)
		}

		log.Println(string(w.Payload))

		var v api.Message

		if err := json.Unmarshal(w.Payload, &v); err != nil {
			panic(err)
		}

		switch v.Opcode {
		case api.OpcodeOpenResponse:
			var openOpResponse api.OpenOpResponse
			if err := json.Unmarshal(w.Payload, &openOpResponse); err != nil {
				panic(err)
			}

			go func() {
				rmFile.OpenCh <- openOpResponse
			}()

			break
		case api.OpcodeCloseResponse:
			var closeOpResponse api.CloseOpResponse
			if err := json.Unmarshal(w.Payload, &closeOpResponse); err != nil {
				panic(err)
			}

			rmFile.CloseCh <- closeOpResponse

			break
		case api.OpcodeReadResponse:
			var readOpResponse api.ReadOpResponse
			if err := json.Unmarshal(w.Payload, &readOpResponse); err != nil {
				panic(err)
			}

			rmFile.ReadCh <- readOpResponse

			break
		case api.OpcodeWriteResponse:
			var writeOpResponse api.WriteOpResponse
			if err := json.Unmarshal(w.Payload, &writeOpResponse); err != nil {
				panic(err)
			}

			rmFile.WriteCh <- writeOpResponse

			break
		case api.OpcodeSeekResponse:
			var seekOpResponse api.SeekOpResponse
			if err := json.Unmarshal(w.Payload, &seekOpResponse); err != nil {
				panic(err)
			}

			rmFile.SeekCh <- seekOpResponse

			break
		}
	}
}

func (c *Callback) GetDebugErrorCallback() func(err error) interface{} {
	return func(err error) interface{} {
		panic(err)
	}
}

func (c *Callback) GetErrorCallback() func(err error) interface{} {
	return func(err error) interface{} {
		log.Fatal(err)

		os.Exit(1)

		return nil
	}
}

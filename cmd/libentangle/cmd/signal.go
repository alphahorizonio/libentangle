package cmd

import (
	"log"
	"net"
	"net/http"
	"os"

	"github.com/alphahorizonio/libentangle/internal/logging"
	api "github.com/alphahorizonio/libentangle/pkg/api/websockets/v1"
	"github.com/alphahorizonio/libentangle/pkg/handlers"
	"github.com/alphahorizonio/libentangle/pkg/signaling"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"nhooyr.io/websocket"
)

const (
	addressKey = "address"
)

var signalCmd = &cobra.Command{
	Use:   "signal",
	Short: "Start a signaling server",
	RunE: func(cmd *cobra.Command, args []string) error {

		for {
			port := os.Getenv("PORT")
			if port == "" {
				port = "9090"
			}

			socket := viper.GetString(addressKey) + ":" + port

			addr, err := net.ResolveTCPAddr("tcp", socket)
			if err != nil {
				panic(err)
			}

			log.Printf("signaling server listening on %v", addr)

			manager := handlers.NewCommunitiesManager()

			l := logging.NewJSONLogger(viper.GetInt(verboseFlag))

			signaler := signaling.NewSignalingServer(
				func(application api.Application, conn *websocket.Conn) error {
					return manager.HandleApplication(application, conn)
				},
				func(ready api.Ready, conn *websocket.Conn) error {
					return manager.HandleReady(ready, conn)
				},
				func(offer api.Offer) error {
					return manager.HandleOffer(offer)
				},
				func(answer api.Answer) error {
					return manager.HandleAnswer(answer)
				},
				func(candidate api.Candidate) error {
					return manager.HandleCandidate(candidate)
				},
				func(exited api.Exited) error {
					return manager.HandleExited(exited)
				},
				l,
			)

			handler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(rw, r, &websocket.AcceptOptions{
					InsecureSkipVerify: true, // CORS
				})
				if err != nil {
					panic(err)
				}

				log.Println("client connected")

				go func() {
					signaler.HandleConn(*conn)
				}()
			})

			http.ListenAndServe(addr.String(), handler)
		}
	},
}

func init() {
	signalCmd.PersistentFlags().String(addressKey, "0.0.0.0", "Listen address")

	if err := viper.BindPFlags(signalCmd.PersistentFlags()); err != nil {
		log.Fatal("could not bind flags:", err)
	}

	rootCmd.AddCommand(signalCmd)
}

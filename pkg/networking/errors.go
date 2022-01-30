package networking

type NoConnectionEstablished struct{}

func (m *NoConnectionEstablished) Error() string {
	return "No connection established so far. Either the Connect() has not been called yet or the connection was still in the making"
}

package cluster

type Connector interface {
	Addr() string
	Ready() bool
	Close()
}

//Cluster manage Connecter
type Cluster interface {
	//Get a Connector object
	Get(opt interface{}) Connector
	GetAllConns() []Connector
	Add(addr string, opt string) error
	Del(addr string)
	Stop()
}

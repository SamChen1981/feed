package cluster

type EtcdConsumer interface {
	Add(addr string, opt string) error
	Del(addr string)
}

type EtcdValueConsumer interface {
	SetValue(addr string, value string)
}

type EtcdStatusConsumer interface {
	SetStatus(addr string, status string)
}

type EtcdReportConsumer interface {
	SetReport(addr string, report string)
}

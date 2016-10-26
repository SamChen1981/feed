package cluster

type input interface {
	Stop()
}

type Packer struct {
	Cluster
	in input
}

func Pack(in input, clus Cluster) Cluster {
	pack := &Packer{
		Cluster: clus,
		in:      in,
	}
	return pack
}

func (pack *Packer) Stop() {
	pack.in.Stop()
	pack.Cluster.Stop()
}

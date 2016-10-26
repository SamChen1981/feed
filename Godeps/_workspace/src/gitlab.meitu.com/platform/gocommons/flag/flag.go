package flag

type Flag struct {
	Position uint32
}

func ContainsFlag(flagVal uint32, flag Flag) bool {
	return (flagVal & (1 << flag.Position)) == (1 << (flag.Position))
}

func MarkFlag(oldVal uint32, flag Flag) uint32 {
	return oldVal | (1 << (flag.Position))
}

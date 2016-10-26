package phpmemcached

func createMask(start, nBits uint32) uint32 {
	return ((1 << nBits) - 1) << start
}

var maskType = createMask(0, 4)
var maskInternal = createMask(4, 12)

type ValueType uint32
type ValueFlag uint32

const (
	String ValueType = iota
	Long
	Double
	Bool
	Serialized
	Igbinary
	JSON
	MessagePack

	Compressed        ValueFlag = 1 << 0
	CompressionZlib   ValueFlag = 1 << 1
	CompressionFastLZ ValueFlag = 1 << 2
)

func GetType(flag uint32) ValueType {
	return ValueType(flag & maskType)
}

func SetType(flags uint32, t ValueType) uint32 {
	return flags | (uint32(t) & maskType)
}

func SetFlag(flags uint32, f ValueFlag) uint32 {
	return flags | ((uint32(f) << 4) & maskInternal)
}

func HasFlag(flags uint32, f ValueFlag) bool {
	return (getFlags(flags) & uint32(f)) == uint32(f)
}

func DeleteFlag(flags uint32, f ValueFlag) uint32 {
	return flags & (^((uint32(f) << 4) & maskInternal))
}

func getFlags(flags uint32) uint32 {
	return (flags & maskInternal) >> 4
}

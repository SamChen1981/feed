package phpmemcached

import (
	"fmt"
	"testing"
)

func TestFlags(*testing.T) {
	f := uint32(80)
	fmt.Println(f, HasFlag(80, Compressed))
	fmt.Println(f, HasFlag(80, CompressionZlib))
	fmt.Println(f, HasFlag(80, CompressionFastLZ))
	fmt.Println("------")
	f = DeleteFlag(f, Compressed)
	fmt.Println(f, HasFlag(f, Compressed))
	f = DeleteFlag(f, CompressionFastLZ)
	fmt.Println(f, HasFlag(f, CompressionFastLZ))
	fmt.Println("------")
	f = 0
	f = SetFlag(f, Compressed)
	fmt.Println(f, HasFlag(f, Compressed))
	f = SetFlag(f, CompressionFastLZ)
	fmt.Println(f, HasFlag(f, Compressed))
	fmt.Println(String, JSON)
}

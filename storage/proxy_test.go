package storage_test

import (
	"fmt"
	"testing"

	"gitlab.meitu.com/platform/gocommons/storage"
)

func TestType(*testing.T) {
	var testFunc = func(a storage.Proxy) {
		fmt.Println("valid")
	}

	testFunc(&storage.DefaultProxy{})
}

package lib

import (
	"testing"
)

func TestNewMysqlPool(t *testing.T) {
	master := "192.168.41.219:3306"
	slaves := []string{"192.168.41.219:3306"}
	_, err := NewMysqlPool(master, slaves, nil)
	if err != nil {
		t.Error(err)
	}
}

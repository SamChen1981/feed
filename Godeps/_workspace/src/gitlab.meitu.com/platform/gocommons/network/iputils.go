package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
)

var privateBlocks []*net.IPNet

var loopbackBlock *net.IPNet

func init() {
	// Add each private block
	privateBlocks = make([]*net.IPNet, 3)
	_, block, err := net.ParseCIDR("10.0.0.0/8")
	if err != nil {
		panic(fmt.Sprintf("Bad CIDR. Got %v", err))
	}
	privateBlocks[0] = block

	_, block, err = net.ParseCIDR("172.16.0.0/12")
	if err != nil {
		panic(fmt.Sprintf("Bad CIDR. Got %v", err))
	}
	privateBlocks[1] = block

	_, block, err = net.ParseCIDR("192.168.0.0/16")
	if err != nil {
		panic(fmt.Sprintf("Bad CIDR. Got %v", err))
	}
	privateBlocks[2] = block

	_, block, err = net.ParseCIDR("127.0.0.0/8")
	if err != nil {
		panic(fmt.Sprintf("Bad CIDR. Got %v", err))
	}
	loopbackBlock = block
}

// IsPrivateIP returns if the given IP is in a private block
func IsPrivateIP(ip net.IP) bool {
	for _, priv := range privateBlocks {
		if priv.Contains(ip) {
			return true
		}
	}
	return false
}

// IsLoopbackIP returns if the given IP is in a loopback block
func IsLoopbackIP(ip net.IP) bool {
	return loopbackBlock.Contains(ip)
}

func GetCurrentIPAddr() (string, error) {
	ifts, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, ift := range ifts {

		if strings.HasPrefix(ift.Name, "docker") ||
			strings.HasPrefix(ift.Name, "vboxnet") ||
			strings.HasPrefix(ift.Name, "VMnet") {
			continue
		}

		if ift.Flags&net.FlagUp == 0 ||
			ift.Flags&net.FlagLoopback != 0 ||
			ift.Flags&net.FlagPointToPoint != 0 {
			continue
		}

		addrs, err := ift.Addrs()
		if err != nil {
			return "", err
		}

		for _, addr := range addrs {
			var ip net.IP
			switch addr := addr.(type) {
			case *net.IPAddr:
				ip = addr.IP
			case *net.IPNet:
				ip = addr.IP
			default:
				continue
			}

			if ip.To4() == nil {
				continue
			}
			if !IsPrivateIP(ip) {
				continue
			}

			return ip.String(), nil
		}
	}
	return "", errors.New("no private IP address found")
}

func IPToUint32(ip net.IP) uint32 {
	if len(ip) == net.IPv6len {
		return binary.BigEndian.Uint32(ip[12:])
	}
	return binary.BigEndian.Uint32(ip)
}

func Uint32ToIP(u uint32) net.IP {
	ip := make(net.IP, net.IPv4len)
	binary.BigEndian.PutUint32(ip, u)
	return ip
}

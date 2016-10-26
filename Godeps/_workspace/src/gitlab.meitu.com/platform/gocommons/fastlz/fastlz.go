// Package fastlz implements FastLZ compression (http://fastlz.org/)
/*
This code is ported from
    https://github.com/ariya/FastLZ/blob/master/fastlz.c
    https://github.com/netty/netty/blob/4.1/codec/src/main/java/io/netty/handler/codec/compression/FastLz.java
*/
package fastlz

import (
	"errors"
	"fmt"
)

// Level define the compression level that fastlz used to compress data
type Level int

const (
	//LevelAuto means fastlz will choose level automatically depending on the length of the input buffer.
	LevelAuto Level = iota
	//Level1 is the fastest compression and generally useful for short data.
	Level1
	//Level2 is slightly slower but it gives better compression ratio.
	Level2
)

const (
	maxDistance    = 8191
	maxFarDistance = 65535 + maxDistance - 1

	maxCopy = 32
	maxLen  = 256 + 8

	hashLog  = 13
	hashSize = 1 << hashLog // 8192
	hashMask = hashSize - 1

	level2Threshold = 64 * 1024 // 65536
)

// CalculateOutputBufferLength is used to calculate the output buffer size
func CalculateOutputBufferLength(inputLength uint32) uint32 {
	computed := uint32(float64(inputLength)*1.05) + 1
	if computed <= 66 {
		return 66
	}

	return computed
}

func hash(data []byte, offset uint32) uint32 {
	v := readU16(data, offset)
	v ^= readU16(data, offset+1) ^ (v >> (16 - hashLog))
	v &= hashMask
	return uint32(v)
}

func readU16(data []byte, offset uint32) uint16 {
	return uint16(data[offset]) | (uint16(data[offset+1]) << 8)
}

// ErrOutputExceeded indicates the output is exceed the length limit
var ErrOutputExceeded = errors.New("fastlz: output limit exceeded")

// ErrInputExceeded indicates the input is exceed the length limit
var ErrInputExceeded = errors.New("fastlz: input limit exceeded")

//Compress a block of data in the input buffer and returns the size of compressed block.
//The size of input buffer is specified by inLength. The minimum input buffer size is 16.
//The output buffer must be at least 5% larger than the input buffer and can not be smaller than 66 bytes.
//If the input is not compressible, the return value might be larger than inLength (input buffer size).
//The input buffer and the output buffer can not overlap.
func Compress(input []byte, inOffset uint32, inLength uint32, output []byte, outOffset uint32, proposedLevel Level) (uint32, error) {
	var level Level
	if proposedLevel == LevelAuto {
		if inLength < level2Threshold {
			level = Level1
		} else {
			level = Level2
		}
	} else if proposedLevel == Level1 || proposedLevel == Level2 {
		level = proposedLevel
	} else {
		return 0, fmt.Errorf("invalid level: %d (expected: %d or %d)", proposedLevel, Level1, Level2)
	}

	var (
		ip, op  uint32
		ipBound = ip + inLength - 2
		ipLimit = ip + inLength - 12

		htab  [hashSize]uint32
		hslot uint32
		hval  uint32

		copy uint32
	)

	/* sanity check */
	if inLength < 4 {
		if inLength != 0 {
			/* create literal copy only */
			output[outOffset+op] = byte(inLength - 1)
			op++
			ipBound++
			for ip <= ipBound {
				output[outOffset+op] = input[inOffset+ip]
				op++
				ip++
			}
			return inLength + 1, nil
		}
		// else
		return 0, nil
	}

	/* initializes hash table */
	for hslot = 0; hslot < hashSize; hslot++ {
		htab[hslot] = ip
	}

	/* we start with literal copy */
	copy = 2
	output[outOffset+op] = byte(maxCopy - 1)
	op++
	output[outOffset+op] = input[inOffset+ip]
	op++
	ip++
	output[outOffset+op] = input[inOffset+ip]
	op++
	ip++

	/* main loop */
	for ip < ipLimit {
		var (
			ref      uint32
			distance uint32
			len      uint32 = 3  // minimum match length
			anchor          = ip //comparison starting-point
		)

		/* check for a run */
		if level == Level2 {
			if input[inOffset+ip] == input[inOffset+ip-1] &&
				readU16(input, inOffset+ip-1) == readU16(input, inOffset+ip+1) {
				distance = 1
				ip += 3
				ref = anchor - 1 + 3
				goto match
			}
		}

		/* find potential match */
		hval = hash(input, inOffset+ip)
		hslot = hval
		ref = htab[hval]

		/* calculate distance to the match */
		distance = anchor - ref

		/* update hash table */
		htab[hslot] = anchor

		if distance == 0 {
			goto literal
		}

		if level == Level1 {
			if distance >= (maxDistance + 1) {
				goto literal
			}
		} else if distance >= maxFarDistance {
			goto literal
		}

		/* check the first 3 bytes */
		for i := 0; i < 3; i++ {
			ne := input[inOffset+ref] != input[inOffset+ip]
			ref++
			ip++
			if ne {
				goto literal
			}
		}

		if level == Level2 {
			/* far, needs at least 5-byte match */
			if distance >= maxDistance {
				for i := 0; i < 2; i++ {
					ne := input[inOffset+ip] != input[inOffset+ref]
					ip++
					ref++
					if ne {
						goto literal
					}
				}
				len += 2
			}
		}

	match:
		/* last matched byte */
		ip = anchor + len

		/* distance is biased */
		distance--

		if distance == 0 {
			/* zero distance means a run */
			x := input[inOffset+ip-1]
			for ip < ipBound {
				ne := input[inOffset+ref] != x
				ref++
				if ne {
					break
				} else {
					ip++
				}
			}
		} else {
		check:
			for {
				/*
					safe because the outer check against ip limit
				*/
				for i := 0; i < 8; i++ {
					ne := input[inOffset+ref] != input[inOffset+ip]
					ref++
					ip++
					if ne {
						break check
					}
				}
				for ip < ipBound {
					ne := input[inOffset+ref] != input[inOffset+ip]
					ref++
					ip++
					if ne {
						break
					}
				}
				break check
			}
		}

		/* if we have copied something, adjust the copy count */
		if copy != 0 {
			/* copy is biased, '0' means 1 byte copy */
			output[outOffset+op-copy-1] = byte(copy - 1)
		} else {
			/* back, to overwrite the copy count */
			op--
		}

		/* reset literal counter */
		copy = 0

		/* length is biased, '1' means a match of 3 bytes */
		ip -= 3
		len = ip - anchor

		/* encode the match */
		if level == Level2 {
			if distance < maxDistance {
				if len < 7 {
					output[outOffset+op] = byte((len << 5) + (distance >> 8))
					op++
					output[outOffset+op] = byte(distance & 255)
					op++
				} else {
					output[outOffset+op] = byte((7 << 5) + (distance >> 8))
					op++
					for len -= 7; len >= 255; len -= 255 {
						output[outOffset+op] = byte(255)
						op++
					}
					output[outOffset+op] = byte(len)
					op++
					output[outOffset+op] = byte(distance & 255)
					op++
				}
			} else {
				//far away, but not yet in the another galaxy...
				if len < 7 {
					distance -= maxDistance
					output[outOffset+op] = byte((len << 5) + 31)
					op++
					output[outOffset+op] = byte(255)
					op++
					output[outOffset+op] = byte(distance >> 8)
					op++
					output[outOffset+op] = byte(distance & 255)
					op++
				} else {
					distance -= maxDistance
					output[outOffset+op] = byte((7 << 5) + 31)
					op++
					for len -= 7; len >= 255; len -= 255 {
						output[outOffset+op] = byte(255)
						op++
					}
					output[outOffset+op] = byte(len)
					op++
					output[outOffset+op] = byte(255)
					op++
					output[outOffset+op] = byte(distance >> 8)
					op++
					output[outOffset+op] = byte(distance & 255)
					op++
				}
			}
		} else {
			for len > maxLen-2 {
				output[outOffset+op] = byte((7 << 5) + (distance >> 8))
				op++
				output[outOffset+op] = byte(maxLen - 2 - 7 - 2)
				op++
				output[outOffset+op] = byte(distance & 255)
				op++
				len -= maxLen - 2
			}

			if len < 7 {
				output[outOffset+op] = byte((len << 5) + (distance >> 8))
				op++
				output[outOffset+op] = byte(distance & 255)
				op++
			} else {
				output[outOffset+op] = byte((7 << 5) + (distance >> 8))
				op++
				output[outOffset+op] = byte(len - 7)
				op++
				output[outOffset+op] = byte(distance & 255)
				op++
			}
		}

		/* update the hash at match boundary */
		hval = hash(input, inOffset+ip)
		htab[hval] = ip
		ip++

		hval = hash(input, inOffset+ip)
		htab[hval] = ip
		ip++

		/* assuming literal copy */
		output[outOffset+op] = byte(maxCopy - 1)
		op++

		continue

	literal:
		output[outOffset+op] = input[inOffset+anchor]
		op++
		anchor++
		ip = anchor
		copy++
		if copy == maxCopy {
			copy = 0
			output[outOffset+op] = byte(maxCopy - 1)
			op++
		}
	}

	/* left-over as literal copy */
	ipBound++
	for ip <= ipBound {
		output[outOffset+op] = input[inOffset+ip]
		op++
		ip++
		copy++
		if copy == maxCopy {
			copy = 0
			output[outOffset+op] = byte(maxCopy - 1)
			op++
		}
	}

	/* if we have copied something, adjust the copy length */
	if copy != 0 {
		output[outOffset+op-copy-1] = byte(copy - 1)
	} else {
		op--
	}

	if level == Level2 {
		/* marker for level 2 */
		output[outOffset] |= 1 << 5
	}

	return op, nil
}

//Decompress a block of compressed data and returns the size of the decompressed block.
// If error occurs, e.g. the compressed data is corrupted or the output buffer is not large enough, then 0 (zero) will be returned instead.
//The input buffer and the output buffer can not overlap.
//Decompression is memory safe and guaranteed not to write the output buffer more than what is specified in outLength.
func Decompress(input []byte, inOffset uint32, inLength uint32, output []byte, outOffset uint32, outLength uint32) (uint32, error) {
	level := Level((input[inOffset] >> 5) + 1)
	if level != Level1 && level != Level2 {
		return 0, fmt.Errorf("invalid level: %d (expected: %d or %d)", level, Level1, Level2)
	}

	var ip, op uint32
	ctrl := uint32(input[inOffset+ip] & 31)
	ip++

	loop := true
	for loop {
		ref := op
		len := ctrl >> 5
		ofs := uint32((ctrl & 31) << 8)

		if ctrl >= 32 {
			len--
			ref -= ofs

			var code uint32
			if len == 6 { // 7 - 1
				if level == Level1 {
					len += uint32(input[inOffset+ip] & 0xFF)
					ip++
				} else {
					for {
						code = uint32(input[inOffset+ip] & 0xFF)
						ip++
						len += code
						if code != 255 {
							break
						}
					}
				}
			}
			if level == Level1 {
				ref -= uint32(input[inOffset+ip] & 0xFF)
				ip++
			} else {
				code = uint32(input[inOffset+ip] & 0xFF)
				ip++
				ref -= code

				/* match from 16-bit distance */
				if code == 255 && ofs == (31<<8) {
					ofs = uint32(input[inOffset+ip]&0xFF) << 8
					ip++
					ofs += uint32(input[inOffset+ip] & 0xFF)
					ip++

					ref = uint32(op - ofs - maxDistance)
				}
			}

			// if the output index + length of block(?) + 3(?) is over the output limit?
			if op+len+3 > outLength {
				return 0, ErrOutputExceeded
			}

			// if the address space of ref-1 is < the address of output?
			// if we are still at the beginning of the output address?
			if ref-1 < 0 {
				return 0, nil
			}

			if loop = ip < inLength; loop {
				ctrl = uint32(input[inOffset+ip] & 0xFF)
				ip++
			}

			if ref == op {
				/* optimize copy for a run */
				b := output[outOffset+ref-1]
				for i := 0; i < 3; i++ {
					output[outOffset+op] = b
					op++
				}
				for ; len != 0; len-- {
					output[outOffset+op] = b
					op++
				}
			} else {
				/* copy from reference */
				ref--
				for i := 0; i < 3; i++ {
					output[outOffset+op] = output[outOffset+ref]
					op++
					ref++
				}
				for ; len != 0; len-- {
					output[outOffset+op] = output[outOffset+ref]
					op++
					ref++
				}
			}
		} else {
			ctrl++

			if op+ctrl > outLength {
				return 0, ErrOutputExceeded
			}
			if ip+ctrl > inLength {
				return 0, ErrInputExceeded
			}

			output[outOffset+op] = input[inOffset+ip]
			op++
			ip++

			for ctrl--; ctrl != 0; ctrl-- {
				output[outOffset+op] = input[inOffset+ip]
				op++
				ip++
			}

			if loop = ip < inLength; loop {
				ctrl = uint32(input[inOffset+ip] & 0xFF)
				ip++
			}
		}
	}

	return op, nil
}

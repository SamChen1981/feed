package treeset

import "github.com/emirpasic/gods/utils"

func Int64Comparator(a, b interface{}) int {
	aInt := a.(int64)
	bInt := b.(int64)
	switch {
	case aInt > bInt:
		return 1
	case aInt < bInt:
		return -1
	default:
		return 0
	}
}

var IntComparator = utils.IntComparator
var StringComparator = utils.StringComparator

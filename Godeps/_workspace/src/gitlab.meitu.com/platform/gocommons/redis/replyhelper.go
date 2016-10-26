package redis

import (
	"github.com/garyburd/redigo/redis"
)

var (
	Bool = func(reply interface{}, err error) (bool, error) {
		ret, err := redis.Bool(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	Bytes = func(reply interface{}, err error) ([]byte, error) {
		ret, err := redis.Bytes(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	BytesSlice = func(reply interface{}, err error) ([][]byte, error) {
		ret, err := redis.ByteSlices(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	String = func(reply interface{}, err error) (string, error) {
		ret, err := redis.String(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	StringSlice = func(reply interface{}, err error) ([]string, error) {
		ret, err := redis.Strings(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	StringMap = func(reply interface{}, err error) (map[string]string, error) {
		ret, err := redis.StringMap(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	Float64 = func(reply interface{}, err error) (float64, error) {
		ret, err := redis.Float64(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	Int = func(reply interface{}, err error) (int, error) {
		ret, err := redis.Int(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	IntSlice = func(reply interface{}, err error) ([]int, error) {
		ret, err := redis.Ints(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	IntMap = func(reply interface{}, err error) (map[string]int, error) {
		ret, err := redis.IntMap(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	Int64 = func(reply interface{}, err error) (int64, error) {
		ret, err := redis.Int64(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	Int64Map = func(reply interface{}, err error) (map[string]int64, error) {
		ret, err := redis.Int64Map(reply, err)
		if err == redis.ErrNil {
			return ret, nil
		}
		return ret, err
	}

	Int64Slice = func(reply interface{}, err error) ([]int64, error) {
		var ints []int64
		values, err := redis.Values(reply, err)
		if err == redis.ErrNil {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if err := redis.ScanSlice(values, &ints); err != nil {
			return ints, err
		}
		return ints, nil
	}
)

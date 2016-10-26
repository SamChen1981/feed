// Package profile prints profile statistics information log in specified duration
package profile

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.meitu.com/platform/gocommons/log"
)

const (
	typeAPI     = "API"
	typeSERVICE = "SERVICE"
	typeMC      = "MC"
	typeREDIS   = "REDIS"
	typeDB      = "DB"
	typeHTTP    = "HTTP"
)

const (
	statPeroid = 10 //second
	slowCost   = 200
	slaPercent = 0.9995

	apiInterval1 = 100
	apiInterval2 = 500
	apiInterval3 = 1000
	apiInterval4 = 2000

	serviceInterval1 = 50
	serviceInterval2 = 100
	serviceInterval3 = 500
	serviceInterval4 = 1000

	resourceInterval1 = 10
	resourceInterval2 = 50
	resourceInterval3 = 100
	resourceInterval4 = 200
)

const (
	mcFireTime    = 500
	dbFireTime    = 1000
	redisFireTime = 500
)

const (
	separate     = "\\|"
	portSeparate = ":"
	ipSeparate   = ","
)

var apiIntervals []int64 = []int64{apiInterval1, apiInterval2, apiInterval3, apiInterval4}
var servIntervals []int64 = []int64{serviceInterval1, serviceInterval2, serviceInterval3, serviceInterval4}
var resIntervals []int64 = []int64{resourceInterval1, resourceInterval2, resourceInterval3, resourceInterval4}
var statItems map[string]*StatItem = make(map[string]*StatItem)
var profileLogger log.Logger = log.GetLogger("profile")
var createStatItemLock *sync.Mutex = new(sync.Mutex)

func init() {
	go func() {
		time.Sleep(statPeroid * time.Second)
		for {
			logAccessStat(true)
			time.Sleep(statPeroid * time.Second)
		}
	}()
}

func AccessMCStat(name string, currentTimeMillis int64, elaspedTimeMillis int64) {
	AccessStat(typeMC, name, currentTimeMillis, elaspedTimeMillis, mcFireTime)
}

func AccessDBStat(name string, currentTimeMillis int64, elaspedTimeMillis int64) {
	AccessStat(typeDB, name, currentTimeMillis, elaspedTimeMillis, dbFireTime)
}

func AccessStat(statType string, name string, currentTimeMillis int64, elaspedTimeMillis int64, slowThredshold int64) {
	item := getStatItem(statType, name, currentTimeMillis, slowThredshold)
	item.stat(currentTimeMillis, elaspedTimeMillis)
}

func getStatItem(statType string, name string, currentTimeMillis int64, slowThredshold int64) *StatItem {
	key := statType + "|" + name
	statItem, exists := statItems[key]
	if !exists {
		createStatItemLock.Lock()
		defer createStatItemLock.Unlock()

		statItem, exists = statItems[key]
		if !exists {
			statItem = NewStatsItem(statType, name, currentTimeMillis, slowThredshold)
			statItems[key] = statItem
		}
	}

	return statItem
}

func logAccessStat(clear bool) {
	curTimeMillis := time.Now().Unix()
	for _, item := range statItems {
		statRs := item.getStatResult(curTimeMillis, statPeroid)

		if clear {
			item.cleanStat(curTimeMillis, statPeroid)
		}

		if statRs.TotalCount == 0 {
			continue
		}

		jsonBytes, err := json.Marshal(statRs)
		if err != nil {
			fmt.Println("error logging profile when convert map 2 json", err)
			continue
		}

		profileLogger.Info(string(jsonBytes))
	}

	profileLogger.Flush()
}

type StatItem struct {
	StatType      string
	Name          string
	CurrentIndex  int
	ElapsedTimes  []int64
	TotalCounter  []uint32
	SlowCounter   []uint32
	Length        int
	SlowThreshold int64
	Interval1     []uint32
	Interval2     []uint32
	Interval3     []uint32
	Interval4     []uint32
	Interval5     []uint32
	resetLock     *sync.Mutex
}

func NewStatsItem(statType string, name string, currentTimeMillis int64, slowThreshold int64) *StatItem {
	item := StatItem{StatType: statType, Name: name, Length: statPeroid * 2, SlowThreshold: slowThreshold}
	item.CurrentIndex = item.getIndex(currentTimeMillis, item.Length)
	item.resetLock = new(sync.Mutex)
	item.ElapsedTimes = make([]int64, item.Length)
	item.TotalCounter = make([]uint32, item.Length)
	item.SlowCounter = make([]uint32, item.Length)
	item.Interval1 = make([]uint32, item.Length)
	item.Interval2 = make([]uint32, item.Length)
	item.Interval3 = make([]uint32, item.Length)
	item.Interval4 = make([]uint32, item.Length)
	item.Interval5 = make([]uint32, item.Length)
	return &item
}

func (item StatItem) getIndex(currentTimeMillis int64, periodSecond int) int {
	return (int)((currentTimeMillis / 1000) % (int64)(periodSecond))
}

func (item *StatItem) stat(currentTimeMillis int64, elapsedTimeMillis int64) {
	tmpIndex := item.getIndex(currentTimeMillis, item.Length)
	if tmpIndex != item.CurrentIndex {
		item.resetLock.Lock()
		if tmpIndex != item.CurrentIndex {
			//这一秒的第一条统计，把对应的存储位的数据置0
			item.reset(tmpIndex)
			item.CurrentIndex = tmpIndex
		}
		item.resetLock.Unlock()
	}

	atomic.AddInt64(&item.ElapsedTimes[item.CurrentIndex], elapsedTimeMillis)
	atomic.AddUint32(&item.TotalCounter[item.CurrentIndex], 1)

	if elapsedTimeMillis >= item.SlowThreshold {
		atomic.AddUint32(&item.SlowCounter[item.CurrentIndex], 1)
	}

	if strings.EqualFold(typeAPI, item.StatType) {
		item.statIntervals(elapsedTimeMillis, apiIntervals)

	} else if strings.EqualFold(typeSERVICE, item.StatType) {
		item.statIntervals(elapsedTimeMillis, servIntervals)

	} else if strings.EqualFold(typeMC, item.StatType) || strings.EqualFold(typeDB, item.StatType) {
		item.statIntervals(elapsedTimeMillis, resIntervals)

	}
}

func (item *StatItem) reset(index int) {
	atomic.StoreInt64(&item.ElapsedTimes[index], 0)
	atomic.StoreUint32(&item.TotalCounter[index], 0)
	atomic.StoreUint32(&item.SlowCounter[index], 0)
	atomic.StoreUint32(&item.Interval1[index], 0)
	atomic.StoreUint32(&item.Interval2[index], 0)
	atomic.StoreUint32(&item.Interval3[index], 0)
	atomic.StoreUint32(&item.Interval4[index], 0)
	atomic.StoreUint32(&item.Interval5[index], 0)
}

func (item *StatItem) statIntervals(elapsedTimeMillis int64, intervals []int64) {
	//	fmt.Printf("\n###\n%v\n", item.Interval5)
	if elapsedTimeMillis < intervals[0] {
		atomic.AddUint32(&item.Interval1[item.CurrentIndex], 1)

	} else if elapsedTimeMillis >= intervals[0] && elapsedTimeMillis < intervals[1] {
		atomic.AddUint32(&item.Interval2[item.CurrentIndex], 1)

	} else if elapsedTimeMillis >= intervals[1] && elapsedTimeMillis < intervals[2] {
		atomic.AddUint32(&item.Interval3[item.CurrentIndex], 1)

	} else if elapsedTimeMillis >= intervals[2] && elapsedTimeMillis < intervals[3] {
		atomic.AddUint32(&item.Interval4[item.CurrentIndex], 1)

	} else {
		atomic.AddUint32(&item.Interval5[item.CurrentIndex], 1)

	}
	//	fmt.Printf("%v\n", item.Interval5)
}

type statResult struct {
	StatType       string `json:"type"`
	Name           string `json:"name"`
	SlowThreshold  int64  `json:"slowThreshold"`
	TotalCount     uint32 `json:"total_count"`
	SlowCount      uint32 `json:"slow_count"`
	maxCount       uint32
	minCount       uint32
	elaspedTime    int64
	AvgElaspedTime float64 `json:"avg_time"`
	Interval1      uint32  `json:"interval1"`
	Interval2      uint32  `json:"interval2"`
	Interval3      uint32  `json:"interval3"`
	Interval4      uint32  `json:"interval4"`
	Interval5      uint32  `json:"interval5"`
}

func (item *StatItem) getStatResult(currentTimeMillis int64, periodSec int) statResult {
	//当前这秒还没完全结束，因此数据不全，统计从上一秒开始，往前推移peroidSecond
	currentTimeSec := currentTimeMillis/1000 - 1
	startIndex := item.getIndex(currentTimeSec*1000, item.Length)

	rs := statResult{SlowThreshold: item.SlowThreshold}
	for i := 0; i < periodSec; i++ {
		tmpIndex := (startIndex - i + item.Length) % item.Length
		rs.StatType = item.StatType
		rs.Name = item.Name
		rs.SlowCount += atomic.LoadUint32(&item.SlowCounter[tmpIndex])
		rs.elaspedTime += atomic.LoadInt64(&item.ElapsedTimes[tmpIndex])
		rs.Interval1 += atomic.LoadUint32(&item.Interval1[tmpIndex])
		rs.Interval2 += atomic.LoadUint32(&item.Interval2[tmpIndex])
		rs.Interval3 += atomic.LoadUint32(&item.Interval3[tmpIndex])
		rs.Interval4 += atomic.LoadUint32(&item.Interval4[tmpIndex])
		rs.Interval5 += atomic.LoadUint32(&item.Interval5[tmpIndex])

		totalCount := atomic.LoadUint32(&item.TotalCounter[tmpIndex])
		rs.TotalCount += totalCount
		if totalCount > rs.maxCount {
			rs.maxCount = totalCount
		}
		if totalCount < rs.minCount || rs.minCount == 0 {
			rs.minCount = totalCount
		}

	}

	if rs.TotalCount > 0 {
		rs.AvgElaspedTime = float64(rs.elaspedTime) / float64(rs.TotalCount)
		rs.AvgElaspedTime, _ = strconv.ParseFloat(fmt.Sprintf("%.3f", rs.AvgElaspedTime), 64)
	}
	return rs
}

func (item *StatItem) cleanStat(currentTimeMillis int64, periodSec int) {
	//当前这秒还没完全结束，因此数据不全，统计从上一秒开始，往前推移peroidSecond
	currentTimeSec := currentTimeMillis/1000 - 1
	startIndex := item.getIndex(currentTimeSec*1000, item.Length)

	for i := 0; i < periodSec; i++ {
		item.reset((startIndex - i + item.Length) % item.Length)
	}
}

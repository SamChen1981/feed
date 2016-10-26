// Package diagnostics record and log time cost by step and stats info.
package diagnostics

import (
	"bytes"
	"strconv"
	"sync/atomic"
	"time"

	"gitlab.meitu.com/platform/gocommons/log"
	"golang.org/x/net/context"
)

type period struct {
	interval  int64 //区间统计的间隔数
	hit       int64 //区间命中次数
	elapsed   int64 //区间的总共消耗时间 nano
	total     int64 //区间总共访问次数
	nextIndex int64 //下一个间隔时间计数
}

//做周期统计时的回调，比如可以做 当命中率小于多少的时候做什么样的事情。
type PeriodRateCallback func(hitRate int64, stat string)

type Monitor struct {
	Name string
	PeriodRateCallback

	period
	logger log.Logger
}
type Step struct {
	Name     string
	UnixNano int64 //UnixNano的时间计数
}

type MonitorItem struct {
	Steps []Step
}

func NewMonitor(name string, interval int64, logger log.Logger) Monitor {
	return Monitor{
		Name: name,
		period: period{
			interval: interval,
		},
		logger: logger,
	}
}
func (monitor *Monitor) Start() MonitorItem {
	item := MonitorItem{
		Steps: []Step{
			Step{
				Name:     "Start",
				UnixNano: time.Now().UnixNano(),
			},
		},
	}
	return item
}
func (monitor *Monitor) doCallback(hitRate int64, stat string) {
	if monitor.PeriodRateCallback != nil {
		monitor.PeriodRateCallback(hitRate, stat)
	}
}
func (monitor *Monitor) End(item MonitorItem, debugInfo string, hit bool, debug bool, debugLimit int64) {
	if len(item.Steps) == 0 {
		return
	}
	start := item.Steps[0]
	last := Step{Name: "Last", UnixNano: time.Now().UnixNano()}
	item.Steps = append(item.Steps, last)
	elapsed := (last.UnixNano - start.UnixNano) / int64(time.Millisecond)
	if elapsed == 0 {
		return
	}

	//累计区间数据
	atomic.AddInt64(&(monitor.total), 1)
	if hit == true {
		atomic.AddInt64(&(monitor.hit), 1)
	}
	atomic.AddInt64(&(monitor.elapsed), elapsed)
	if debug || (debugLimit > 0 && elapsed > debugLimit) {
		var buffer bytes.Buffer
		buffer.WriteString("[useTime-" + monitor.Name + "]")
		buffer.WriteString(" hit:" + strconv.FormatBool(hit))
		if len(item.Steps) > 2 {
			//stepElapseds := make(map[string]int64) //暂时没有需要暂存每个步骤花费的时间
			for i := 0; i < len(item.Steps)-1; {
				stepElapsed := (item.Steps[i+1].UnixNano - item.Steps[i].UnixNano) / int64(time.Millisecond)
				//stepElapsed[item.Steps[i].Name] = stepElapsed
				i++
				buffer.WriteString(" " + item.Steps[i].Name + ":" + strconv.FormatInt(stepElapsed, 10))
			}
		}
		buffer.WriteString(" Total:" + strconv.FormatInt(elapsed, 10))
		if len(debugInfo) > 0 {
			buffer.WriteString("\tdebugInfo:" + debugInfo)
		}
		if debug {
			monitor.logger.Debug(buffer.String())
		} else {
			monitor.logger.Warn(buffer.String())
		}
	}

	if monitor.interval <= 0 {
		return
	}

	periodPrint := false
	periodIndex := ((last.UnixNano + monitor.interval) / monitor.interval)
	if periodIndex >= monitor.nextIndex && monitor.total > 100 {
		if monitor.nextIndex == 0 {
			atomic.CompareAndSwapInt64(&(monitor.nextIndex), 0, monitor.nextIndex+1)
		} else {
			atomic.AddInt64(&(monitor.nextIndex), 1)
			periodPrint = true
		}
	}

	//避免对主逻辑的影响，复制一个新的对象进行统计计算。
	//不使用锁的方式保证线程安全，会导致主逻辑的停顿等待。因为统计需求对一致性的要求没有那么严格
	monitorCopy := *monitor
	if periodPrint && monitorCopy.total > 0 {
		//还原间隔计数,可能会有重复清零的情况出现
		atomic.StoreInt64(&(monitor.elapsed), 0)
		atomic.StoreInt64(&(monitor.hit), 0)
		atomic.StoreInt64(&(monitor.total), 0)

		var buffer bytes.Buffer
		buffer.WriteString("[visitStats-")
		buffer.WriteString(monitor.Name)
		buffer.WriteString("]\ttotal:")
		buffer.WriteString(strconv.FormatInt(monitorCopy.total, 10))
		buffer.WriteString("\thit:")
		buffer.WriteString(strconv.FormatInt(monitorCopy.hit, 10))
		buffer.WriteString("\ttotalTime:")
		buffer.WriteString(strconv.FormatInt(monitorCopy.elapsed, 10))
		buffer.WriteString("\tavgTime: ")
		buffer.WriteString(strconv.FormatInt((monitorCopy.elapsed / monitorCopy.total), 10))
		buffer.WriteString("\thitRate:")
		hitRate := (monitorCopy.hit * 100 / monitorCopy.total)
		buffer.WriteString(strconv.FormatInt(hitRate, 10))

		monitorCopy.logger.Info(buffer.String())
		go monitor.doCallback(hitRate, buffer.String())

	}
}

func (monitorItem *MonitorItem) AppendStep(name string) {
	monitorItem.Steps = append(monitorItem.Steps, Step{Name: name, UnixNano: time.Now().UnixNano()})
}

const MonitorItemKey = "mItem"

func GetMonitorItemFromContext(ctx context.Context) (*MonitorItem, bool) {
	monitorItem, ok := ctx.Value(MonitorItemKey).(*MonitorItem)
	return monitorItem, ok
}

func SetMonitorItemToContext(ctx context.Context, item *MonitorItem) context.Context {
	return context.WithValue(ctx, MonitorItemKey, item)
}

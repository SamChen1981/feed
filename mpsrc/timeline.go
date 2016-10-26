package mpsrc

import (
	"encoding/json"
	"feed/storage"
	"github.com/Shopify/sarama"
	"golang.org/x/net/context"
	"sort"
	"strconv"
	"strings"
)

const (
	DefaultNum          = 100
	PushLimitNum        = 200
	DefaultExpireTime   = 300
	DeleteTime          = 1
	NEWEST              = "Newest"
	FANS                = "Fans"
	LIKES               = "Likes"
	FRIENDS             = "Friends"
	UNREAD              = "Unread"
	FRIENDSTIMELINE     = "friendstimeline"
	ADDPERSONALTIMELINE = "addpersonaltimeline"
	DELPERSONALTIMELINE = "delpersonaltimeline"
	ADDLIKES            = "addlikes"
	DELLIKES            = "dellikes"
	ADDVALUE            = "addvalue"
	DELVALUE            = "delvalue"
	ADDFANS             = "addfans"
	DELFANS             = "delfans"
)

//动态的key及其属性，供排序用
type TimelineKey struct {
	UserID    uint64
	Timestamp uint64
	//redis 里面存储
	// LikesNum      uint64
	// CollectionNum uint64
	// ForwardingNum uint64
	// WatchNum      uint64
	// TimelineNum   uint64
	ValueKey string //指向真正内容的key
}

type Timelines []*TimelineKey
type Likes []uint64
type Fans []uint64
type UnReadNum uint64

func (tls Timelines) Len() int { return len(tls) }
func (tls Timelines) Swap(i, j int) {
	tls[i], tls[j] = tls[j], tls[i]
}
func (tls Timelines) Less(i, j int) bool {
	return tls[i].Timestamp < tls[j].Timestamp
}

func setItem(infos interface{}, cas uint64) (*storage.Item, []byte, error) {
	value, err := json.Marshal(infos)
	if err != nil {
		return nil, nil, err
	}
	item := &storage.Item{
		Value:       value,
		DataVersion: cas,
		ExpireAt:    DefaultExpireTime,
	}
	return item, value, nil
}

func getFriendsInfo(userID, infoType string) []uint64 {
	key := userID + infoType
	ids := make([]uint64, 0)
	rs := storageProxy.Get(storage.SetReadStrategyToContent(context.Background(), storage.CacheOnly), key)
	if rs != nil {
		if v, ok := rs.Value.([]byte); ok {
			json.Unmarshal(v, &ids)
			return ids
		}
		//脏数据，返回空数组
		return ids
	}
	//查找DB，并回写cache
	return getFriendsInfoFromDB(userID, infoType, key)
}

func updateFriendsInfo(info, userID, infoType, opt string) error {
	switch infoType {
	case FANS:
		if opt == "add" {
			producer.Input() <- &sarama.ProducerMessage{Topic: ADDFANS, Key: sarama.StringEncoder(userID),
				Value: sarama.StringEncoder(info), Partition: 0}
		} else if opt == "delete" {
			producer.Input() <- &sarama.ProducerMessage{Topic: DELFANS, Key: sarama.StringEncoder(userID),
				Value: sarama.StringEncoder(info), Partition: 0}
		} else {
			return ErrOpt
		}

	case LIKES:
		if opt == "add" {
			producer.Input() <- &sarama.ProducerMessage{Topic: ADDLIKES, Key: sarama.StringEncoder(userID),
				Value: sarama.StringEncoder(info), Partition: 0}
		} else if opt == "delete" {
			producer.Input() <- &sarama.ProducerMessage{Topic: DELLIKES, Key: sarama.StringEncoder(userID),
				Value: sarama.StringEncoder(info), Partition: 0}
		} else {
			return ErrOpt
		}
	}
	return nil
}

//基于时间的缓存，需要其他附属手段增加命中率
func getPersonalTimelineKey(timestampBegin, timestampEnd, userID string) (Timelines, uint64, error) {
	key := userID + timestampBegin + timestampEnd
	tls := make(Timelines, 0)
	rs := storageProxy.Get(storage.SetReadStrategyToContent(context.Background(), storage.CacheOnly), key)
	if rs != nil {
		if v, ok := rs.Value.([]byte); ok {
			json.Unmarshal(v, &tls)
			return tls, rs.DataVersion, nil
		}
		//脏数据，返回空数组
		return tls, rs.DataVersion, nil
	}

	uid, err := strconv.Atoi(userID)
	if err != nil {
		return tls, 0, err
	}
	tsBegin, err := strconv.Atoi(timestampBegin)
	if err != nil {
		return tls, 0, err
	}
	tsEnd, err := strconv.Atoi(timestampEnd)
	if err != nil {
		return tls, 0, err
	}
	return getPersonalTimelineKeyFromDB(uint64(uid), uint64(tsBegin), uint64(tsEnd), key), 0, nil
}

//先取出Valuekey，然后取回真正的Value
func getPersonalTimeline(timestampBegin, timestampEnd, userID string) (Timelines, error) {
	tls, _, err := getPersonalTimelineKey(timestampBegin, timestampEnd, userID)
	if err != nil {
		return nil, err
	}
	//按照时间排序并获取真正的Value替换ValueKey
	sort.Sort(tls)
	return MGetValue(tls), nil
}

func push(ts, valueKey, userID string, fans []uint64) {
	for _, fan := range fans {
		value := userID + "," + ts + "," + valueKey
		producer.Input() <- &sarama.ProducerMessage{Topic: FRIENDSTIMELINE, Key: sarama.StringEncoder(strconv.Itoa(int(fan))),
			Value: sarama.StringEncoder(value), Partition: 0}
	}
}

func pushTimeline(ts, valueKey, userID string) {
	//根据push规则(只推送给粉丝列表（有序的）前200的粉丝，超出部分pull)推送，先获取fans列表，然后异步推送（fans未读数过大，则不推送？？？）
	fans := getFriendsInfo(userID, FANS)
	if len(fans) <= PushLimitNum {
		go push(ts, valueKey, userID, fans)
		return
	}
	go push(ts, valueKey, userID, fans[:PushLimitNum])
}

func addPersonalTimeline(userID, ts, valueKey string) {
	value := ts + "," + valueKey
	producer.Input() <- &sarama.ProducerMessage{Topic: ADDPERSONALTIMELINE, Key: sarama.StringEncoder(userID),
		Value: sarama.StringEncoder(value), Partition: 0}
	//粉丝未读数＋1
	go func(userID string) {
		producer.Input() <- &sarama.ProducerMessage{Topic: UNREAD, Key: sarama.StringEncoder("increase"),
			Value: sarama.StringEncoder(userID), Partition: 0}

	}(userID)
	//异步push
	go pushTimeline(ts, valueKey, userID)
}

func delPersonalTimeline(userID, ts, value string) {
	producer.Input() <- &sarama.ProducerMessage{Topic: DELPERSONALTIMELINE, Key: sarama.StringEncoder(userID),
		Value: sarama.StringEncoder(ts), Partition: 0}
	//粉丝未读数－1
	go func(userID string) {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: UNREAD, Key: sarama.StringEncoder("decrease"),
			Value: sarama.StringEncoder(userID), Partition: 0}:
		}
	}(userID)
	//删除真正的value
	DelValue(value)
}

func getPullList(ids []uint64, friendsTimeline Timelines) []uint64 {
	pullList := make([]uint64, 0)
	exist := false
	for _, userID := range ids {
		for _, tl := range friendsTimeline {
			if userID == tl.UserID {
				exist = true
				break
			}
		}
		if !exist {
			pullList = append(pullList, userID)
		}
	}
	return pullList
}

func getMore() {
	//TODO:
	//增加是否获取nextkey的判断和操作,根据时间或者条数
}

//muti get
func pullTimeline(pullList []uint64, pullChan chan Timelines, timestampBegin, timestampEnd string) {
	var v []byte
	personalTimeline := make(Timelines, 0)
	keys := make([]string, 0)
	for _, pull := range pullList {
		key := strconv.Itoa(int(pull)) + timestampBegin + timestampEnd
		keys = append(keys, key)
	}

	rss := storageProxy.GetMulti(storage.SetReadStrategyToContent(context.Background(), storage.CacheOnly), keys...)
	for _, key := range keys {
		if rs, ok := rss[key]; !ok {
			userID := strings.TrimSuffix(key, (timestampBegin + timestampEnd))
			uid, _ := strconv.Atoi(userID)
			tsBegin, _ := strconv.Atoi(timestampBegin)
			tsEnd, _ := strconv.Atoi(timestampEnd)
			personalTimeline = getPersonalTimelineKeyFromDB(uint64(uid), uint64(tsBegin), uint64(tsEnd), key)
		} else {
			if v, ok = rs.Value.([]byte); !ok {
				continue
			}
			json.Unmarshal(v, &personalTimeline)
		}
		pullChan <- personalTimeline
	}
}

func getPullReply(pullChan chan Timelines, pullFriendstimeline Timelines, pullNum, lenPullList int) Timelines {
	for {
		select {
		//主动关闭chan
		case personalTimeline := <-pullChan:
			pullFriendstimeline = append(pullFriendstimeline, personalTimeline...)
			pullNum += 1
			if pullNum >= lenPullList {
				close(pullChan)
				return pullFriendstimeline
			}
		}
	}
}

/*
*首先获取关注列表，确定没有得到push的列表，进行pull
*综合结果，进行排序，提供按照不同属性的排序
*删选出展示的key，去获取真正的内容，并返回
 */
func getFriendsTimeline(timestampBegin, timestampEnd, userID string) (Timelines, error) {
	key := userID + FRIENDS + timestampBegin + timestampEnd
	pullFriendstimeline := make(Timelines, 0)
	pushFriendsTimeline := make(Timelines, 0)
	//获取push到的内容
	rs := storageProxy.Get(storage.SetReadStrategyToContent(context.Background(), storage.CacheMasterOnly), key)
	if rs != nil {
		if v, ok := rs.Value.([]byte); ok {
			json.Unmarshal(v, &pushFriendsTimeline)
		}
	} else {
		uid, err := strconv.Atoi(userID)
		if err != nil {
			return pushFriendsTimeline, err
		}
		tsBegin, err := strconv.Atoi(timestampBegin)
		if err != nil {
			return pushFriendsTimeline, err
		}
		tsEnd, err := strconv.Atoi(timestampEnd)
		if err != nil {
			return pushFriendsTimeline, err
		}
		pushFriendsTimeline, err = getPushFriendsTimelineFromDB(uint64(uid), uint64(tsBegin), uint64(tsEnd), key)

		if err != nil {
			return pushFriendsTimeline, err
		}
	}
	//获取关注列表
	ids := getFriendsInfo(userID, LIKES)
	//确定pull列表,并发拉取数据
	pullList := getPullList(ids, pushFriendsTimeline)
	lenPullList := len(pullList)
	pullChan := make(chan Timelines, lenPullList)
	pullNum := 0
	if lenPullList == 0 {
		goto RESULT
	}
	go pullTimeline(pullList, pullChan, timestampBegin, timestampEnd)
	pullFriendstimeline = getPullReply(pullChan, pullFriendstimeline, pullNum, lenPullList)
RESULT:
	//综合结果，并排序，得到不超过默认数目的动态key数
	timelines := append(pushFriendsTimeline, pullFriendstimeline...)
	sort.Sort(timelines)
	//获取Value，并返回
	return MGetValue(timelines), nil
}

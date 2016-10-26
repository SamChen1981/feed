package mpsrc

import (
	"crypto/md5"
	"encoding/hex"
	"github.com/Shopify/sarama"
	"gitlab.meitu.com/platform/gocommons/storage"
	"golang.org/x/net/context"
	"io"
)

//md5获取value生成的key，并将消息放入队列
func StoreValue(value, userID string) string {
	h := md5.New()
	//添加时间
	io.WriteString(h, userID+value)
	valueKey := hex.EncodeToString(h.Sum(nil))
	producer.Input() <- &sarama.ProducerMessage{Topic: ADDVALUE, Key: sarama.StringEncoder(valueKey),
			Value: sarama.StringEncoder(value), Partition: 0}
	return valueKey
}
//删除value
func DelValue(value string) {
	producer.Input() <- &sarama.ProducerMessage{Topic: DELVALUE, Key: sarama.StringEncoder(value),
			Value: sarama.StringEncoder(""), Partition: 0}
}

//查询key对应的value
func MGetValue(tls Timelines) Timelines {
	valueKeys := make([]string, 0)
	for _, tl := range tls {
		valueKeys = append(valueKeys, tl.ValueKey)
	}
	rss := storageProxy.GetMulti(storage.SetReadStrategyToContent(context.Background(), storage.CacheOnly), valueKeys...)
	for id, _ := range tls {
		rs, ok := rss[tls[id].ValueKey]
		if !ok {
			tls[id].ValueKey = getValueFromDB(tls[id].ValueKey)
		} else {
			tls[id].ValueKey = string(rs.Value.([]byte))
		}
	}
	return tls
}


package mpsrc

import (
	"github.com/Shopify/sarama"
	"strconv"
)

//timeline的属性（获赞数一类的）用redis存储
func handleFansUnread(fans []uint64, opt string) {
	conn := redisPool.GetClient(true)
	if conn == nil {
		return
	}
	defer conn.Close()

	for _, fan := range fans {
		key := strconv.Itoa(int(fan)) + UNREAD
		_, err := conn.Do("WATCH", key)
		if err != nil {
			mpLogger.Error(err, key)
			return
		}
		_, err = conn.Do("MULTI")
		if err != nil {
			mpLogger.Error(err, key)
			return
		}
		_, err = conn.Do(opt, key)
		if err != nil {
			mpLogger.Error(err, key)
			return
		}
		_, err = conn.Do("EXEC")
		if err != nil {
			mpLogger.Error(err, key)
			return
		}
	}
}

//通知所有fans列表里的对象,更新未读数
func handleDataChange(cm *sarama.ConsumerMessage) {
	key := string(cm.Key)
	value := string(cm.Value)
	if key != "increase" && key != "decrease" {
		return
	}
	fans := getFriendsInfo(value, FANS)
	if key == "increase" {
		go handleFansUnread(fans, "INCR")
	} else {
		go handleFansUnread(fans, "DECR")
	}
}

func watchDataChange() {
	consumer, err := sarama.NewConsumer([]string{config.Kafka.Addr}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()

	unReadPartitionConsumer, err := consumer.ConsumePartition(UNREAD, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		return
	}

	defer func() {
		if err := unReadPartitionConsumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()
PartitionConsumerLoop:
	for {
		select {
		case cm := <-unReadPartitionConsumer.Messages():
			handleDataChange(cm)
		case <-Stop:
			break PartitionConsumerLoop
		}
	}
}

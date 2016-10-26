/*
* 鉴于kafka server配置，队列消费的实现有冗余，此外在消费方可以通过
* 定时调用更新db的方法，来进行批量写入，优化写入速度的case
*/
package mpsrc

import (
	"github.com/Shopify/sarama"
	"log"
	"time"
	"strconv"
	"strings"
)

var (
	Stop      chan bool = make(chan bool, 7)
	producer  sarama.AsyncProducer
	// consumer  sarama.Consumer
	// err       error
)

//生产者
func runProducer() {
	producer, err = sarama.NewAsyncProducer([]string{config.Kafka.ProAddr}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err = producer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()

ProducerLoop:
	for {
		select {
		case <-Stop:
			break ProducerLoop
		}
	}

	log.Println("Closed")
}

//通知consumer正常关闭
func stopKafka() {
	Stop <- true
	Stop <- true
	Stop <- true
	Stop <- true
	Stop <- true
	Stop <- true
	Stop <- true
	time.Sleep(time.Millisecond * 100)
}

//粉丝列表的变更消费
func updateFansOfDB() {
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

	fansAddPartitionConsumer, err := consumer.ConsumePartition(ADDFANS, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		return
	}

	defer func() {
		if err := fansAddPartitionConsumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()
	fansDelPartitionConsumer, err := consumer.ConsumePartition(DELFANS, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		return
	}

	defer func() {
		if err := fansDelPartitionConsumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()
FansPartitionConsumerLoop:
	for {
		select {
		case cm := <-fansAddPartitionConsumer.Messages():
			userID, err := strconv.Atoi(string(cm.Key))
			if err != nil {
				continue
			}
			fanID, err := strconv.Atoi(string(cm.Value))
			if err != nil {
				continue
			}
			updateFriendsInfoOfDB("fid", "fanslist", "add", uint64(userID), uint64(fanID))
		case cm := <-fansDelPartitionConsumer.Messages():
			userID, err := strconv.Atoi(string(cm.Key))
			if err != nil {
				continue
			}
			fanID, err := strconv.Atoi(string(cm.Value))
			if err != nil {
				continue
			}
			updateFriendsInfoOfDB("fid", "fanslist", "delete", uint64(userID), uint64(fanID))
		case <-Stop:
			break FansPartitionConsumerLoop
		}
	}
}

//关注关系的变更消费
func updateLikesOfDB() {
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

	LikesAddPartitionConsumer, err := consumer.ConsumePartition(ADDLIKES, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		return
	}

	defer func() {
		if err := LikesAddPartitionConsumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()
	LikesDelPartitionConsumer, err := consumer.ConsumePartition(DELLIKES, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		return
	}

	defer func() {
		if err := LikesDelPartitionConsumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()
LikesPartitionConsumerLoop:
	for {
		select {
		case cm := <-LikesAddPartitionConsumer.Messages():
			userID, err := strconv.Atoi(string(cm.Key))
			if err != nil {
				continue
			}
			likeID, err := strconv.Atoi(string(cm.Value))
			if err != nil {
				continue
			}
			updateFriendsInfoOfDB("lid", "likeslist", "add", uint64(userID), uint64(likeID))
		case cm := <-LikesDelPartitionConsumer.Messages():
			userID, err := strconv.Atoi(string(cm.Key))
			if err != nil {
				continue
			}
			likeID, err := strconv.Atoi(string(cm.Value))
			if err != nil {
				continue
			}
			updateFriendsInfoOfDB("lid", "likeslist", "delete", uint64(userID), uint64(likeID))
		case <-Stop:
			break LikesPartitionConsumerLoop
		}
	}
}

//push动态的消费
func updatePushFriendsTimelineOfDB() {
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

	pushPartitionConsumer, err := consumer.ConsumePartition(FRIENDSTIMELINE, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		return
	}

	defer func() {
		if err := pushPartitionConsumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()
PushPartitionConsumerLoop:
	for {
		select {
		case cm := <-pushPartitionConsumer.Messages():
			value := strings.Split(string(cm.Value), ",")
			userID, err := strconv.Atoi(string(cm.Key))
			if err != nil {
				continue
			}
			likesID, err := strconv.Atoi(value[0])
			if err != nil {
				continue
			}
			ts, err := strconv.Atoi(value[1])
			if err != nil {
				continue
			}
			
			addPushFriendsTimeline(uint64(userID), uint64(likesID), uint64(ts), value[2])
		case <-Stop:
			break PushPartitionConsumerLoop
		}
	}
}

//个人动态的消费
func updatePersonalTimelineOfDB() {
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

	AddPersonalPartitionConsumer, err := consumer.ConsumePartition(ADDPERSONALTIMELINE, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		return
	}

	defer func() {
		if err := AddPersonalPartitionConsumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()
	DelPersonalPartitionConsumer, err := consumer.ConsumePartition(DELPERSONALTIMELINE, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		return
	}

	defer func() {
		if err := DelPersonalPartitionConsumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()
PerPartitionConsumerLoop:
	for {
		select {
		case cm := <-AddPersonalPartitionConsumer.Messages():
			value := strings.Split(string(cm.Value), ",")
			uid, err := strconv.Atoi(string(cm.Key))
			if err != nil {
				continue
			}
			ts, err := strconv.Atoi(value[0])
			if err != nil {
				continue
			}
			updatePersonalTimeline(uint64(uid), uint64(ts), value[1], "add")
		case cm := <-DelPersonalPartitionConsumer.Messages():
			uid, err := strconv.Atoi(string(cm.Key))
			if err != nil {
				continue
			}
			ts, err := strconv.Atoi(string(cm.Value))
			if err != nil {
				continue
			}
			updatePersonalTimeline(uint64(uid), uint64(ts), "", "delete")
		case <-Stop:
			break PerPartitionConsumerLoop
		}
	}
}

//value增删的消费
func updateValueOfDB() {
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

	AddValueConsumer, err := consumer.ConsumePartition(ADDVALUE, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		return
	}

	defer func() {
		if err := AddValueConsumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()
	DelValueConsumer, err := consumer.ConsumePartition(DELVALUE, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		return
	}

	defer func() {
		if err := DelValueConsumer.Close(); err != nil {
			mpLogger.Error(err)
			return
		}
	}()
ValuePartitionConsumerLoop:
	for {
		select {
		case cm := <-AddValueConsumer.Messages():
			addValueToDB(string(cm.Key), string(cm.Value))
		case cm := <-DelValueConsumer.Messages():
			delValueFromDB(string(cm.Key))
		case <-Stop:
			break ValuePartitionConsumerLoop
		}
	}
}

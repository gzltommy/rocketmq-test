// 消费者消费消息
package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
	c, err := rocketmq.NewPushConsumer(
		// 指定 Group 可以实现消费者负载均衡进行消费，并且保证他们的Topic+Tag要一样。
		// 如果同一个 GroupID 下的不同消费者实例，订阅了不同的 Topic+Tag 将导致在对Topic 的消费队列进行负载均衡的时候产生不正确的结果，最终导致消息丢失。(官方源码设计)
		consumer.WithGroupName("testGroup"),
		consumer.WithNameServer([]string{"127.0.0.1:9876"}),
	)
	if err != nil {
		panic(err)
	}
	err = c.Subscribe("test", consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			fmt.Printf("subscribe callback: %+v \n", msg)
		}
		// 消费成功，进行ack确认
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		panic(err)
	}
	err = c.Start()
	if err != nil {
		panic(err)
	}
	defer func() {
		err = c.Shutdown()
		if err != nil {

			fmt.Printf("shutdown Consumer error: %s", err.Error())
		}
	}()
	<-(chan interface{})(nil)
}

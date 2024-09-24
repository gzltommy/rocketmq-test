// 生产者发送延迟消息
package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"os"
)

func main() {
	p, err := rocketmq.NewProducer(
		producer.WithNameServer([]string{"127.0.0.1:9876"}),
	)
	if err != nil {
		panic(err)
	}
	err = p.Start()
	defer func() {
		err = p.Shutdown()
		if err != nil {
			fmt.Printf("shutdown producer error: %s", err.Error())
		}
	}()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}
	msg := &primitive.Message{
		Topic: "test",
		Body:  []byte("Hello RocketMQ Go Client!"),
	}
	//  1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
	msg.WithDelayTimeLevel(3) // 设置10s后，消息才能被消费
	res, err := p.SendSync(context.Background(), msg)
	if err != nil {
		panic(err)
	}
	fmt.Println("发送成功,result:", res.String())
}

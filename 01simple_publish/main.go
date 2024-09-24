// 生产者发送普通消息
// 注意：如果程序有多个生产者消费者，千万不能使用
// Producer.Shutdown()
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
	res, err := p.SendSync(context.Background(), msg)
	if err != nil {
		panic(err)
	}
	fmt.Println("发送成功,result:", res.String())
}

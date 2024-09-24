// 生产者发送事务消息
package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"os"
	"time"
)

type Listener struct{}

func (l *Listener) ExecuteLocalTransaction(message *primitive.Message) primitive.LocalTransactionState {
	fmt.Println("开始执行本地业务逻辑入库")
	time.Sleep(5 * time.Second)
	fmt.Println("本地业务逻辑入库成功")
	// primitive.CommitMessageState 通知 rocketmq 正常提交进topic，不会执行 CheckLocalTransaction
	// primitive.RollbackMessageState 通知 rocketmq 失败，消息丢弃，不会执行 CheckLocalTransaction
	// primitive.UnknowState 通知 rocketmq 异常，让 rocketmq 执行 CheckLocalTransaction
	// 这里如果能执行到，返回，那么 SendMessageInTransaction 时都会认为成功
	return primitive.UnknowState
}

// 如果宕机，ExecuteLocalTransaction 还没来得及提交消息状态 ，重启启动程序后，rocketmq还是会主动回调 CheckLocalTransaction
// 确认half消息的最终状态
func (l *Listener) CheckLocalTransaction(ext *primitive.MessageExt) primitive.LocalTransactionState {
	fmt.Println("收到Rocketmq主动请求信息,msgID:", ext.MsgId)
	// primitive.CommitMessageState 通知 rocketmq 正常提交进topic
	// primitive.RollbackMessageState 通知 rocketmq 失败，消息丢弃
	return primitive.CommitMessageState
}

func main() {
	l := Listener{}
	p, err := rocketmq.NewTransactionProducer(
		&l,
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
		Topic: "transTopic",
		Body:  []byte("Hello RocketMQ Go Client!"),
	}
	// 1. SendMessageInTransaction 阻塞，然后执行 ExecuteLocalTransaction
	// 2. ExecuteLocalTransaction 执行结束后 SendMessageInTransaction 解除阻塞
	res, err := p.SendMessageInTransaction(context.Background(), msg)
	if err != nil {
		panic(err)
	}
	fmt.Printf("发送成功,data:%s,msgID:=%s\n", res.String(), res.MsgID)
	<-(chan interface{})(nil)
}

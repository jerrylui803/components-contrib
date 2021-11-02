package mqlite

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/asaskevich/EventBus"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"

	"fmt"

	pb "github.com/dapr/components-contrib/pubsub/mq-lite/helloworld"
	"google.golang.org/grpc"
)

const (
	//address     = "localhost:8080"
	address = "localhost:9000"
)

type bus struct {
	bus    EventBus.Bus
	ctx    context.Context
	log    logger.Logger
	client pb.MessageBusClient
}

func New(logger logger.Logger) pubsub.PubSub {
	return &bus{
		log: logger,
	}
}

func (a *bus) Close() error {
	return nil
}

func (a *bus) Features() []pubsub.Feature {
	return nil
}

func (a *bus) Init(metadata pubsub.Metadata) error {
	a.bus = EventBus.New()
	a.ctx = context.Background()

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	//defer conn.Close()
	a.client = pb.NewMessageBusClient(conn)
	println("FINISHED INIT")
	println("FINISHED INIT")
	println("FINISHED INIT")

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()

	return nil
}

func (a *bus) Publish(req *pubsub.PublishRequest) error {

	println("my gRPC publish()")
	println("my gRPC publish()")
	println("my gRPC publish()")
	println(req.Topic)
	println(req.Data)
	a.bus.Publish(req.Topic, a.ctx, req.Data)

	dataMap := map[string]string{}
	json.Unmarshal(req.Data, &dataMap)
	fmt.Println("Printing DataMap:\n%v", dataMap)

	myTopic := req.Topic
	myString := string(req.Data)
	//myString := dataMap
	println("This is myString in publish")
	println(myString)
	println("This is dataMap['data'] in publish")
	//println(dataMap["data"])

	response, err := a.client.Publish(a.ctx,
		&pb.LamtaEventInfo{Source: "mySource", Priority: 123, Subject: myTopic,
			Data: &pb.LamtaGrpcData{Kind: pb.LamtaGrpcDataType_PROTOBUF_V3, Schema: myString}, Dapr: []byte(dataMap["data"])})

	if err != nil {
		fmt.Printf("could not publish222: %v\n", err)
	}

	fmt.Printf("Response of Publish222():\n")
	println(response)

	return nil
}

func (a *bus) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	println("my gRPC subscribe()")
	println("my gRPC subscribe()")
	println("my gRPC subscribe()")

	go func() {
		myTopic := req.Topic

		stream, err := a.client.Subscribe(context.TODO())
		if err != nil {
			fmt.Printf("rpc error1: %v\n", err)
		}

		myTopics := []string{(myTopic)}
		stream.Send(&pb.LamtaEventSubscribeRequest{
			Subjects: myTopics,
		})

		for i := 0; i < 99; i++ {
			feature, err := stream.Recv()
			//println(fmt.Sprintf("%#v", feature))
			commits := map[string]string{
			    "testing": "if python receives",
			}

			if err := handler(a.ctx, &pubsub.NewMessage{Data: []byte{123, 34, 105, 100, 34, 58, 34, 98, 51, 48, 52, 98, 100, 53, 100, 45, 100, 100, 98, 54, 45, 52, 50, 55, 100, 45, 98, 56, 100, 56, 45, 97, 57, 97, 99, 53, 55, 51, 53, 99, 102, 101, 49, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 99, 111, 109, 46, 100, 97, 112, 114, 46, 101, 118, 101, 110, 116, 46, 115, 101, 110, 116, 34, 44, 34, 112, 117, 98, 115, 117, 98, 110, 97, 109, 101, 34, 58, 34, 109, 101, 115, 115, 97, 103, 101, 115, 34, 44, 34, 116, 114, 97, 99, 101, 105, 100, 34, 58, 34, 48, 48, 45, 99, 52, 102, 56, 51, 56, 52, 101, 99, 97, 51, 57, 99, 52, 51, 97, 54, 49, 53, 52, 55, 51, 53, 102, 98, 56, 50, 101, 99, 98, 98, 51, 45, 54, 54, 52, 51, 55, 102, 98, 53, 98, 100, 55, 99, 56, 52, 56, 53, 45, 48, 49, 34, 44, 34, 100, 97, 116, 97, 34, 58, 34, 112, 105, 110, 103, 34, 44, 34, 115, 112, 101, 99, 118, 101, 114, 115, 105, 111, 110, 34, 58, 34, 49, 46, 48, 34, 44, 34, 100, 97, 116, 97, 99, 111, 110, 116, 101, 110, 116, 116, 121, 112, 101, 34, 58, 34, 116, 101, 120, 116, 47, 112, 108, 97, 105, 110, 34, 44, 34, 115, 111, 117, 114, 99, 101, 34, 58, 34, 112, 117, 98, 34, 44, 34, 116, 111, 112, 105, 99, 34, 58, 34, 110, 101, 119, 111, 114, 100, 101, 114, 34, 125}, Topic: req.Topic, 
				
				//Metadata: req.Metadata}); err != nil {
				Metadata: commits}); err != nil {
				a.log.Error("jl logging in handler" + err.Error())
				continue
			}

			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("rpc error2: %v\n", err)
			}
			log.Println(feature)
			log.Println("This is meta data")
			log.Printf("%v\n", req.Metadata)
			log.Printf("\n\n%v\n", req)
		}
		fmt.Printf("Have received 3 messages, now client will close connection\n")

		stream.Send(&pb.LamtaEventSubscribeRequest{
			Disconnect: true,
		})

		// Wait a bit to finish sending disconnect request
		time.Sleep(time.Second)
		return

	}()

	return nil

}

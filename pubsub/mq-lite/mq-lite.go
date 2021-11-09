package mqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"

	pb "github.com/dapr/components-contrib/pubsub/mq-lite/helloworld"
	"google.golang.org/grpc"
)

type bus struct {
	ctx    context.Context
	log    logger.Logger
	client pb.MessageBusClient

	address   string
	sizeLimit int
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

	if val, ok := metadata.Properties["mqliteHost"]; ok && val != "" {
		a.address = val
		log.Printf("mqliteHost is set to: " + a.address)
	} else {
		log.Printf("mqliteHost is unspecified. Using: " + a.address)
	}

	if val, ok := metadata.Properties["sizeLimit"]; ok && val != "" {
		sizeLimit, err := strconv.Atoi(val)
		if err != nil {
			sizeLimit = -1
			log.Printf("Error when reading sizeLimit")
			return err
		}
		a.sizeLimit = sizeLimit
		log.Printf("sizeLimit is set to: " + a.address)
	} else {
		a.sizeLimit = -1
	}

	a.ctx = context.Background()
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()

	// Set up a connection to the server.
	conn, err := grpc.Dial(a.address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	a.client = pb.NewMessageBusClient(conn)

	return nil
}

type DaprReq struct {
	Data DaprReqData `json:"data"`
}

type DaprReqData struct {
	Priority *int `json:"priority"`
}

func (a *bus) Publish(req *pubsub.PublishRequest) error {

	//log.Printf("\nmy mq-lite gRPC publish()\n")

	myTopic := req.Topic

	if a.sizeLimit != -1 && len(req.Data) > a.sizeLimit {
		errStr := "Cannot publish. Data size " + strconv.Itoa(len(req.Data)) + " exceeds size limit of " + strconv.Itoa(a.sizeLimit)
		log.Println(errStr)
		return fmt.Errorf(errStr)
	}

	var priority int
	var daprReq DaprReq
	err := json.Unmarshal(req.Data, &daprReq)
	if err != nil {
		log.Println(err)
		return err
	}
	if daprReq.Data.Priority != nil {
		priority = *(daprReq.Data.Priority)
	} else {
		priority = -1
	}

	response, err := a.client.Publish(a.ctx,
		&pb.LamtaEventInfo{Source: "this_is_dapr_source", Priority: int32(priority), Subject: myTopic,
			Data: &pb.LamtaGrpcData{Kind: pb.LamtaGrpcDataType_PROTOBUF_V3, Schema: "this_is_a_schema", Data: []byte(req.Data)}})

	if err != nil {
		log.Println(err)
		return err
	}

	if response.Code != 123 {
		log.Printf("Response of Publish():\n")
		log.Printf("%+v\n", response)
	}

	return nil
}

func (a *bus) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {

	// log.Printf("\nmy mq-lite gRPC subscribe()\n")

	go func() {
		myTopic := req.Topic

		stream, err := a.client.Subscribe(context.TODO())
		if err != nil {
			log.Printf("rpc error: %v\n", err)
		}

		myTopics := []string{(myTopic)}

		stream.Send(&pb.LamtaEventSubscribeRequest{
			Subjects: myTopics,
		})

		for {
			lamtaGrpcEventResponse, err := stream.Recv()

			if err := handler(a.ctx, &pubsub.NewMessage{Data: []byte(lamtaGrpcEventResponse.Event.Basic.Data.Data), Topic: req.Topic, Metadata: req.Metadata}); err != nil {
				continue
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("rpc error: %v\n", err)
			}
		}

		// send disconnect request

		// stream.Send(&pb.LamtaEventSubscribeRequest{
		// 	Disconnect: true,
		// })

		// // Wait a bit to finish sending disconnect request
		// time.Sleep(time.Second)

	}()
	return nil
}

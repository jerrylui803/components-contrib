package mqlite

import (
	"context"
	"io"
	"log"

	"github.com/asaskevich/EventBus"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"

	pb "github.com/dapr/components-contrib/pubsub/mq-lite/helloworld"
	"google.golang.org/grpc"
)

const (
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
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	a.client = pb.NewMessageBusClient(conn)
	//log.Printf("Init(), printing metadata:\n %+v\n", metadata)

	return nil
}

func (a *bus) Publish(req *pubsub.PublishRequest) error {

	//log.Printf("\nmy mq-lite gRPC publish()\n")

	a.bus.Publish(req.Topic, a.ctx, req.Data)

	myTopic := req.Topic

	response, err := a.client.Publish(a.ctx,
		&pb.LamtaEventInfo{Source: "this_is_dapr_source", Priority: 123, Subject: myTopic,
			Data: &pb.LamtaGrpcData{Kind: pb.LamtaGrpcDataType_PROTOBUF_V3, Schema: "this_is_a_schema", Data: []byte(req.Data)}})

	if err != nil {
		log.Printf("could not publish: %v\n", err)
	}

	if (response.Code != 123) {
		log.Printf("Response of Publish():\n")
		log.Printf("%+v\n", response)
	}

	// log.Printf("Response of Publish():\n")
	// log.Printf("%+v\n", response)

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

			// log.Println("component mq-lite: Received from grpc server:")
			// log.Printf("%+v\n\n", lamtaGrpcEventResponse)
			// log.Printf("%+v\n", lamtaGrpcEventResponse.Event.Basic.Data.Data)

			if err := handler(a.ctx, &pubsub.NewMessage{Data: []byte(lamtaGrpcEventResponse.Event.Basic.Data.Data), Topic: req.Topic, Metadata: req.Metadata}); err != nil {
				continue
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("rpc error: %v\n", err)
			}
			// log.Println("This is meta data")
			// log.Printf("%v\n", req.Metadata)
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

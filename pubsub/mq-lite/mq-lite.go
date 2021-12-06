package mqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"

	pb "github.com/dapr/components-contrib/pubsub/mq-lite/helloworld"
	"google.golang.org/grpc"
)

type bus struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	log       logger.Logger
	client    pb.MessageBusClient

	address   string
	sizeLimit int

	reconnectMutex sync.RWMutex
	reconnectTime  int
}

func New(logger logger.Logger) pubsub.PubSub {
	return &bus{
		log:           logger,
		reconnectTime: 0,
		ctxCancel:     nil,
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

	a.reconnectMutex.Lock()
	defer a.reconnectMutex.Unlock()
	err := a.initGrpcClientWithRetry()
	if err != nil {
		log.Printf("failed to connect gRPC. No more retry")
		//log.Fatal("failed to connect gRPC. No more retry")
		return err
	}

	return nil
}

// steps to reconnect
// - lock reconnectMutex
// - try the previously failing operation again
// - only if it fails, call initGrpcClientWithRetry
// (in case some other thread has already fixed the connection while being blocked by reconnectMutex)
func (a *bus) initGrpcClientWithRetry() error {
	reconnectionInterval := 10
	if time.Now().Unix()-int64(a.reconnectTime) < int64(reconnectionInterval) {
		errStr := "error:  gRPC connection failed. too many reconnection! the last reconnection was " + (strconv.Itoa(reconnectionInterval)) + "seconds ago " +
			" exiting."

		log.Println(errStr)
		//log.Fatal(errStr)
		return errors.New(errStr)
	}

	// set reconnectTime when exiting
	reconnectTimeFunc := func() {
		a.reconnectTime = int(time.Now().Unix())
	}
	defer reconnectTimeFunc()

	// cancel previous context, if this is not the first time dailing gRPC
	if a.ctxCancel != nil {
		a.ctxCancel()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	a.ctx = ctx
	a.ctxCancel = cancel

	conn, err := grpc.DialContext(ctx, a.address, grpc.WithInsecure(), grpc.WithBlock())

	retries := 3

	i := 0
	for i < retries && err != nil {
		a.ctxCancel()

		log.Printf("failed to reconnect: %v\n", err)
		log.Printf("attempting to reconnect...")
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		a.ctx = ctx
		a.ctxCancel = cancel
		conn, err = grpc.DialContext(ctx, a.address, grpc.WithInsecure(), grpc.WithBlock())

		// wait a bit until next attempt to reconnect
		time.Sleep(3 * time.Second)
		i += 1
	}
	if err != nil {
		log.Printf("failed to reconnect")
		a.ctxCancel()
		return err
	}

	a.client = pb.NewMessageBusClient(conn)
	log.Printf("gRPC connection successful")

	return nil
}

type DaprReq struct {
	Data DaprReqData `json:"data"`
}

type DaprReqData struct {
	Priority *int `json:"priority"`
}

func (a *bus) Publish(req *pubsub.PublishRequest) error {
	// TODO: this function might be called repeatedly as long as publishing message. There is no `no more retry connection` since
	// this function will be called repectedly even if I return an error
	// where as subscribe returns error in an goroutine (and get stuck)
	// should fail fatally?

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

	payload := &pb.LamtaEventInfo{Source: "this_is_dapr_source", Priority: int32(priority), Subject: myTopic,
		Data: &pb.LamtaGrpcData{Kind: pb.LamtaGrpcDataType_PROTOBUF_V3, Schema: "this_is_a_schema", Data: []byte(req.Data)}}

	response, err := a.client.Publish(context.TODO(), payload)

	if err != nil {
		a.reconnectMutex.Lock()
		defer a.reconnectMutex.Unlock()
		// try again, maybe someone else reconnected already
		response, err = a.client.Publish(context.TODO(), payload)
		if err != nil {
			err = a.initGrpcClientWithRetry()
			if err != nil {
				log.Printf("failed to connect gRPC. No more retry")
				return err
			}
			response, err = a.client.Publish(context.TODO(), payload)

			// failed after reconnect
			if err != nil {
				log.Println(err)
				return err
			}
		}
	}

	if response.Code != 123 {
		log.Printf("Response of Publish():\n")
		log.Printf("%+v\n", response)
	}

	return nil
}

func (a *bus) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {

	go func() {
		myTopic := req.Topic

		//stream, err := a.client.Subscribe(context.TODO())
		stream, err := a.client.Subscribe(context.TODO())
		if err != nil {
			a.reconnectMutex.Lock()
			// try again, maybe someone else reconnected already
			stream, err = a.client.Subscribe(context.TODO())
			if err != nil {
				err = a.initGrpcClientWithRetry()
				if err != nil {
					log.Printf("failed to connect gRPC. No more retry")
					a.reconnectMutex.Unlock()
					return
				}
				stream, err = a.client.Subscribe(context.TODO())
				if err != nil {
					log.Println(err)
					a.reconnectMutex.Unlock()
					return
				}
			}
			a.reconnectMutex.Unlock()
		}

		myTopics := []string{(myTopic)}

		err = stream.Send(&pb.LamtaEventSubscribeRequest{
			Subjects: myTopics,
		})

		if err != nil {
			a.reconnectMutex.Lock()
			// try again, maybe someone else reconnected already
			stream, err = a.client.Subscribe(context.TODO())
			if err == nil {
				err = stream.Send(&pb.LamtaEventSubscribeRequest{
					Subjects: myTopics,
				})
			}
			if err != nil {
				err = a.initGrpcClientWithRetry()
				if err != nil {
					log.Printf("failed to connect. No more reconnect")
					log.Println(err)
					a.reconnectMutex.Unlock()
					return
				}
				// ignore error, let it fail later
				stream, _ = a.client.Subscribe(context.TODO())
				err = stream.Send(&pb.LamtaEventSubscribeRequest{
					Subjects: myTopics,
				})
				if err != nil {
					log.Println(err)
					a.reconnectMutex.Unlock()
					return
				}
			}

		}

		for {
			lamtaGrpcEventResponse, err := stream.Recv()

			if err != nil {
				a.reconnectMutex.Lock()
				// try again, maybe someone else reconnected already
				stream, err = a.client.Subscribe(context.TODO())
				if err == nil {
					err = stream.Send(&pb.LamtaEventSubscribeRequest{
						Subjects: myTopics,
					})
					if err == nil {
						lamtaGrpcEventResponse, err = stream.Recv()
					}
				}
				if err != nil {

					err = a.initGrpcClientWithRetry()
					a.reconnectMutex.Unlock()
					if err != nil {
						log.Printf("failed to connect. No more reconnect")
						log.Println(err)
						return
					} else {
						// reconnect successful, now set up the subscription again
						// ,and run the loop again
						stream, err = a.client.Subscribe(context.TODO())
						if err == nil {
							err = stream.Send(&pb.LamtaEventSubscribeRequest{
								Subjects: myTopics,
							})
							if err != nil {
								log.Printf("failed to connect. No more reconnect")
								log.Println(err)
								return

							}
						}
						continue
					}
				}
			}

			err = handler(context.TODO(), &pubsub.NewMessage{Data: []byte(lamtaGrpcEventResponse.Event.Basic.Data.Data), Topic: req.Topic, Metadata: req.Metadata})

			if err == io.EOF {
				//break
				log.Println("Received EOF, skipping")
				continue
			}
			if err != nil {
				log.Println("error running handler on payload. payload below:")
				log.Println([]byte(lamtaGrpcEventResponse.Event.Basic.Data.Data))
				log.Println(err)
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

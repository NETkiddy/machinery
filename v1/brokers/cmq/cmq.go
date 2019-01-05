package cmq

import (
	"sync"
	"errors"
	"time"
	"fmt"
	"encoding/json"
	"strings"

	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/NETkiddy/cmq-go"
)

const (
	maxCMQDelay = time.Minute * 60 // Max supported CMQ delay is 60 min
)

// Broker represents an Tencent CMQ broker
type Broker struct {
	common.Broker
	account           *cmq_go.Account
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	stopReceivingChan chan int
}

// New creates new Broker instance
func New(cnf *config.Config, endpointQueue, secretId, secretKey string) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	if cnf.CMQ != nil {
		b.account = cnf.CMQ.Client
	} else {
		b.account = cmq_go.NewAccount(endpointQueue, secretId, secretKey)
	}

	return b
}

func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)
	queueName := b.GetConfig().DefaultQueue
	deliveries := make(chan *cmq_go.Message)

	b.stopReceivingChan = make(chan int)
	b.receivingWG.Add(1)

	go func() {
		defer b.receivingWG.Done()

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopReceivingChan:
				return
			default:
				output, err := b.receiveMessage(queueName)
				if err != nil {
					log.ERROR.Printf("Queue consume error: %s", err)
					continue
				}
				if output == nil || len(output.MsgBody) == 0 {
					continue
				}

				deliveries <- output
			}

			whetherContinue, err := b.continueReceivingMessages(queueName, deliveries)
			if err != nil {
				log.ERROR.Printf("Error when receiving messages. Error: %v", err)
			}
			if whetherContinue == false {
				return
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	return b.GetRetry(), nil
}

func (b *Broker) StopConsuming() {

}

func (b *Broker) Publish(signature *tasks.Signature) error {
	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check that signature.RoutingKey is set, if not switch to DefaultQueue
	b.AdjustRoutingKey(signature)

	// get queue by queuename
	queue := b.account.GetQueue(signature.RoutingKey)

	// Check the ETA signature field, if it is set and it is in the future,
	// set a delay in seconds for the task.
	if signature.ETA != nil {
		now := time.Now().UTC()
		delay := signature.ETA.Sub(now)
		if delay > 0 {
			if delay > maxCMQDelay {
				return errors.New("Max Tencent CMQ delay exceeded")
			}
			msgId, err, _ := queue.SendDelayMessage(string(msg), int(delay.Seconds()))
			if err != nil {
				log.ERROR.Printf("Error when sending a message: %v", err)
				return err
			}
			log.INFO.Printf("Sending a message successfully, the messageId is %v", msgId)
		}
	}

	msgId, err, _ := queue.SendMessage(string(msg))
	if err != nil {
		log.ERROR.Printf("Error when sending a message: %v", err)
		return err
	}
	log.INFO.Printf("Sending a message successfully, the messageId is %v", msgId)
	return nil

}

// receiveMessage is a method receives a message from specified queue
func (b *Broker) receiveMessage(queueName string) (msg *cmq_go.Message, err error) {
	var waitTimeSeconds int
	if b.GetConfig().SQS != nil {
		waitTimeSeconds = b.GetConfig().CMQ.WaitTimeSeconds
	} else {
		waitTimeSeconds = 0
	}
	queue := b.account.GetQueue(queueName)

	// wait for msg
	for {
		var code int
		*msg, err, code = queue.ReceiveMessage(waitTimeSeconds)
		if err != nil {
			if code == 7000 {
				log.DEBUG.Printf("no message")
				time.Sleep(1)
				continue
			} else if code == 6070 {
				log.WARNING.Printf("too many unacked(inactive messages or delayed messages), wait and retry")
				time.Sleep(1)
				continue
			} else {
				return
			}
		}
	}

	log.DEBUG.Printf("ReceiveMessage msgId: %v", msg.MsgId)
	return
}

// continueReceivingMessages is a method returns a continue signal
func (b *Broker) continueReceivingMessages(queueName string, deliveries chan *cmq_go.Message) (bool, error) {
	select {
	// A way to stop this goroutine from b.StopConsuming
	case <-b.stopReceivingChan:
		return false, nil
	default:
		output, err := b.receiveMessage(queueName)
		if err != nil {
			return true, err
		}
		if output == nil || len(output.MsgBody) == 0 {
			return true, nil
		}
		go func() { deliveries <- output }()
	}
	return true, nil
}

// consume is a method which keeps consuming deliveries from a channel, until there is an error or a stop signal
func (b *Broker) consume(deliveries <-chan *cmq_go.Message, concurrency int, taskProcessor iface.TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		b.initializePool(pool, concurrency)
	}()

	errorsChan := make(chan error)

	for {
		whetherContinue, err := b.consumeDeliveries(deliveries, concurrency, taskProcessor, pool, errorsChan)
		if err != nil {
			return err
		}
		if whetherContinue == false {
			return nil
		}
	}
}

// initializePool is a method which initializes concurrency pool
func (b *Broker) initializePool(pool chan struct{}, concurrency int) {
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}
}

// consumeDeliveries is a method consuming deliveries from deliveries channel
func (b *Broker) consumeDeliveries(deliveries <-chan *cmq_go.Message, concurrency int, taskProcessor iface.TaskProcessor, pool chan struct{}, errorsChan chan error) (bool, error) {
	select {
	case err := <-errorsChan:
		return false, err
	case d := <-deliveries:
		if concurrency > 0 {
			// get worker from pool (blocks until one is available)
			<-pool
		}

		b.processingWG.Add(1)

		// Consume the task inside a goroutine so multiple tasks
		// can be processed concurrently
		go func() {

			if err := b.consumeOne(d, taskProcessor); err != nil {
				errorsChan <- err
			}

			b.processingWG.Done()

			if concurrency > 0 {
				// give worker back to pool
				pool <- struct{}{}
			}
		}()
	case <-b.GetStopChan():
		return false, nil
	}
	return true, nil
}

// consumeOne is a method consumes a delivery.
// If a delivery was consumed successfully, it will be deleted from Tencent CMQ
func (b *Broker) consumeOne(delivery *cmq_go.Message, taskProcessor iface.TaskProcessor) error {
	defer func() {

	}()
	if len(delivery.MsgBody) == 0 {
		log.ERROR.Printf("received an empty message, the delivery was %v", delivery)
		return errors.New("received empty message, the delivery msgId is " + delivery.MsgId)
	}

	sig := new(tasks.Signature)
	decoder := json.NewDecoder(strings.NewReader(delivery.MsgBody))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		log.ERROR.Printf("unmarshal error. the delivery is %v", delivery)
		return err
	}

	// If the task is not registered return an error
	// and leave the message in the queue
	if !b.IsTaskRegistered(sig.Name) {
		return fmt.Errorf("task %s is not registered", sig.Name)
	}

	err := taskProcessor.Process(sig)
	if err != nil {
		return err
	}
	// Delete message after successfully consuming and processing the message
	if err = b.deleteOne(delivery); err != nil {
		log.ERROR.Printf("error when deleting the delivery. the delivery is %v", delivery)
	}
	return err
}

// deleteOne is a method delete a delivery from Tencent CMQ
func (b *Broker) deleteOne(delivery *cmq_go.Message) error {
	queueName := b.GetConfig().DefaultQueue

	err, _ := b.account.GetQueue(queueName).DeleteMessage(delivery.ReceiptHandle)
	if err != nil {
		return err
	}
	return nil
}

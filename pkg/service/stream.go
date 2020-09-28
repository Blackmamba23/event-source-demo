package service

import (
	"encoding/json"
	"event-source-demo/pkg/model"

	"github.com/NYTimes/gizmo/pubsub/kafka"
	"github.com/NYTimes/gizmo/server"
	Log "github.com/sirupsen/logrus"
)

var (
	// upgrader = websocket.Upgrader{
	// 	ReadBufferSize:  1024,
	// 	WriteBufferSize: 1024,
	// 	CheckOrigin: func(r *http.Request) bool {
	// 		return true
	// 	},
	// }
	employeeOffsetValue = 0
)

func zeroOffset() int64          { return 0 }
func discardOffset(offset int64) {}
func broadcastEmployeeOffset(offset int64) {
	employeeOffsetValue = int(offset)
}

// ConsumeEmployeeTopic will init a new pubsub.Subscriber
// Any messages consumed from Kafka will be published to the badger DB cache.
func (s *StreamService) ConsumeEmployeeTopic() {
	cfg := *s.cfg
	cfg.Topic = "stream-employee"
	Log.WithField("topic", cfg.Topic).Info("New consumer started")

	// fetch the offset here and stream from that offset
	employeeOffset, err := s.employeeRepository.FetchLastEmployeeOffset()
	if err != nil {
		Log.WithField("error", err).Error("unable to retrieve last employee offset")
		return
	}
	Log.WithField("topic", cfg.Topic).Info("Stream from this offset: ", employeeOffset)
	sub, err := kafka.NewSubscriber(&cfg, func() int64 { return int64(employeeOffset) }, broadcastEmployeeOffset)
	if err != nil {
		Log.WithField("error", err).Error("unable to create sub")
		return
	}
	defer func() {
		if err := sub.Stop(); err != nil {
			Log.WithField("error", err).Error("unable to stop sub")
		}
	}()

	subscriberDone := make(chan bool, 1)
	stopSubscriber := make(chan bool, 1)
	go func() {
		defer func() { subscriberDone <- true }()
		var (
			payload []byte
			msgs    = sub.Start()
		)
		for {
			select {
			case msg := <-msgs:
				payload = msg.Message()

				var employeeMessage model.EmployeeMessage

				err := json.Unmarshal(payload, &employeeMessage)
				if err != nil {
					Log.WithField("error", err).Error("unable to decode employeeMessage")
				}

				readEmployee := model.ReadEmployee{
					MessageID:         employeeMessage.MessageID,
					EmployeeFirstName: employeeMessage.EmployeeFirstName,
					EmployeeLastName:  employeeMessage.EmployeeLastName,
					EmployeeEmail:     employeeMessage.EmployeeEmail,
					Deleted:           employeeMessage.Deleted,
					CreatedAt:         employeeMessage.CreatedAt,
					UpdatedAt:         employeeMessage.UpdatedAt,
				}

				// save the message in badgerDB
				err = s.employeeRepository.SaveReadEmployee(&readEmployee)
				if err != nil {
					server.Log.WithField("error", err).Error("unable to save read employee to view database")
				}

				switch employeeMessage.EventName {
				case EmployeeCreatedEvent:
					Log.WithField("topic-stream", cfg.Topic).Info("New event - EMPLOYEE CREATED")
				case EmployeeUpdatedEvent:
					Log.WithField("topic-stream", cfg.Topic).Info("New event - EMPLOYEE UPDATED")
				case EmployeeDeletedEvent:
					Log.WithField("topic-stream", cfg.Topic).Info("New event - EMPLOYEE DLETED")
				default:
				}
				err = msg.Done()
				if err != nil {
					Log.WithField("error", err).Error("unable to mark user message as done")
				}
				// set the last offset to stream from
				s.employeeRepository.SetLastEmployeeOffset(employeeOffsetValue)
				Log.WithField("topic", cfg.Topic).Info("New message consumed at offset:", employeeOffsetValue)
			case <-stopSubscriber:
				return
			}
		}
	}()

	<-subscriberDone
	Log.WithField("topic", cfg.Topic).Info("leaving company topic stream")
}

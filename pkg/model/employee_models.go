package model

import (
	"encoding/json"
	"time"

	"github.com/NYTimes/gizmo/pubsub"
	"github.com/NYTimes/gizmo/pubsub/kafka"
	Log "github.com/sirupsen/logrus"
	validator "gopkg.in/go-playground/validator.v9"
)

// EmployeeMessage ...a struct to represent a Employee in the eventStore:
type EmployeeMessage struct {
	Cfg               *kafka.Config
	EventName         string    `json:"event_name" validate:"required"`
	MessageID         string    `json:"message_id" validate:"required"`
	EmployeeFirstName string    `json:"employee_first_name"`
	EmployeeLastName  string    `json:"employee_last_name"`
	EmployeeEmail     string    `json:"employee_email" validate:"required"`
	VersionNumber     int       `json:"version_number"`
	Deleted           bool      `json:"deleted"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// EmployeeCreatedEvent ...Create event for the message
type EmployeeCreatedEvent struct {
	Event             string    `json:"event" validate:"required"`
	Version           int       `json:"version"`
	CreatedAt         time.Time `json:"created_at"`
	MessageID         string    `json:"message_id" validate:"required"`
	EmployeeFirstName string    `json:"employee_first_name" validate:"required"`
	EmployeeLastName  string    `json:"employee_last_name" validate:"required"`
	EmployeeEmail     string    `json:"employee_email" validate:"required"`
}

// ValidateEmployeeCreatedEvent ... Validate the struct
func (b EmployeeCreatedEvent) ValidateEmployeeCreatedEvent() error {
	var validate *validator.Validate
	validate = validator.New()
	err := validate.Struct(b)

	if err != nil {
		return err
	}
	return err
}

// EventName implements part of the Event interface
func (b EmployeeCreatedEvent) EventName() string {
	return b.Event
}

// EventVersion implements part of the Event interface
func (b EmployeeCreatedEvent) EventVersion() int {
	return b.Version + 1
}

// EventAt implements part of the Event interface
func (b EmployeeCreatedEvent) EventAt() time.Time {
	return b.CreatedAt
}

// EmployeeUpdatedEvent ...Create event for the message
type EmployeeUpdatedEvent struct {
	Event             string    `json:"event" validate:"required"`
	Version           int       `json:"version"`
	CreatedAt         time.Time `json:"created_at"`
	EmployeeFirstName string    `json:"employee_first_name" validate:"required"`
	EmployeeLastName  string    `json:"employee_last_name" validate:"required"`
	EmployeeEmail     string    `json:"employee_email" validate:"required"`
}

// ValidateEmployeeUpdatedEvent ... Validate the struct
func (b EmployeeUpdatedEvent) ValidateEmployeeUpdatedEvent() error {
	var validate *validator.Validate
	validate = validator.New()
	err := validate.Struct(b)

	if err != nil {
		return err
	}
	return err
}

// EventName implements part of the Event interface
func (b EmployeeUpdatedEvent) EventName() string {
	return b.Event
}

// EventVersion implements part of the Event interface
func (b EmployeeUpdatedEvent) EventVersion() int {
	return b.Version + 1
}

// EventAt implements part of the Event interface
func (b EmployeeUpdatedEvent) EventAt() time.Time {
	return b.CreatedAt
}

// EmployeeDeletedEvent ...Create event for the message
type EmployeeDeletedEvent struct {
	Event     string    `json:"event" validate:"required"`
	Version   int       `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	Deleted   bool      `json:"deleted" validate:"required"`
}

// ValidateEmployeeDeletedEvent ... Validate the struct
func (b EmployeeDeletedEvent) ValidateEmployeeDeletedEvent() error {
	var validate *validator.Validate
	validate = validator.New()
	err := validate.Struct(b)

	if err != nil {
		return err
	}
	return err
}

// EventName implements part of the Event interface
func (b EmployeeDeletedEvent) EventName() string {
	return b.Event
}

// EventVersion implements part of the Event interface
func (b EmployeeDeletedEvent) EventVersion() int {
	return b.Version + 1
}

// EventAt implements part of the Event interface
func (b EmployeeDeletedEvent) EventAt() time.Time {
	return b.CreatedAt
}

// ReadEmployee ...projection struct
type ReadEmployee struct {
	MessageID         string    `json:"message_id"`
	EmployeeFirstName string    `json:"employee_first_name"`
	EmployeeLastName  string    `json:"employee_last_name"`
	EmployeeEmail     string    `json:"employee_email"`
	VersionNumber     int       `json:"version_number"`
	Deleted           bool      `json:"deleted"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// PublishEmployeeMessageInTopic ...
func (m EmployeeMessage) PublishEmployeeMessageInTopic(r ReadEmployee, event Event) error {
	m.MessageID = r.MessageID
	m.EmployeeFirstName = r.EmployeeFirstName
	m.EmployeeLastName = r.EmployeeLastName
	m.EmployeeEmail = r.EmployeeEmail
	m.VersionNumber = r.VersionNumber
	m.Deleted = r.Deleted
	m.CreatedAt = r.CreatedAt
	m.UpdatedAt = time.Now()

	switch v := event.(type) {
	case *EmployeeCreatedEvent:
		Log.WithField("event", "employeeCreated").Info("New event triggered")
		m.EventName = v.Event
		m.MessageID = v.MessageID
		m.EmployeeFirstName = v.EmployeeFirstName
		m.EmployeeLastName = v.EmployeeLastName
		m.EmployeeEmail = v.EmployeeEmail
		m.CreatedAt = time.Now()
		m.VersionNumber = r.VersionNumber + v.Version
		err := v.ValidateEmployeeCreatedEvent()
		if err != nil {
			return err
		}
		return m.Publish()
	case *EmployeeUpdatedEvent:
		Log.WithField("event", "employeeUpdated").Info("New event triggered")
		m.EventName = v.Event
		m.VersionNumber = r.VersionNumber + v.Version
		m.CreatedAt = time.Now()
		m.EmployeeFirstName = v.EmployeeFirstName
		m.EmployeeLastName = v.EmployeeLastName
		m.EmployeeEmail = v.EmployeeEmail
		err := v.ValidateEmployeeUpdatedEvent()
		if err != nil {
			return err
		}
		return m.Publish()
	case *EmployeeDeletedEvent:
		Log.WithField("event", "employeeDeleted").Info("New event triggered")
		m.EventName = v.Event
		m.VersionNumber = r.VersionNumber + v.Version
		m.CreatedAt = time.Now()
		m.Deleted = v.Deleted
		err := v.ValidateEmployeeDeletedEvent()
		if err != nil {
			return err
		}
		return m.Publish()
	default:
		Log.WithField("event", "No event matched").Info("New event triggered")
		return nil
	}

}

// Publish ...
func (m EmployeeMessage) Publish() error {
	// setting up the kafka settings
	cfg := *m.Cfg
	cfg.Topic = "stream-employee"
	Log.WithField("topic", cfg.Topic).Info("Publish a new message to topic")

	var pub pubsub.Publisher
	pub, err := kafka.NewPublisher(&cfg)
	if err != nil {
		Log.WithField("error", err).Error("unable to create pub")
		return err
	}
	defer func() {
		kpub, ok := pub.(*kafka.Publisher)
		if ok {
			if err := kpub.Stop(); err != nil {
				Log.WithField("error", err).Error("unable to stop pub")
			}
		}
	}()

	m.Cfg.Config = nil
	payload, err := json.Marshal(m)
	if err != nil {
		Log.WithField("error", err).Error("unable to read payload")
		return err
	}

	// publish the message to the kafka Employee topic
	err = pub.PublishRaw(nil, cfg.Topic, payload)
	if err != nil {
		Log.WithField("error", err).Error("unable to publish payload")
		return err
	}

	return nil
}

// EncodeReadEmployee ...
func (r ReadEmployee) EncodeReadEmployee() []byte {
	data, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}

	return data
}

// DecodeReadEmployee ...
func (r ReadEmployee) DecodeReadEmployee(readEmployeeData []byte) ReadEmployee {
	var employee ReadEmployee
	err := json.Unmarshal(readEmployeeData, &employee)
	if err != nil {
		panic(err)
	}

	return employee
}

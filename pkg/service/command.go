package service

import (
	"event-source-demo/pkg/model"
	"net/http"

	"github.com/gorilla/mux"
	Log "github.com/sirupsen/logrus"
)

// CreateEmployee will init a new pubsub.Subscribe and retrieve results from the event cache (Badger DB)
func (s *StreamService) CreateEmployee(r *http.Request) (int, interface{}, error) {
	// r.ParseForm()
	// generate a unique ID (IDs dont exist in topic so need a unique identifer)
	messageID, err := s.createPrimaryID()
	if err != nil {
		Log.WithField("create-primary-id", messageID).Error(err)
		return http.StatusBadRequest, struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{"Error", "primary key error occured"}, nil
	}
	// build the employee create event
	eventCreateEmployee := &model.EmployeeCreatedEvent{
		Event:             EmployeeCreatedEvent,
		Version:           1,
		MessageID:         messageID,
		EmployeeFirstName: r.FormValue("first_name"),
		EmployeeLastName:  r.FormValue("last_name"),
		EmployeeEmail:     r.FormValue("email"),
	}
	// init the message model
	message := &model.EmployeeMessage{}
	message.Cfg = s.cfg
	// publish the event to the topic
	err = message.PublishEmployeeMessageInTopic(model.ReadEmployee{}, eventCreateEmployee)
	if err != nil {
		Log.WithField("topic", "stream-topic").Error(err)
		return http.StatusBadRequest, struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{"Error", err.Error()}, nil
	}
	return http.StatusOK, struct {
		Status   string             `json:"status"`
		Message  string             `json:"message"`
		Employee model.ReadEmployee `json:"employee"`
	}{"Success!", "employeess successfully fetched", model.ReadEmployee{
		MessageID:         eventCreateEmployee.MessageID,
		EmployeeFirstName: eventCreateEmployee.EmployeeFirstName,
		EmployeeLastName:  eventCreateEmployee.EmployeeLastName,
		EmployeeEmail:     eventCreateEmployee.EmployeeEmail,
	}}, nil
}

// UpdateEmployee will init a new pubsub.Subscribe and retrieve results from the event cache (Badger DB)
func (s *StreamService) UpdateEmployee(r *http.Request) (int, interface{}, error) {
	var employeeID string
	if len(r.FormValue("employee_id")) == 0 {
		params := mux.Vars(r)
		employeeID = params["employee_id"]
	} else {
		employeeID = r.FormValue("employee_id")
	}
	// fetch employee to update
	readEmployee, err := s.employeeRepository.GetEmployeeByID(employeeID)
	if err != nil {
		return http.StatusBadRequest, struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{"Error", err.Error()}, nil
	}
	// build the employee update event
	eventUpdateEmployee := &model.EmployeeUpdatedEvent{
		Event:             EmployeeUpdatedEvent,
		Version:           readEmployee.VersionNumber,
		EmployeeFirstName: r.FormValue("first_name"),
		EmployeeLastName:  r.FormValue("last_name"),
		EmployeeEmail:     r.FormValue("email"),
	}
	// init the message model
	message := &model.EmployeeMessage{}
	message.Cfg = s.cfg
	// publish the event to the topic
	err = message.PublishEmployeeMessageInTopic(*readEmployee, eventUpdateEmployee)
	if err != nil {
		Log.WithField("topic", "stream-topic").Error(err)
		return http.StatusBadRequest, struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{"Error", err.Error()}, nil
	}

	// You can cheat and pass the just updated employee here or wait for eventual consistency eg until its saved to event store

	// readEmployee.EmployeeFirstName = eventUpdateEmployee.EmployeeFirstName
	// readEmployee.EmployeeLastName = eventUpdateEmployee.EmployeeLastName
	// readEmployee.EmployeeEmail = eventUpdateEmployee.EmployeeEmail

	return http.StatusOK, struct {
		Status  string             `json:"status"`
		Message string             `json:"message"`
		Data    model.ReadEmployee `json:"data"`
	}{"Success!", "employee details successfully updated", *readEmployee}, nil
}

// DeleteEmployee will init a new pubsub.Subscribe and retrieve results from the event cache (Badger DB)
func (s *StreamService) DeleteEmployee(r *http.Request) (int, interface{}, error) {
	var employeeID string
	if len(r.FormValue("employee_id")) == 0 {
		params := mux.Vars(r)
		employeeID = params["employee_id"]
	} else {
		employeeID = r.FormValue("employee_id")
	}
	// fetch employee to update
	readEmployee, err := s.employeeRepository.GetEmployeeByID(employeeID)
	if err != nil {
		return http.StatusBadRequest, struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{"Error", err.Error()}, nil
	}
	// build the employee update event
	eventDeleteEmployee := &model.EmployeeDeletedEvent{
		Event:   EmployeeDeletedEvent,
		Version: readEmployee.VersionNumber,
		Deleted: true,
	}
	// init the message model
	message := &model.EmployeeMessage{}
	message.Cfg = s.cfg
	// publish the event to the topic
	err = message.PublishEmployeeMessageInTopic(*readEmployee, eventDeleteEmployee)
	if err != nil {
		Log.WithField("topic", "stream-topic").Error(err)
		return http.StatusBadRequest, struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{"Error", err.Error()}, nil
	}
	return http.StatusOK, struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}{"Success!", "employee successfully deleted"}, nil
}

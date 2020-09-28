package service

import (
	"event-source-demo/pkg/model"
	"net/http"

	"github.com/gorilla/mux"
)

// GetAllEmployees will init a new pubsub.Subscribe and retrieve results from the event cache (Badger DB)
func (s *StreamService) GetAllEmployees(r *http.Request) (int, interface{}, error) {

	employees, err := s.employeeRepository.GetEmployees()
	if err != nil {
		return http.StatusBadRequest, struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{"Error", err.Error()}, nil
	}
	return http.StatusOK, struct {
		Status  string               `json:"status"`
		Message string               `json:"message"`
		Data    []model.ReadEmployee `json:"data"`
	}{"Success!", "employees successfully fetched", *employees}, nil
}

// GetAllEmployeeByID will init a new pubsub.Subscribe and retrieve results from the event cache (Badger DB)
func (s *StreamService) GetAllEmployeeByID(r *http.Request) (int, interface{}, error) {
	var employeeID string
	if len(r.FormValue("employee_id")) == 0 {
		params := mux.Vars(r)
		employeeID = params["employee_id"]
	} else {
		employeeID = r.FormValue("employee_id")
	}

	if len(employeeID) == 0 {
		return http.StatusBadRequest, struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{"Error", "employee ID is required"}, nil
	}

	employee, err := s.employeeRepository.GetEmployeeByID(employeeID)
	if err != nil {
		return http.StatusBadRequest, struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		}{"Error", err.Error()}, nil
	}
	return http.StatusOK, struct {
		Status  string             `json:"status"`
		Message string             `json:"message"`
		Data    model.ReadEmployee `json:"data"`
	}{"Success!", "employees successfully fetched", *employee}, nil
}

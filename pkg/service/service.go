package service

import (
	"event-source-demo/internal/employee"
	"net/http"

	"github.com/NYTimes/gizmo/pubsub/kafka"
	"github.com/NYTimes/gizmo/server"
)

// StreamService offers three endpoints: one to create a new topic in
// Kafka, a second to expose the topic over a websocket and a third
// to host a web page that provides a demo.
type StreamService struct {
	port               int
	cfg                *kafka.Config
	employeeRepository employee.Repository
}

// NewStreamService will return a new stream service instance.
// If the given config is empty, it will default to localhost.
func NewStreamService(port int, cfg *kafka.Config, employeeRepo employee.Repository) *StreamService {
	if cfg == nil {
		cfg = &kafka.Config{BrokerHosts: []string{"localhost:9092"}}
	}
	return &StreamService{
		port,
		cfg,
		employeeRepo,
	}
}

// Prefix is the string prefixed to all endpoint routes.
func (s *StreamService) Prefix() string {
	return "/svc/v1"
}

// Middleware in this service will do nothing.
func (s *StreamService) Middleware(h http.Handler) http.Handler {
	return server.NoCacheHandler(h)
}

// Endpoints returns the two endpoints for our stream service.
func (s *StreamService) Endpoints() map[string]map[string]http.HandlerFunc {
	return map[string]map[string]http.HandlerFunc{
		"/employees": {
			"POST": server.JSONToHTTP(s.CreateEmployee).ServeHTTP,
			"GET":  server.JSONToHTTP(s.GetAllEmployees).ServeHTTP,
		},
		"/employees/{employee_id}": {
			"GET":    server.JSONToHTTP(s.GetAllEmployeeByID).ServeHTTP,
			"DELETE": server.JSONToHTTP(s.DeleteEmployee).ServeHTTP,
			"PUT":    server.JSONToHTTP(s.UpdateEmployee).ServeHTTP,
		},
	}
}

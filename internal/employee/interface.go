package employee

import "event-source-demo/pkg/model"

// Repository Interface
type Repository interface {
	SaveReadEmployee(employee *model.ReadEmployee) error
	SetLastEmployeeOffset(offset int) error
	FetchLastEmployeeOffset() (int, error)
	GetEmployeeByUserName(userName string) (*model.ReadEmployee, error)
	GetEmployeeByID(employeeID string) (*model.ReadEmployee, error)
	GetEmployees() (*[]model.ReadEmployee, error)
}

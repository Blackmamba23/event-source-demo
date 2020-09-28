package employee

import (
	"encoding/json"
	"errors"
	"event-source-demo/pkg/model"
	"strconv"

	badger "github.com/dgraph-io/badger/v2"
	Log "github.com/sirupsen/logrus"
)

//Data ...
type repository struct {
	DB *badger.DB
}

// NewRepository ...
func NewRepository(db *badger.DB) Repository {
	return repository{DB: db}
}

func (r repository) SaveReadEmployee(employee *model.ReadEmployee) error {
	err := r.DB.Update(func(txn *badger.Txn) error {
		// Use the transaction...
		e := badger.NewEntry([]byte(employee.MessageID), employee.EncodeReadEmployee()).WithMeta(byte(1))
		err := txn.SetEntry(e)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (r repository) SetLastEmployeeOffset(offset int) error {
	offsetString := strconv.Itoa(offset)
	err := r.DB.Update(func(txn *badger.Txn) error {
		// Use the transaction...
		e := badger.NewEntry([]byte("last_employee_offset"), []byte(offsetString)).WithMeta(byte(01))
		err := txn.SetEntry(e)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (r repository) FetchLastEmployeeOffset() (int, error) {
	var offsetValue []byte
	var offset int
	err := r.DB.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("last_employee_offset"))
		if err != nil {

			return err

		}
		offsetValue, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil && err.Error() == "Key not found" {
		err = r.DB.Update(func(txn *badger.Txn) error {
			// Use the transaction...
			e := badger.NewEntry([]byte("last_employee_offset"), []byte("0")).WithMeta(byte(01))
			err := txn.SetEntry(e)
			if err != nil {
				return err
			}
			offsetValue = []byte("0")
			return nil
		})

	}

	err = json.Unmarshal(offsetValue, &offset)
	if err != nil {
		return 0, err
	}

	return offset, err
}

func (r repository) GetEmployeeByUserName(userName string) (*model.ReadEmployee, error) {
	var employee model.ReadEmployee
	err := r.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item.UserMeta() == 1 {
				err := item.Value(func(v []byte) error {
					err := json.Unmarshal(v, &employee)
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					continue
				}
				if employee.EmployeeEmail == userName {
					break
				} else {
					employee = model.ReadEmployee{}
				}
			}
		}
		return nil
	})
	if err != nil && err.Error() == "json: cannot unmarshal number into Go value of type model.ReadEmployee" {
		return &model.ReadEmployee{}, errors.New("empty result set")
	}

	return &employee, err
}

func (r repository) GetEmployeeByID(employeeID string) (*model.ReadEmployee, error) {
	var employee model.ReadEmployee
	err := r.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(employeeID))
		if err != nil {
			return err
		}
		err = item.Value(func(v []byte) error {
			err := json.Unmarshal(v, &employee)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil && (err.Error() == "Key not found" || err.Error() == "json: cannot unmarshal number into Go value of type model.ReadEmployee") {
		return &model.ReadEmployee{}, errors.New("empty result set")
	}

	if employee.Deleted {
		return &model.ReadEmployee{}, errors.New("the employee is deleted")
	}

	return &employee, err
}

func (r repository) GetEmployees() (*[]model.ReadEmployee, error) {
	var employees []model.ReadEmployee
	var employee model.ReadEmployee
	err := r.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item.UserMeta() == 1 {
				err := item.Value(func(v []byte) error {
					err := json.Unmarshal(v, &employee)
					if err != nil {
						Log.WithField("db-error", err).Error("unable to unmarshal")
						return err
					}
					return nil
				})
				if err != nil {
					continue
				}

				employees = append(employees, employee)

			}

		}
		return nil
	})
	if err != nil && err.Error() == "json: cannot unmarshal number into Go value of type model.employee" || employee.MessageID == "" {
		return &[]model.ReadEmployee{}, errors.New("empty result set")
	}

	return &employees, nil
}

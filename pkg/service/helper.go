package service

import (
	"fmt"
	"os"
	"os/exec"
	"time"

	random "math/rand"

	"github.com/oklog/ulid/v2"
)

// Events
const (
	EmployeeCreatedEvent = "employeeCreated"
	EmployeeUpdatedEvent = "employeeUpdated"
	EmployeeDeletedEvent = "employeeDeleted"
)

func topicName(id int64) string {
	return fmt.Sprintf("stream-%d", id)
}

// generate a new primary ID
func (s *StreamService) createPrimaryID() (string, error) {
	entropy := ulid.Monotonic(random.New(random.NewSource(time.Now().UnixNano())), 0)
	uLID, err := ulid.New(ulid.Timestamp(time.Now()), entropy)
	if err != nil {
		return "", err
	}
	return uLID.String(), nil
}

func createTopic(name string) error {
	cmd := exec.Command("kafka-topics.sh",
		"--create",
		"--zookeeper",
		"localhost:2181",
		"--replication-factor",
		"1",
		"--partition",
		"1",
		"--topic",
		name)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	return cmd.Run()
}

type jsonErr struct {
	Err error `json:"error"`
}

func (e jsonErr) Error() string {
	return e.Err.Error()
}

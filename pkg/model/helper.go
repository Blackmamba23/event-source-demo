package model

import "time"

// Event describes a change that happened.
//
// * Past tense, EmailChanged
// * Contains intent, EmailChanged is better than EmailSet
type Event interface {
	// EventName returns the aggregate id of the event
	EventName() string

	// Version contains version number of aggregate
	EventVersion() int

	// At indicates when the event took place
	EventAt() time.Time
}

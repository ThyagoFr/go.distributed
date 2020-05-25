package server

import (
	"fmt"
	"sync"
)

// Log - Struct with sync.Mutex, []Record
type Log struct {
	mu      sync.Mutex
	records []Record
}

// NewLog - Create a Log struct instance and return a pointer
func NewLog() *Log {

	return &Log{}

}

// Append - Add a new record to Log struct
func (c *Log) Append(record Record) (uint64, error) {

	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil

}

// Read - Read an especific log and return
func (c *Log) Read(offset uint64) (Record, error) {

	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil

}


// Record - Struct with []byte json Value, uint64 json Offset
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// ErrOffsetNotFound - Enum to an error when the offset doesnt exist
var ErrOffsetNotFound = fmt.Errorf("offset not found")

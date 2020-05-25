package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

const lenWidth = 8

type store struct {
	File   *os.File
	mu     sync.Mutex
	buffer *bufio.Writer
	size   uint64
}

func newStore(f *os.File) (*store, error) {

	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(file.Size())
	return &store{
		File:   f,
		size:   size,
		buffer: bufio.NewWriter(f),
	}, nil

}

func (s *store) Append(content []byte) (n uint64, position uint64, err error) {

	s.mu.Lock()
	defer s.mu.Unlock()
	position = s.size
	if err := binary.Write(s.buffer, enc, uint64(len(content))); err != nil {
		return 0, 0, err
	}
	w, err := s.buffer.Write(content)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), position, nil

}

func (s *store) ReadAt(position uint64) ([]byte, error) {

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buffer.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(position)); err != nil {
		return nil, err
	}
	content := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(content, int64(position+lenWidth)); err != nil {
		return nil, err
	}
	return content, nil
}

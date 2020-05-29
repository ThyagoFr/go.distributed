package log

import (
	"fmt"
	"os"
	"path"

	proglog "github.com/go.distruted/internal/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	seg := segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if seg.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if seg.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := seg.index.Read(-1); err != nil {
		seg.nextOffset = baseOffset
	} else {
		seg.nextOffset = baseOffset + uint64(off) + 1
	}
	return &seg, nil
}

func (s *segment) Append(record *proglog.Record) (offset uint64, err error) {

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	cursor := s.nextOffset
	s.nextOffset++
	return cursor, nil

}

func (s *segment) Read(off uint64) (*proglog.Record, error){
	
}

package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/maraino/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + uint64(lenWidth)
)

func TestStore(t *testing.T) {
	file, err := ioutil.TempFile("", "store_test")
	defer os.Remove(file.Name())
	require.NoError(t, err)
	store, err := newStore(file)
	require.NoError(t, err)
	testAppend(t, store)
	testRead(t, store)

	store, err = newStore(file)
	require.NoError(t, err)
	testRead(t, store)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)

	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.ReadAt(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

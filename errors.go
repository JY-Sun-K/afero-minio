package miniofs

import (
	"errors"
	"fmt"
	"os"
)

var (
	// ErrNoBucketInName indicates no bucket was specified in the DSN
	ErrNoBucketInName = errors.New("miniofs: no bucket name found in the DSN")

	// ErrEmptyObjectName indicates an empty object name was provided
	ErrEmptyObjectName = errors.New("miniofs: object name is empty")

	// ErrFileClosed indicates operation on a closed file
	ErrFileClosed = os.ErrClosed

	// ErrOutOfRange indicates offset is out of valid range
	ErrOutOfRange = errors.New("miniofs: offset out of range")

	// ErrNotSupported indicates operation is not supported
	ErrNotSupported = errors.New("miniofs: operation not supported")

	// ErrInvalidSeekWhence indicates invalid whence parameter for Seek
	ErrInvalidSeekWhence = errors.New("miniofs: invalid seek whence")

	// ErrNegativeOffset indicates negative offset was provided
	ErrNegativeOffset = errors.New("miniofs: negative offset")

	// ErrReadOnlyFile indicates write operation on read-only file
	ErrReadOnlyFile = errors.New("miniofs: file opened as read-only")

	// ErrWriteOnlyFile indicates read operation on write-only file
	ErrWriteOnlyFile = errors.New("miniofs: file opened as write-only")

	// ErrContextCanceled indicates context was canceled
	ErrContextCanceled = errors.New("miniofs: context canceled")
)

// PathError records an error and the operation and file path that caused it
type PathError struct {
	Op   string
	Path string
	Err  error
}

func (e *PathError) Error() string {
	return fmt.Sprintf("miniofs: %s %s: %v", e.Op, e.Path, e.Err)
}

func (e *PathError) Unwrap() error {
	return e.Err
}

// NewPathError creates a new PathError
func NewPathError(op, path string, err error) error {
	if err == nil {
		return nil
	}
	return &PathError{
		Op:   op,
		Path: path,
		Err:  err,
	}
}

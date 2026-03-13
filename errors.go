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

	// ErrAppendNotSupported indicates append is unavailable with the current backend or strategy.
	ErrAppendNotSupported = errors.New("miniofs: append is not supported by the configured strategy")

	// ErrLargeWriteRequiresStaging indicates the write exceeds direct mode limits and needs another strategy.
	ErrLargeWriteRequiresStaging = errors.New("miniofs: large object mutation requires temp_file or compose append strategy")
)

type PathError = os.PathError

// NewPathError creates a new PathError
func NewPathError(op, path string, err error) error {
	if err == nil {
		return nil
	}
	return &os.PathError{Op: fmt.Sprintf("miniofs: %s", op), Path: path, Err: err}
}

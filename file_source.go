package miniofs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/minio/minio-go/v7"
)

//const (
//	maxWriteSize int = 1e4
//)

type readerAtCloser interface {
	io.ReadCloser
	io.ReaderAt
}

type minioFileResource struct {
	ctx context.Context
	fs  *Fs

	name     string
	fileMode os.FileMode

	currentIoSize int64 // Current size of the file
	sizeKnown     bool  // Whether currentIoSize has been determined
	offset        int64
	reader        readerAtCloser
	writer        io.WriteCloser

	closed bool
}

func (o *minioFileResource) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	return o.maybeCloseIo()
}

func (o *minioFileResource) maybeCloseIo() error {
	if err := o.maybeCloseReader(); err != nil {
		return fmt.Errorf("error closing reader: %v", err)
	}
	if err := o.maybeCloseWriter(); err != nil {
		return fmt.Errorf("error closing writer: %v", err)
	}

	return nil
}

func (o *minioFileResource) maybeCloseReader() error {
	if o.reader == nil {
		return nil
	}
	if err := o.reader.Close(); err != nil {
		return err
	}
	o.reader = nil
	return nil
}

func (o *minioFileResource) maybeCloseWriter() error {
	if o.writer == nil {
		return nil
	}

	// In cases of partial writes (e.g. to the middle of a file stream), we need to
	// append any remaining data from the original file before we close the reader (and
	// commit the results.)
	// For small writes it can be more efficient
	// to keep the original reader but that is for another iteration
	//if o.currentIoSize > o.offset {
	//
	//}

	if err := o.writer.Close(); err != nil {
		return err
	}
	o.writer = nil
	return nil
}

func (o *minioFileResource) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if off < 0 {
		return 0, ErrNegativeOffset
	}

	// Check context
	if err := o.ctx.Err(); err != nil {
		return 0, ErrContextCanceled
	}

	// If any writers have written anything; commit it first so we can read it back.
	if err = o.maybeCloseIo(); err != nil {
		return 0, err
	}

	opts := minio.GetObjectOptions{}
	r, err := o.fs.client.GetObject(o.ctx, o.fs.bucket, o.name, opts)
	if err != nil {
		return 0, NewPathError("read", o.name, err)
	}
	defer func() {
		if closeErr := r.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	// Get object info to check size
	stat, err := r.Stat()
	if err != nil {
		return 0, NewPathError("stat", o.name, err)
	}

	// Update current size
	o.currentIoSize = stat.Size
	o.sizeKnown = true

	// If offset is beyond file size, return EOF
	if off >= stat.Size {
		return 0, io.EOF
	}

	// Read at offset
	read, err := r.ReadAt(p, off)
	o.offset = off + int64(read)
	return read, err
}

func (o *minioFileResource) WriteAt(b []byte, off int64) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	if off < 0 {
		return 0, ErrNegativeOffset
	}

	// Check context
	if err := o.ctx.Err(); err != nil {
		return 0, ErrContextCanceled
	}

	// Ensure readers must be closed before writing
	if err = o.maybeCloseIo(); err != nil {
		return 0, err
	}

	// Determine current file size if not known
	if !o.sizeKnown {
		stat, err := o.fs.client.StatObject(o.ctx, o.fs.bucket, o.name, minio.StatObjectOptions{})
		if err != nil {
			// If file doesn't exist, size is 0
			mErr, ok := err.(minio.ErrorResponse)
			if !ok || mErr.Code != "NoSuchKey" {
				return 0, NewPathError("stat", o.name, err)
			}
			o.currentIoSize = 0
		} else {
			o.currentIoSize = stat.Size
		}
		o.sizeKnown = true
	}

	// For MinIO, we need to handle writes carefully
	// If writing at offset 0 or to a new file, we can directly upload
	if off == 0 || o.currentIoSize == 0 {
		buffer := bytes.NewReader(b)
		opts := minio.PutObjectOptions{
			ContentType: http.DetectContentType(b),
		}
		_, err = o.fs.client.PutObject(o.ctx, o.fs.bucket, o.name, buffer, int64(len(b)), opts)
		if err != nil {
			return 0, NewPathError("write", o.name, err)
		}
		o.offset = int64(len(b))
		o.currentIoSize = int64(len(b))
		return len(b), nil
	}

	// For writes at non-zero offsets, we need to read existing content,
	// modify it, and write back (MinIO doesn't support partial updates)
	var existingData []byte
	if o.currentIoSize > 0 {
		// Read existing file content
		opts := minio.GetObjectOptions{}
		reader, err := o.fs.client.GetObject(o.ctx, o.fs.bucket, o.name, opts)
		if err != nil {
			return 0, NewPathError("read", o.name, err)
		}
		defer reader.Close()

		existingData, err = io.ReadAll(reader)
		if err != nil {
			return 0, NewPathError("read", o.name, err)
		}
	}

	// Expand buffer if necessary
	newSize := off + int64(len(b))
	if int64(len(existingData)) < newSize {
		newData := make([]byte, newSize)
		copy(newData, existingData)
		existingData = newData
	}

	// Write new data at offset
	copy(existingData[off:], b)

	// Upload modified content
	buffer := bytes.NewReader(existingData)
	opts := minio.PutObjectOptions{
		ContentType: http.DetectContentType(existingData),
	}
	_, err = o.fs.client.PutObject(o.ctx, o.fs.bucket, o.name, buffer, int64(len(existingData)), opts)
	if err != nil {
		return 0, NewPathError("write", o.name, err)
	}

	o.offset = off + int64(len(b))
	o.currentIoSize = int64(len(existingData))
	return len(b), nil
}

func (o *minioFileResource) Truncate(size int64) error {
	if size < 0 {
		return ErrNegativeOffset
	}

	// Check context
	if err := o.ctx.Err(); err != nil {
		return ErrContextCanceled
	}

	// Close any open readers/writers
	if err := o.maybeCloseIo(); err != nil {
		return err
	}

	// If truncating to 0, just create empty file
	if size == 0 {
		opts := minio.PutObjectOptions{}
		_, err := o.fs.client.PutObject(o.ctx, o.fs.bucket, o.name, bytes.NewReader([]byte{}), 0, opts)
		if err != nil {
			return NewPathError("truncate", o.name, err)
		}
		o.currentIoSize = 0
		o.sizeKnown = true
		o.offset = 0
		return nil
	}

	// Read current content
	opts := minio.GetObjectOptions{}
	reader, err := o.fs.client.GetObject(o.ctx, o.fs.bucket, o.name, opts)
	if err != nil {
		// If file doesn't exist, create it with zeros
		mErr, ok := err.(minio.ErrorResponse)
		if ok && mErr.Code == "NoSuchKey" {
			newData := make([]byte, size)
			putOpts := minio.PutObjectOptions{}
			_, err = o.fs.client.PutObject(o.ctx, o.fs.bucket, o.name, bytes.NewReader(newData), size, putOpts)
			if err != nil {
				return NewPathError("truncate", o.name, err)
			}
			o.currentIoSize = size
			o.sizeKnown = true
			return nil
		}
		return NewPathError("truncate", o.name, err)
	}
	defer reader.Close()

	existingData, err := io.ReadAll(reader)
	if err != nil {
		return NewPathError("read", o.name, err)
	}

	var newData []byte
	if int64(len(existingData)) > size {
		// Truncate to smaller size
		newData = existingData[:size]
	} else if int64(len(existingData)) < size {
		// Expand with zeros
		newData = make([]byte, size)
		copy(newData, existingData)
	} else {
		// Same size, no change needed
		o.currentIoSize = size
		o.sizeKnown = true
		return nil
	}

	// Upload truncated/expanded content
	putOpts := minio.PutObjectOptions{}
	_, err = o.fs.client.PutObject(o.ctx, o.fs.bucket, o.name, bytes.NewReader(newData), int64(len(newData)), putOpts)
	if err != nil {
		return NewPathError("truncate", o.name, err)
	}

	o.currentIoSize = size
	o.sizeKnown = true
	return nil
}

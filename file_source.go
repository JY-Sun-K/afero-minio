package miniofs

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
)

type minioFileResource struct {
	ctx context.Context
	fs  *Fs

	name     string
	fileMode os.FileMode

	currentSize int64
	sizeKnown   bool

	tempFile  *os.File
	tempPath  string
	tempDirty bool

	closed bool
}

func (o *minioFileResource) Close() error {
	if o.closed {
		return nil
	}

	if err := o.Sync(); err != nil {
		return err
	}

	if o.tempFile != nil {
		_ = o.tempFile.Close()
	}
	if o.tempPath != "" {
		_ = os.Remove(o.tempPath)
	}
	o.tempFile = nil
	o.tempPath = ""
	o.closed = true
	return nil
}

func (o *minioFileResource) Sync() error {
	if o.tempFile == nil || !o.tempDirty {
		return nil
	}

	info, err := o.tempFile.Stat()
	if err != nil {
		return err
	}
	if _, err := o.tempFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	opts := minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	}
	if err := o.fs.putObject(o.name, o.tempFile, info.Size(), opts); err != nil {
		return err
	}

	o.tempDirty = false
	o.currentSize = info.Size()
	o.sizeKnown = true
	_, _ = o.tempFile.Seek(0, io.SeekStart)
	return nil
}

func (o *minioFileResource) Stat() (os.FileInfo, error) {
	if o.tempFile != nil {
		size, err := o.Size()
		if err != nil {
			return nil, err
		}
		return &FileInfo{
			name:     o.name,
			size:     size,
			updated:  time.Now(),
			isDir:    false,
			fileMode: o.fileMode,
		}, nil
	}

	return o.fs.Stat(o.name)
}

func (o *minioFileResource) Size() (int64, error) {
	if o.tempFile != nil {
		info, err := o.tempFile.Stat()
		if err != nil {
			return 0, err
		}
		o.currentSize = info.Size()
		o.sizeKnown = true
		return info.Size(), nil
	}

	if o.sizeKnown {
		return o.currentSize, nil
	}

	info, err := o.fs.statObjectByKey(o.fs.objectKey(o.name))
	if err != nil {
		if errorsIsNotExist(err) {
			o.currentSize = 0
			o.sizeKnown = true
			return 0, nil
		}
		return 0, err
	}

	o.currentSize = info.Size
	o.sizeKnown = true
	return info.Size, nil
}

func (o *minioFileResource) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, ErrNegativeOffset
	}

	if o.tempFile != nil {
		return o.tempFile.ReadAt(p, off)
	}

	reader, cancel, err := o.fs.getObjectReader(o.name, minio.GetObjectOptions{})
	if err != nil {
		return 0, NewPathError("read", o.name, err)
	}
	defer cancel()
	defer reader.Close()

	info, err := reader.Stat()
	if err != nil {
		return 0, NewPathError("stat", o.name, mapMinioError(err))
	}
	o.currentSize = info.Size
	o.sizeKnown = true
	if off >= info.Size {
		return 0, io.EOF
	}

	n, err := reader.ReadAt(p, off)
	if err != nil && err != io.EOF {
		return n, NewPathError("read", o.name, mapMinioError(err))
	}
	return n, err
}

func (o *minioFileResource) Append(b []byte) (int, error) {
	size, err := o.Size()
	if err != nil {
		return 0, err
	}

	mode, err := selectWriteStrategy(writePlan{
		options:      o.fs.options,
		currentSize:  size,
		targetOffset: size,
		writeLen:     len(b),
		appendOnly:   true,
	})
	if err != nil {
		return 0, err
	}

	return o.writeWithMode(mode, b, size)
}

func (o *minioFileResource) WriteAt(b []byte, off int64) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, ErrNegativeOffset
	}

	if o.tempFile != nil {
		return o.writeTempAt(b, off)
	}

	size, err := o.Size()
	if err != nil {
		return 0, err
	}

	mode, err := selectWriteStrategy(writePlan{
		options:      o.fs.options,
		currentSize:  size,
		targetOffset: off,
		writeLen:     len(b),
	})
	if err != nil {
		return 0, err
	}

	return o.writeWithMode(mode, b, off)
}

func (o *minioFileResource) writeWithMode(mode writeMode, b []byte, off int64) (int, error) {
	switch mode {
	case writeModeDirect:
		return o.writeDirect(b, off)
	case writeModeTempFile:
		if err := o.ensureTempFileLoaded(); err != nil {
			return 0, err
		}
		return o.writeTempAt(b, off)
	case writeModeComposeAppend:
		return o.composeAppend(b)
	case writeModeNativeAppend:
		return o.nativeAppend(b)
	default:
		return 0, ErrNotSupported
	}
}

func (o *minioFileResource) writeDirect(b []byte, off int64) (int, error) {
	currentSize, err := o.Size()
	if err != nil {
		return 0, err
	}

	if currentSize == 0 && off == 0 {
		if err := o.fs.putObject(o.name, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{
			ContentType: http.DetectContentType(b),
		}); err != nil {
			return 0, err
		}
		o.currentSize = int64(len(b))
		o.sizeKnown = true
		return len(b), nil
	}

	existing, err := o.readCurrentData()
	if err != nil {
		return 0, err
	}

	newSize := off + int64(len(b))
	if int64(len(existing)) < newSize {
		grown := make([]byte, newSize)
		copy(grown, existing)
		existing = grown
	}
	copy(existing[off:], b)

	if err := o.fs.putObject(o.name, bytes.NewReader(existing), int64(len(existing)), minio.PutObjectOptions{
		ContentType: http.DetectContentType(existing),
	}); err != nil {
		return 0, err
	}

	o.currentSize = int64(len(existing))
	o.sizeKnown = true
	return len(b), nil
}

func (o *minioFileResource) writeTempAt(b []byte, off int64) (int, error) {
	if _, err := o.tempFile.WriteAt(b, off); err != nil {
		return 0, err
	}

	end := off + int64(len(b))
	if end > o.currentSize {
		o.currentSize = end
	}
	o.sizeKnown = true
	o.tempDirty = true
	return len(b), nil
}

func (o *minioFileResource) composeAppend(b []byte) (int, error) {
	currentSize, err := o.Size()
	if err != nil {
		return 0, err
	}
	if currentSize == 0 {
		return o.writeDirect(b, 0)
	}

	tempKey := o.composeTempKey()
	defer func() {
		_ = o.fs.removeObjectByKey(tempKey)
	}()

	if err := o.fs.putObjectByKey(tempKey, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{
		ContentType: http.DetectContentType(b),
	}); err != nil {
		return 0, err
	}

	if err := o.fs.composeObjectByKey(o.fs.objectKey(o.name),
		minio.CopySrcOptions{Bucket: o.fs.bucket, Object: o.fs.objectKey(o.name)},
		minio.CopySrcOptions{Bucket: o.fs.bucket, Object: tempKey},
	); err != nil {
		return 0, err
	}

	o.currentSize = currentSize + int64(len(b))
	o.sizeKnown = true
	return len(b), nil
}

func (o *minioFileResource) nativeAppend(b []byte) (int, error) {
	if err := o.fs.appendObject(o.name, bytes.NewReader(b), int64(len(b))); err != nil {
		return 0, err
	}

	o.currentSize += int64(len(b))
	o.sizeKnown = true
	return len(b), nil
}

func (o *minioFileResource) Truncate(size int64) error {
	if size < 0 {
		return ErrNegativeOffset
	}

	if o.tempFile != nil {
		if err := o.tempFile.Truncate(size); err != nil {
			return err
		}
		o.currentSize = size
		o.sizeKnown = true
		o.tempDirty = true
		return nil
	}

	currentSize, err := o.Size()
	if err != nil {
		return err
	}

	if size == 0 {
		if err := o.fs.putEmptyObject(o.name, "application/octet-stream"); err != nil {
			return err
		}
		o.currentSize = 0
		o.sizeKnown = true
		return nil
	}

	if currentSize == size {
		return nil
	}

	if currentSize <= o.fs.options.MaxDirectObjectSize && size <= o.fs.options.MaxDirectObjectSize {
		existing, err := o.readCurrentData()
		if err != nil {
			return err
		}

		var newData []byte
		switch {
		case int64(len(existing)) > size:
			newData = append([]byte(nil), existing[:size]...)
		case int64(len(existing)) < size:
			newData = make([]byte, size)
			copy(newData, existing)
		default:
			newData = existing
		}

		if err := o.fs.putObject(o.name, bytes.NewReader(newData), int64(len(newData)), minio.PutObjectOptions{
			ContentType: http.DetectContentType(newData),
		}); err != nil {
			return err
		}
		o.currentSize = size
		o.sizeKnown = true
		return nil
	}

	if o.fs.options.LargeObjectStrategy == LargeObjectStrategyTempFile || o.fs.options.WriteStrategy == WriteStrategyStaging {
		if err := o.ensureTempFileLoaded(); err != nil {
			return err
		}
		if err := o.tempFile.Truncate(size); err != nil {
			return err
		}
		o.currentSize = size
		o.sizeKnown = true
		o.tempDirty = true
		return nil
	}

	return ErrLargeWriteRequiresStaging
}

func (o *minioFileResource) ensureTempFileLoaded() error {
	if o.tempFile != nil {
		return nil
	}

	tmpFile, err := os.CreateTemp(o.fs.options.TempDir, "miniofs-*")
	if err != nil {
		return err
	}

	currentSize, err := o.Size()
	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return err
	}

	if currentSize > 0 {
		reader, cancel, err := o.fs.getObjectReader(o.name, minio.GetObjectOptions{})
		if err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpFile.Name())
			return err
		}
		defer cancel()
		if _, err := io.Copy(tmpFile, reader); err != nil {
			reader.Close()
			_ = tmpFile.Close()
			_ = os.Remove(tmpFile.Name())
			return err
		}
		_ = reader.Close()
	}

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return err
	}

	o.tempFile = tmpFile
	o.tempPath = tmpFile.Name()
	o.tempDirty = false
	o.sizeKnown = true
	return nil
}

func (o *minioFileResource) readCurrentData() ([]byte, error) {
	currentSize, err := o.Size()
	if err != nil {
		return nil, err
	}
	if currentSize == 0 {
		return []byte{}, nil
	}

	reader, cancel, err := o.fs.getObjectReader(o.name, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (o *minioFileResource) composeTempKey() string {
	parts := []string{}
	if o.fs.options.Prefix != "" {
		parts = append(parts, o.fs.options.Prefix)
	}
	parts = append(parts, ".miniofs-tmp", uuid.NewString())
	return strings.Join(parts, "/")
}

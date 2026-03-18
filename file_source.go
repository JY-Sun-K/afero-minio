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
	start := time.Now()
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
	o.fs.perfLog("⏱️ [Close] name=%s, elapsed=%v", o.name, time.Since(start))
	return nil
}

func (o *minioFileResource) Sync() error {
	start := time.Now()
	if o.tempFile == nil || !o.tempDirty {
		o.fs.perfLog("⏱️ [Sync] name=%s, skipped (no tempFile or not dirty), elapsed=%v", o.name, time.Since(start))
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
	putStart := time.Now()
	if err := o.fs.putObject(o.name, o.tempFile, info.Size(), opts); err != nil {
		return err
	}
	o.fs.perfLog("⏱️ [Sync.putObject] name=%s, size=%d, elapsed=%v", o.name, info.Size(), time.Since(putStart))

	o.tempDirty = false
	o.currentSize = info.Size()
	o.sizeKnown = true
	_, _ = o.tempFile.Seek(0, io.SeekStart)
	o.fs.perfLog("⏱️ [Sync] name=%s, total elapsed=%v", o.name, time.Since(start))
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
	start := time.Now()
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, ErrNegativeOffset
	}

	if o.tempFile != nil {
		n, err := o.tempFile.ReadAt(p, off)
		o.fs.perfLog("⏱️ [ReadAt] name=%s, off=%d, len=%d, n=%d, tempFile=true, elapsed=%v", o.name, off, len(p), n, time.Since(start))
		return n, err
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
		o.fs.perfLog("⏱️ [ReadAt] name=%s, off=%d, len=%d, n=%d, elapsed=%v, err=%v", o.name, off, len(p), n, time.Since(start), err)
		return n, NewPathError("read", o.name, mapMinioError(err))
	}
	o.fs.perfLog("⏱️ [ReadAt] name=%s, off=%d, len=%d, n=%d, elapsed=%v", o.name, off, len(p), n, time.Since(start))
	return n, err
}

func (o *minioFileResource) Append(b []byte) (int, error) {
	size, err := o.Size()
	if err != nil {
		return 0, err
	}
	nativeReady, err := o.nativeAppendReady(size, size, true)
	if err != nil {
		return 0, err
	}

	mode, err := selectWriteStrategy(writePlan{
		options:           o.fs.options,
		currentSize:       size,
		targetOffset:      size,
		writeLen:          len(b),
		appendOnly:        true,
		nativeAppendReady: nativeReady,
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
	nativeReady, err := o.nativeAppendReady(size, off, false)
	if err != nil {
		return 0, err
	}

	mode, err := selectWriteStrategy(writePlan{
		options:           o.fs.options,
		currentSize:       size,
		targetOffset:      off,
		writeLen:          len(b),
		nativeAppendReady: nativeReady,
	})
	if err != nil {
		return 0, err
	}

	return o.writeWithMode(mode, b, off)
}

func (o *minioFileResource) writeWithMode(mode writeMode, b []byte, off int64) (int, error) {
	start := time.Now()
	var n int
	var err error

	switch mode {
	case writeModeDirect:
		n, err = o.writeDirect(b, off)
	case writeModeTempFile:
		if err = o.ensureTempFileLoaded(); err != nil {
			return 0, err
		}
		n, err = o.writeTempAt(b, off)
	case writeModeComposeAppend:
		n, err = o.composeAppend(b)
	case writeModeNativeAppend:
		n, err = o.nativeAppend(b)
	default:
		err = ErrNotSupported
	}

	o.fs.perfLog("⏱️ [writeWithMode] mode=%v, off=%d, len=%d, written=%d, elapsed=%v, err=%v", mode, off, len(b), n, time.Since(start), err)
	return n, err
}

func (o *minioFileResource) writeDirect(b []byte, off int64) (int, error) {
	start := time.Now()
	currentSize, err := o.Size()
	if err != nil {
		return 0, err
	}

	if currentSize == 0 && off == 0 {
		putStart := time.Now()
		if err := o.fs.putObject(o.name, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{
			ContentType: http.DetectContentType(b),
		}); err != nil {
			return 0, err
		}
		o.fs.perfLog("⏱️ [writeDirect.putObject] name=%s, size=%d (new file), putElapsed=%v, totalElapsed=%v", o.name, len(b), time.Since(putStart), time.Since(start))
		o.currentSize = int64(len(b))
		o.sizeKnown = true
		return len(b), nil
	}

	readStart := time.Now()
	existing, err := o.readCurrentData()
	if err != nil {
		return 0, err
	}
	o.fs.perfLog("⏱️ [writeDirect.readCurrentData] name=%s, existingSize=%d, readElapsed=%v", o.name, len(existing), time.Since(readStart))

	newSize := off + int64(len(b))
	if int64(len(existing)) < newSize {
		grown := make([]byte, newSize)
		copy(grown, existing)
		existing = grown
	}
	copy(existing[off:], b)

	putStart := time.Now()
	if err := o.fs.putObject(o.name, bytes.NewReader(existing), int64(len(existing)), minio.PutObjectOptions{
		ContentType: http.DetectContentType(existing),
	}); err != nil {
		return 0, err
	}
	o.fs.perfLog("⏱️ [writeDirect.putObject] name=%s, size=%d (overwrite), putElapsed=%v", o.name, len(existing), time.Since(putStart))

	o.currentSize = int64(len(existing))
	o.sizeKnown = true
	o.fs.perfLog("⏱️ [writeDirect] name=%s, off=%d, len=%d, totalElapsed=%v", o.name, off, len(b), time.Since(start))
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
	start := time.Now()
	currentSize, err := o.Size()
	if err != nil {
		return 0, err
	}
	if currentSize == 0 {
		o.fs.perfLog("⏱️ [composeAppend] name=%s, currentSize=0, fallback to writeDirect", o.name)
		return o.writeDirect(b, 0)
	}

	tempKey := o.composeTempKey()
	defer func() {
		_ = o.fs.removeObjectByKey(tempKey)
	}()

	putStart := time.Now()
	if err := o.fs.putObjectByKey(tempKey, bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{
		ContentType: http.DetectContentType(b),
	}); err != nil {
		return 0, err
	}
	o.fs.perfLog("⏱️ [composeAppend.putTempObject] tempKey=%s, size=%d, elapsed=%v", tempKey, len(b), time.Since(putStart))

	composeStart := time.Now()
	if err := o.fs.composeObjectByKey(o.fs.objectKey(o.name),
		minio.CopySrcOptions{Bucket: o.fs.bucket, Object: o.fs.objectKey(o.name)},
		minio.CopySrcOptions{Bucket: o.fs.bucket, Object: tempKey},
	); err != nil {
		return 0, err
	}
	o.fs.perfLog("⏱️ [composeAppend.composeObject] name=%s, src1=%s, src2=%s, elapsed=%v", o.name, o.fs.objectKey(o.name), tempKey, time.Since(composeStart))

	o.currentSize = currentSize + int64(len(b))
	o.sizeKnown = true
	o.fs.perfLog("⏱️ [composeAppend] name=%s, appendLen=%d, newSize=%d, totalElapsed=%v", o.name, len(b), o.currentSize, time.Since(start))
	return len(b), nil
}

func (o *minioFileResource) nativeAppend(b []byte) (int, error) {
	start := time.Now()
	if err := o.fs.appendObject(o.name, bytes.NewReader(b), int64(len(b))); err != nil {
		o.fs.perfLog("⏱️ [nativeAppend] name=%s, len=%d, elapsed=%v, err=%v", o.name, len(b), time.Since(start), err)
		return 0, err
	}

	o.currentSize += int64(len(b))
	o.sizeKnown = true
	o.fs.perfLog("⏱️ [nativeAppend] name=%s, appendLen=%d, newSize=%d, elapsed=%v", o.name, len(b), o.currentSize, time.Since(start))
	return len(b), nil
}

func (o *minioFileResource) Truncate(size int64) error {
	start := time.Now()
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
		o.fs.perfLog("⏱️ [Truncate] name=%s, size=%d, tempFile=true, elapsed=%v", o.name, size, time.Since(start))
		return nil
	}

	currentSize, err := o.Size()
	if err != nil {
		return err
	}

	if size == 0 {
		putStart := time.Now()
		if err := o.fs.putEmptyObject(o.name, "application/octet-stream"); err != nil {
			return err
		}
		o.currentSize = 0
		o.sizeKnown = true
		o.fs.perfLog("⏱️ [Truncate.putEmptyObject] name=%s, size=0, putElapsed=%v, totalElapsed=%v", o.name, time.Since(putStart), time.Since(start))
		return nil
	}

	if currentSize == size {
		o.fs.perfLog("⏱️ [Truncate] name=%s, size=%d, no change, elapsed=%v", o.name, size, time.Since(start))
		return nil
	}

	if currentSize <= o.fs.options.MaxDirectObjectSize && size <= o.fs.options.MaxDirectObjectSize {
		readStart := time.Now()
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

		putStart := time.Now()
		if err := o.fs.putObject(o.name, bytes.NewReader(newData), int64(len(newData)), minio.PutObjectOptions{
			ContentType: http.DetectContentType(newData),
		}); err != nil {
			return err
		}
		o.currentSize = size
		o.sizeKnown = true
		o.fs.perfLog("⏱️ [Truncate] name=%s, oldSize=%d, newSize=%d, readElapsed=%v, putElapsed=%v, totalElapsed=%v", o.name, currentSize, size, time.Since(readStart), time.Since(putStart), time.Since(start))
		return nil
	}

	if o.fs.options.LargeObjectStrategy == LargeObjectStrategyTempFile || o.fs.options.WriteStrategy == WriteStrategyStaging {
		loadStart := time.Now()
		if err := o.ensureTempFileLoaded(); err != nil {
			return err
		}
		if err := o.tempFile.Truncate(size); err != nil {
			return err
		}
		o.currentSize = size
		o.sizeKnown = true
		o.tempDirty = true
		o.fs.perfLog("⏱️ [Truncate] name=%s, size=%d, loadElapsed=%v, totalElapsed=%v", o.name, size, time.Since(loadStart), time.Since(start))
		return nil
	}

	o.fs.perfLog("⏱️ [Truncate] name=%s, size=%d, err=ErrLargeWriteRequiresStaging, elapsed=%v", o.name, size, time.Since(start))
	return ErrLargeWriteRequiresStaging
}

func (o *minioFileResource) ensureTempFileLoaded() error {
	start := time.Now()
	if o.tempFile != nil {
		o.fs.perfLog("⏱️ [ensureTempFileLoaded] name=%s, already loaded, elapsed=%v", o.name, time.Since(start))
		return nil
	}

	createStart := time.Now()
	tmpFile, err := os.CreateTemp(o.fs.options.TempDir, "miniofs-*")
	if err != nil {
		return err
	}
	o.fs.perfLog("⏱️ [ensureTempFileLoaded.createTemp] name=%s, tempPath=%s, elapsed=%v", o.name, tmpFile.Name(), time.Since(createStart))

	sizeStart := time.Now()
	currentSize, err := o.Size()
	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return err
	}
	o.fs.perfLog("⏱️ [ensureTempFileLoaded.Size] name=%s, currentSize=%d, elapsed=%v", o.name, currentSize, time.Since(sizeStart))

	if currentSize > 0 {
		getStart := time.Now()
		reader, cancel, err := o.fs.getObjectReader(o.name, minio.GetObjectOptions{})
		if err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpFile.Name())
			return err
		}
		defer cancel()

		copyStart := time.Now()
		copied, err := io.Copy(tmpFile, reader)
		if err != nil {
			reader.Close()
			_ = tmpFile.Close()
			_ = os.Remove(tmpFile.Name())
			return err
		}
		_ = reader.Close()
		o.fs.perfLog("⏱️ [ensureTempFileLoaded.copyData] name=%s, copied=%d, getElapsed=%v, copyElapsed=%v", o.name, copied, time.Since(getStart), time.Since(copyStart))
	}

	seekStart := time.Now()
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return err
	}
	o.fs.perfLog("⏱️ [ensureTempFileLoaded.seek] name=%s, elapsed=%v", o.name, time.Since(seekStart))

	o.tempFile = tmpFile
	o.tempPath = tmpFile.Name()
	o.tempDirty = false
	o.sizeKnown = true
	o.fs.perfLog("⏱️ [ensureTempFileLoaded] name=%s, totalElapsed=%v", o.name, time.Since(start))
	return nil
}

func (o *minioFileResource) readCurrentData() ([]byte, error) {
	start := time.Now()
	currentSize, err := o.Size()
	if err != nil {
		return nil, err
	}
	if currentSize == 0 {
		o.fs.perfLog("⏱️ [readCurrentData] name=%s, currentSize=0, elapsed=%v", o.name, time.Since(start))
		return []byte{}, nil
	}

	getStart := time.Now()
	reader, cancel, err := o.fs.getObjectReader(o.name, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer reader.Close()

	readStart := time.Now()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	o.fs.perfLog("⏱️ [readCurrentData] name=%s, currentSize=%d, readSize=%d, getElapsed=%v, readElapsed=%v, totalElapsed=%v", o.name, currentSize, len(data), time.Since(getStart), time.Since(readStart), time.Since(start))
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

func (o *minioFileResource) nativeAppendReady(currentSize, targetOffset int64, appendOnly bool) (bool, error) {
	opts := o.fs.options.withDefaults()
	if opts.AppendStrategy != AppendStrategyNative || !opts.AssumeNativeAppendSupported {
		return false, nil
	}
	if currentSize == 0 || (!appendOnly && targetOffset != currentSize) {
		return false, nil
	}
	if o.tempFile != nil {
		return false, nil
	}

	info, err := o.fs.statObjectByKeyWithOptions(o.fs.objectKey(o.name), minio.StatObjectOptions{Checksum: true})
	if err != nil {
		if errorsIsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.ChecksumMode == minio.ChecksumFullObjectMode.String(), nil
}

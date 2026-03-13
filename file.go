package miniofs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

type MinioFile struct {
	mu        sync.Mutex
	openFlags int
	fhOffset  int64
	dirOffset int
	closed    bool
	resource  *minioFileResource
}

func NewMinioFile(ctx context.Context, fs *Fs, openFlags int, fileMode os.FileMode, name string) *MinioFile {
	return &MinioFile{
		openFlags: openFlags,
		resource: &minioFileResource{
			ctx:      ctx,
			fs:       fs,
			name:     name,
			fileMode: fileMode,
		},
	}
}

func (o *MinioFile) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return ErrFileClosed
	}
	if err := o.resource.Close(); err != nil {
		return err
	}
	o.closed = true
	return nil
}

func (o *MinioFile) Seek(offset int64, whence int) (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return 0, ErrFileClosed
	}

	var base int64
	switch whence {
	case io.SeekStart:
		base = 0
	case io.SeekCurrent:
		base = o.fhOffset
	case io.SeekEnd:
		size, err := o.resource.Size()
		if err != nil {
			return 0, err
		}
		base = size
	default:
		return 0, ErrInvalidSeekWhence
	}

	next := base + offset
	if next < 0 {
		return 0, ErrNegativeOffset
	}
	o.fhOffset = next
	return next, nil
}

func (o *MinioFile) Read(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return 0, ErrFileClosed
	}
	if o.openFlags&os.O_WRONLY != 0 {
		return 0, ErrWriteOnlyFile
	}

	n, err := o.resource.ReadAt(p, o.fhOffset)
	o.fhOffset += int64(n)
	return n, err
}

func (o *MinioFile) ReadAt(p []byte, off int64) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return 0, ErrFileClosed
	}
	if off < 0 {
		return 0, ErrNegativeOffset
	}
	if o.openFlags&os.O_WRONLY != 0 {
		return 0, ErrWriteOnlyFile
	}

	return o.resource.ReadAt(p, off)
}

func (o *MinioFile) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return 0, ErrFileClosed
	}
	if o.openFlags&os.O_RDONLY != 0 || o.openFlags&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, ErrReadOnlyFile
	}

	var (
		n   int
		err error
	)
	if o.openFlags&os.O_APPEND != 0 {
		n, err = o.resource.Append(p)
		if err != nil {
			return n, err
		}
		size, sizeErr := o.resource.Size()
		if sizeErr != nil {
			return n, sizeErr
		}
		o.fhOffset = size
		return n, nil
	}

	n, err = o.resource.WriteAt(p, o.fhOffset)
	o.fhOffset += int64(n)
	return n, err
}

func (o *MinioFile) WriteAt(p []byte, off int64) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return 0, ErrFileClosed
	}
	if off < 0 {
		return 0, ErrNegativeOffset
	}
	if o.openFlags&os.O_RDONLY != 0 || o.openFlags&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, ErrReadOnlyFile
	}
	if o.openFlags&os.O_APPEND != 0 {
		return 0, ErrAppendNotSupported
	}

	return o.resource.WriteAt(p, off)
}

func (o *MinioFile) Name() string {
	return filepath.FromSlash(o.resource.name)
}

func (o *MinioFile) Readdir(count int) ([]os.FileInfo, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, ErrFileClosed
	}

	fi, err := o.readdirChunk(count)
	if err != nil {
		return []os.FileInfo{}, err
	}

	result := make([]os.FileInfo, 0, len(fi))
	for _, entry := range fi {
		result = append(result, entry)
	}
	return result, nil
}

func (o *MinioFile) Readdirnames(n int) ([]string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, ErrFileClosed
	}

	fi, err := o.readdirChunk(n)
	if err != nil {
		return []string{}, err
	}

	names := make([]string, len(fi))
	for i, entry := range fi {
		names[i] = entry.Name()
	}
	return names, err
}

func (o *MinioFile) readdirChunk(count int) ([]*FileInfo, error) {
	entries, err := o.readdirImpl()
	if err != nil {
		return nil, err
	}

	if count <= 0 {
		if o.dirOffset >= len(entries) {
			return []*FileInfo{}, nil
		}

		chunk := entries[o.dirOffset:]
		o.dirOffset = len(entries)
		return chunk, nil
	}

	if o.dirOffset >= len(entries) {
		return []*FileInfo{}, io.EOF
	}

	end := o.dirOffset + count
	if end > len(entries) {
		end = len(entries)
	}

	chunk := entries[o.dirOffset:end]
	o.dirOffset = end
	return chunk, nil
}

func (o *MinioFile) readdirImpl() ([]*FileInfo, error) {
	info, err := o.resource.fs.Stat(o.resource.name)
	if err != nil {
		if o.resource.name != "" {
			return nil, err
		}
	} else if !info.IsDir() {
		return nil, syscall.ENOTDIR
	}

	logicalPrefix := o.resource.name
	if logicalPrefix != "" {
		logicalPrefix += o.resource.fs.separator
	}

	remotePrefix := o.resource.fs.listPrefix(o.resource.name)
	seen := make(map[string]bool)
	var result []*FileInfo

	objects, cancel := o.resource.fs.listObjects(remotePrefix, false, 0)
	defer cancel()

	for object := range objects {
		if object.Err != nil {
			return nil, mapMinioError(object.Err)
		}

		logicalName := o.resource.fs.logicalNameFromKey(object.Key)
		if logicalName == o.resource.name || logicalName == strings.TrimSuffix(logicalPrefix, o.resource.fs.separator) {
			continue
		}

		relative := strings.TrimPrefix(logicalName, logicalPrefix)
		if relative == "" {
			continue
		}

		childName := relative
		isDir := strings.HasSuffix(object.Key, o.resource.fs.separator)
		if idx := strings.Index(relative, o.resource.fs.separator); idx >= 0 {
			childName = relative[:idx]
			isDir = true
		}

		if seen[childName] {
			continue
		}
		seen[childName] = true

		fullName := childName
		if o.resource.name != "" {
			fullName = o.resource.name + o.resource.fs.separator + childName
		}

		entry := &FileInfo{
			eTag:     object.ETag,
			name:     fullName,
			size:     object.Size,
			updated:  object.LastModified,
			isDir:    isDir,
			fileMode: o.resource.fileMode,
		}
		if isDir && entry.size == 0 {
			entry.size = folderSize
		}
		result = append(result, entry)
	}

	sort.Sort(ByName(result))
	return result, nil
}

func (o *MinioFile) Stat() (os.FileInfo, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return nil, ErrFileClosed
	}
	return o.resource.Stat()
}

func (o *MinioFile) Sync() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return ErrFileClosed
	}
	return o.resource.Sync()
}

func (o *MinioFile) Truncate(size int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return ErrFileClosed
	}
	if size < 0 {
		return ErrNegativeOffset
	}
	if o.openFlags&os.O_RDONLY != 0 || o.openFlags&(os.O_WRONLY|os.O_RDWR) == 0 {
		return ErrReadOnlyFile
	}

	if err := o.resource.Truncate(size); err != nil {
		return err
	}
	if o.fhOffset > size {
		o.fhOffset = size
	}
	return nil
}

func (o *MinioFile) WriteString(s string) (int, error) {
	return o.Write([]byte(s))
}

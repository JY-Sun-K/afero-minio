package miniofs

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/minio/minio-go/v7"
)

// MinioFile is the Afero version adapted for Minio
type MinioFile struct {
	openFlags int
	fhOffset  int64 // File handle specific offset
	closed    bool
	resource  *minioFileResource
}

func NewMinioFile(ctx context.Context, fs *Fs, openFlags int, fileMode os.FileMode, name string) *MinioFile {
	return &MinioFile{
		openFlags: openFlags,
		fhOffset:  0,
		closed:    false,
		resource: &minioFileResource{
			ctx:           ctx,
			fs:            fs,
			name:          name,
			fileMode:      fileMode,
			currentIoSize: 0,
			sizeKnown:     false,
			offset:        0,
			reader:        nil,
			writer:        nil,
			closed:        false,
		},
	}
}

func (o *MinioFile) Close() error {
	if o.closed {
		// the afero spec expects the call to Close on a closed file to return an error
		return ErrFileClosed
	}
	o.closed = true
	return o.resource.Close()
}

func (o *MinioFile) Seek(newOffset int64, whence int) (int64, error) {
	if o.closed {
		return 0, ErrFileClosed
	}

	// Since this is an expensive operation; let's make sure we need it
	if (whence == io.SeekStart && newOffset == o.fhOffset) || (whence == io.SeekCurrent && newOffset == 0) {
		return o.fhOffset, nil
	}

	// Log warning for performance awareness (optional, can be removed in production)
	if whence != io.SeekStart || newOffset != 0 {
		log.Printf("WARNING: Seek behavior triggered, highly inefficient. Offset before seek is at %d\n", o.fhOffset)
	}

	// Force the reader/writers to be reopened (at correct offset)
	err := o.Sync()
	if err != nil {
		return 0, err
	}

	// Calculate new offset based on whence
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = newOffset
	case io.SeekCurrent:
		newPos = o.fhOffset + newOffset
	case io.SeekEnd:
		stat, err := o.Stat()
		if err != nil {
			return 0, err
		}
		newPos = stat.Size() + newOffset
	default:
		return 0, ErrInvalidSeekWhence
	}

	// Validate new position
	if newPos < 0 {
		return 0, ErrNegativeOffset
	}

	o.fhOffset = newPos
	return o.fhOffset, nil
}

func (o *MinioFile) Read(p []byte) (n int, err error) {
	return o.ReadAt(p, o.fhOffset)
}

func (o *MinioFile) ReadAt(p []byte, off int64) (n int, err error) {
	if o.closed {
		return 0, ErrFileClosed
	}

	if off < 0 {
		return 0, ErrNegativeOffset
	}

	// Check for read permission
	if o.openFlags&os.O_WRONLY != 0 {
		return 0, ErrWriteOnlyFile
	}

	read, err := o.resource.ReadAt(p, off)
	o.fhOffset += int64(read)
	return read, err
}

func (o *MinioFile) Write(p []byte) (n int, err error) {
	return o.WriteAt(p, o.fhOffset)
}

func (o *MinioFile) WriteAt(b []byte, off int64) (n int, err error) {
	if o.closed {
		return 0, ErrFileClosed
	}

	if off < 0 {
		return 0, ErrNegativeOffset
	}

	if o.openFlags&os.O_RDONLY != 0 {
		return 0, ErrReadOnlyFile
	}

	// Check for write permission
	if o.openFlags&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, ErrReadOnlyFile
	}

	written, err := o.resource.WriteAt(b, off)
	o.fhOffset += int64(written)
	return written, err
}

func (o *MinioFile) Name() string {
	return filepath.FromSlash(o.resource.name)
}

func (o *MinioFile) readdirImpl(count int) ([]*FileInfo, error) {
	err := o.Sync()
	if err != nil {
		return nil, err
	}

	var ownInfo os.FileInfo
	ownInfo, err = o.Stat()
	if err != nil {
		// If stat fails, try to list anyway (it might be root or virtual dir)
		if o.resource.name != "" && o.resource.name != "." {
			return nil, err
		}
	} else if !ownInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}

	// Ensure prefix ends with separator for proper directory listing
	prefix := o.resource.name
	if prefix != "" && !strings.HasSuffix(prefix, o.resource.fs.separator) {
		prefix += o.resource.fs.separator
	}

	// Use non-recursive listing to get only direct children
	opts := minio.ListObjectsOptions{
		Recursive: false,
		Prefix:    prefix,
	}

	seen := make(map[string]bool)
	var res []*FileInfo

	for obj := range o.resource.fs.client.ListObjects(o.resource.ctx, o.resource.fs.bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}

		// Skip the directory object itself
		if obj.Key == prefix || obj.Key == strings.TrimSuffix(prefix, o.resource.fs.separator) {
			continue
		}

		// Extract the immediate child name
		relativePath := strings.TrimPrefix(obj.Key, prefix)
		if relativePath == "" {
			continue
		}

		// For subdirectories, get only the first part
		childName := relativePath
		if idx := strings.Index(relativePath, o.resource.fs.separator); idx > 0 {
			childName = relativePath[:idx]
		}

		// Avoid duplicates
		if seen[childName] {
			continue
		}
		seen[childName] = true

		// Create FileInfo with full path for proper stat
		fullPath := prefix + childName
		isDir := strings.HasSuffix(obj.Key, o.resource.fs.separator) ||
			strings.Contains(strings.TrimPrefix(obj.Key, prefix), o.resource.fs.separator)

		fi := &FileInfo{
			eTag:     obj.ETag,
			name:     fullPath,
			size:     obj.Size,
			updated:  obj.LastModified,
			isDir:    isDir,
			fileMode: o.resource.fileMode,
		}

		if isDir && fi.size == 0 {
			fi.size = folderSize
		}

		res = append(res, fi)
	}

	// Sort results
	sort.Sort(ByName(res))

	// Apply count limit if specified
	if count > 0 && len(res) > count {
		res = res[:count]
	}

	if len(res) == 0 {
		return res, io.EOF
	}

	return res, nil
}

func (o *MinioFile) Readdir(count int) ([]os.FileInfo, error) {
	fi, err := o.readdirImpl(count)
	if err != nil {
		// 【关键修复】：如果错误是 EOF，说明目录为空，返回空列表而不是错误
		if err == io.EOF {
			return []os.FileInfo{}, nil
		}
		return nil, err
	}

	var res []os.FileInfo
	for _, f := range fi {
		res = append(res, f)
	}
	return res, nil
}

func (o *MinioFile) Readdirnames(n int) ([]string, error) {
	fi, err := o.Readdir(n)
	if err != nil && err != io.EOF {
		return nil, err
	}
	names := make([]string, len(fi))

	for i, f := range fi {
		names[i] = f.Name()
	}
	return names, err
}

func (o *MinioFile) Stat() (os.FileInfo, error) {
	err := o.Sync()
	if err != nil {
		return nil, err
	}

	return o.resource.fs.Stat(o.resource.name)
}

func (o *MinioFile) Sync() error {
	return o.resource.maybeCloseIo()
}

func (o *MinioFile) Truncate(wantedSize int64) error {
	if o.closed {
		return ErrFileClosed
	}

	if wantedSize < 0 {
		return ErrNegativeOffset
	}

	if o.openFlags&os.O_RDONLY != 0 {
		return ErrReadOnlyFile
	}

	// Check for write permission
	if o.openFlags&(os.O_WRONLY|os.O_RDWR) == 0 {
		return ErrReadOnlyFile
	}

	return o.resource.Truncate(wantedSize)
}

func (o *MinioFile) WriteString(s string) (ret int, err error) {
	return o.Write([]byte(s))
}

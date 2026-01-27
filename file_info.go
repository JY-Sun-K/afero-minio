package miniofs

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

const (
	folderSize = 42
)

type FileInfo struct {
	eTag     string
	name     string
	size     int64
	updated  time.Time
	isDir    bool
	fileMode os.FileMode
}

func newFileInfoFromAttrs(obj minio.ObjectInfo, fileMode os.FileMode) *FileInfo {
	// Check if this is a directory (ends with separator)
	isDir := strings.HasSuffix(obj.Key, "/")
	size := obj.Size

	// Empty objects with trailing slash are directories
	if isDir && size == 0 {
		size = folderSize
	}

	res := &FileInfo{
		eTag:     obj.ETag,
		name:     obj.Key,
		size:     size,
		updated:  obj.LastModified,
		isDir:    isDir,
		fileMode: fileMode,
	}

	return res
}

func (fi *FileInfo) Name() string {
	return filepath.Base(filepath.FromSlash(fi.name))
}

func (fi *FileInfo) Size() int64 {
	return fi.size
}

func (fi *FileInfo) Mode() os.FileMode {
	if fi.IsDir() {
		return os.ModeDir | fi.fileMode
	}
	return fi.fileMode
}

func (fi *FileInfo) ModTime() time.Time {
	return fi.updated
}

func (fi *FileInfo) IsDir() bool {
	return fi.isDir
}

func (fi *FileInfo) Sys() interface{} {
	return nil
}

type ByName []*FileInfo

func (a ByName) Len() int { return len(a) }

// Swap exchanges the elements with indexes i and j
func (a ByName) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Less reports whether the element with index i should sort before the element with index j
func (a ByName) Less(i, j int) bool {
	return strings.Compare(a[i].Name(), a[j].Name()) < 0
}

package miniofs

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/spf13/afero"
)

const (
	defaultFileMode = 0o755
)

// Fs is a Fs implementation that uses functions provided by google cloud storage
type Fs struct {
	ctx       context.Context
	client    *minio.Client
	bucket    string
	separator string
}

func NewMinioFs(ctx context.Context, dsn string) (afero.Fs, error) {
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	minioOpts, err := ParseURL(dsn)
	if err != nil {
		return nil, err
	}

	client, err := minio.New(parsedURL.Host, minioOpts)
	if err != nil {
		return nil, err
	}

	bucket := strings.TrimPrefix(parsedURL.Path, "/")
	if bucket == "" {
		return nil, ErrNoBucketInName
	}

	return NewFs(ctx, client, bucket), nil
}

func NewFs(ctx context.Context, client *minio.Client, bucket string) *Fs {
	return &Fs{
		ctx:       ctx,
		client:    client,
		bucket:    bucket,
		separator: "/",
	}
}

// normSeparators will normalize all "\\" and "/" to the provided separator
func (fs *Fs) normSeparators(s string) string {
	return strings.Replace(strings.Replace(s, "\\", fs.separator, -1), "/", fs.separator, -1)
}

func (fs *Fs) ensureNoLeadingSeparator(s string) string {
	if len(s) > 0 && strings.HasPrefix(s, fs.separator) {
		s = s[len(fs.separator):]
	}

	return s
}

//func (fs *Fs) getObj(name string) (*minio.Object, error) {
//	bucketName, path := fs.splitName(name)
//	getObjectOptions := minio.GetObjectOptions{}
//
//	return fs.client.GetObject(fs.ctx, bucketName, path, getObjectOptions)
//}

func (fs *Fs) Name() string { return "MinioFs" }

func (fs *Fs) Create(name string) (afero.File, error) {
	return fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0)
}

func (fs *Fs) Mkdir(name string, _ os.FileMode) error {
	name = fs.ensureNoLeadingSeparator(fs.normSeparators(name))

	// Check context
	if err := fs.ctx.Err(); err != nil {
		return ErrContextCanceled
	}

	// Check if parent exists
	if name != "" && name != "." {
		parent := strings.TrimSuffix(name, fs.separator)
		lastSep := strings.LastIndex(parent, fs.separator)
		if lastSep > 0 {
			parentPath := parent[:lastSep]
			if _, err := fs.Stat(parentPath); err != nil {
				return NewPathError("mkdir", name, err)
			}
		}
	}

	// Check if directory already exists
	if _, err := fs.Stat(name); err == nil {
		return NewPathError("mkdir", name, os.ErrExist)
	}

	// Create directory by uploading an empty object with trailing separator
	dirName := strings.TrimSuffix(name, fs.separator) + fs.separator
	if err := fs.createEmptyObject(dirName); err != nil {
		return NewPathError("mkdir", name, err)
	}
	return nil
}

func (fs *Fs) MkdirAll(name string, perm os.FileMode) error {
	name = fs.ensureNoLeadingSeparator(fs.normSeparators(name))

	if name == "" || name == "." {
		return nil
	}

	// Check context
	if err := fs.ctx.Err(); err != nil {
		return ErrContextCanceled
	}

	// Check if already exists
	if fi, err := fs.Stat(name); err == nil {
		if fi.IsDir() {
			return nil
		}
		return NewPathError("mkdir", name, os.ErrExist)
	}

	// Create all parent directories
	parts := strings.Split(strings.TrimSuffix(name, fs.separator), fs.separator)
	currentPath := ""

	for _, part := range parts {
		if part == "" {
			continue
		}

		if currentPath != "" {
			currentPath += fs.separator
		}
		currentPath += part

		// Try to create directory, ignore if exists
		if _, err := fs.Stat(currentPath); err != nil {
			dirName := currentPath + fs.separator
			if err := fs.createEmptyObject(dirName); err != nil {
				return NewPathError("mkdir", currentPath, err)
			}
		}
	}

	return nil
}

// createEmptyObject creates an empty object to simulate a directory
func (fs *Fs) createEmptyObject(name string) error {
	opts := minio.PutObjectOptions{
		ContentType: "application/x-directory",
	}
	_, err := fs.client.PutObject(fs.ctx, fs.bucket, name, strings.NewReader(""), 0, opts)
	return err
}

func (fs *Fs) Open(name string) (afero.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

func (fs *Fs) OpenFile(name string, flag int, fileMode os.FileMode) (afero.File, error) {
	if flag&os.O_APPEND != 0 {
		return nil, NewPathError("open", name, errors.New("O_APPEND not supported for MinIO"))
	}

	name = fs.ensureNoLeadingSeparator(fs.normSeparators(name))

	// Check if file exists
	_, statErr := fs.Stat(name)
	fileExists := statErr == nil

	// Handle O_CREATE flag
	if flag&os.O_CREATE != 0 {
		if fileExists && flag&os.O_EXCL != 0 {
			return nil, os.ErrExist
		}
	} else {
		// If not creating and file doesn't exist, return error
		if !fileExists {
			return nil, NewPathError("open", name, os.ErrNotExist)
		}
	}

	file := NewMinioFile(fs.ctx, fs, flag, fileMode, name)

	// Handle O_CREATE - create empty file if it doesn't exist
	if flag&os.O_CREATE != 0 && !fileExists {
		if _, err := file.WriteString(""); err != nil {
			file.Close()
			return nil, NewPathError("create", name, err)
		}
	}

	// Handle O_TRUNC - truncate file to zero length
	if flag&os.O_TRUNC != 0 && fileExists {
		if err := file.Truncate(0); err != nil {
			file.Close()
			return nil, NewPathError("truncate", name, err)
		}
	}

	return file, nil
}

func (fs *Fs) Remove(name string) error {
	name = fs.ensureNoLeadingSeparator(fs.normSeparators(name))

	// Check context
	if err := fs.ctx.Err(); err != nil {
		return ErrContextCanceled
	}

	err := fs.client.RemoveObject(fs.ctx, fs.bucket, name, minio.RemoveObjectOptions{
		GovernanceBypass: true,
	})
	if err != nil {
		return NewPathError("remove", name, err)
	}
	return nil
}

func (fs *Fs) RemoveAll(path string) error {
	path = fs.ensureNoLeadingSeparator(fs.normSeparators(path))

	// Check context
	if err := fs.ctx.Err(); err != nil {
		return ErrContextCanceled
	}

	objectsCh := make(chan minio.ObjectInfo)
	errCh := make(chan error, 1)

	// Start goroutine to list objects
	go func() {
		defer close(objectsCh)
		opts := minio.ListObjectsOptions{Prefix: path, Recursive: true}
		for object := range fs.client.ListObjects(fs.ctx, fs.bucket, opts) {
			if object.Err != nil {
				errCh <- NewPathError("list", path, object.Err)
				return
			}
			objectsCh <- object
		}
	}()

	// Remove objects in batch
	removeErrCh := fs.client.RemoveObjects(fs.ctx, fs.bucket, objectsCh, minio.RemoveObjectsOptions{})

	// Collect errors
	var firstErr error
	for e := range removeErrCh {
		if firstErr == nil {
			firstErr = NewPathError("remove", e.ObjectName, e.Err)
		}
	}

	// Check if listing encountered an error
	select {
	case err := <-errCh:
		if err != nil && firstErr == nil {
			return err
		}
	default:
	}

	return firstErr
}

func (fs *Fs) Rename(oldName, newName string) error {
	if oldName == newName {
		return nil
	}

	oldName = fs.ensureNoLeadingSeparator(fs.normSeparators(oldName))
	newName = fs.ensureNoLeadingSeparator(fs.normSeparators(newName))

	// Check context
	if err := fs.ctx.Err(); err != nil {
		return ErrContextCanceled
	}

	// Check if source exists
	if _, err := fs.Stat(oldName); err != nil {
		return NewPathError("rename", oldName, os.ErrNotExist)
	}

	// Check if destination already exists
	if _, err := fs.Stat(newName); err == nil {
		return NewPathError("rename", newName, os.ErrExist)
	}

	// Copy object
	src := minio.CopySrcOptions{
		Bucket: fs.bucket,
		Object: oldName,
	}
	dst := minio.CopyDestOptions{
		Bucket: fs.bucket,
		Object: newName,
	}
	_, err := fs.client.CopyObject(fs.ctx, dst, src)
	if err != nil {
		return NewPathError("rename", oldName, err)
	}

	// Remove old object
	if err := fs.Remove(oldName); err != nil {
		// Try to clean up the copy if delete fails
		_ = fs.Remove(newName)
		return NewPathError("rename", oldName, err)
	}

	return nil
}

func (fs *Fs) Stat(name string) (os.FileInfo, error) {
	name = fs.ensureNoLeadingSeparator(fs.normSeparators(name))

	// Handle root directory
	if name == "" || name == "." {
		return &FileInfo{
			name:     "",
			size:     0,
			updated:  time.Now(),
			isDir:    true,
			fileMode: defaultFileMode,
		}, nil
	}

	// Try to stat as an object
	stat, err := fs.client.StatObject(fs.ctx, fs.bucket, name, minio.StatObjectOptions{})
	if err == nil {
		return newFileInfoFromAttrs(stat, defaultFileMode), nil
	}

	// Try as directory (with trailing separator)
	dirName := strings.TrimSuffix(name, fs.separator) + fs.separator
	stat, err = fs.client.StatObject(fs.ctx, fs.bucket, dirName, minio.StatObjectOptions{})
	if err == nil {
		fi := newFileInfoFromAttrs(stat, defaultFileMode)
		fi.isDir = true
		if fi.size == 0 {
			fi.size = folderSize
		}
		return fi, nil
	}

	// Check if it's a virtual directory (has children)
	opts := minio.ListObjectsOptions{
		Prefix:    dirName,
		MaxKeys:   1,
		Recursive: false,
	}

	for obj := range fs.client.ListObjects(fs.ctx, fs.bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		// If we found any objects with this prefix, it's a virtual directory
		return &FileInfo{
			name:     name,
			size:     folderSize,
			updated:  time.Now(),
			isDir:    true,
			fileMode: defaultFileMode,
		}, nil
	}

	return nil, os.ErrNotExist
}

func (fs *Fs) Chmod(_ string, _ os.FileMode) error {
	return errors.New("method Chmod is not implemented in Minio")
}

func (fs *Fs) Chtimes(_ string, _, _ time.Time) error {
	return errors.New("method Chtimes is not implemented. Create, Delete, Updated times are read only fields in Minio and set implicitly")
}

func (fs *Fs) Chown(_ string, _, _ int) error {
	return errors.New("method Chown is not implemented for Minio")
}

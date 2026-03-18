package miniofs

import (
	"context"
	"errors"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/spf13/afero"
)

const defaultFileMode = 0o755

type Fs struct {
	ctx          context.Context
	client       *minio.Client
	appendClient *minio.Client
	bucket       string
	separator    string
	options      Options
}

func NewMinioFs(ctx context.Context, dsn string) (afero.Fs, error) {
	return NewMinioFsWithOptions(ctx, dsn, DefaultOptions())
}

func NewMinioFsWithOptions(ctx context.Context, dsn string, opts Options) (afero.Fs, error) {
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	minioOpts, err := ParseURL(dsn)
	if err != nil {
		return nil, err
	}

	opts = opts.withDefaults()
	applyBaseOptionsToMinioClient(minioOpts, opts)

	client, err := minio.New(parsedURL.Host, minioOpts)
	if err != nil {
		return nil, err
	}

	var appendClient *minio.Client
	if shouldCreateDedicatedAppendClient(opts) {
		appendOpts := *minioOpts
		applyAppendOptionsToMinioClient(&appendOpts, opts)

		appendClient, err = minio.New(parsedURL.Host, &appendOpts)
		if err != nil {
			return nil, err
		}
	}

	bucket := strings.TrimPrefix(parsedURL.Path, "/")
	if bucket == "" {
		return nil, ErrNoBucketInName
	}

	return NewFsWithClients(ctx, client, appendClient, bucket, opts)
}

func NewFs(ctx context.Context, client *minio.Client, bucket string) *Fs {
	fs, _ := NewFsWithOptions(ctx, client, bucket, DefaultOptions())
	return fs
}

func NewFsWithOptions(ctx context.Context, client *minio.Client, bucket string, opts Options) (*Fs, error) {
	return newFs(ctx, client, nil, bucket, opts)
}

func NewFsWithClients(ctx context.Context, client *minio.Client, appendClient *minio.Client, bucket string, opts Options) (*Fs, error) {
	return newFs(ctx, client, appendClient, bucket, opts)
}

func newFs(ctx context.Context, client *minio.Client, appendClient *minio.Client, bucket string, opts Options) (*Fs, error) {
	if bucket == "" {
		return nil, ErrNoBucketInName
	}

	opts = opts.withDefaults()
	appendClient, opts = resolveAppendClient(client, appendClient, opts)
	fs := &Fs{
		ctx:          ctx,
		client:       client,
		appendClient: appendClient,
		bucket:       bucket,
		separator:    "/",
		options:      opts,
	}

	if opts.AppName != "" {
		client.SetAppInfo(opts.AppName, opts.AppVersion)
		if appendClient != nil && appendClient != client {
			appendClient.SetAppInfo(opts.AppName, opts.AppVersion)
		}
	}
	if opts.TraceOutput != nil {
		client.TraceOn(opts.TraceOutput)
		if appendClient != nil && appendClient != client {
			appendClient.TraceOn(opts.TraceOutput)
		}
	}
	if opts.ValidateBucketOnInit {
		opCtx, cancel := fs.operationContext()
		defer cancel()

		exists, err := client.BucketExists(opCtx, bucket)
		if err != nil {
			return nil, mapMinioError(err)
		}
		if !exists {
			return nil, NewPathError("bucket", bucket, os.ErrNotExist)
		}
	}
	if opts.LargeObjectStrategy == LargeObjectStrategyTempFile {
		// 如果使用临时文件，则需要使用本地磁盘作为中间件
		if err := os.MkdirAll(opts.TempDir, 0o755); err != nil {
			return nil, NewPathError("mkdir", opts.TempDir, err)
		}
	}
	return fs, nil
}

func applyBaseOptionsToMinioClient(dst *minio.Options, opts Options) {
	opts = opts.withDefaults()

	if opts.Transport != nil {
		dst.Transport = opts.Transport
	}
	if opts.MaxRetries > 0 {
		dst.MaxRetries = opts.MaxRetries
	}
	if opts.BucketLookup != 0 {
		dst.BucketLookup = opts.BucketLookup
	}
}

func applyAppendOptionsToMinioClient(dst *minio.Options, opts Options) {
	applyBaseOptionsToMinioClient(dst, opts)
	if opts.AppendStrategy == AppendStrategyNative && opts.AssumeNativeAppendSupported {
		dst.TrailingHeaders = true
	}
}

func shouldCreateDedicatedAppendClient(opts Options) bool {
	opts = opts.withDefaults()
	return opts.AppendStrategy == AppendStrategyNative && opts.AssumeNativeAppendSupported
}

func resolveAppendClient(client *minio.Client, appendClient *minio.Client, opts Options) (*minio.Client, Options) {
	opts = opts.withDefaults()
	if !shouldCreateDedicatedAppendClient(opts) {
		return nil, opts
	}
	if appendClient != nil {
		return appendClient, opts
	}

	endpoint := client.EndpointURL()
	if endpoint != nil && endpoint.Scheme == "http" {
		opts.AssumeNativeAppendSupported = false
		return nil, opts
	}

	return client, opts
}

func (fs *Fs) Name() string { return "MinioFs" }

func (fs *Fs) Create(name string) (afero.File, error) {
	return fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0)
}

func (fs *Fs) Open(name string) (afero.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

func (fs *Fs) OpenFile(name string, flag int, fileMode os.FileMode) (afero.File, error) {
	start := time.Now()
	name = fs.normalizeName(name)

	statStart := time.Now()
	info, err := fs.Stat(name)
	log.Printf("⏱️ [OpenFile.Stat] name=%s, elapsed=%v, err=%v", name, time.Since(statStart), err)

	pathExists := err == nil
	isDir := pathExists && info.IsDir()
	fileExists := pathExists && !isDir
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, NewPathError("open", name, err)
	}

	if isDir {
		if flag&(os.O_WRONLY|os.O_RDWR|os.O_TRUNC|os.O_CREATE|os.O_APPEND) != 0 {
			return nil, NewPathError("open", name, os.ErrInvalid)
		}
		log.Printf("⏱️ [OpenFile] name=%s (dir), total elapsed=%v", name, time.Since(start))
		return NewMinioFile(fs.ctx, fs, flag, fileMode, name), nil
	}

	if flag&os.O_CREATE != 0 {
		if fileExists && flag&os.O_EXCL != 0 {
			return nil, NewPathError("open", name, os.ErrExist)
		}
		if !fileExists {
			putStart := time.Now()
			if err := fs.putEmptyObject(name, "application/octet-stream"); err != nil {
				return nil, NewPathError("create", name, err)
			}
			log.Printf("⏱️ [OpenFile.putEmptyObject] name=%s, elapsed=%v", name, time.Since(putStart))
			fileExists = true
		}
	} else if !fileExists {
		return nil, NewPathError("open", name, os.ErrNotExist)
	}

	file := NewMinioFile(fs.ctx, fs, flag, fileMode, name)

	if flag&os.O_TRUNC != 0 {
		truncStart := time.Now()
		if err := file.Truncate(0); err != nil {
			_ = file.Close()
			return nil, NewPathError("truncate", name, err)
		}
		log.Printf("⏱️ [OpenFile.Truncate] name=%s, elapsed=%v", name, time.Since(truncStart))
	}

	log.Printf("⏱️ [OpenFile] name=%s, total elapsed=%v", name, time.Since(start))
	return file, nil
}

func (fs *Fs) Mkdir(name string, _ os.FileMode) error {
	name = fs.normalizeName(name)
	if name == "" {
		return nil
	}

	if err := fs.ctx.Err(); err != nil {
		return ErrContextCanceled
	}

	parent := path.Dir(name)
	if parent != "." && parent != "" {
		if _, err := fs.Stat(parent); err != nil {
			return NewPathError("mkdir", name, err)
		}
	}

	if _, err := fs.Stat(name); err == nil {
		return NewPathError("mkdir", name, os.ErrExist)
	} else if !errors.Is(err, os.ErrNotExist) {
		return NewPathError("mkdir", name, err)
	}

	if err := fs.createEmptyObject(fs.dirObjectKey(name), "application/x-directory"); err != nil {
		return NewPathError("mkdir", name, err)
	}

	return nil
}

func (fs *Fs) MkdirAll(name string, _ os.FileMode) error {
	name = fs.normalizeName(name)
	if name == "" {
		return nil
	}

	if err := fs.ctx.Err(); err != nil {
		return ErrContextCanceled
	}

	// 快速路径：完整路径已存在
	if _, err := fs.Stat(name); err == nil {
		return nil
	}

	// 分割路径
	parts := strings.Split(name, fs.separator)
	// 过滤空部分
	var validParts []string
	for _, part := range parts {
		if part != "" {
			validParts = append(validParts, part)
		}
	}
	if len(validParts) == 0 {
		return nil
	}

	// 反向查找：从后往前找第一个存在的父目录
	firstExistIndex := -1
	for i := len(validParts) - 1; i >= 0; i-- {
		current := strings.Join(validParts[:i+1], fs.separator)
		if _, err := fs.Stat(current); err == nil {
			firstExistIndex = i
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return NewPathError("mkdir", current, err)
		}
	}

	// 从第一个存在的目录的下一级开始创建
	for i := firstExistIndex + 1; i < len(validParts); i++ {
		current := strings.Join(validParts[:i+1], fs.separator)
		if err := fs.createEmptyObject(fs.dirObjectKey(current), "application/x-directory"); err != nil {
			return NewPathError("mkdir", current, err)
		}
	}

	return nil
}

func (fs *Fs) Remove(name string) error {
	name = fs.normalizeName(name)

	info, err := fs.Stat(name)
	if err != nil {
		return NewPathError("remove", name, err)
	}

	if info.IsDir() {
		children, err := fs.listChildren(name, 1)
		if err != nil {
			return NewPathError("remove", name, err)
		}
		if len(children) > 0 {
			return NewPathError("remove", name, os.ErrInvalid)
		}
		if err := fs.removeObjectByKey(fs.dirObjectKey(name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return NewPathError("remove", name, err)
		}
		return nil
	}

	if err := fs.removeObjectByKey(fs.objectKey(name)); err != nil {
		return NewPathError("remove", name, err)
	}
	return nil
}

func (fs *Fs) RemoveAll(pathName string) error {
	pathName = fs.normalizeName(pathName)

	keys, err := fs.collectRemovalKeys(pathName)
	if err != nil {
		return NewPathError("remove", pathName, err)
	}
	if len(keys) == 0 {
		return nil
	}

	objectsCh := make(chan minio.ObjectInfo, len(keys))
	for _, key := range keys {
		objectsCh <- minio.ObjectInfo{Key: key}
	}
	close(objectsCh)

	opCtx, cancel := fs.operationContext()
	defer cancel()

	removeErrCh := fs.client.RemoveObjects(opCtx, fs.bucket, objectsCh, minio.RemoveObjectsOptions{})
	for e := range removeErrCh {
		if e.Err != nil {
			return NewPathError("remove", fs.logicalNameFromKey(e.ObjectName), mapMinioError(e.Err))
		}
	}

	return nil
}

func (fs *Fs) Rename(oldName, newName string) error {
	oldName = fs.normalizeName(oldName)
	newName = fs.normalizeName(newName)
	if oldName == newName {
		return nil
	}

	info, err := fs.Stat(oldName)
	if err != nil {
		return NewPathError("rename", oldName, err)
	}

	if info.IsDir() {
		if _, err := fs.Stat(newName); err == nil {
			return NewPathError("rename", newName, os.ErrExist)
		} else if !errors.Is(err, os.ErrNotExist) {
			return NewPathError("rename", newName, err)
		}
		return fs.renameDir(oldName, newName)
	}

	dstExists := false
	if dstInfo, err := fs.Stat(newName); err == nil {
		if dstInfo.IsDir() {
			return NewPathError("rename", newName, os.ErrExist)
		}
		dstExists = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return NewPathError("rename", newName, err)
	}

	srcKey := fs.objectKey(oldName)
	dstKey := fs.objectKey(newName)
	if err := fs.copyObject(srcKey, dstKey); err != nil {
		return NewPathError("rename", oldName, err)
	}
	if err := fs.removeObjectByKey(srcKey); err != nil {
		if !dstExists {
			_ = fs.removeObjectByKey(dstKey)
		}
		return NewPathError("rename", oldName, err)
	}
	return nil
}

func (fs *Fs) renameDir(oldName, newName string) error {
	oldPrefix := fs.listPrefix(oldName)
	newPrefix := fs.listPrefix(newName)

	var keys []string
	objects, cancel := fs.listObjects(oldPrefix, true, 0)
	defer cancel()

	for object := range objects {
		if object.Err != nil {
			return mapMinioError(object.Err)
		}
		keys = append(keys, object.Key)
	}
	if len(keys) == 0 {
		if err := fs.createEmptyObject(fs.dirObjectKey(newName), "application/x-directory"); err != nil {
			return err
		}
		return fs.removeObjectByKey(fs.dirObjectKey(oldName))
	}

	for _, srcKey := range keys {
		suffix := strings.TrimPrefix(srcKey, oldPrefix)
		dstKey := newPrefix + suffix
		if err := fs.copyObject(srcKey, dstKey); err != nil {
			return err
		}
	}

	for _, key := range keys {
		if err := fs.removeObjectByKey(key); err != nil {
			return err
		}
	}

	return nil
}

func (fs *Fs) Stat(name string) (os.FileInfo, error) {
	start := time.Now()
	name = fs.normalizeName(name)
	if name == "" {
		return &FileInfo{
			name:     "",
			size:     0,
			updated:  time.Now(),
			isDir:    true,
			fileMode: defaultFileMode,
		}, nil
	}

	statStart := time.Now()
	objectInfo, err := fs.statObjectByKey(fs.objectKey(name))
	if err == nil {
		log.Printf("⏱️ [Stat] name=%s (file), statObjectByKey elapsed=%v", name, time.Since(statStart))
		return newFileInfoFromAttrs(objectInfo, name, defaultFileMode), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	dirStatStart := time.Now()
	dirInfo, dirErr := fs.statObjectByKey(fs.dirObjectKey(name))
	if dirErr == nil {
		log.Printf("⏱️ [Stat] name=%s (dir), statObjectByKey elapsed=%v", name, time.Since(dirStatStart))
		fi := newFileInfoFromAttrs(dirInfo, name, defaultFileMode)
		fi.isDir = true
		if fi.size == 0 {
			fi.size = folderSize
		}
		return fi, nil
	}
	if !errors.Is(dirErr, os.ErrNotExist) {
		return nil, dirErr
	}

	listStart := time.Now()
	objects, cancel := fs.listObjects(fs.listPrefix(name), false, 1)
	defer cancel()

	for object := range objects {
		if object.Err != nil {
			return nil, mapMinioError(object.Err)
		}
		log.Printf("⏱️ [Stat] name=%s (dir via list), listObjects elapsed=%v", name, time.Since(listStart))
		return &FileInfo{
			name:     name,
			size:     folderSize,
			updated:  time.Now(),
			isDir:    true,
			fileMode: defaultFileMode,
		}, nil
	}

	log.Printf("⏱️ [Stat] name=%s, not found, total elapsed=%v", name, time.Since(start))
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

func (fs *Fs) normSeparators(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\\", fs.separator), "/", fs.separator)
}

func (fs *Fs) ensureNoLeadingSeparator(s string) string {
	return strings.TrimLeft(s, fs.separator)
}

func (fs *Fs) normalizeName(name string) string {
	name = fs.ensureNoLeadingSeparator(fs.normSeparators(name))
	if name == "." {
		return ""
	}
	return strings.TrimSuffix(name, fs.separator)
}

func (fs *Fs) objectKey(name string) string {
	name = fs.normalizeName(name)
	if fs.options.Prefix == "" {
		return name
	}
	if name == "" {
		return fs.options.Prefix
	}
	return fs.options.Prefix + fs.separator + name
}

func (fs *Fs) dirObjectKey(name string) string {
	key := fs.objectKey(name)
	if key == "" {
		return ""
	}
	if !strings.HasSuffix(key, fs.separator) {
		key += fs.separator
	}
	return key
}

func (fs *Fs) listPrefix(name string) string {
	name = fs.normalizeName(name)
	if name == "" {
		if fs.options.Prefix == "" {
			return ""
		}
		return fs.options.Prefix + fs.separator
	}
	return fs.dirObjectKey(name)
}

func (fs *Fs) logicalNameFromKey(key string) string {
	key = strings.TrimPrefix(key, fs.options.Prefix)
	key = strings.TrimPrefix(key, fs.separator)
	return strings.TrimSuffix(key, fs.separator)
}

func (fs *Fs) operationContext() (context.Context, context.CancelFunc) {
	if fs.options.OperationTimeout <= 0 {
		return fs.ctx, func() {}
	}
	return context.WithTimeout(fs.ctx, fs.options.OperationTimeout)
}

func (fs *Fs) listObjects(prefix string, recursive bool, maxKeys int) (<-chan minio.ObjectInfo, context.CancelFunc) {
	opCtx, cancel := fs.operationContext()
	return fs.client.ListObjects(opCtx, fs.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: recursive,
		MaxKeys:   maxKeys,
	}), cancel
}

func (fs *Fs) statObjectByKey(key string) (minio.ObjectInfo, error) {
	return fs.statObjectByKeyWithOptions(key, minio.StatObjectOptions{})
}

func (fs *Fs) statObjectByKeyWithOptions(key string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	start := time.Now()
	opCtx, cancel := fs.operationContext()
	defer cancel()

	info, err := fs.client.StatObject(opCtx, fs.bucket, key, opts)
	if err != nil {
		mappedErr := mapMinioError(err)
		log.Printf("⏱️ [StatObject] key=%s, elapsed=%v, err=%v", key, time.Since(start), mappedErr)
		return minio.ObjectInfo{}, mappedErr
	}
	log.Printf("⏱️ [StatObject] key=%s, size=%d, elapsed=%v", key, info.Size, time.Since(start))
	return info, nil
}

func (fs *Fs) createEmptyObject(key string, contentType string) error {
	if key == "" {
		return nil
	}
	start := time.Now()
	opCtx, cancel := fs.operationContext()
	defer cancel()

	_, err := fs.client.PutObject(opCtx, fs.bucket, key, strings.NewReader(""), 0, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		log.Printf("⏱️ [CreateEmptyObject] key=%s, elapsed=%v, err=%v", key, time.Since(start), err)
		return mapMinioError(err)
	}
	log.Printf("⏱️ [CreateEmptyObject] key=%s, elapsed=%v", key, time.Since(start))
	return nil
}

func (fs *Fs) putEmptyObject(name string, contentType string) error {
	return fs.createEmptyObject(fs.objectKey(name), contentType)
}

func (fs *Fs) removeObjectByKey(key string) error {
	start := time.Now()
	opCtx, cancel := fs.operationContext()
	defer cancel()

	err := fs.client.RemoveObject(opCtx, fs.bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		log.Printf("⏱️ [RemoveObject] key=%s, elapsed=%v, err=%v", key, time.Since(start), err)
		return mapMinioError(err)
	}
	log.Printf("⏱️ [RemoveObject] key=%s, elapsed=%v", key, time.Since(start))
	return nil
}

func (fs *Fs) copyObject(srcKey, dstKey string) error {
	start := time.Now()
	opCtx, cancel := fs.operationContext()
	defer cancel()

	_, err := fs.client.CopyObject(opCtx, minio.CopyDestOptions{
		Bucket: fs.bucket,
		Object: dstKey,
	}, minio.CopySrcOptions{
		Bucket: fs.bucket,
		Object: srcKey,
	})
	if err != nil {
		log.Printf("⏱️ [CopyObject] src=%s, dst=%s, elapsed=%v, err=%v", srcKey, dstKey, time.Since(start), err)
		return mapMinioError(err)
	}
	log.Printf("⏱️ [CopyObject] src=%s, dst=%s, elapsed=%v", srcKey, dstKey, time.Since(start))
	return nil
}

func (fs *Fs) collectRemovalKeys(pathName string) ([]string, error) {
	keySet := make(map[string]struct{})

	if pathName == "" {
		objects, cancel := fs.listObjects(fs.listPrefix(""), true, 0)
		defer cancel()

		for object := range objects {
			if object.Err != nil {
				return nil, mapMinioError(object.Err)
			}
			keySet[object.Key] = struct{}{}
		}
		return mapKeys(keySet), nil
	}

	objectKey := fs.objectKey(pathName)
	if _, err := fs.statObjectByKey(objectKey); err == nil {
		keySet[objectKey] = struct{}{}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	dirKey := fs.dirObjectKey(pathName)
	if dirKey != "" {
		if _, err := fs.statObjectByKey(dirKey); err == nil {
			keySet[dirKey] = struct{}{}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		objects, cancel := fs.listObjects(dirKey, true, 0)
		defer cancel()

		for object := range objects {
			if object.Err != nil {
				return nil, mapMinioError(object.Err)
			}
			keySet[object.Key] = struct{}{}
		}
	}

	return mapKeys(keySet), nil
}

func (fs *Fs) listChildren(name string, maxKeys int) ([]minio.ObjectInfo, error) {
	prefix := fs.listPrefix(name)
	objects, cancel := fs.listObjects(prefix, false, maxKeys)
	defer cancel()

	var result []minio.ObjectInfo
	for object := range objects {
		if object.Err != nil {
			return nil, mapMinioError(object.Err)
		}
		if object.Key == prefix {
			continue
		}
		result = append(result, object)
	}
	return result, nil
}

func (fs *Fs) getObjectReader(name string, opts minio.GetObjectOptions) (*minio.Object, context.CancelFunc, error) {
	start := time.Now()
	opCtx, cancel := fs.operationContext()

	key := fs.objectKey(name)
	reader, err := fs.client.GetObject(opCtx, fs.bucket, key, opts)
	if err != nil {
		cancel()
		log.Printf("⏱️ [GetObject] key=%s, elapsed=%v, err=%v", key, time.Since(start), err)
		return nil, func() {}, mapMinioError(err)
	}
	log.Printf("⏱️ [GetObject] key=%s, elapsed=%v", key, time.Since(start))
	return reader, cancel, nil
}

func (fs *Fs) putObject(name string, reader io.Reader, size int64, opts minio.PutObjectOptions) error {
	return fs.putObjectByKey(fs.objectKey(name), reader, size, opts)
}

func (fs *Fs) putObjectByKey(key string, reader io.Reader, size int64, opts minio.PutObjectOptions) error {
	start := time.Now()
	opCtx, cancel := fs.operationContext()
	defer cancel()

	objInfo, err := fs.client.PutObject(opCtx, fs.bucket, key, reader, size, opts)
	if err != nil {
		log.Printf("⏱️ [PutObject] key=%s, size=%d, elapsed=%v, err=%v", key, size, time.Since(start), err)
		return mapMinioError(err)
	}
	log.Printf("⏱️ [PutObject] key=%s, size=%d, uploaded=%d, elapsed=%v", key, size, objInfo.Size, time.Since(start))
	return nil
}

func (fs *Fs) appendObject(name string, reader io.Reader, size int64) error {
	start := time.Now()
	opCtx, cancel := fs.operationContext()
	defer cancel()

	opts := minio.AppendObjectOptions{}
	if fs.options.NativeAppendChunkSize > 0 && size > int64(fs.options.NativeAppendChunkSize) {
		opts.ChunkSize = fs.options.NativeAppendChunkSize
	}
	appendClient := fs.client
	if fs.appendClient != nil {
		appendClient = fs.appendClient
	}
	key := fs.objectKey(name)
	objInfo, err := appendClient.AppendObject(opCtx, fs.bucket, key, reader, size, opts)
	if err != nil {
		log.Printf("⏱️ [AppendObject] key=%s, size=%d, elapsed=%v, err=%v", key, size, time.Since(start), err)
		return mapMinioError(err)
	}
	log.Printf("⏱️ [AppendObject] key=%s, size=%d, uploaded=%d, elapsed=%v", key, size, objInfo.Size, time.Since(start))
	return nil
}

func (fs *Fs) composeObjects(dst string, srcs ...minio.CopySrcOptions) error {
	return fs.composeObjectByKey(fs.objectKey(dst), srcs...)
}

func (fs *Fs) composeObjectByKey(dstKey string, srcs ...minio.CopySrcOptions) error {
	start := time.Now()
	opCtx, cancel := fs.operationContext()
	defer cancel()

	srcKeys := make([]string, len(srcs))
	for i, src := range srcs {
		srcKeys[i] = src.Object
	}

	_, err := fs.client.ComposeObject(opCtx, minio.CopyDestOptions{
		Bucket: fs.bucket,
		Object: dstKey,
	}, srcs...)
	if err != nil {
		log.Printf("⏱️ [ComposeObject] dst=%s, srcs=%v, elapsed=%v, err=%v", dstKey, srcKeys, time.Since(start), err)
		return mapMinioError(err)
	}
	log.Printf("⏱️ [ComposeObject] dst=%s, srcs=%v, elapsed=%v", dstKey, srcKeys, time.Since(start))
	return nil
}

func mapKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	return out
}

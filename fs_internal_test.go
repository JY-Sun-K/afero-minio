package miniofs

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/afero"
)

var _ io.ReaderFrom = (*MinioFile)(nil)

func TestReadFileUsesLiveGetObjectContext(t *testing.T) {
	transport := newFakeS3Transport()
	transport.setObject("unit-test-bucket", "hello.txt", []byte("hello world"))

	opts := DefaultOptions()
	opts.OperationTimeout = time.Second

	fs := newFakeTransportFs(t, context.Background(), transport, opts)

	data, err := afero.ReadFile(fs, "hello.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("unexpected data %q", string(data))
	}
}

func TestListObjectsUsesOperationTimeout(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	transport := newFakeS3Transport()
	transport.blockListUntilContextDone = true

	opts := DefaultOptions()
	opts.OperationTimeout = 20 * time.Millisecond

	fs := newFakeTransportFs(t, parentCtx, transport, opts)
	objects, cancel := fs.listObjects("", false, 1)
	defer cancel()

	select {
	case object, ok := <-objects:
		if !ok {
			t.Fatal("expected listObjects to emit a timeout error")
		}
		if object.Err == nil {
			t.Fatal("expected timeout error from listObjects")
		}
	case <-time.After(250 * time.Millisecond):
		cancelParent()
		t.Fatal("listObjects did not honor OperationTimeout")
	}
}

func TestResourceCloseSyncFailureRetainsStagingState(t *testing.T) {
	transport := newFakeS3Transport()
	transport.putStatus = http.StatusInternalServerError

	fs := newFakeTransportFs(t, context.Background(), transport, DefaultOptions())

	tmpFile, err := os.CreateTemp("", "miniofs-stage-*")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	tempPath := tmpFile.Name()
	t.Cleanup(func() {
		_ = tmpFile.Close()
		_ = os.Remove(tempPath)
	})

	if _, err := tmpFile.WriteString("staged payload"); err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	resource := &minioFileResource{
		fs:          fs,
		name:        "staged.txt",
		tempFile:    tmpFile,
		tempPath:    tempPath,
		tempDirty:   true,
		currentSize: int64(len("staged payload")),
		sizeKnown:   true,
	}

	err = resource.Close()
	if err == nil {
		t.Fatal("expected Close to surface sync failure")
	}
	if resource.closed {
		t.Fatal("resource should remain open after sync failure")
	}
	if _, statErr := os.Stat(tempPath); statErr != nil {
		t.Fatalf("expected temp file to remain for retry/recovery, got %v", statErr)
	}
}

func TestReaddirAdvancesCursorAndReturnsEOF(t *testing.T) {
	transport := newFakeS3Transport()
	transport.setObject("unit-test-bucket", "dir/", []byte{})
	transport.setObject("unit-test-bucket", "dir/a.txt", []byte("a"))
	transport.setObject("unit-test-bucket", "dir/b.txt", []byte("b"))

	fs := newFakeTransportFs(t, context.Background(), transport, DefaultOptions())

	f, err := fs.Open("dir")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	first, err := f.Readdir(1)
	if err != nil {
		t.Fatalf("first Readdir failed: %v", err)
	}
	if len(first) != 1 || first[0].Name() != "a.txt" {
		t.Fatalf("unexpected first Readdir result: %#v", first)
	}

	second, err := f.Readdir(1)
	if err != nil {
		t.Fatalf("second Readdir failed: %v", err)
	}
	if len(second) != 1 || second[0].Name() != "b.txt" {
		t.Fatalf("unexpected second Readdir result: %#v", second)
	}

	last, err := f.Readdir(1)
	if err != io.EOF {
		t.Fatalf("expected io.EOF at end of directory, got %v", err)
	}
	if len(last) != 0 {
		t.Fatalf("expected no entries at EOF, got %#v", last)
	}
}

func TestRemoveDoesNotSendGovernanceBypassByDefault(t *testing.T) {
	transport := newFakeS3Transport()
	transport.setObject("unit-test-bucket", "delete-me.txt", []byte("payload"))

	fs := newFakeTransportFs(t, context.Background(), transport, DefaultOptions())

	if err := fs.Remove("delete-me.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if got := transport.lastDeleteHeaders.Get("X-Amz-Bypass-Governance-Retention"); got != "" {
		t.Fatalf("expected delete without governance bypass header, got %q", got)
	}
}

func TestCopyUsesReadFromToAggregateSequentialWrites(t *testing.T) {
	transport := newFakeS3Transport()

	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 8 << 20
	opts.AppendStrategy = AppendStrategyNative
	opts.AssumeNativeAppendSupported = true
	opts.StreamChunkSize = 5 << 20

	fs := newFakeTransportFs(t, context.Background(), transport, opts)

	f, err := fs.OpenFile("stream.bin", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	transport.resetCounts()

	src := &chunkedLiteralReader{
		remaining: 13 << 20,
		chunkSize: 32 << 10,
		fill:      'a',
	}
	n, err := io.Copy(f, src)
	if err != nil {
		t.Fatalf("io.Copy failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if n != 13<<20 {
		t.Fatalf("expected to copy 13 MiB, got %d", n)
	}

	stats := transport.snapshotCounts()
	if stats.puts != 1 {
		t.Fatalf("expected single initial put, got %+v", stats)
	}
	if stats.appends != 2 {
		t.Fatalf("expected two append requests, got %+v", stats)
	}
	if stats.gets != 0 {
		t.Fatalf("expected no object rewrite reads, got %+v", stats)
	}
}

func TestCopyAtEOFPrefersNativeAppendOverDirectRewrite(t *testing.T) {
	transport := newFakeS3Transport()

	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 8 << 20
	opts.AppendStrategy = AppendStrategyNative
	opts.AssumeNativeAppendSupported = true
	opts.StreamChunkSize = 5 << 20

	fs := newFakeTransportFs(t, context.Background(), transport, opts)

	initial := bytes.Repeat([]byte("n"), 6<<20)
	if err := afero.WriteFile(fs, "append.bin", initial, 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	transport.resetCounts()

	f, err := fs.OpenFile("append.bin", os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := f.Seek(int64(len(initial)), io.SeekStart); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	src := &chunkedLiteralReader{
		remaining: 1 << 20,
		chunkSize: 32 << 10,
		fill:      't',
	}
	n, err := io.Copy(f, src)
	if err != nil {
		t.Fatalf("io.Copy failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if n != 1<<20 {
		t.Fatalf("expected to copy 1 MiB, got %d", n)
	}

	stats := transport.snapshotCounts()
	if stats.puts != 0 {
		t.Fatalf("expected no direct rewrite put, got %+v", stats)
	}
	if stats.appends != 1 {
		t.Fatalf("expected single native append request, got %+v", stats)
	}
	if stats.gets != 0 {
		t.Fatalf("expected no readback for append, got %+v", stats)
	}
}

func TestCopyAtNonEOFUsesCompatibleWritePath(t *testing.T) {
	transport := newFakeS3Transport()

	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 4
	opts.LargeObjectStrategy = LargeObjectStrategyTempFile
	opts.StreamChunkSize = 5 << 20

	fs := newFakeTransportFs(t, context.Background(), transport, opts)

	if err := afero.WriteFile(fs, "mutate.bin", []byte("abcdefgh"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, err := fs.OpenFile("mutate.bin", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := f.Seek(1, io.SeekStart); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	src := &chunkedByteSliceReader{
		data:      []byte("ZZZ"),
		chunkSize: 1,
	}
	if _, err := io.Copy(f, src); err != nil {
		t.Fatalf("io.Copy failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	data, err := afero.ReadFile(fs, "mutate.bin")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(data) != "aZZZefgh" {
		t.Fatalf("unexpected mutated data %q", string(data))
	}
}

func TestNewFsWithOptionsSingleHTTPClientFallsBackFromNativeAppend(t *testing.T) {
	transport := newFakeS3Transport()

	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 4
	opts.AppendStrategy = AppendStrategyNative
	opts.AssumeNativeAppendSupported = true
	opts.LargeObjectStrategy = LargeObjectStrategyTempFile

	fs := newFakeProvidedClientFs(t, context.Background(), transport, opts)

	largePrefix := bytes.Repeat([]byte("a"), 1024)
	if err := afero.WriteFile(fs, "append-single-client.bin", largePrefix, 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	transport.resetCounts()

	f, err := fs.OpenFile("append-single-client.bin", os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := f.Write([]byte("tail")); err != nil {
		t.Fatalf("append write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	stats := transport.snapshotCounts()
	if stats.appends != 0 {
		t.Fatalf("expected single HTTP client path to avoid native append, got %+v", stats)
	}
}

func TestNewFsWithClientsRoutesAppendRequestsToDedicatedAppendClient(t *testing.T) {
	backend := newFakeS3Transport()
	baseRecorder := newRecordingTransport(backend)
	appendRecorder := newRecordingTransport(backend)

	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 4
	opts.AppendStrategy = AppendStrategyNative
	opts.AssumeNativeAppendSupported = true

	client := newFakeMinioClient(t, baseRecorder)
	appendClient := newFakeAppendMinioClient(t, appendRecorder, opts)

	fs, err := NewFsWithClients(context.Background(), client, appendClient, "unit-test-bucket", opts)
	if err != nil {
		t.Fatalf("NewFsWithClients failed: %v", err)
	}
	if fs.appendClient == nil {
		t.Fatal("expected dedicated append client to be configured")
	}

	largePrefix := bytes.Repeat([]byte("a"), int(minComposePartSize))
	if err := afero.WriteFile(fs, "append-dedicated-client.bin", largePrefix, 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, err := fs.OpenFile("append-dedicated-client.bin", os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := f.Write([]byte("tail")); err != nil {
		t.Fatalf("append write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if appendRecorder.snapshot().appendPuts != 1 {
		t.Fatalf("expected dedicated append client to handle append request, got %+v", appendRecorder.snapshot())
	}
	if baseRecorder.snapshot().appendPuts != 0 {
		t.Fatalf("expected base client to avoid append request, got %+v", baseRecorder.snapshot())
	}
}

func newFakeTransportFs(t *testing.T, ctx context.Context, transport http.RoundTripper, opts Options) *Fs {
	t.Helper()

	client := newFakeMinioClient(t, transport)

	var appendClient *minio.Client
	if shouldCreateDedicatedAppendClient(opts) {
		appendClient = newFakeAppendMinioClient(t, transport, opts)
	}

	fs, err := NewFsWithClients(ctx, client, appendClient, "unit-test-bucket", opts)
	if err != nil {
		t.Fatalf("NewFsWithClients failed: %v", err)
	}
	return fs
}

func newFakeProvidedClientFs(t *testing.T, ctx context.Context, transport http.RoundTripper, opts Options) *Fs {
	t.Helper()

	client, err := minio.New("fake.minio.local", &minio.Options{
		Creds:        credentials.NewStaticV4("test-access", "test-secret", ""),
		Secure:       false,
		Region:       "us-east-1",
		BucketLookup: minio.BucketLookupPath,
		Transport:    transport,
	})
	if err != nil {
		t.Fatalf("minio.New failed: %v", err)
	}

	fs, err := NewFsWithOptions(ctx, client, "unit-test-bucket", opts)
	if err != nil {
		t.Fatalf("NewFsWithOptions failed: %v", err)
	}
	return fs
}

func newFakeMinioClient(t *testing.T, transport http.RoundTripper) *minio.Client {
	t.Helper()

	client, err := minio.New("fake.minio.local", &minio.Options{
		Creds:        credentials.NewStaticV4("test-access", "test-secret", ""),
		Secure:       false,
		Region:       "us-east-1",
		BucketLookup: minio.BucketLookupPath,
		Transport:    transport,
	})
	if err != nil {
		t.Fatalf("minio.New failed: %v", err)
	}
	return client
}

func newFakeAppendMinioClient(t *testing.T, transport http.RoundTripper, opts Options) *minio.Client {
	t.Helper()

	minioOpts := &minio.Options{
		Creds:        credentials.NewStaticV4("test-access", "test-secret", ""),
		Secure:       false,
		Region:       "us-east-1",
		BucketLookup: minio.BucketLookupPath,
		Transport:    transport,
	}
	applyAppendOptionsToMinioClient(minioOpts, opts)
	minioOpts.TrailingHeaders = true

	client, err := minio.New("fake.minio.local", minioOpts)
	if err != nil {
		t.Fatalf("minio.New append client failed: %v", err)
	}
	return client
}

type fakeS3Transport struct {
	mu                        sync.Mutex
	objects                   map[string][]byte
	putStatus                 int
	lastDeleteHeaders         http.Header
	blockListUntilContextDone bool
	putCount                  int
	appendCount               int
	getCount                  int
	headCount                 int
}

func newFakeS3Transport() *fakeS3Transport {
	return &fakeS3Transport{
		objects: make(map[string][]byte),
	}
}

func (t *fakeS3Transport) setObject(bucket, key string, data []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.objects[bucket+"/"+key] = append([]byte(nil), data...)
}

func (t *fakeS3Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}

	if req.URL.Query().Has("location") {
		return fakeHTTPResponse(req, http.StatusOK, "<LocationConstraint>us-east-1</LocationConstraint>", nil), nil
	}

	bucket, key := splitBucketAndKey(req.URL.Path)
	if bucket == "" {
		return fakeErrorResponse(req, http.StatusBadRequest, "InvalidBucketName"), nil
	}

	if req.URL.Query().Get("list-type") == "2" {
		if t.blockListUntilContextDone {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		return fakeHTTPResponse(req, http.StatusOK, t.listBucketV2Result(bucket, req.URL.Query().Get("prefix"), req.URL.Query().Get("delimiter")), map[string]string{
			"Content-Type": "application/xml",
		}), nil
	}

	switch req.Method {
	case http.MethodHead:
		t.incrementHeadCount()
		data, ok := t.getObject(bucket, key)
		if !ok {
			return fakeErrorResponse(req, http.StatusNotFound, "NoSuchKey"), nil
		}
		return fakeHTTPResponse(req, http.StatusOK, "", fakeObjectHeaders(len(data))), nil
	case http.MethodGet:
		t.incrementGetCount()
		data, ok := t.getObject(bucket, key)
		if !ok {
			return fakeErrorResponse(req, http.StatusNotFound, "NoSuchKey"), nil
		}
		body, status, headers, err := applyRange(data, req.Header.Get("Range"))
		if err != nil {
			return nil, err
		}
		for k, v := range fakeObjectHeaders(len(data)) {
			headers[k] = v
		}
		headers["Content-Length"] = fmt.Sprintf("%d", len(body))
		return fakeHTTPResponse(req, status, string(body), headers), nil
	case http.MethodPut:
		if t.putStatus != 0 {
			return fakeErrorResponse(req, t.putStatus, "InternalError"), nil
		}
		payload, err := readRequestPayload(req)
		if err != nil {
			return nil, err
		}
		if offsetHeader := req.Header.Get("X-Amz-Write-Offset-Bytes"); offsetHeader != "" {
			size, err := t.appendObject(bucket, key, offsetHeader, payload)
			if err != nil {
				return nil, err
			}
			return fakeHTTPResponse(req, http.StatusOK, "", map[string]string{
				"ETag":              `"etag"`,
				"x-amz-object-size": fmt.Sprintf("%d", size),
			}), nil
		}
		t.incrementPutCount()
		t.setObject(bucket, key, payload)
		return fakeHTTPResponse(req, http.StatusOK, "", map[string]string{
			"ETag": `"etag"`,
		}), nil
	case http.MethodDelete:
		t.recordDeleteHeaders(req.Header)
		t.deleteObject(bucket, key)
		return fakeHTTPResponse(req, http.StatusNoContent, "", nil), nil
	default:
		return fakeErrorResponse(req, http.StatusNotImplemented, "NotImplemented"), nil
	}
}

func (t *fakeS3Transport) appendObject(bucket, key, offsetHeader string, payload []byte) (int, error) {
	offset, err := strconv.ParseInt(offsetHeader, 10, 64)
	if err != nil {
		return 0, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	fullKey := bucket + "/" + key
	current := append([]byte(nil), t.objects[fullKey]...)
	if int64(len(current)) != offset {
		return 0, fmt.Errorf("unexpected append offset %d for %s with size %d", offset, key, len(current))
	}
	t.appendCount++
	current = append(current, payload...)
	t.objects[fullKey] = current
	return len(current), nil
}

func (t *fakeS3Transport) getObject(bucket, key string) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	data, ok := t.objects[bucket+"/"+key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), data...), true
}

func (t *fakeS3Transport) deleteObject(bucket, key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.objects, bucket+"/"+key)
}

func (t *fakeS3Transport) recordDeleteHeaders(header http.Header) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastDeleteHeaders = header.Clone()
}

func (t *fakeS3Transport) incrementPutCount() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.putCount++
}

func (t *fakeS3Transport) incrementGetCount() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.getCount++
}

func (t *fakeS3Transport) incrementHeadCount() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.headCount++
}

func (t *fakeS3Transport) resetCounts() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.putCount = 0
	t.appendCount = 0
	t.getCount = 0
	t.headCount = 0
}

type requestCounts struct {
	puts    int
	appends int
	gets    int
	heads   int
}

type recordingTransport struct {
	base *fakeS3Transport
	mu   sync.Mutex
	reqs recordingCounts
}

type recordingCounts struct {
	puts       int
	appendPuts int
	gets       int
	heads      int
}

func newRecordingTransport(base *fakeS3Transport) *recordingTransport {
	return &recordingTransport{base: base}
}

func (t *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	switch req.Method {
	case http.MethodPut:
		if req.Header.Get("X-Amz-Write-Offset-Bytes") != "" {
			t.reqs.appendPuts++
		} else {
			t.reqs.puts++
		}
	case http.MethodGet:
		t.reqs.gets++
	case http.MethodHead:
		t.reqs.heads++
	}
	t.mu.Unlock()

	return t.base.RoundTrip(req)
}

func (t *recordingTransport) snapshot() recordingCounts {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reqs
}

func (t *fakeS3Transport) snapshotCounts() requestCounts {
	t.mu.Lock()
	defer t.mu.Unlock()
	return requestCounts{
		puts:    t.putCount,
		appends: t.appendCount,
		gets:    t.getCount,
		heads:   t.headCount,
	}
}

type chunkedLiteralReader struct {
	remaining int64
	chunkSize int
	fill      byte
}

func (r *chunkedLiteralReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.chunkSize {
		n = r.chunkSize
	}
	if int64(n) > r.remaining {
		n = int(r.remaining)
	}
	for i := 0; i < n; i++ {
		p[i] = r.fill
	}
	r.remaining -= int64(n)
	return n, nil
}

type chunkedByteSliceReader struct {
	data      []byte
	offset    int
	chunkSize int
}

func (r *chunkedByteSliceReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.chunkSize {
		n = r.chunkSize
	}
	remaining := len(r.data) - r.offset
	if n > remaining {
		n = remaining
	}
	copy(p[:n], r.data[r.offset:r.offset+n])
	r.offset += n
	return n, nil
}

func splitBucketAndKey(path string) (string, string) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", ""
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func fakeObjectHeaders(size int) map[string]string {
	return map[string]string{
		"Content-Length":        fmt.Sprintf("%d", size),
		"Content-Type":          "application/octet-stream",
		"ETag":                  `"etag"`,
		"Last-Modified":         time.Unix(0, 0).UTC().Format(http.TimeFormat),
		"X-Amz-Checksum-Crc32c": "AAAAAA==",
		"X-Amz-Checksum-Type":   minio.ChecksumFullObjectMode.String(),
	}
}

func readRequestPayload(req *http.Request) ([]byte, error) {
	payload, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	if req.Header.Get("X-Amz-Decoded-Content-Length") == "" &&
		!strings.Contains(strings.ToLower(req.Header.Get("Content-Encoding")), "aws-chunked") {
		return payload, nil
	}
	return decodeAWSChunkedPayload(payload)
}

func decodeAWSChunkedPayload(payload []byte) ([]byte, error) {
	rest := payload
	decoded := make([]byte, 0, len(payload))

	for {
		lineEnd := bytes.Index(rest, []byte("\r\n"))
		if lineEnd < 0 {
			return nil, fmt.Errorf("invalid aws-chunked payload: missing chunk header terminator")
		}

		header := string(rest[:lineEnd])
		rest = rest[lineEnd+2:]

		sizeField := header
		if idx := strings.IndexByte(sizeField, ';'); idx >= 0 {
			sizeField = sizeField[:idx]
		}

		size, err := strconv.ParseInt(strings.TrimSpace(sizeField), 16, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid aws-chunked payload: %w", err)
		}
		if size == 0 {
			return decoded, nil
		}
		if int64(len(rest)) < size+2 {
			return nil, fmt.Errorf("invalid aws-chunked payload: short chunk body")
		}

		decoded = append(decoded, rest[:size]...)
		rest = rest[size:]
		if len(rest) < 2 || rest[0] != '\r' || rest[1] != '\n' {
			return nil, fmt.Errorf("invalid aws-chunked payload: missing chunk terminator")
		}
		rest = rest[2:]
	}
}

func (t *fakeS3Transport) listBucketV2Result(bucket, prefix, delimiter string) string {
	type listContents struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int    `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	}
	type commonPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	type listBucketResult struct {
		XMLName        xml.Name       `xml:"ListBucketResult"`
		Name           string         `xml:"Name"`
		Prefix         string         `xml:"Prefix"`
		Delimiter      string         `xml:"Delimiter,omitempty"`
		MaxKeys        int            `xml:"MaxKeys"`
		IsTruncated    bool           `xml:"IsTruncated"`
		Contents       []listContents `xml:"Contents"`
		CommonPrefixes []commonPrefix `xml:"CommonPrefixes,omitempty"`
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	keys := make([]string, 0, len(t.objects))
	for fullKey := range t.objects {
		if strings.HasPrefix(fullKey, bucket+"/") {
			keys = append(keys, strings.TrimPrefix(fullKey, bucket+"/"))
		}
	}
	sort.Strings(keys)

	result := listBucketResult{
		Name:        bucket,
		Prefix:      prefix,
		Delimiter:   delimiter,
		MaxKeys:     len(keys),
		IsTruncated: false,
	}
	seenPrefixes := map[string]struct{}{}
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		rest := strings.TrimPrefix(key, prefix)
		if delimiter != "" && rest != "" {
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				common := prefix + rest[:idx+1]
				if _, ok := seenPrefixes[common]; !ok {
					seenPrefixes[common] = struct{}{}
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix{Prefix: common})
				}
				continue
			}
		}

		result.Contents = append(result.Contents, listContents{
			Key:          key,
			LastModified: time.Unix(0, 0).UTC().Format(time.RFC3339),
			ETag:         "&quot;etag&quot;",
			Size:         len(t.objects[bucket+"/"+key]),
			StorageClass: "STANDARD",
		})
	}

	out, err := xml.Marshal(result)
	if err != nil {
		panic(err)
	}
	return string(out)
}

func applyRange(data []byte, header string) ([]byte, int, map[string]string, error) {
	headers := map[string]string{
		"Content-Type":   "application/octet-stream",
		"Content-Length": fmt.Sprintf("%d", len(data)),
	}
	if header == "" {
		return data, http.StatusOK, headers, nil
	}

	var start, end int
	if _, err := fmt.Sscanf(header, "bytes=%d-%d", &start, &end); err != nil {
		return nil, 0, nil, err
	}
	if end == 0 || end >= len(data) {
		end = len(data) - 1
	}
	if start < 0 || start >= len(data) || start > end {
		return nil, 0, nil, fmt.Errorf("invalid range %q", header)
	}

	body := data[start : end+1]
	headers["Content-Length"] = fmt.Sprintf("%d", len(body))
	headers["Content-Range"] = fmt.Sprintf("bytes %d-%d/%d", start, end, len(data))
	return body, http.StatusPartialContent, headers, nil
}

func fakeErrorResponse(req *http.Request, status int, code string) *http.Response {
	body := fmt.Sprintf("<Error><Code>%s</Code><Message>%s</Message></Error>", code, code)
	return fakeHTTPResponse(req, status, body, map[string]string{
		"Content-Type": "application/xml",
	})
}

func fakeHTTPResponse(req *http.Request, status int, body string, headers map[string]string) *http.Response {
	header := make(http.Header, len(headers))
	for k, v := range headers {
		header.Set(k, v)
	}

	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     header,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Request:    req,
	}
}

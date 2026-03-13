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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/afero"
)

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

func newFakeTransportFs(t *testing.T, ctx context.Context, transport http.RoundTripper, opts Options) *Fs {
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

type fakeS3Transport struct {
	mu                        sync.Mutex
	objects                   map[string][]byte
	putStatus                 int
	lastDeleteHeaders         http.Header
	blockListUntilContextDone bool
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
		data, ok := t.getObject(bucket, key)
		if !ok {
			return fakeErrorResponse(req, http.StatusNotFound, "NoSuchKey"), nil
		}
		return fakeHTTPResponse(req, http.StatusOK, "", map[string]string{
			"Content-Length": fmt.Sprintf("%d", len(data)),
			"Content-Type":   "application/octet-stream",
			"ETag":           `"etag"`,
			"Last-Modified":  time.Unix(0, 0).UTC().Format(http.TimeFormat),
		}), nil
	case http.MethodGet:
		data, ok := t.getObject(bucket, key)
		if !ok {
			return fakeErrorResponse(req, http.StatusNotFound, "NoSuchKey"), nil
		}
		body, status, headers, err := applyRange(data, req.Header.Get("Range"))
		if err != nil {
			return nil, err
		}
		headers["ETag"] = `"etag"`
		headers["Last-Modified"] = time.Unix(0, 0).UTC().Format(http.TimeFormat)
		return fakeHTTPResponse(req, status, string(body), headers), nil
	case http.MethodPut:
		if t.putStatus != 0 {
			return fakeErrorResponse(req, t.putStatus, "InternalError"), nil
		}
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
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

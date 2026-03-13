package miniofs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
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
		return fakeHTTPResponse(req, http.StatusOK, emptyListBucketV2Result(bucket, req.URL.Query().Get("prefix")), map[string]string{
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

func emptyListBucketV2Result(bucket, prefix string) string {
	return fmt.Sprintf(
		"<ListBucketResult><Name>%s</Name><Prefix>%s</Prefix><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>",
		bucket,
		prefix,
	)
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

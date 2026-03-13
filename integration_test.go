package miniofs

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/afero"
)

func getIntegrationFs(t *testing.T) afero.Fs {
	t.Helper()
	return getIntegrationFsWithOptions(t, DefaultOptions())
}

func getIntegrationFsWithOptions(t *testing.T, opts Options) afero.Fs {
	t.Helper()

	dsn := os.Getenv("MINIOFS_TEST_DSN")
	if dsn == "" {
		t.Skip("MINIOFS_TEST_DSN is not set")
	}

	ensureTestBucket(t, dsn)

	fs, err := NewMinioFsWithOptions(context.Background(), dsn, opts)
	if err != nil {
		t.Fatalf("NewMinioFsWithOptions failed: %v", err)
	}

	return fs
}

func requireNativeAppendIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("MINIOFS_TEST_NATIVE_APPEND") == "" {
		t.Skip("MINIOFS_TEST_NATIVE_APPEND is not set")
	}
}

func nativeAppendIntegrationOptions() Options {
	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 8 << 20
	opts.AppendStrategy = AppendStrategyNative
	opts.AssumeNativeAppendSupported = true
	opts.StreamChunkSize = uint64(minComposePartSize)
	opts.NativeAppendChunkSize = uint64(minComposePartSize)
	return opts
}

func TestIntegrationCreateEmptyObject(t *testing.T) {
	fs := getIntegrationFs(t)
	const name = "integration-empty-object.txt"

	t.Cleanup(func() {
		_ = fs.Remove(name)
	})

	f, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer f.Close()

	info, err := fs.Stat(name)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected empty object size, got %d", info.Size())
	}
}

func TestIntegrationLargeWriteFallback(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 4
	opts.LargeObjectStrategy = LargeObjectStrategyTempFile

	fs := getIntegrationFsWithOptions(t, opts)
	const name = "integration-large-write.txt"

	t.Cleanup(func() {
		_ = fs.Remove(name)
	})

	f, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	if _, err := f.Write([]byte("0123456789")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	data, err := afero.ReadFile(fs, name)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(data) != "0123456789" {
		t.Fatalf("unexpected object data %q", string(data))
	}
}

func TestIntegrationAppendSmallCompat(t *testing.T) {
	fs := getIntegrationFs(t)
	const name = "integration-append-small.txt"

	t.Cleanup(func() {
		_ = fs.Remove(name)
	})

	if err := afero.WriteFile(fs, name, []byte("abcd"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, err := fs.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := f.Write([]byte("ef")); err != nil {
		t.Fatalf("append write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	data, err := afero.ReadFile(fs, name)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("unexpected append result %q", string(data))
	}
}

func TestIntegrationAppendCompose(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 4
	opts.AppendStrategy = AppendStrategyCompose

	fs := getIntegrationFsWithOptions(t, opts)
	const name = "integration-append-compose.txt"
	largePrefix := strings.Repeat("a", int(minComposePartSize))

	t.Cleanup(func() {
		_ = fs.Remove(name)
	})

	if err := afero.WriteFile(fs, name, []byte(largePrefix), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, err := fs.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := f.Write([]byte("ef")); err != nil {
		t.Fatalf("compose append failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	data, err := afero.ReadFile(fs, name)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(data) != largePrefix+"ef" {
		t.Fatalf("unexpected append result length %d", len(data))
	}
}

func TestIntegrationLargeRandomWriteError(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 4
	opts.LargeObjectStrategy = LargeObjectStrategyError

	fs := getIntegrationFsWithOptions(t, opts)
	const name = "integration-large-random.txt"

	t.Cleanup(func() {
		_ = fs.Remove(name)
	})

	if err := afero.WriteFile(fs, name, []byte("abcd"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, err := fs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer f.Close()

	if _, err := f.WriteAt([]byte("ZZ"), 1); !errors.Is(err, ErrLargeWriteRequiresStaging) {
		t.Fatalf("expected large write strategy error, got %v", err)
	}
}

func TestIntegrationLargeRandomWriteComposeFallback(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 4
	opts.LargeObjectStrategy = LargeObjectStrategyCompose

	fs := getIntegrationFsWithOptions(t, opts)
	const name = "integration-large-random-compose.txt"

	t.Cleanup(func() {
		_ = fs.Remove(name)
	})

	if err := afero.WriteFile(fs, name, []byte("abcdefgh"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, err := fs.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	if _, err := f.WriteAt([]byte("ZZ"), 1); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	data, err := afero.ReadFile(fs, name)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(data) != "aZZdefgh" {
		t.Fatalf("unexpected compose fallback result %q", string(data))
	}
}

func TestIntegrationRemoveAllNamespaceSafety(t *testing.T) {
	fs := getIntegrationFs(t)

	t.Cleanup(func() {
		_ = fs.RemoveAll("integration-removeall")
		_ = fs.RemoveAll("integration-removeall-2")
	})

	if err := fs.MkdirAll("integration-removeall/sub", 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := fs.MkdirAll("integration-removeall-2/sub", 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := afero.WriteFile(fs, "integration-removeall/sub/a.txt", []byte("a"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := afero.WriteFile(fs, "integration-removeall-2/sub/b.txt", []byte("b"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if err := fs.RemoveAll("integration-removeall"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	if _, err := fs.Stat("integration-removeall/sub/a.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected removed path to disappear, got %v", err)
	}
	if _, err := fs.Stat("integration-removeall-2/sub/b.txt"); err != nil {
		t.Fatalf("expected sibling namespace to remain, got %v", err)
	}
}

func TestIntegrationRenameDirectory(t *testing.T) {
	fs := getIntegrationFs(t)

	t.Cleanup(func() {
		_ = fs.RemoveAll("integration-rename-old")
		_ = fs.RemoveAll("integration-rename-new")
	})

	if err := fs.MkdirAll("integration-rename-old/sub", 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := afero.WriteFile(fs, "integration-rename-old/a.txt", []byte("a"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := afero.WriteFile(fs, "integration-rename-old/sub/b.txt", []byte("b"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if err := fs.Rename("integration-rename-old", "integration-rename-new"); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	if _, err := fs.Stat("integration-rename-old"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected old directory to disappear, got %v", err)
	}
	if _, err := fs.Stat("integration-rename-new/a.txt"); err != nil {
		t.Fatalf("expected renamed file to exist, got %v", err)
	}
	if _, err := fs.Stat("integration-rename-new/sub/b.txt"); err != nil {
		t.Fatalf("expected nested renamed file to exist, got %v", err)
	}
}

func TestIntegrationRenameDirectoryRejectsExistingTarget(t *testing.T) {
	fs := getIntegrationFs(t)

	t.Cleanup(func() {
		_ = fs.RemoveAll("integration-rename-conflict-src")
		_ = fs.RemoveAll("integration-rename-conflict-dst")
	})

	if err := fs.MkdirAll("integration-rename-conflict-src/sub", 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := fs.MkdirAll("integration-rename-conflict-dst/existing", 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := afero.WriteFile(fs, "integration-rename-conflict-src/sub/a.txt", []byte("a"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := afero.WriteFile(fs, "integration-rename-conflict-dst/existing/b.txt", []byte("b"), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	err := fs.Rename("integration-rename-conflict-src", "integration-rename-conflict-dst")
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("expected rename conflict error, got %v", err)
	}

	if _, statErr := fs.Stat("integration-rename-conflict-src/sub/a.txt"); statErr != nil {
		t.Fatalf("expected source directory to remain intact, got %v", statErr)
	}
	if _, statErr := fs.Stat("integration-rename-conflict-dst/existing/b.txt"); statErr != nil {
		t.Fatalf("expected destination directory to remain intact, got %v", statErr)
	}
}

func TestIntegrationConcurrentHandlesObserveSyncedWrites(t *testing.T) {
	fs := getIntegrationFs(t)
	const name = "integration-concurrent-handles.txt"

	t.Cleanup(func() {
		_ = fs.Remove(name)
	})

	writer, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("OpenFile writer failed: %v", err)
	}
	defer writer.Close()

	reader, err := fs.Open(name)
	if err != nil {
		t.Fatalf("Open reader failed: %v", err)
	}
	defer reader.Close()

	if _, err := writer.WriteString("visible"); err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	buf := make([]byte, len("visible"))
	n, err := reader.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if string(buf[:n]) != "visible" {
		t.Fatalf("unexpected concurrent read data %q", string(buf[:n]))
	}
}

func TestIntegrationAppendNativeOptIn(t *testing.T) {
	requireNativeAppendIntegration(t)

	fs := getIntegrationFsWithOptions(t, nativeAppendIntegrationOptions())
	const name = "integration-append-native.txt"

	t.Cleanup(func() {
		_ = fs.Remove(name)
	})

	if err := afero.WriteFile(fs, name, []byte(strings.Repeat("n", int(minComposePartSize))), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, err := fs.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if _, err := f.Write([]byte("tail")); err != nil {
		t.Fatalf("native append failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestIntegrationCopyToEmptyObjectUsesReadFrom(t *testing.T) {
	requireNativeAppendIntegration(t)

	fs := getIntegrationFsWithOptions(t, nativeAppendIntegrationOptions())
	const name = "integration-copy-empty-native.txt"

	t.Cleanup(func() {
		_ = fs.Remove(name)
	})

	f, err := fs.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

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

	data, err := afero.ReadFile(fs, name)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(data) != 13<<20 {
		t.Fatalf("unexpected object size %d", len(data))
	}
	if data[0] != 'a' || data[len(data)-1] != 'a' {
		t.Fatalf("unexpected object boundaries %q...%q", data[:1], data[len(data)-1:])
	}
}

func TestIntegrationCopyAtEOFPrefersNativeAppend(t *testing.T) {
	requireNativeAppendIntegration(t)

	fs := getIntegrationFsWithOptions(t, nativeAppendIntegrationOptions())
	const name = "integration-copy-append-native.txt"
	initial := strings.Repeat("n", 6<<20)

	t.Cleanup(func() {
		_ = fs.Remove(name)
	})

	if err := afero.WriteFile(fs, name, []byte(initial), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, err := fs.OpenFile(name, os.O_WRONLY, 0o644)
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

	data, err := afero.ReadFile(fs, name)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(data) != len(initial)+(1<<20) {
		t.Fatalf("unexpected appended size %d", len(data))
	}
	if data[0] != 'n' || data[len(initial)] != 't' || data[len(data)-1] != 't' {
		t.Fatalf("unexpected append boundaries %q %q %q", data[:1], data[len(initial):len(initial)+1], data[len(data)-1:])
	}
}

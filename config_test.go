package miniofs

import (
	"errors"
	"net/http"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.WriteStrategy != WriteStrategyDirect {
		t.Fatalf("expected direct write strategy, got %q", opts.WriteStrategy)
	}

	if opts.LargeObjectStrategy != LargeObjectStrategyError {
		t.Fatalf("expected large object error strategy, got %q", opts.LargeObjectStrategy)
	}

	if opts.AppendStrategy != AppendStrategyCompat {
		t.Fatalf("expected compat append strategy, got %q", opts.AppendStrategy)
	}

	if opts.MaxDirectObjectSize <= 0 {
		t.Fatalf("expected positive MaxDirectObjectSize, got %d", opts.MaxDirectObjectSize)
	}
}

func TestOptionsWithDefaults(t *testing.T) {
	opts := (Options{}).withDefaults()

	if opts.WriteStrategy != WriteStrategyDirect {
		t.Fatalf("expected direct write strategy, got %q", opts.WriteStrategy)
	}

	if opts.LargeObjectStrategy != LargeObjectStrategyError {
		t.Fatalf("expected large object error strategy, got %q", opts.LargeObjectStrategy)
	}

	if opts.AppendStrategy != AppendStrategyCompat {
		t.Fatalf("expected compat append strategy, got %q", opts.AppendStrategy)
	}

	if opts.MaxDirectObjectSize != DefaultOptions().MaxDirectObjectSize {
		t.Fatalf("expected default MaxDirectObjectSize, got %d", opts.MaxDirectObjectSize)
	}
}

func TestOptionsWithDefaultsPreservesExplicitDirectoryMarkersFalse(t *testing.T) {
	opts := DefaultOptions()
	opts.DirectoryMarkers = false

	opts = opts.withDefaults()

	if opts.DirectoryMarkers {
		t.Fatal("expected explicit DirectoryMarkers=false to be preserved")
	}
}

func TestSelectWriteStrategy(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 8
	opts.LargeObjectStrategy = LargeObjectStrategyTempFile
	opts.AppendStrategy = AppendStrategyCompose

	mode, err := selectWriteStrategy(writePlan{
		options:      opts,
		currentSize:  4,
		targetOffset: 4,
		writeLen:     2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != writeModeDirect {
		t.Fatalf("expected direct write mode, got %q", mode)
	}

	mode, err = selectWriteStrategy(writePlan{
		options:      opts,
		currentSize:  minComposePartSize,
		targetOffset: minComposePartSize,
		writeLen:     4,
		appendOnly:   true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != writeModeComposeAppend {
		t.Fatalf("expected compose append mode, got %q", mode)
	}

	mode, err = selectWriteStrategy(writePlan{
		options:      opts,
		currentSize:  8,
		targetOffset: 2,
		writeLen:     4,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != writeModeTempFile {
		t.Fatalf("expected temp file mode, got %q", mode)
	}
}

func TestSelectWriteStrategyErrors(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxDirectObjectSize = 8
	opts.LargeObjectStrategy = LargeObjectStrategyError
	opts.AppendStrategy = AppendStrategyDisabled

	_, err := selectWriteStrategy(writePlan{
		options:      opts,
		currentSize:  8,
		targetOffset: 8,
		writeLen:     4,
		appendOnly:   true,
	})
	if !errors.Is(err, ErrAppendNotSupported) {
		t.Fatalf("expected append not supported error, got %v", err)
	}

	_, err = selectWriteStrategy(writePlan{
		options:      opts,
		currentSize:  8,
		targetOffset: 2,
		writeLen:     4,
	})
	if !errors.Is(err, ErrLargeWriteRequiresStaging) {
		t.Fatalf("expected large write strategy error, got %v", err)
	}
}

func TestMapMinioError(t *testing.T) {
	err := mapMinioError(minio.ErrorResponse{Code: "NoSuchKey"})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected not exist error, got %v", err)
	}

	accessDenied := minio.ErrorResponse{Code: "AccessDenied"}
	err = mapMinioError(accessDenied)
	var response minio.ErrorResponse
	if !errors.As(err, &response) || response.Code != "AccessDenied" {
		t.Fatalf("expected access denied response, got %v", err)
	}
}

func TestParseURLConfiguresInsecureTransport(t *testing.T) {
	minioOptions, err := ParseURL("https://access:secret@example.com/bucket?insecure=true")
	if err != nil {
		t.Fatalf("ParseURL returned error: %v", err)
	}
	if minioOptions.Transport == nil {
		t.Fatal("expected custom transport for insecure TLS")
	}
	if _, ok := minioOptions.Transport.(*http.Transport); !ok {
		t.Fatalf("expected *http.Transport, got %T", minioOptions.Transport)
	}
}

func TestParseURLRejectsInvalidInsecureValue(t *testing.T) {
	_, err := ParseURL("https://access:secret@example.com/bucket?insecure=definitely-not-bool")
	if err == nil {
		t.Fatal("expected invalid insecure parameter error")
	}
}

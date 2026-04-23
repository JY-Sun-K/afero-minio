package miniofs

import (
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

const defaultMaxDirectObjectSize int64 = 16 << 20
const minComposePartSize int64 = 5 << 20
const defaultStreamChunkSize uint64 = 5 << 20

type WriteStrategy string

const (
	WriteStrategyDirect  WriteStrategy = "direct"
	WriteStrategyStaging WriteStrategy = "staging"
)

type LargeObjectStrategy string

const (
	LargeObjectStrategyError    LargeObjectStrategy = "error"
	LargeObjectStrategyTempFile LargeObjectStrategy = "temp_file"
	LargeObjectStrategyCompose  LargeObjectStrategy = "compose"
)

type AppendStrategy string

const (
	AppendStrategyCompat   AppendStrategy = "compat"
	AppendStrategyCompose  AppendStrategy = "compose"
	AppendStrategyNative   AppendStrategy = "native"
	AppendStrategyDisabled AppendStrategy = "disabled"
)

type Options struct {
	Prefix                      string
	ValidateBucketOnInit        bool
	OperationTimeout            time.Duration
	BucketLookup                minio.BucketLookupType
	MaxRetries                  int
	Transport                   http.RoundTripper
	AppName                     string
	AppVersion                  string
	TraceOutput                 io.Writer
	DirectoryMarkers            bool
	WriteStrategy               WriteStrategy
	MaxDirectObjectSize         int64
	LargeObjectStrategy         LargeObjectStrategy
	TempDir                     string
	AppendStrategy              AppendStrategy
	AssumeNativeAppendSupported bool
	NativeAppendChunkSize       uint64
	StreamChunkSize             uint64
	OptimisticWriteOpen         bool
	StatCacheTTL                time.Duration
	DeferEmptyObjectWrite       bool
	// EnablePerfLog 启用性能日志输出（默认 false）
	EnablePerfLog bool
}

func DefaultOptions() Options {
	return Options{
		DirectoryMarkers:    true,
		WriteStrategy:       WriteStrategyDirect,
		MaxDirectObjectSize: defaultMaxDirectObjectSize,
		LargeObjectStrategy: LargeObjectStrategyError,
		AppendStrategy:      AppendStrategyCompat,
		StreamChunkSize:     defaultStreamChunkSize,
	}
}

func (o Options) withDefaults() Options {
	defaults := DefaultOptions()

	if o.Prefix != "" {
		o.Prefix = normalizePrefix(o.Prefix)
	}
	if o.WriteStrategy == "" {
		o.WriteStrategy = defaults.WriteStrategy
	}
	if o.MaxDirectObjectSize <= 0 {
		o.MaxDirectObjectSize = defaults.MaxDirectObjectSize
	}
	if o.LargeObjectStrategy == "" {
		o.LargeObjectStrategy = defaults.LargeObjectStrategy
	}
	if o.AppendStrategy == "" {
		o.AppendStrategy = defaults.AppendStrategy
	}
	switch {
	case o.StreamChunkSize == 0:
		o.StreamChunkSize = defaults.StreamChunkSize
	case o.StreamChunkSize < defaultStreamChunkSize:
		o.StreamChunkSize = defaultStreamChunkSize
	}
	if shouldDefaultDirectoryMarkers(o) {
		o.DirectoryMarkers = defaults.DirectoryMarkers
	}
	if o.TempDir == "" {
		o.TempDir = os.TempDir()
	}
	return o
}

func shouldDefaultDirectoryMarkers(o Options) bool {
	if o.DirectoryMarkers {
		return false
	}
	return o.Prefix == "" &&
		!o.ValidateBucketOnInit &&
		o.OperationTimeout == 0 &&
		o.BucketLookup == 0 &&
		o.MaxRetries == 0 &&
		o.Transport == nil &&
		o.AppName == "" &&
		o.AppVersion == "" &&
		o.TraceOutput == nil &&
		o.WriteStrategy == "" &&
		o.MaxDirectObjectSize == 0 &&
		o.LargeObjectStrategy == "" &&
		o.TempDir == "" &&
		o.AppendStrategy == "" &&
		!o.AssumeNativeAppendSupported &&
		o.NativeAppendChunkSize == 0 &&
		o.StreamChunkSize == 0 &&
		!o.OptimisticWriteOpen &&
		o.StatCacheTTL == 0 &&
		!o.DeferEmptyObjectWrite
}

func normalizePrefix(prefix string) string {
	prefix = strings.ReplaceAll(prefix, "\\", "/")
	return strings.Trim(prefix, "/")
}

type writeMode string

const (
	writeModeDirect        writeMode = "direct"
	writeModeTempFile      writeMode = "temp_file"
	writeModeComposeAppend writeMode = "compose_append"
	writeModeNativeAppend  writeMode = "native_append"
)

type writePlan struct {
	options           Options
	currentSize       int64
	targetOffset      int64
	writeLen          int
	appendOnly        bool
	nativeAppendReady bool
}

func selectWriteStrategy(plan writePlan) (writeMode, error) {
	opts := plan.options.withDefaults()

	if opts.WriteStrategy == WriteStrategyStaging {
		return writeModeTempFile, nil
	}

	if plan.currentSize == 0 && plan.targetOffset == 0 {
		return writeModeDirect, nil
	}

	if (plan.targetOffset == plan.currentSize || plan.appendOnly) &&
		opts.AppendStrategy == AppendStrategyNative &&
		opts.AssumeNativeAppendSupported {
		if plan.nativeAppendReady {
			return writeModeNativeAppend, nil
		}
		if plan.currentSize < minComposePartSize {
			if opts.LargeObjectStrategy == LargeObjectStrategyTempFile {
				return writeModeTempFile, nil
			}
			return "", ErrLargeWriteRequiresStaging
		}
		return writeModeComposeAppend, nil
	}

	newEnd := plan.targetOffset + int64(plan.writeLen)
	if plan.currentSize < opts.MaxDirectObjectSize && newEnd <= opts.MaxDirectObjectSize {
		return writeModeDirect, nil
	}

	if plan.targetOffset == plan.currentSize || plan.appendOnly {
		switch opts.AppendStrategy {
		case AppendStrategyCompose:
			if plan.currentSize < minComposePartSize {
				if opts.LargeObjectStrategy == LargeObjectStrategyTempFile {
					return writeModeTempFile, nil
				}
				return "", ErrLargeWriteRequiresStaging
			}
			return writeModeComposeAppend, nil
		case AppendStrategyNative:
			if opts.AssumeNativeAppendSupported {
				return writeModeNativeAppend, nil
			}
			if plan.currentSize < minComposePartSize {
				if opts.LargeObjectStrategy == LargeObjectStrategyTempFile {
					return writeModeTempFile, nil
				}
				return "", ErrLargeWriteRequiresStaging
			}
			return writeModeComposeAppend, nil
		case AppendStrategyCompat:
			if opts.LargeObjectStrategy == LargeObjectStrategyTempFile {
				return writeModeTempFile, nil
			}
			return "", ErrLargeWriteRequiresStaging
		case AppendStrategyDisabled:
			return "", ErrAppendNotSupported
		}
	}

	switch opts.LargeObjectStrategy {
	case LargeObjectStrategyTempFile:
		return writeModeTempFile, nil
	case LargeObjectStrategyCompose:
		return writeModeTempFile, nil
	default:
		return "", ErrLargeWriteRequiresStaging
	}
}

func (o Options) readFromChunkSize() int {
	opts := o.withDefaults()
	size := opts.StreamChunkSize
	if opts.NativeAppendChunkSize > 0 && opts.NativeAppendChunkSize < size {
		size = opts.NativeAppendChunkSize
	}
	return int(size)
}

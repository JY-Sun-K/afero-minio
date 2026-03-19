# BasePathFile does not forward io.WriterTo/io.ReaderFrom, disabling io.Copy fast paths for wrapped filesystems

## Summary

`afero.BasePathFs` returns `*BasePathFile` from `Open`, `Create`, and `OpenFile`.

`BasePathFile` wraps an underlying `afero.File`, but it does not preserve optional interfaces such as:

- `io.WriterTo`
- `io.ReaderFrom`

Because Go's `io.Copy` relies on runtime interface assertions for these fast paths, wrapping a filesystem with `BasePathFs` can change copy behavior in practice: optimized copy paths implemented by the underlying file are no longer visible through the wrapper.

## Why this matters

Per the Go standard library, `io.Copy` behaves as follows:

- if `src` implements `io.WriterTo`, `io.Copy` calls `src.WriteTo(dst)`
- otherwise, if `dst` implements `io.ReaderFrom`, `io.Copy` calls `dst.ReadFrom(src)`
- otherwise it falls back to the generic read/write loop

For some filesystem implementations, these are not just small optimizations.

For example, in an object-storage-backed filesystem, a file implementation may use `io.WriterTo` to translate:

```go
io.Copy(dst, src)
```

into a server-side copy operation rather than streaming all data through the application process.

Once the filesystem is wrapped by `BasePathFs`, that optional capability is hidden by `BasePathFile`, and `io.Copy` falls back to generic streaming.

In practice, this can change behavior from:

```text
storage -> storage
```

to:

```text
storage -> application -> storage
```

which can have a significant performance impact.

## Current behavior

`BasePathFile` is defined roughly like this:

```go
type BasePathFile struct {
    File
    path string
}
```

and `BasePathFs.Open`, `Create`, and `OpenFile` return `*BasePathFile`.

Since `afero.File` itself does not include `io.WriterTo` or `io.ReaderFrom`, those optional capabilities are no longer discoverable once a file handle is wrapped by `BasePathFile`.

As a result, code like this can lose the `io.Copy` fast path:

```go
src, _ := basePathFs.Open("a.txt")
dst, _ := basePathFs.Create("b.txt")
_, _ = io.Copy(dst, src)
```

even if the underlying wrapped file implementation supports `io.WriterTo` or `io.ReaderFrom`.

## Expected behavior

For wrapper file types that are effectively transparent decorators, `BasePathFile` should preserve optional capabilities of the wrapped file where possible.

In particular, it would be useful if `BasePathFile` forwarded:

- `io.WriterTo`
- `io.ReaderFrom`

when the underlying wrapped file implements them.

This would allow `io.Copy` to continue using optimized paths while still preserving the path-jail behavior of `BasePathFs`.

## Minimal reproduction

This is a simplified conceptual example:

```go
type fastFile struct {
    afero.File
}

func (f *fastFile) WriteTo(w io.Writer) (int64, error) {
    fmt.Println("fast path used")
    return 0, nil
}
```

If a filesystem returns `*fastFile` directly, `io.Copy(dst, src)` can observe `io.WriterTo`.

If the same file is returned through `BasePathFs` as `*BasePathFile`, the `io.WriterTo` capability is hidden, and `io.Copy` no longer sees the fast path.

## Possible direction

One possible fix would be to make `BasePathFile` explicitly forward optional interfaces such as:

- `io.WriterTo`
- `io.ReaderFrom`

when the wrapped file supports them.

A minimal approach could be:

- keep `afero.File` unchanged
- add optional interface forwarding on `BasePathFile`
- delegate to the wrapped file when those interfaces are implemented

More broadly, this may also apply to other transparent wrapper file types in `afero`, but `BasePathFile` seems like the clearest place to start.

## Notes

This is not a request to expand the core `afero.File` interface.

The issue is specifically about wrapper transparency: once a file handle is wrapped by `BasePathFile`, optional interface capabilities of the wrapped value are no longer visible through standard Go interface assertions, and that changes `io.Copy` behavior in observable ways.

## Environment

- Go version: `<fill in>`
- afero version: `<fill in>`
- OS: `<fill in>`

# MinIO SDK for Afero

[![Go Report Card](https://goreportcard.com/badge/github.com/cpyun/afero-minio)](https://goreportcard.com/report/github.com/cpyun/afero-minio)
[![GoDoc](https://godoc.org/github.com/cpyun/afero-minio?status.svg)](https://godoc.org/github.com/cpyun/afero-minio)

[English](#english) | [中文](#中文)

---

## English

### About

A production-ready [Afero](https://github.com/spf13/afero) filesystem implementation for [MinIO](https://min.io/) object storage. This package provides a complete, drop-in afero.Fs interface backed by MinIO, enabling seamless integration with any code that uses afero abstractions.

### Features

✅ **Complete Implementation**
- ✓ Full afero.Fs interface support
- ✓ File creation, reading, writing, deletion
- ✓ Directory operations (Mkdir, MkdirAll, ReadDir)
- ✓ File operations (Rename, Truncate, Stat)
- ✓ Streaming reads and writes
- ✓ Seeking support with optimizations
- ✓ Comprehensive error handling

✅ **Production Ready**
- ✓ Robust error handling
- ✓ Proper boundary condition handling
- ✓ Directory simulation using empty objects
- ✓ Virtual directory detection
- ✓ File-handle locking for seek/read/write state

✅ **Well Tested**
- ✓ Unit tests and env-driven integration tests
- ✓ Edge case coverage
- ✓ Integration test support

### Installation

```bash
go get github.com/cpyun/afero-minio
```

### Quick Start

#### Method 1: Using DSN String

```go
import (
    "context"
    "log"
    
    "github.com/cpyun/afero-minio"
    "github.com/spf13/afero"
)

func main() {
    // DSN format: scheme://accessKey:secretKey@endpoint/bucket?region=us-east-1
    dsn := "https://YOUR_ACCESS_KEY:YOUR_SECRET_KEY@play.min.io/my-bucket?region=us-east-1"
    
    fs, err := miniofs.NewMinioFs(context.Background(), dsn)
    if err != nil {
        log.Fatal(err)
    }
    
    // Use it like any afero.Fs
    err = afero.WriteFile(fs, "hello.txt", []byte("Hello, MinIO!"), 0644)
    if err != nil {
        log.Fatal(err)
    }
    
    content, err := afero.ReadFile(fs, "hello.txt")
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Content: %s", content)
}
```

#### Method 2: Using MinIO Client

```go
import (
    "context"
    "log"
    
    "github.com/minio/minio-go/v7"
    "github.com/minio/minio-go/v7/pkg/credentials"
    "github.com/cpyun/afero-minio"
)

func main() {
    // Create MinIO client
    client, err := minio.New("play.min.io", &minio.Options{
        Creds:  credentials.NewStaticV4("YOUR_ACCESS_KEY", "YOUR_SECRET_KEY", ""),
        Secure: true,
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // Create filesystem
    fs := miniofs.NewFs(context.Background(), client, "my-bucket")
    
    // Use it
    file, err := fs.Create("test.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()
    
    _, err = file.WriteString("Hello, World!")
    if err != nil {
        log.Fatal(err)
    }
}
```

#### Append-Throughput Profile (Pure MinIO)

```go
opts := miniofs.DefaultOptions()
opts.MaxDirectObjectSize = 8 << 20
opts.LargeObjectStrategy = miniofs.LargeObjectStrategyTempFile
opts.AppendStrategy = miniofs.AppendStrategyNative
opts.AssumeNativeAppendSupported = true
opts.StreamChunkSize = 5 << 20
opts.NativeAppendChunkSize = 5 << 20

fs, err := miniofs.NewMinioFsWithOptions(context.Background(), dsn, opts)
if err != nil {
    log.Fatal(err)
}
```

- `StreamChunkSize` only affects `MinioFile.ReadFrom`, so `io.Copy(file, src)` now batches reads into larger writes automatically
- With `AppendStrategyNative` and `AssumeNativeAppendSupported=true`, sequential writes at EOF prefer native append before the small-object direct rewrite branch
- `LargeObjectStrategyTempFile` remains the fallback for non-sequential large writes
- DSN-based constructors now create a dedicated append client automatically when native append is enabled, so temp-file `Close/Sync` uploads continue to use the normal client
- If you build your own `minio.Client`, use `NewFsWithClients(ctx, client, appendClient, bucket, opts)` to provide a dedicated append client
- If you only provide a single HTTP client through `NewFs`/`NewFsWithOptions`, native append is disabled automatically and the adapter falls back to compose/temp-file paths to protect `Close/Sync`

#### Advanced: Split Normal IO and Native Append Clients

```go
baseClient, err := minio.New("10.0.5.240:9000", &minio.Options{
    Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
    Secure: false,
})
if err != nil {
    log.Fatal(err)
}

appendClient, err := minio.New("10.0.5.240:9000", &minio.Options{
    Creds:           credentials.NewStaticV4("minioadmin", "minioadmin", ""),
    Secure:          false,
    TrailingHeaders: true,
})
if err != nil {
    log.Fatal(err)
}

opts := miniofs.DefaultOptions()
opts.AppendStrategy = miniofs.AppendStrategyNative
opts.AssumeNativeAppendSupported = true

fs, err := miniofs.NewFsWithClients(context.Background(), baseClient, appendClient, "my-bucket", opts)
if err != nil {
    log.Fatal(err)
}
```

- Application code still uses `afero.File` normally: `Write`, `io.Copy`, `Sync`, and `Close`
- In temp-file fallback mode, final persistence still happens at `Sync()` or `Close()`
- Always check the error returned by `Close()`, because that is where staged writes are committed

### Usage Examples

#### File Operations

```go
// Create a file
file, _ := fs.Create("document.txt")
file.WriteString("content")
file.Close()

// Read a file
data, _ := afero.ReadFile(fs, "document.txt")

// Delete a file
fs.Remove("document.txt")

// Rename a file
fs.Rename("old.txt", "new.txt")

// Get file info
info, _ := fs.Stat("document.txt")
fmt.Printf("Size: %d, ModTime: %v\n", info.Size(), info.ModTime())
```

#### Directory Operations

```go
// Create a directory
fs.Mkdir("mydir", 0755)

// Create nested directories
fs.MkdirAll("path/to/nested/dir", 0755)

// List directory contents
file, _ := fs.Open("mydir")
entries, _ := file.Readdir(-1)
for _, entry := range entries {
    fmt.Printf("%s (dir: %v)\n", entry.Name(), entry.IsDir())
}

// Remove directory and all contents
fs.RemoveAll("mydir")
```

#### Advanced Operations

```go
// Truncate file
file, _ := fs.OpenFile("data.txt", os.O_RDWR, 0644)
file.Truncate(100)
file.Close()

// Seek in file
file, _ := fs.Open("data.txt")
file.Seek(10, io.SeekStart)
data := make([]byte, 20)
file.Read(data)
file.Close()

// Write at specific offset
file, _ := fs.OpenFile("data.txt", os.O_RDWR, 0644)
file.WriteAt([]byte("insert"), 10)
file.Close()
```

### DSN Format

The DSN (Data Source Name) format is:

```
scheme://accessKey:secretKey@endpoint/bucket?param=value
```

- **scheme**: `http`, `https`, or `minio`
- **accessKey**: MinIO access key
- **secretKey**: MinIO secret key
- **endpoint**: MinIO server endpoint (e.g., `play.min.io`)
- **bucket**: Bucket name
- **Optional parameters**:
  - `region`: AWS region (default: `us-east-1`)
  - `token`: Session token for temporary credentials

**Example:**
```
https://Q3AM3UQ867SPQQA43P2F:zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG@play.min.io/my-bucket?region=us-east-1
```

### Implementation Details

#### Directory Simulation

MinIO is object storage without native directory support. This implementation simulates directories by:
1. Creating empty objects with trailing `/` for explicit directories
2. Detecting virtual directories from object key prefixes
3. Properly handling directory listing with prefix filtering

#### Write Operations

Since MinIO doesn't support generic in-place object mutation:
- Small-object writes are handled directly
- Large append paths can use compose/native append strategies when configured
- Large random writes can fall back to local temp-file staging or return a strategy error

#### Seeking

- Seeking for reads is efficient (uses MinIO range requests)
- Seeking for writes triggers a sync and repositions the offset
- Multiple seeks should be avoided for best performance

### Known Limitations

⚠️ **Limitations inherent to MinIO:**

1. **Append Depends on Strategy**: `O_APPEND` support is configurable and native append depends on backend capabilities
2. **No Chmod/Chown**: POSIX permissions are not supported (returns error)
3. **No Chtimes**: Modification time is managed by MinIO (returns error)
4. **Large Random Writes**: Large in-place mutation may require temp-file staging or be rejected by policy

⚠️ **Performance Considerations:**

- Seeking during writes is expensive (triggers full object rewrite)
- Partial updates are not atomic
- Directory listings may be slow for large prefixes

### API Coverage

| Operation | Status | Notes |
|-----------|--------|-------|
| Create | ✅ | Full support |
| Open | ✅ | Full support |
| OpenFile | ✅ | Includes configurable `O_APPEND` support |
| Remove | ✅ | Full support |
| RemoveAll | ✅ | Deletes all objects with prefix |
| Rename | ✅ | Non-atomic copy + delete, rejects existing target |
| Stat | ✅ | Supports files, dirs, virtual dirs |
| Mkdir | ✅ | Creates empty object with `/` suffix |
| MkdirAll | ✅ | Creates all parent directories |
| Chmod | ❌ | Not supported by MinIO |
| Chown | ❌ | Not supported by MinIO |
| Chtimes | ❌ | Not supported by MinIO |
| Read/ReadAt | ✅ | Streaming support |
| Write/WriteAt | ✅ | Full support (may rewrite object) |
| Seek | ✅ | Optimized for reads |
| Truncate | ✅ | Full support |
| Readdir | ✅ | Lists direct children |
| Readdirnames | ✅ | Lists direct children names |

### Testing

Run the default unit-oriented suite:

```bash
go test -v
```

Run integration tests against your own MinIO instance by setting `MINIOFS_TEST_DSN`:

```bash
MINIOFS_TEST_DSN='minio://minioadmin:minioadmin@127.0.0.1:9000/test-bucket' go test -v
```

### Contributing

Contributions are welcome! Please feel free to submit issues or pull requests for:

- Bug fixes
- Performance improvements
- Additional test coverage
- Documentation improvements

### License

Released under the Apache 2.0 license. See [LICENSE](LICENSE) for details.

### Acknowledgments

This project is inspired by and builds upon:
- [spf13/afero](https://github.com/spf13/afero) - The universal filesystem abstraction
- [fclairamb/afero-s3](https://github.com/fclairamb/afero-s3) - S3 implementation reference
- [MinIO](https://min.io/) - High-performance object storage

---

## 中文

### 关于

为 [MinIO](https://min.io/) 对象存储提供的生产就绪的 [Afero](https://github.com/spf13/afero) 文件系统实现。该包提供了完整的、即插即用的 afero.Fs 接口，由 MinIO 支持，可与任何使用 afero 抽象的代码无缝集成。

### 特性

✅ **完整实现**
- ✓ 完整的 afero.Fs 接口支持
- ✓ 文件创建、读取、写入、删除
- ✓ 目录操作（Mkdir、MkdirAll、ReadDir）
- ✓ 文件操作（Rename、Truncate、Stat）
- ✓ 流式读写
- ✓ 优化的 Seek 支持
- ✓ 全面的错误处理

✅ **生产就绪**
- ✓ 健壮的错误处理
- ✓ 正确的边界条件处理
- ✓ 使用空对象模拟目录
- ✓ 虚拟目录检测
- ✓ 文件句柄级的读写/Seek 锁保护

✅ **完善的测试**
- ✓ 单元测试与环境变量驱动的集成测试
- ✓ 边界情况覆盖
- ✓ 集成测试支持

### 安装

```bash
go get github.com/cpyun/afero-minio
```

### 快速开始

#### 方法 1: 使用 DSN 字符串

```go
import (
    "context"
    "log"
    
    "github.com/cpyun/afero-minio"
    "github.com/spf13/afero"
)

func main() {
    // DSN 格式: scheme://accessKey:secretKey@endpoint/bucket?region=us-east-1
    dsn := "https://YOUR_ACCESS_KEY:YOUR_SECRET_KEY@play.min.io/my-bucket?region=us-east-1"
    
    fs, err := miniofs.NewMinioFs(context.Background(), dsn)
    if err != nil {
        log.Fatal(err)
    }
    
    // 像使用任何 afero.Fs 一样使用它
    err = afero.WriteFile(fs, "hello.txt", []byte("你好，MinIO！"), 0644)
    if err != nil {
        log.Fatal(err)
    }
    
    content, err := afero.ReadFile(fs, "hello.txt")
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("内容: %s", content)
}
```

#### 方法 2: 使用 MinIO 客户端

```go
import (
    "context"
    "log"
    
    "github.com/minio/minio-go/v7"
    "github.com/minio/minio-go/v7/pkg/credentials"
    "github.com/cpyun/afero-minio"
)

func main() {
    // 创建 MinIO 客户端
    client, err := minio.New("play.min.io", &minio.Options{
        Creds:  credentials.NewStaticV4("YOUR_ACCESS_KEY", "YOUR_SECRET_KEY", ""),
        Secure: true,
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 创建文件系统
    fs := miniofs.NewFs(context.Background(), client, "my-bucket")
    
    // 使用它
    file, err := fs.Create("test.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()
    
    _, err = file.WriteString("你好，世界！")
    if err != nil {
        log.Fatal(err)
    }
}
```

#### 纯 MinIO 的追加吞吐型配置

```go
opts := miniofs.DefaultOptions()
opts.MaxDirectObjectSize = 8 << 20
opts.LargeObjectStrategy = miniofs.LargeObjectStrategyTempFile
opts.AppendStrategy = miniofs.AppendStrategyNative
opts.AssumeNativeAppendSupported = true
opts.StreamChunkSize = 5 << 20
opts.NativeAppendChunkSize = 5 << 20

fs, err := miniofs.NewMinioFsWithOptions(context.Background(), dsn, opts)
if err != nil {
    log.Fatal(err)
}
```

- `StreamChunkSize` 只影响 `MinioFile.ReadFrom`，所以 `io.Copy(file, src)` 会自动聚合成更大的写入块
- 在 `AppendStrategyNative` 且 `AssumeNativeAppendSupported=true` 时，顺序写到 EOF 会优先走 native append，而不是先落到小对象 direct rewrite
- 非顺序的大对象写入仍然由 `LargeObjectStrategyTempFile` 兜底
- 使用 DSN 构造时，启用 native append 会自动创建专用 append client，因此 temp-file 的 `Close/Sync` 上传仍然走普通 client
- 如果你自行创建 `minio.Client`，推荐改用 `NewFsWithClients(ctx, client, appendClient, bucket, opts)` 显式传入专用 append client
- 如果你只通过 `NewFs`/`NewFsWithOptions` 传入单个 HTTP client，插件会自动关闭 native append，并安全回退到 compose/temp-file 路径，避免影响 `Close/Sync`

#### 高级用法：拆分普通对象 IO 与 Native Append Client

```go
baseClient, err := minio.New("10.0.5.240:9000", &minio.Options{
    Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
    Secure: false,
})
if err != nil {
    log.Fatal(err)
}

appendClient, err := minio.New("10.0.5.240:9000", &minio.Options{
    Creds:           credentials.NewStaticV4("minioadmin", "minioadmin", ""),
    Secure:          false,
    TrailingHeaders: true,
})
if err != nil {
    log.Fatal(err)
}

opts := miniofs.DefaultOptions()
opts.AppendStrategy = miniofs.AppendStrategyNative
opts.AssumeNativeAppendSupported = true

fs, err := miniofs.NewFsWithClients(context.Background(), baseClient, appendClient, "my-bucket", opts)
if err != nil {
    log.Fatal(err)
}
```

- 业务代码仍然只按 `afero.File` 的方式使用：`Write`、`io.Copy`、`Sync`、`Close`
- 走 temp-file fallback 时，真正的远端提交边界仍然是 `Sync()` 或 `Close()`
- 不要忽略 `Close()` 返回值，因为 staged write 的最终提交就在这里发生

### 使用示例

详细的使用示例请参见英文版文档。

### DSN 格式

DSN（数据源名称）格式为：

```
scheme://accessKey:secretKey@endpoint/bucket?param=value
```

**示例：**
```
https://Q3AM3UQ867SPQQA43P2F:zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG@play.min.io/my-bucket?region=us-east-1
```

### 已知限制

⚠️ **MinIO 固有限制：**

1. **追加能力依赖策略**: `O_APPEND` 是否可用取决于配置策略和后端能力
2. **无 Chmod/Chown**: 不支持 POSIX 权限（返回错误）
3. **无 Chtimes**: 修改时间由 MinIO 管理（返回错误）
4. **大对象随机写有限制**: 大对象就地修改可能需要本地暂存，或按策略直接拒绝

### API 覆盖

完整的 API 覆盖表请参见英文版文档。

### 测试

```bash
go test -v
```

如需运行真实 MinIO 集成测试，请设置：

```bash
MINIOFS_TEST_DSN='minio://minioadmin:minioadmin@127.0.0.1:9000/test-bucket' go test -v
```

### 贡献

欢迎贡献！请随时提交问题或拉取请求。

### 许可证

根据 Apache 2.0 许可证发布。详见 [LICENSE](LICENSE)。

### 致谢

本项目受以下项目启发并基于：
- [spf13/afero](https://github.com/spf13/afero) - 通用文件系统抽象
- [fclairamb/afero-s3](https://github.com/fclairamb/afero-s3) - S3 实现参考
- [MinIO](https://min.io/) - 高性能对象存储

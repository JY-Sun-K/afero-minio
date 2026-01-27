package miniofs

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/afero"
)

const (
	// 使用 MinIO play server 进行测试
	// 注意：这是公共测试服务器，请不要存储敏感数据
	minioDsn = "https://Q3AM3UQ867SPQQA43P2F:zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG@play.min.io/test-bucket?region=us-east-1"
)

// 辅助函数：获取测试用的 fs
func getTestFs(t *testing.T) afero.Fs {
	fs, err := NewMinioFs(context.Background(), minioDsn)
	if err != nil {
		t.Skipf("无法连接到 MinIO: %v (跳过测试)", err)
	}
	return fs
}

func TestNewMinioFs(t *testing.T) {
	t.Run("valid DSN", func(t *testing.T) {
		fs, err := NewMinioFs(context.Background(), minioDsn)
		if err != nil {
			t.Skipf("无法连接到 MinIO: %v", err)
		}
		if fs == nil {
			t.Error("fs 不应该为 nil")
		}
	})

	t.Run("invalid DSN", func(t *testing.T) {
		_, err := NewMinioFs(context.Background(), "invalid://url")
		if err == nil {
			t.Error("应该返回错误")
		}
	})

	t.Run("missing bucket", func(t *testing.T) {
		_, err := NewMinioFs(context.Background(), "https://user:pass@play.min.io")
		if err != ErrNoBucketInName {
			t.Errorf("期望 ErrNoBucketInName，得到 %v", err)
		}
	})
}

func TestCreateAndWrite(t *testing.T) {
	fs := getTestFs(t)

	testFile := "test-create-write.txt"
	testContent := "Hello, MinIO!"

	t.Cleanup(func() {
		_ = fs.Remove(testFile)
	})

	t.Run("create file", func(t *testing.T) {
		f, err := fs.Create(testFile)
		if err != nil {
			t.Fatalf("创建文件失败: %v", err)
		}
		defer f.Close()

		n, err := f.WriteString(testContent)
		if err != nil {
			t.Fatalf("写入文件失败: %v", err)
		}

		if n != len(testContent) {
			t.Errorf("期望写入 %d 字节，实际写入 %d 字节", len(testContent), n)
		}
	})

	t.Run("read file", func(t *testing.T) {
		content, err := afero.ReadFile(fs, testFile)
		if err != nil {
			t.Fatalf("读取文件失败: %v", err)
		}

		if string(content) != testContent {
			t.Errorf("期望内容 %q，得到 %q", testContent, string(content))
		}
	})
}

func TestReadWrite(t *testing.T) {
	fs := getTestFs(t)

	testFile := "test-read-write.txt"
	testData := []byte("This is a test file with some content.")

	t.Cleanup(func() {
		_ = fs.Remove(testFile)
	})

	// 写入文件
	if err := afero.WriteFile(fs, testFile, testData, 0644); err != nil {
		t.Fatalf("写入文件失败: %v", err)
	}

	// 读取文件
	data, err := afero.ReadFile(fs, testFile)
	if err != nil {
		t.Fatalf("读取文件失败: %v", err)
	}

	if !bytes.Equal(data, testData) {
		t.Errorf("数据不匹配\n期望: %q\n得到: %q", testData, data)
	}
}

func TestMkdir(t *testing.T) {
	fs := getTestFs(t)

	testDir := "test-dir"

	t.Cleanup(func() {
		_ = fs.RemoveAll(testDir)
	})

	t.Run("create directory", func(t *testing.T) {
		err := fs.Mkdir(testDir, 0755)
		if err != nil {
			t.Fatalf("创建目录失败: %v", err)
		}

		// 验证目录存在
		fi, err := fs.Stat(testDir)
		if err != nil {
			t.Fatalf("Stat 失败: %v", err)
		}

		if !fi.IsDir() {
			t.Error("应该是目录")
		}
	})

	t.Run("create existing directory", func(t *testing.T) {
		err := fs.Mkdir(testDir, 0755)
		if !os.IsExist(err) {
			t.Errorf("期望 os.ErrExist，得到 %v", err)
		}
	})
}

func TestMkdirAll(t *testing.T) {
	fs := getTestFs(t)

	testPath := "test-dir/sub1/sub2/sub3"

	t.Cleanup(func() {
		_ = fs.RemoveAll("test-dir")
	})

	err := fs.MkdirAll(testPath, 0755)
	if err != nil {
		t.Fatalf("MkdirAll 失败: %v", err)
	}

	// 验证所有目录都存在
	paths := []string{"test-dir", "test-dir/sub1", "test-dir/sub1/sub2", testPath}
	for _, path := range paths {
		fi, err := fs.Stat(path)
		if err != nil {
			t.Errorf("Stat %s 失败: %v", path, err)
			continue
		}
		if !fi.IsDir() {
			t.Errorf("%s 应该是目录", path)
		}
	}
}

func TestRemove(t *testing.T) {
	fs := getTestFs(t)

	testFile := "test-remove.txt"

	// 创建文件
	err := afero.WriteFile(fs, testFile, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("创建文件失败: %v", err)
	}

	// 删除文件
	err = fs.Remove(testFile)
	if err != nil {
		t.Fatalf("删除文件失败: %v", err)
	}

	// 验证文件不存在
	_, err = fs.Stat(testFile)
	if !os.IsNotExist(err) {
		t.Errorf("文件应该不存在，但 Stat 返回: %v", err)
	}
}

func TestRemoveAll(t *testing.T) {
	fs := getTestFs(t)

	testDir := "test-remove-all"

	t.Cleanup(func() {
		_ = fs.RemoveAll(testDir)
	})

	// 创建目录结构
	files := []string{
		testDir + "/file1.txt",
		testDir + "/file2.txt",
		testDir + "/sub/file3.txt",
		testDir + "/sub/deep/file4.txt",
	}

	for _, file := range files {
		if err := fs.MkdirAll(strings.TrimSuffix(file, "/"+strings.Split(file, "/")[len(strings.Split(file, "/"))-1]), 0755); err != nil {
			t.Fatalf("创建目录失败: %v", err)
		}
		if err := afero.WriteFile(fs, file, []byte("test"), 0644); err != nil {
			t.Fatalf("创建文件 %s 失败: %v", file, err)
		}
	}

	// 删除所有
	err := fs.RemoveAll(testDir)
	if err != nil {
		t.Fatalf("RemoveAll 失败: %v", err)
	}

	// 验证所有文件都被删除
	for _, file := range files {
		_, err := fs.Stat(file)
		if !os.IsNotExist(err) {
			t.Errorf("文件 %s 应该被删除", file)
		}
	}
}

func TestRename(t *testing.T) {
	fs := getTestFs(t)

	oldName := "test-rename-old.txt"
	newName := "test-rename-new.txt"
	testContent := "rename test content"

	t.Cleanup(func() {
		_ = fs.Remove(oldName)
		_ = fs.Remove(newName)
	})

	// 创建原文件
	if err := afero.WriteFile(fs, oldName, []byte(testContent), 0644); err != nil {
		t.Fatalf("创建文件失败: %v", err)
	}

	// 重命名
	err := fs.Rename(oldName, newName)
	if err != nil {
		t.Fatalf("重命名失败: %v", err)
	}

	// 验证新文件存在且内容正确
	content, err := afero.ReadFile(fs, newName)
	if err != nil {
		t.Fatalf("读取新文件失败: %v", err)
	}

	if string(content) != testContent {
		t.Errorf("内容不匹配")
	}

	// 验证旧文件不存在
	_, err = fs.Stat(oldName)
	if !os.IsNotExist(err) {
		t.Error("旧文件应该不存在")
	}
}

func TestStat(t *testing.T) {
	fs := getTestFs(t)

	testFile := "test-stat.txt"
	testContent := []byte("stat test")

	t.Cleanup(func() {
		_ = fs.Remove(testFile)
	})

	// 创建文件
	if err := afero.WriteFile(fs, testFile, testContent, 0644); err != nil {
		t.Fatalf("创建文件失败: %v", err)
	}

	// Stat
	fi, err := fs.Stat(testFile)
	if err != nil {
		t.Fatalf("Stat 失败: %v", err)
	}

	if fi.IsDir() {
		t.Error("不应该是目录")
	}

	if fi.Size() != int64(len(testContent)) {
		t.Errorf("期望大小 %d，得到 %d", len(testContent), fi.Size())
	}

	if fi.Name() != testFile {
		t.Errorf("期望名称 %s，得到 %s", testFile, fi.Name())
	}
}

func TestReaddir(t *testing.T) {
	fs := getTestFs(t)

	testDir := "test-readdir"

	t.Cleanup(func() {
		_ = fs.RemoveAll(testDir)
	})

	// 创建目录和文件
	if err := fs.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("创建目录失败: %v", err)
	}

	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, name := range files {
		path := testDir + "/" + name
		if err := afero.WriteFile(fs, path, []byte("test"), 0644); err != nil {
			t.Fatalf("创建文件 %s 失败: %v", path, err)
		}
	}

	// 创建子目录
	if err := fs.Mkdir(testDir+"/subdir", 0755); err != nil {
		t.Fatalf("创建子目录失败: %v", err)
	}

	// 读取目录
	f, err := fs.Open(testDir)
	if err != nil {
		t.Fatalf("打开目录失败: %v", err)
	}
	defer f.Close()

	entries, err := f.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Readdir 失败: %v", err)
	}

	// 至少应该有文件和子目录
	if len(entries) < 4 {
		t.Errorf("期望至少 4 个条目（3个文件+1个目录），得到 %d", len(entries))
	}
}

func TestTruncate(t *testing.T) {
	fs := getTestFs(t)

	testFile := "test-truncate.txt"
	originalContent := []byte("This is original content with more data")

	t.Cleanup(func() {
		_ = fs.Remove(testFile)
	})

	// 创建文件
	if err := afero.WriteFile(fs, testFile, originalContent, 0644); err != nil {
		t.Fatalf("创建文件失败: %v", err)
	}

	// 打开文件并截断
	f, err := fs.OpenFile(testFile, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}
	defer f.Close()

	newSize := int64(10)
	if err := f.Truncate(newSize); err != nil {
		t.Fatalf("Truncate 失败: %v", err)
	}

	// 验证文件大小
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat 失败: %v", err)
	}

	if fi.Size() != newSize {
		t.Errorf("期望大小 %d，得到 %d", newSize, fi.Size())
	}
}

func TestSeek(t *testing.T) {
	fs := getTestFs(t)

	testFile := "test-seek.txt"
	testContent := []byte("0123456789ABCDEFGHIJ")

	t.Cleanup(func() {
		_ = fs.Remove(testFile)
	})

	// 创建文件
	if err := afero.WriteFile(fs, testFile, testContent, 0644); err != nil {
		t.Fatalf("创建文件失败: %v", err)
	}

	// 打开文件
	f, err := fs.Open(testFile)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}
	defer f.Close()

	// Seek 到位置 10
	offset, err := f.Seek(10, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek 失败: %v", err)
	}

	if offset != 10 {
		t.Errorf("期望偏移量 10，得到 %d", offset)
	}

	// 读取剩余内容
	buf := make([]byte, 5)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}

	expected := "ABCDE"
	if string(buf[:n]) != expected {
		t.Errorf("期望读取 %q，得到 %q", expected, string(buf[:n]))
	}
}

func TestOpenFile(t *testing.T) {
	fs := getTestFs(t)

	testFile := "test-openfile.txt"

	t.Cleanup(func() {
		_ = fs.Remove(testFile)
	})

	t.Run("create with O_CREATE", func(t *testing.T) {
		f, err := fs.OpenFile(testFile, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatalf("OpenFile 失败: %v", err)
		}
		defer f.Close()

		if _, err := f.WriteString("test"); err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	})

	t.Run("truncate with O_TRUNC", func(t *testing.T) {
		f, err := fs.OpenFile(testFile, os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			t.Fatalf("OpenFile 失败: %v", err)
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			t.Fatalf("Stat 失败: %v", err)
		}

		// 文件应该被清空
		if fi.Size() != 0 {
			t.Errorf("期望大小 0，得到 %d", fi.Size())
		}
	})
}

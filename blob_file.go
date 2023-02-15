package blobfs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log"
	"path/filepath"
	"strings"
	"time"

	"gocloud.dev/blob"
)

// blobFile satisifes the fs.FileInfo and fs.DirEntry interfaces.
type blobFile struct {
	fs       *blobFS
	name     string
	attrs    *blob.Attributes
	isDir    bool
	contents []byte
	offset   int64

	iter *blob.ListIterator
}

var (
	// Statically assert that blobFile implements fs.FileInfo
	_ fs.FileInfo = (*blobFile)(nil)
	// Statically assert that blobFile implements fs.DirEntry
	_ fs.DirEntry = (*blobFile)(nil)
	// Statically assert that blobFile implements fs.ReadDirFile
	_ fs.ReadDirFile = (*blobFile)(nil)
	// Statically assert that blobFile implements io.Seeker
	_ io.Seeker = (*blobFile)(nil)
)

func (b *blobFile) getAttrs() *blob.Attributes {
	if b.attrs != nil {
		return b.attrs
	}
	var err error
	var exists bool
	exists, err = b.fs.bucket.Exists(context.Background(), b.name)
	if err != nil {
		log.Println("blobfs: getAttrs exists issue:", err)
	}
	if exists {
		b.attrs, err = b.fs.bucket.Attributes(context.Background(), b.name)
		if err != nil {
			log.Println("blobfs: getAttrs issue:", err)
		}
	}
	if b.attrs == nil {
		return &blob.Attributes{}
	}
	return b.attrs
}

// Stat returns a FileInfo describing the named file from the file system.
func (b *blobFile) Stat() (fs.FileInfo, error) {
	// TODO(tmc): consider possible heuristic to minimize stat calls.
	var err error
	b.attrs, err = b.fs.bucket.Attributes(context.Background(), b.name)
	if err != nil {
		r, _, err := b.fs.bucket.ListPage(context.Background(), blob.FirstPageToken, 1, &blob.ListOptions{
			Prefix:    b.name,
			Delimiter: "/",
		})
		if len(r) == 0 {
			return nil, &fs.PathError{
				Op:   "stat",
				Path: b.name,
				Err:  fs.ErrNotExist,
			}
		}
		b.isDir = true
		return b, err
	}
	return b, err
}

// Name returns the name of the file (or subdirectory) described by the entry.
// This name is only the final element of the path (the base name), not the entire path.
// For example, Name would return "hello.go" not "home/gopher/hello.go".
func (b *blobFile) Name() string {
	return filepath.Base(b.name)
}

// Size returns the size of the file in bytes.
func (b *blobFile) Size() int64 {
	attrs := b.getAttrs()
	return attrs.Size
}

// Mode returns the FileMode for the underlying blobFile.
func (b *blobFile) Mode() fs.FileMode {
	if b.IsDir() {
		return fs.ModeDir
	}
	return fs.FileMode(0)
}

// ModTime returns the modification time of the file.
func (b *blobFile) ModTime() time.Time {
	modTime := b.getAttrs().ModTime
	return modTime
}

// IsDir reports whether the entry describes a directory.
func (b *blobFile) IsDir() bool {
	name := b.name
	if filepath.Ext(name) != "" {
		return false
	}
	if name == "." {
		name = "/"
	}
	if b.isDir {
		return true
	}
	fileExists, err := b.fs.bucket.Exists(context.Background(), name)
	if fileExists {
		return false
	}
	if err != nil {
		log.Println("blobfs: encountered error checking if file exists", err)
	}
	r, _, _ := b.fs.bucket.ListPage(context.Background(), blob.FirstPageToken, 1, &blob.ListOptions{
		Prefix:    name,
		Delimiter: "/",
	})
	hasChildren := len(r) > 0
	return hasChildren
}

// Sys returns underlying data for a file.
func (b *blobFile) Sys() any {
	return nil
}

// Read reads the named file into buf and returns the number of bytes read and an error, if any.
func (b *blobFile) Read(buf []byte) (int, error) {
	if buf == nil {
		return 0, nil
	}
	if b.contents == nil {
		var err error
		b.contents, err = b.fs.bucket.ReadAll(context.Background(), b.name)
		if err != nil {
			return 0, err
		}
	}
	n := copy(buf, b.contents[b.offset:])
	b.offset += int64(n)
	if n == 0 {
		return int(n), io.EOF
	}
	return int(n), nil
}

// Close closes the file.
func (b *blobFile) Close() error {
	return nil
}

// ReadDir reads the named directory and returns a list of directory entries sorted by filename.
func (b *blobFile) ReadDir(n int) ([]fs.DirEntry, error) {
	name := b.name
	name = strings.TrimPrefix(name, ".")
	if name != "" && !strings.HasSuffix(name, "/") {
		name += "/"
	}
	if b.iter == nil {
		b.iter = b.fs.bucket.List(&blob.ListOptions{
			Prefix:    name,
			Delimiter: "/",
		})
	}
	var (
		err    error
		result *blob.ListObject
	)
	results := []fs.DirEntry{}
	for {
		result, err = b.iter.Next(context.Background())
		if err != nil {
			if err == io.EOF && n < 0 {
				return results, nil
			}
			break
		}
		results = append(results, &blobFile{
			fs:    b.fs,
			name:  result.Key,
			isDir: strings.HasSuffix(result.Key, "/"),
		})
		if n > 0 && len(results) >= n {
			break
		}
	}
	return results, err
}

// Type returns the type bits for the entry.
// The type bits are a subset of the usual FileMode bits, those returned by the FileMode.Type method.
func (b *blobFile) Type() fs.FileMode {
	if b.IsDir() {
		return fs.ModeDir
	}
	return fs.FileMode(0)
}

// Info returns the FileInfo for the file or subdirectory described by the entry.
// The returned FileInfo may be from the time of the original directory read
// or from the time of the call to Info. If the file has been removed or renamed
// since the directory read, Info may return an error satisfying errors.Is(err, ErrNotExist).
// If the entry denotes a symbolic link, Info reports the information about the link itself,
// not the link's target.
func (b *blobFile) Info() (fs.FileInfo, error) {
	return b, nil
}

// Seek satisfies io.Seeker which the net/http library uses as an optimization.
func (b *blobFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.offset = offset
	case io.SeekCurrent:
		b.offset += offset
	case io.SeekEnd:
		b.offset = b.Size() + offset
	default:
		return 0, errors.New("invalid whence")
	}
	return b.offset, nil

}

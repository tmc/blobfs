package blobfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"gocloud.dev/blob"
	// gcs support
	_ "gocloud.dev/blob/gcsblob"
)

type version string

// V1 encodes the asset serving strategy.
// In version 1, the assets are served from a subdirectory in the bucket that matches the scheme:
// "gcs://{bucket}/v1/{subPath}/{version}/*".
const V1 = "v1"

type blobFS struct {
	bucket *blob.Bucket
}

// Open opens the named file.
//
// When Open returns an error, it should be of type *PathError
// with the Op field set to "open", the Path field set to name,
// and the Err field describing the problem.
//
// Open should reject attempts to open names that do not satisfy
// ValidPath(name), returning a *PathError with Err set to
// ErrInvalid or ErrNotExist.
func (b *blobFS) Open(name string) (fs.File, error) {
	return &blobFile{
		fs:   b,
		path: name,
	}, nil
}

// Statically decare that blobFS implements fs.FS
var _ fs.FS = (*blobFS)(nil)

type blobFile struct {
	fs   *blobFS
	path string
}

func (b *blobFile) Stat() (fs.FileInfo, error) {
	fmt.Println("stat:", b.path)
	attrs, err := b.fs.bucket.Attributes(context.Background(), b.path)
	return &blobFileInfo{
		name:  b.path,
		attrs: attrs,
	}, err
}

// Read reads the named file into buf and returns the number of bytes read and an error, if any.
func (b *blobFile) Read(buf []byte) (int, error) {
	contents, err := b.fs.bucket.ReadAll(context.Background(), b.path)
	if err != nil {
		fmt.Println("read error", err)
		return 0, err
	}
	// TODO(tmc): see if we can avoid this copy.
	w := bytes.NewBuffer(buf)
	n, err := io.Copy(w, bytes.NewBuffer(contents))
	return int(n), err
}

func (b *blobFile) Close() error {
	return nil
}

type blobFileInfo struct {
	name  string
	attrs *blob.Attributes
}

func (b *blobFileInfo) Name() string {
	return b.name
}

func (b *blobFileInfo) Size() int64 {
	fmt.Println("reading size:", b.attrs.Size)
	return b.attrs.Size
}

func (b *blobFileInfo) Mode() fs.FileMode {
	fmt.Println("Mode called")
	panic("not implemented") // TODO: Implement
}

func (b *blobFileInfo) ModTime() time.Time {
	fmt.Println("mod time callled:", b.attrs.ModTime)
	return b.attrs.ModTime
}

func (b *blobFileInfo) IsDir() bool {
	fmt.Println("IsDir called")
	return strings.HasSuffix(b.name, "/")
}

func (b *blobFileInfo) Sys() any {
	fmt.Println("Sys called")
	panic("not implemented") // TODO: Implement
}

// NewHandler returns a new http.Handler that serves assets from the given bucket.
func NewHandler(ctx context.Context, bucketName string, version version, subPath string) (http.Handler, error) {
	var (
		h   *blobFS
		err error
	)
	h = &blobFS{}
	blobBucketURL := "gs://" + bucketName
	h.bucket, err = blob.OpenBucket(ctx, blobBucketURL)
	fmt.Println("open bucket:", blobBucketURL, h, err)
	if err != nil {
		// TODO: consider if we want some fallback behavior.
		return nil, err
	}
	fs, err := fs.Sub(h, fmt.Sprintf("v1/%v", subPath))
	if err != nil {
		// TODO: consider if we want some fallback behavior.
		return nil, err
	}
	return http.FileServer(http.FS(fs)), err
}

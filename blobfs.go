package blobfs

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"gocloud.dev/blob"
)

// version defines a set of rules and policies regarding how files are served out of a blobfs source.
type version string

// blobFS is a filesystem backed by a gocloud *blob.Bucket.
type blobFS struct {
	version version
	bucket  *blob.Bucket
	prefix  string // The surrounding Handler calls fs.Sub() so this should be infrequently accessed.
}

// Statically assert that blobFS implements fs.FS.
var _ fs.FS = (*blobFS)(nil)

// New returns a new blobFS.
func New(version version, bucket *blob.Bucket, prefix string) (*blobFS, error) {
	subPath := fmt.Sprintf("%s/%s/", version, strings.TrimSuffix(prefix, "/"))
	b := &blobFS{
		version: version,
		bucket:  blob.PrefixedBucket(bucket, subPath),
		prefix:  prefix,
	}
	return b, nil
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
	if name == "." {
		return &blobFile{
			fs:   b,
			name: "",
		}, nil
	}
	exists, err := b.exists(name)
	if !exists {
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fs.ErrNotExist,
		}
	}
	if err != nil {
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  err,
		}
	}
	bf := &blobFile{
		fs:   b,
		name: name,
	}
	return bf, nil
}

// exists returns true if the given name exists.
func (b *blobFS) exists(name string) (bool, error) {
	// first check for exact blob.
	fileExists, err := b.bucket.Exists(context.Background(), name)
	if fileExists {
		return true, err
	}
	// then check for prefix.
	r, _, err := b.bucket.ListPage(context.Background(), blob.FirstPageToken, 1, &blob.ListOptions{
		Prefix:    name,
		Delimiter: "/",
	})
	if err != nil {
		return false, err
	}
	return len(r) > 0, nil
}

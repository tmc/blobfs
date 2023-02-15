package blobfs

import (
	"context"
	"testing"
	"testing/fstest"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
)

// newMemoryBucket returns a new in-memory bucket.
func newMemoryBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	ctx := context.Background()
	b, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// newTestFS returns a new blofs for testing.
func newTestFS(t *testing.T, prefix string) *blobFS {
	t.Helper()
	b := newMemoryBucket(t)
	fs, err := New("", b, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

func TestBlobFSTestFS(t *testing.T) {
	cases := []struct {
		name          string
		prefix        string
		expectedFiles []string
	}{
		{name: "empty prefix", prefix: ""},
		{name: "non-empty prefix without trailing slash", prefix: "foo"},
		{name: "non-empty prefix with trailing slash", prefix: "foo/"},
		{name: "ep, foo", prefix: "", expectedFiles: []string{"foo"}},
		{name: "ep, foo bar", prefix: "", expectedFiles: []string{"foo", "bar"}},
		{name: "ep, foo bar baz", prefix: "", expectedFiles: []string{"foo", "bar", "baz/bar"}},
		{name: "dir", prefix: "", expectedFiles: []string{"foo/bar/baz"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newTestFS(t, tc.prefix)

			for _, f := range tc.expectedFiles {
				if err := fs.bucket.WriteAll(context.Background(), f, []byte("hello"), nil); err != nil {
					t.Error(err)
				}
			}
			err := fstest.TestFS(fs, tc.expectedFiles...)
			if err != nil {
				t.Fatal(err)
			}

		})

	}
}

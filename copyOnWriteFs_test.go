package afero

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCopyOnWrite(t *testing.T) {
	osFs := NewOsFs()
	writeDir, err := TempDir(osFs, "", "copy-on-write-test")
	if err != nil {
		t.Fatal("error creating tempDir", err)
	}
	defer osFs.RemoveAll(writeDir)

	compositeFs := NewCopyOnWriteFs(NewReadOnlyFs(NewOsFs()), osFs)

	var dir = filepath.Join(writeDir, "some/path")

	err = compositeFs.MkdirAll(dir, 0744)
	if err != nil {
		t.Fatal(err)
	}
	_, err = compositeFs.Create(filepath.Join(dir, "newfile"))
	if err != nil {
		t.Fatal(err)
	}

	// https://github.com/spf13/afero/issues/189
	// We want the composite file system to behave like the OS file system
	// on Mkdir and MkdirAll
	for _, fs := range []Fs{osFs, compositeFs} {
		err = fs.Mkdir(dir, 0744)
		if err == nil || !os.IsExist(err) {
			t.Errorf("Mkdir: Got %q for %T", err, fs)
		}

		// MkdirAll does not return an error when the directory already exists
		err = fs.MkdirAll(dir, 0744)
		if err != nil {
			t.Errorf("MkdirAll:  Got %q for %T", err, fs)
		}

	}
}

func TestCopyOnWriteFileInMemMapBase(t *testing.T) {
	base := &MemMapFs{}
	layer := &MemMapFs{}

	if err := WriteFile(base, "base.txt", []byte("base"), 0755); err != nil {
		t.Fatalf("Failed to write file: %s", err)
	}

	ufs := NewCopyOnWriteFs(base, layer)

	_, err := ufs.Stat("base.txt")
	if err != nil {
		t.Fatal(err)
	}
}

// Related: https://github.com/spf13/afero/issues/149
func TestCopyOnWriteMkdir(t *testing.T) {
	memFs := NewMemMapFs()
	osFs := NewOsFs()
	writeDir, err := TempDir(osFs, "", "copy-on-write-test")
	if err != nil {
		t.Fatal("error creating tempDir", err)
	}
	defer osFs.RemoveAll(writeDir)

	compositeFs := NewCopyOnWriteFs(NewReadOnlyFs(osFs), memFs)

	err = compositeFs.Mkdir(filepath.Join(writeDir, "some/path"), 0700)
	if !os.IsNotExist(err) {
		t.Fatal("Mkdir should fail if parent directory does not exist:", err)
	}
}

func TestCopyOnWriteCreateNoParent(t *testing.T) {
	base := NewMemMapFs()
	layer := NewMemMapFs()

	fs := NewCopyOnWriteFs(NewReadOnlyFs(base), layer)
	_, err := fs.Create("foo/bar")
	pathErr, ok := err.(*os.PathError)
	if !ok {
		t.Fatal("Create should fail with *os.PathError when parent directory does not exist")
	}
	if pathErr.Op != "open" {
		t.Error("Create errors should be Op 'open', found:", pathErr.Op)
	}
	if !os.IsNotExist(pathErr.Err) {
		t.Error("Error should be 'does not exist' but found:", pathErr.Err)
	}
	if pathErr.Path != "foo/bar" {
		t.Error("Error path should 'foo/bar', found:", pathErr.Path)
	}
}

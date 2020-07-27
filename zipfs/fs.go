package zipfs

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"syscall/js"
	"time"

	"github.com/spf13/afero"
)

func log(args ...interface{}) {
	js.Global().Get("console").Call("warn", "zipfs: "+fmt.Sprint(args...))
}

type Fs struct {
	r     *zip.Reader
	files map[string]map[string]*zip.File
}

func normalizePath(path string) string {
	path = filepath.ToSlash(path)
	if len(path) == 0 || path[0] != '/' {
		path = "/" + path
	}
	return filepath.Clean(path)
}

func splitpath(name string) (dir, file string) {
	name = normalizePath(name)
	dir, file = filepath.Split(name)
	dir = filepath.Clean(dir)
	return
}

func New(r *zip.Reader) afero.Fs {
	fs := &Fs{r: r, files: make(map[string]map[string]*zip.File)}
	for _, file := range r.File {
		if file.FileInfo().IsDir() {
			fs.mkdirAll(file.Name)
		} else {
			d, f := splitpath(file.Name)
			fs.mkdirAll(d)
			fs.files[d][f] = file
		}
	}
	return fs
}

func (fs *Fs) mkdirAll(path string) {
	const slash = "/"
	var dirs []string
	for dir := normalizePath(path); dir != slash; dir, _ = splitpath(dir) {
		dirs = append(dirs, dir)
	}
	if fs.files[slash] == nil { // ensure root dir
		fs.files[slash] = make(map[string]*zip.File)
	}

	parentFiles := fs.files[slash]
	for i := len(dirs) - 1; i >= 0; i-- {
		dir := dirs[i]

		// ensure parent file entry
		_, base := splitpath(dir)
		if _, ok := parentFiles[base]; !ok {
			parentFiles[base] = &zip.File{FileHeader: zip.FileHeader{Name: dir + "/"}}
		}

		if _, ok := fs.files[dir]; !ok { // ensure directory exists
			fs.files[dir] = make(map[string]*zip.File)
		}
		parentFiles = fs.files[dir]
	}
}

func (fs *Fs) Create(name string) (afero.File, error) { return nil, syscall.EPERM }

func (fs *Fs) Mkdir(name string, perm os.FileMode) error { return syscall.EPERM }

func (fs *Fs) MkdirAll(path string, perm os.FileMode) error { return syscall.EPERM }

func (fs *Fs) Open(name string) (afero.File, error) {
	d, f := splitpath(name)
	if f == "" {
		return &File{fs: fs, isdir: true}, nil
	}
	if _, ok := fs.files[d]; !ok {
		return nil, &os.PathError{Op: "stat", Path: name, Err: syscall.ENOENT}
	}
	file, ok := fs.files[d][f]
	if !ok {
		return nil, &os.PathError{Op: "stat", Path: name, Err: syscall.ENOENT}
	}
	log("opening file ", name, " : ", file.FileInfo().Mode())
	retFile := &File{fs: fs, zipfile: file, isdir: file.FileInfo().IsDir()}
	if !retFile.isdir && name == "/go/src/unsafe" {
		log("isdir IS WRONG")
	}
	return retFile, nil
}

func (fs *Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if flag != os.O_RDONLY {
		return nil, syscall.EPERM
	}
	return fs.Open(name)
}

func (fs *Fs) Remove(name string) error { return syscall.EPERM }

func (fs *Fs) RemoveAll(path string) error { return syscall.EPERM }

func (fs *Fs) Rename(oldname, newname string) error { return syscall.EPERM }

type pseudoRoot struct{}

func (p *pseudoRoot) Name() string       { return string(filepath.Separator) }
func (p *pseudoRoot) Size() int64        { return 0 }
func (p *pseudoRoot) Mode() os.FileMode  { return os.ModeDir | os.ModePerm }
func (p *pseudoRoot) ModTime() time.Time { return time.Now() }
func (p *pseudoRoot) IsDir() bool        { return true }
func (p *pseudoRoot) Sys() interface{}   { return nil }

func (fs *Fs) Stat(name string) (os.FileInfo, error) {
	d, f := splitpath(name)
	if f == "" {
		log("fs.stating pseudo root ", name)
		return &pseudoRoot{}, nil
	}
	if _, ok := fs.files[d]; !ok {
		return nil, &os.PathError{Op: "stat", Path: name, Err: syscall.ENOENT}
	}
	file, ok := fs.files[d][f]
	if !ok {
		return nil, &os.PathError{Op: "stat", Path: name, Err: syscall.ENOENT}
	}
	log("fs.stating: ", name, " isdir = ", file.FileInfo().IsDir())
	return file.FileInfo(), nil
}

func (fs *Fs) Name() string { return "zipfs" }

func (fs *Fs) Chmod(name string, mode os.FileMode) error { return syscall.EPERM }

func (fs *Fs) Chtimes(name string, atime time.Time, mtime time.Time) error { return syscall.EPERM }

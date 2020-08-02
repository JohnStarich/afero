// Copyright © 2014 Steve Francia <spf@spf13.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package afero

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/afero/mem"
)

const chmodBits = os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky // Only a subset of bits are allowed to be changed. Documented under os.Chmod()

type MemMapFs struct {
	mu   sync.RWMutex
	data map[string]*mem.FileData
	init sync.Once
}

func NewMemMapFs() Fs {
	return &MemMapFs{}
}

func (m *MemMapFs) getData() map[string]*mem.FileData {
	m.init.Do(func() {
		m.data = make(map[string]*mem.FileData)
		// Root should always exist, right?
		// TODO: what about windows?
		root := mem.CreateDir(FilePathSeparator)
		mem.SetMode(root, os.ModeDir|0755)
		m.data[FilePathSeparator] = root
	})
	return m.data
}

func (*MemMapFs) Name() string { return "MemMapFS" }

func (m *MemMapFs) Create(name string) (File, error) {
	const createPerm = 0666

	name = normalizePath(name)
	err := m.requireParentDirectory("open", name)
	if err != nil {
		return nil, err
	}

	info, err := m.Stat(name)
	switch {
	case os.IsNotExist(err):
		// if not exist or is a file, truncate
		m.mu.Lock()
		m.lockFreeRemoveAll(name)
		file := mem.CreateFile(name)
		mem.SetMode(file, createPerm)
		m.getData()[name] = file
		m.registerWithParent(file)
		m.mu.Unlock()
		return mem.NewFileHandle(file), nil
	case err != nil:
		return nil, err
	case info.IsDir():
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrIsDir} // uses 'open' in os.Create
	default:
		// exists and is a file
		m.mu.RLock()
		fileData := m.getData()[name]
		m.mu.RUnlock()
		file := mem.NewFileHandle(fileData)
		err := file.Truncate(0)
		return file, err
	}
}

func (m *MemMapFs) unRegisterWithParent(fileName string) error {
	f, err := m.lockfreeOpen(fileName)
	if err != nil {
		return err
	}
	parent := m.findParent(f)
	if parent == nil {
		log.Panic("parent of ", f.Name(), " is nil")
	}

	parent.Lock()
	mem.RemoveFromMemDir(parent, f)
	parent.Unlock()
	return nil
}

// requireParentDirectory requires the parent to 'path' exists and is a directory
func (m *MemMapFs) requireParentDirectory(operationName, path string) error {
	path = normalizePath(path)
	parentPath := filepath.Dir(path)
	parent, parentErr := m.Stat(parentPath)
	if parentErr != nil {
		if os.IsNotExist(parentErr) {
			return &os.PathError{Op: operationName, Path: path, Err: os.ErrNotExist}
		}
		return parentErr
	}
	if !parent.IsDir() {
		return &os.PathError{Op: operationName, Path: path, Err: ErrNotDir}
	}
	return nil
}

func (m *MemMapFs) findParent(f *mem.FileData) *mem.FileData {
	pdir, _ := filepath.Split(f.Name())
	pdir = filepath.Clean(pdir)
	pfile, err := m.lockfreeOpen(pdir)
	if err != nil {
		return nil
	}
	return pfile
}

func (m *MemMapFs) registerWithParent(f *mem.FileData) {
	if f == nil {
		return
	}
	parent := m.findParent(f)
	if parent == nil {
		panic("parent does not exist for file: " + f.Name())
	}

	parent.Lock()
	mem.InitializeDir(parent)
	mem.AddToMemDir(parent, f)
	parent.Unlock()
}

func (m *MemMapFs) Mkdir(name string, perm os.FileMode) error {
	perm &= chmodBits
	name = normalizePath(name)

	m.mu.RLock()
	_, ok := m.getData()[name]
	m.mu.RUnlock()
	if ok {
		return &os.PathError{Op: "mkdir", Path: name, Err: ErrFileExists}
	}

	err := m.requireParentDirectory("mkdir", name)
	if err != nil {
		return err
	}

	m.mu.Lock()
	item := mem.CreateDir(name)
	m.getData()[name] = item
	m.registerWithParent(item)
	m.mu.Unlock()

	return m.unrestrictedChmod(name, perm|os.ModeDir)
}

func (m *MemMapFs) MkdirAll(path string, perm os.FileMode) error {
	perm &= chmodBits
	missingDirs, err := m.findMissingDirs(path)
	if err != nil {
		return err
	}
	for i := len(missingDirs) - 1; i >= 0; i-- { // missingDirs are in reverse order
		err := m.Mkdir(missingDirs[i], perm)
		if err != nil {
			return err
		}
	}
	return nil
}

// findMissingDirs returns all paths that must be created, in reverse order
func (m *MemMapFs) findMissingDirs(path string) ([]string, error) {
	path = normalizePath(path)
	var missingDirs []string
	for currentPath := path; currentPath != FilePathSeparator; currentPath = filepath.Dir(currentPath) {
		info, err := m.Stat(currentPath)
		switch {
		case os.IsNotExist(err):
			missingDirs = append(missingDirs, currentPath)
		case err != nil:
			return nil, err
		case info.IsDir():
			// found a directory in the chain, return early
			return missingDirs, nil
		case !info.IsDir():
			// a file is found where we want a directory, fail with ENOTDIR
			return nil, &os.PathError{Op: "mkdirall", Path: currentPath, Err: ErrNotDir}
		}
	}
	return missingDirs, nil
}

// Handle some relative paths
func normalizePath(path string) string {
	path = filepath.Clean(FilePathSeparator + path) // prepend "/" to ensure "/tmp" and "tmp" are identical files

	switch path {
	case ".":
		return FilePathSeparator
	case "..":
		return FilePathSeparator
	default:
		return path
	}
}

func (m *MemMapFs) Open(name string) (File, error) {
	f, err := m.open(name)
	if f != nil {
		return mem.NewReadOnlyFileHandle(f), err
	}
	return nil, err
}

func (m *MemMapFs) openWrite(name string) (File, error) {
	f, err := m.open(name)
	if f != nil {
		return mem.NewFileHandle(f), err
	}
	return nil, err
}

func (m *MemMapFs) open(name string) (*mem.FileData, error) {
	name = normalizePath(name)

	m.mu.RLock()
	f, ok := m.getData()[name]
	m.mu.RUnlock()
	if !ok {
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileNotFound}
	}
	return f, nil
}

func (m *MemMapFs) lockfreeOpen(name string) (*mem.FileData, error) {
	name = normalizePath(name)
	f, ok := m.getData()[name]
	if ok {
		return f, nil
	} else {
		return nil, ErrFileNotFound
	}
}

func (m *MemMapFs) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	perm &= chmodBits
	chmod := false
	file, err := m.openWrite(name)
	if err == nil && (flag&os.O_EXCL > 0) {
		return nil, &os.PathError{Op: "open", Path: name, Err: ErrFileExists}
	}
	if os.IsNotExist(err) && (flag&os.O_CREATE > 0) {
		file, err = m.Create(name)
		chmod = true
	}
	if err != nil {
		return nil, err
	}
	if flag == os.O_RDONLY {
		file = mem.NewReadOnlyFileHandle(file.(*mem.File).Data())
	}
	if flag&os.O_APPEND > 0 {
		_, err = file.Seek(0, os.SEEK_END)
		if err != nil {
			file.Close()
			return nil, err
		}
	}
	if flag&os.O_TRUNC > 0 && flag&(os.O_RDWR|os.O_WRONLY) > 0 {
		err = file.Truncate(0)
		if err != nil {
			file.Close()
			return nil, err
		}
	}
	if chmod {
		return file, m.unrestrictedChmod(name, perm)
	}
	return file, nil
}

func (m *MemMapFs) Remove(name string) error {
	name = normalizePath(name)

	m.mu.Lock()
	defer m.mu.Unlock()

	if f, ok := m.getData()[name]; ok {
		if mem.GetFileInfo(f).IsDir() {
			dir, err := mem.ReadMemDir(f)
			if err != nil {
				panic("Directory failed to list entries: " + err.Error())
			}
			if len(dir) != 0 {
				return &os.PathError{Op: "remove", Path: name, Err: ErrNotEmpty}
			}
		}
		err := m.unRegisterWithParent(name)
		if err != nil {
			return &os.PathError{Op: "remove", Path: name, Err: err}
		}
		delete(m.getData(), name)
	} else {
		return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}
	return nil
}

func (m *MemMapFs) RemoveAll(path string) error {
	m.mu.Lock()
	m.lockFreeRemoveAll(path)
	m.mu.Unlock()
	return nil
}

func (m *MemMapFs) lockFreeRemoveAll(path string) {
	path = normalizePath(path)
	fileData, err := m.lockfreeOpen(path)
	if err == ErrFileNotFound {
		return
	}
	if err != nil {
		panic("impossible case: other err from lockfreeOpen")
	}
	err = m.unRegisterWithParent(path)
	if err != nil {
		panic("failed to unregister with parent: " + err.Error())
	}
	defer delete(m.getData(), path)

	dir, err := mem.ReadMemDir(fileData)
	if err == nil {
		for _, f := range dir {
			m.lockFreeRemoveAll(filepath.Join(path, f.Name()))
		}
	}
	return
}

func (m *MemMapFs) Rename(oldname, newname string) error {
	oldname = normalizePath(oldname)
	newname = normalizePath(newname)

	if oldname == newname {
		return nil
	}
	if strings.HasPrefix(newname, oldname+FilePathSeparator) {
		// new path must not be inside the old path
		return &os.PathError{Op: "rename", Path: newname, Err: os.ErrInvalid}
	}

	m.mu.RLock()
	_, ok := m.getData()[oldname]
	m.mu.RUnlock()
	if ok {
		// File existed a moment ago. Upgrade to full write lock, then double-check 'ok' is still true.
		m.mu.Lock()
		defer m.mu.Unlock()
		_, ok = m.getData()[oldname]
	}
	if !ok {
		return &os.PathError{Op: "rename", Path: oldname, Err: ErrFileNotFound}
	}

	newParentDir := filepath.Dir(newname)
	if _, ok := m.getData()[newParentDir]; !ok {
		return &os.PathError{Op: "rename", Path: newParentDir, Err: ErrFileNotFound}
	}

	// proceed with rename. if newname exists, delete it
	m.lockFreeRemoveAll(newname)

	m.lockFreeRename(oldname, newname)
	return nil
}

func (m *MemMapFs) lockFreeRename(oldname, newname string) {
	// 1. add file data to new map location
	fileData, ok := m.getData()[oldname]
	if !ok {
		panic("File not found: " + oldname)
	}
	m.getData()[newname] = fileData

	// 2. record children entries before rename
	dir, err := mem.ReadMemDir(fileData)
	if err != nil && !IsNotDir(err) {
		panic(err)
	}

	// 3. remove old parent directory's child entry
	if err := m.unRegisterWithParent(oldname); err != nil {
		panic(err)
	}

	// 4. rename file itself to the new name
	mem.ChangeFileName(fileData, newname)

	// 5. add new parent directory's child entry
	m.registerWithParent(fileData)

	// 6. recurse into children, renaming each one
	for _, f := range dir {
		m.lockFreeRename(
			filepath.Join(oldname, f.Name()),
			filepath.Join(newname, f.Name()),
		)
	}

	// 7. delete old file data from map
	delete(m.getData(), oldname)
}

func (m *MemMapFs) Stat(name string) (os.FileInfo, error) {
	f, err := m.Open(name)
	if err != nil {
		return nil, err
	}
	fi := mem.GetFileInfo(f.(*mem.File).Data())
	return fi, nil
}

func (m *MemMapFs) Chmod(name string, mode os.FileMode) error {
	name = normalizePath(name)
	mode &= chmodBits

	m.mu.RLock()
	f, ok := m.getData()[name]
	m.mu.RUnlock()
	if !ok {
		return &os.PathError{Op: "chmod", Path: name, Err: ErrFileNotFound}
	}
	prevOtherBits := mem.GetFileInfo(f).Mode() & ^chmodBits

	mode = prevOtherBits | mode
	return m.unrestrictedChmod(name, mode)
}

func (m *MemMapFs) unrestrictedChmod(name string, mode os.FileMode) error {
	name = normalizePath(name)

	m.mu.RLock()
	f, ok := m.getData()[name]
	m.mu.RUnlock()
	if !ok {
		return &os.PathError{Op: "chmod", Path: name, Err: ErrFileNotFound}
	}

	m.mu.Lock()
	mem.SetMode(f, mode)
	m.mu.Unlock()

	return nil
}

func (m *MemMapFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	name = normalizePath(name)

	m.mu.RLock()
	f, ok := m.getData()[name]
	m.mu.RUnlock()
	if !ok {
		return &os.PathError{Op: "chtimes", Path: name, Err: ErrFileNotFound}
	}

	m.mu.Lock()
	mem.SetModTime(f, mtime)
	m.mu.Unlock()

	return nil
}

func (m *MemMapFs) List() {
	for _, x := range mem.DirMap(m.data).Files() {
		y := mem.FileInfo{FileData: x}
		fmt.Println(x.Name(), y.Size())
	}
}

// func debugMemMapList(fs Fs) {
// 	if x, ok := fs.(*MemMapFs); ok {
// 		x.List()
// 	}
// }

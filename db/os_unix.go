//go:build unix

package db

import (
	"os"
	"path"
	"syscall"
)

/*
- On Unix, fsync ensure file data is written.
  But if the directory entry has not been written to disk,
  the file cannot be reached, even if its data is on disk.
- To fix this, we must call fsync on the directory.
*/

/* Open or create the file and fsync parent directory. */
func createFileSync(file string) (*os.File, error) {
	fp, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err = syncDir(path.Base(file)); err != nil {
		_ = fp.Close()
		return nil, err
	}
	return fp, nil
}

func syncDir(file string) error {
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dir_fd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return err
	}
	defer syscall.Close(dir_fd)
	return syscall.Fsync(dir_fd)
}

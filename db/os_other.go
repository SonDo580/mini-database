//go:build !unix

package db

import "os"

func createFileSync(file string) (*os.File, error) {
	return os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0o644)
}

// TODO: handle Windows
func renameSync(oldPath string, newPath string) error {
	return os.Rename(oldPath, newPath)
}

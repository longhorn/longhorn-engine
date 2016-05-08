package sparse

import (
	"os"
	"syscall"

	fio "github.com/rancher/sparse-tools/directfio"
	"github.com/rancher/sparse-tools/log"
)

// FoldFile folds child snapshot data into its parent
func FoldFile(childFileName, parentFileName string) error {

	childFInfo, err := os.Stat(childFileName)
	if err != nil {
		panic("os.Stat(childFileName) failed, error: " + err.Error())
	}
	parentFInfo, err := os.Stat(parentFileName)
	if err != nil {
		panic("os.Stat(parentFileName) failed, error: " + err.Error())
	}

	// ensure no directory
	if childFInfo.IsDir() || parentFInfo.IsDir() {
		panic("at least one file is directory, not a normal file")
	}

	// ensure file sizes are equal
	if childFInfo.Size() != parentFInfo.Size() {
		panic("file sizes are not equal")
	}

	// open child and parent files
	childFile, err := fio.OpenFile(childFileName, os.O_RDONLY, 0)
	if err != nil {
		panic("Failed to open childFile, error: " + err.Error())
	}
	defer childFile.Close()

	parentFile, err := fio.OpenFile(parentFileName, os.O_WRONLY, 0)
	if err != nil {
		panic("Failed to open parentFile, error: " + err.Error())
	}
	defer parentFile.Close()

	return coalesce(parentFile, childFile)
}

func coalesce(parentFile *os.File, childFile *os.File) error {
	blockSize, err := getFileSystemBlockSize(childFile)
	if err != nil {
		panic("can't get FS block size, error: " + err.Error())
	}
	var data, hole int64
	for {
		data, err = syscall.Seek(int(childFile.Fd()), hole, seekData)
		if err != nil {
			// reaches EOF
			errno := err.(syscall.Errno)
			if errno == syscall.ENXIO {
				break
			} else {
				// unexpected errors
				log.Fatal("Failed to syscall.Seek SEEK_DATA")
				return err
			}
		}
		hole, err = syscall.Seek(int(childFile.Fd()), data, seekHole)
		if err != nil {
			log.Fatal("Failed to syscall.Seek SEEK_HOLE")
			return err
		}

		// now we have a data start offset and length(hole - data)
		// let's read from child and write to parent file block by block
		_, err = parentFile.Seek(data, os.SEEK_SET)
		if err != nil {
			log.Fatal("Failed to os.Seek os.SEEK_SET")
			return err
		}

		offset := data
		buffer := fio.AllocateAligned(blockSize)
		for offset != hole {
			// read a block from child, maybe use bufio or Reader stream
			n, err := fio.ReadAt(childFile, buffer, offset)
			if n != len(buffer) || err != nil {
				log.Fatal("Failed to read from childFile")
				return err
			}
			// write a block to parent
			n, err = fio.WriteAt(parentFile, buffer, offset)
			if n != len(buffer) || err != nil {
				log.Fatal("Failed to write to parentFile")
				return err
			}
			offset += int64(n)
		}
	}

	return nil
}

// get the file system block size
func getFileSystemBlockSize(f *os.File) (int, error) {
	var stat syscall.Stat_t
	err := syscall.Stat(f.Name(), &stat)
	return int(stat.Blksize), err
}

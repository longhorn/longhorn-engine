package backupstore

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/longhorn/backupstore/util"
)

const (
	LOCKS_DIRECTORY       = "locks"
	LOCK_PREFIX           = "lock"
	LOCK_SUFFIX           = ".lck"
	LOCK_DURATION         = time.Second * 150
	LOCK_MAX_WAIT_TIME    = time.Second * 30
	LOCK_REFRESH_INTERVAL = time.Second * 60
	LOCK_CHECK_INTERVAL   = time.Second * 10
	LOCK_CHECK_WAIT_TIME  = time.Second * 2
)

type LockType int

const UNTYPED_LOCK LockType = 0
const BACKUP_LOCK LockType = 1
const RESTORE_LOCK LockType = 1
const DELETION_LOCK LockType = 2

type FileLock struct {
	Name       string
	Type       LockType
	Acquired   bool
	driver     BackupStoreDriver
	volume     string
	count      int32
	serverTime time.Time
	keepAlive  chan struct{}
	mutex      sync.Mutex
}

func New(driver BackupStoreDriver, volumeName string, lockType LockType) (*FileLock, error) {
	return &FileLock{driver: driver, volume: volumeName,
		Type: lockType, Name: util.GenerateName(LOCK_PREFIX)}, nil
}

// isExpired checks whether the current lock is expired
func (lock *FileLock) isExpired() bool {
	isExpired := time.Now().Sub(lock.serverTime) > LOCK_DURATION
	return isExpired
}

func (lock *FileLock) canAcquire() bool {
	canAcquire := true
	locks := getLocksForVolume(lock.volume, lock.driver)
	for _, serverLock := range locks {
		serverLockHasDifferentType := serverLock.Type != lock.Type
		serverLockHasPriority := compareLocks(serverLock, lock) < 0
		if serverLockHasDifferentType && serverLockHasPriority && !serverLock.isExpired() {
			canAcquire = false
			break
		}
	}

	return canAcquire
}

func (lock *FileLock) Lock() error {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	if lock.Acquired {
		atomic.AddInt32(&lock.count, 1)
		_ = saveLock(lock)
		return nil
	}

	// we create first then retrieve all locks
	// because this way if another client creates at the same time
	// one of us will be first in the times array
	// the servers modification time is only the initial lock creation time
	// and we do not need to start lock refreshing till after we acquired the lock
	// since lock expiration is based on the serverTime + LOCK_MAX_WAIT_TIME
	if err := saveLock(lock); err != nil {
		return err
	}

	// since the node times might not be perfectly in sync and the servers file time has second precision
	// we wait 2 seconds before retrieving the current set of locks, this eliminates a race condition
	// where 2 processes request a lock at the same time
	time.Sleep(LOCK_CHECK_WAIT_TIME)

	for acquired := lock.Acquired; !acquired; acquired = lock.Acquired {
		if lock.canAcquire() {
			file := getLockFilePath(lock.volume, lock.Name)
			log.Debugf("Acquired lock %v type %v on backupstore", file, lock.Type)
			lock.Acquired = true
			atomic.AddInt32(&lock.count, 1)
			if err := saveLock(lock); err != nil {
				_ = removeLock(lock)
				return err
			}

			// enable lock refresh
			lock.keepAlive = make(chan struct{})
			go func() {
				refreshTimer := time.NewTicker(LOCK_REFRESH_INTERVAL)
				defer refreshTimer.Stop()
				for {
					select {
					case <-lock.keepAlive:
						return
					case <-refreshTimer.C:
						lock.mutex.Lock()
						if lock.Acquired {
							_ = saveLock(lock)
						}
						lock.mutex.Unlock()
					}
				}
			}()
			break
		}

		if timeout := time.Now().Sub(lock.serverTime) > LOCK_MAX_WAIT_TIME; timeout {
			file := getLockFilePath(lock.volume, lock.Name)
			_ = removeLock(lock)
			return fmt.Errorf("failed lock %v type %v acquisition, exceeded maximum wait time %v",
				file, lock.Type, LOCK_MAX_WAIT_TIME)
		}

		if !lock.Acquired {
			time.Sleep(LOCK_CHECK_INTERVAL)
		}
	}

	return nil
}

func (lock *FileLock) Unlock() error {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	if atomic.AddInt32(&lock.count, -1) <= 0 {
		lock.Acquired = false
		if lock.keepAlive != nil {
			close(lock.keepAlive)
		}
		if err := removeLock(lock); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func loadLock(volumeName string, name string, driver BackupStoreDriver) (*FileLock, error) {
	lock := &FileLock{}
	file := getLockFilePath(volumeName, name)
	if err := loadConfigInBackupStore(file, driver, lock); err != nil {
		return nil, err
	}
	lock.serverTime = driver.FileTime(file)
	log.Debugf("Loaded lock %v type %v on backupstore", file, lock.Type)
	return lock, nil
}

func removeLock(lock *FileLock) error {
	file := getLockFilePath(lock.volume, lock.Name)
	if err := lock.driver.Remove(file); err != nil {
		return err
	}
	log.Debugf("Removed lock %v type %v on backupstore", file, lock.Type)
	return nil
}

func saveLock(lock *FileLock) error {
	file := getLockFilePath(lock.volume, lock.Name)
	if err := saveConfigInBackupStore(file, lock.driver, lock); err != nil {
		return err
	}
	lock.serverTime = lock.driver.FileTime(file)
	log.Debugf("Stored lock %v type %v on backupstore", file, lock.Type)
	return nil
}

// compareLocks compares the locks by Acquired
// then by serverTime followed by Name
func compareLocks(a *FileLock, b *FileLock) int {
	if a.Acquired == b.Acquired {
		if a.serverTime.Equal(b.serverTime) {
			return strings.Compare(a.Name, b.Name)
		} else if a.serverTime.Before(b.serverTime) {
			return -1
		} else {
			return 1
		}
	} else if a.Acquired {
		return -1
	} else {
		return 1
	}
}

func getLockNamesForVolume(volumeName string, driver BackupStoreDriver) []string {
	fileList, err := driver.List(getLockPath(volumeName))
	if err != nil {
		// path doesn't exist
		return []string{}
	}
	names := util.ExtractNames(fileList, "", LOCK_SUFFIX)
	return names
}

func getLocksForVolume(volumeName string, driver BackupStoreDriver) []*FileLock {
	fileList := getLockNamesForVolume(volumeName, driver)
	locks := make([]*FileLock, 0, len(fileList))
	for _, name := range fileList {
		lock, err := loadLock(volumeName, name, driver)
		if err != nil {
			file := getLockFilePath(volumeName, name)
			log.Warnf("failed to load lock %v on backupstore reason %v", file, err)
			continue
		}
		locks = append(locks, lock)
	}

	return locks
}

func getLockPath(volumeName string) string {
	return filepath.Join(getVolumePath(volumeName), LOCKS_DIRECTORY) + "/"
}

func getLockFilePath(volumeName string, name string) string {
	path := getLockPath(volumeName)
	fileName := name + LOCK_SUFFIX
	return filepath.Join(path, fileName)
}

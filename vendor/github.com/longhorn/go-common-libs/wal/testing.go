package wal

// ForceCloseForTest drops the underlying fd and releases the flock
// without writing a final CHECKPOINT. It exists so external test
// packages can simulate an abrupt process death (SIGKILL) where the
// journal file is left exactly as it was at the last successful
// fsync. Production code must use Close().
func ForceCloseForTest(j *Journal) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	var firstErr error
	if j.f != nil {
		if err := j.f.Close(); err != nil {
			firstErr = err
		}
	}
	if j.lock != nil {
		if err := j.lock.Unlock(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

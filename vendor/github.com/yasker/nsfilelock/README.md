# NSFileLock
File lock (flock) in another namespace.

# Motivation:

I am working on various projects rely on a privileged container(Docker), in
which case the process running inside the container will have the ability to
switch namespace to the host. Sometimes, we want to make sure certain thing
won't happen simultaneously on the host in the case of multiple instances of the
container is running. But bind-mount an additional directory just for this
purpose seems unnecessarily cumbersome, and especially we already have the
ability to switch to the host namespace (especially mount namespace) for certain
operations.

But as a Go program, it's hard to hold a file descriptor in another namespace to
perform flock operation, since `nsenter`, which is the easiest way to switch
namespace, is a bash program. Of course, you can call `setns` syscall used by
`nsenter` to get the process into the new namespace, though that means you got
to rewrite the `nsenter` just for grabbing a lock.

This library provides a reliable way to do so without rewrite `nsenter`.

# Target

The target is simply: simulate the behavior of `flock` in another namespace.

The most import behavior of `flock` is:

The lock will expire immediately when the process holding file descriptor exits.

It's the key reason why we don't want to create a file in the other
namespace to simulate the lock. In that case, the crash of process holding the
lock will result in infinitely blocking all the following processes.

For `flock`, since it's only related to opened file descriptor, as soon as the
file descriptor is closed (e.g. unlock, or process crashed so OS release all its
resources), the lock will expire.

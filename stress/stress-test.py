import subprocess
import time
import cattle
import copy
import random
import sys
import threading
from multiprocessing import Process, Manager, Array
import multiprocessing
import binascii
import time

SIZE = 20 * 1024 * 1024 * 1024
SIZE_STR = str(SIZE)
BLOCK_SIZE = 4096
BATCH_SIZE = 128

INIT_TIME = time.time()
MAX_SNAPSHOTS = 16
MAX_TIME_SLACK = 3000
MAX_BLOCK_OFFSET = SIZE / BLOCK_SIZE

def gen_blockdata(blockoffset, nblocks, pattern):
  d = bytearray(nblocks * BLOCK_SIZE)
  for i in range(nblocks):
    d[i * BLOCK_SIZE + 0] = (blockoffset + i) & 0xFF
    d[i * BLOCK_SIZE + 1] = ((blockoffset + i) >> 8) & 0xFF
    d[i * BLOCK_SIZE + 2] = ((blockoffset + i) >> 16) & 0xFF
    d[i * BLOCK_SIZE + 3] = ((blockoffset + i) >> 24) & 0xFF
    d[i * BLOCK_SIZE + 4] = pattern & 0xFF
    d[i * BLOCK_SIZE + 5] = (pattern >> 8) & 0xFF
    d[i * BLOCK_SIZE + 6] = (pattern >> 16) & 0xFF
    d[i * BLOCK_SIZE + 7] = (pattern >> 24) & 0xFF
  return d

def create_testdata():
  return Array('i', SIZE / BLOCK_SIZE * (MAX_SNAPSHOTS + 1))

def rebuild_replica():
  if subprocess.check_output('ps -ef'.split()).find("longhorn add") > 0:
    print "Rebuild replica still in progress, skipping."
    return
  if random.random() > 0.5:
    replica_host = "localhost:9502"
  else:
    replica_host = "localhost:9505"
  print "Rebuild " + replica_host
  subprocess.call(['./bin/longhorn', 'rm', "tcp://" + replica_host])
  c = cattle.from_env(url = "http://" + replica_host + "/v1/schemas")
  for r in c.list_replica():
    r.open(size=str(SIZE))
  subprocess.call("nohup ./bin/longhorn add tcp://" + replica_host + " > rebuild.out &", shell = True)


def gen_pattern():
  return int((time.time() - INIT_TIME) * 1000)

def random_write(snapshots, testdata, iterations):
  print "Starting random write in process " + str(multiprocessing.current_process().pid)
  fd = open("/dev/longhorn/foo", "r+")
  base = snapshots["livedata"]
  for iteration in range(iterations):
    if iteration % 100 == 0:
      print "Iteration " + str(iteration) + " random write in process " + str(multiprocessing.current_process().pid)
    blockoffset = int(MAX_BLOCK_OFFSET * random.random())
    nblocks = int(BATCH_SIZE * random.random())
    if nblocks + blockoffset > MAX_BLOCK_OFFSET:
      nblocks = MAX_BLOCK_OFFSET - blockoffset
    pattern = gen_pattern()
    for i in range(nblocks):
      testdata[base + blockoffset + i] = pattern
    fd.seek(blockoffset * BLOCK_SIZE)
    fd.write(gen_blockdata(blockoffset, nblocks, pattern))
  print "Completed random write in process " + str(multiprocessing.current_process().pid)
  fd.close()

def read_and_check(snapshots, testdata, iterations):
  data_blocks = 0
  hole_blocks = 0
  print "Starting read and check in process " + str(multiprocessing.current_process().pid)
  fd = open("/dev/longhorn/foo", "r")
  base = snapshots["livedata"]
  for iteration in range(iterations):
    if iteration % 100 == 0:
      print "Iteration " + str(iteration) + " read and check in process " + str(multiprocessing.current_process().pid)
    blockoffset = int(MAX_BLOCK_OFFSET * random.random())
    nblocks = int(BATCH_SIZE * random.random())
    if nblocks + blockoffset > MAX_BLOCK_OFFSET:
      nblocks = MAX_BLOCK_OFFSET - blockoffset
    fd.seek(blockoffset * BLOCK_SIZE)
    d = fd.read(BLOCK_SIZE * nblocks)
    current_pattern = gen_pattern()
    for i in range(nblocks):
      stored_blockoffset = ord(d[BLOCK_SIZE * i + 0]) + (ord(d[BLOCK_SIZE * i + 1]) << 8) + (ord(d[BLOCK_SIZE * i + 2]) << 16) + (ord(d[BLOCK_SIZE * i + 3]) << 24)
      stored_pattern = ord(d[BLOCK_SIZE * i + 4]) + (ord(d[BLOCK_SIZE * i + 5]) << 8) + (ord(d[BLOCK_SIZE * i + 6]) << 16) + (ord(d[BLOCK_SIZE * i + 7]) << 24)
      pattern = testdata[base + blockoffset + i]
      if current_pattern - pattern < MAX_TIME_SLACK or current_pattern - stored_pattern < MAX_TIME_SLACK:
        continue
      try:
        if pattern != 0 and blockoffset != 0:
          assert stored_blockoffset == blockoffset + i
          if stored_pattern > pattern:
            diff = stored_pattern - pattern
          else:
            diff = pattern - stored_pattern
#          if diff > 0:
#            print "Difference at " + str(stored_blockoffset) + " is " + str(diff)
          assert diff < MAX_TIME_SLACK
          data_blocks = data_blocks + 1
        else:
          assert stored_blockoffset == 0
          assert stored_pattern == 0
          hole_blocks = hole_blocks + 1
      except AssertionError:
        print "current_pattern = " + str(current_pattern) + " nblocks = " + str(nblocks) + " blockoffset = " + str(blockoffset) + " i = " + str(i) + " stored_blockoffset = " + str(stored_blockoffset) + " pattern = " + str(pattern) + " stored_pattern = " + str(stored_pattern)
        raise
  print "data_blocks: " + str(data_blocks) + " hole_blocks: " + str(hole_blocks)
  print "Completed read and check in process " + str(multiprocessing.current_process().pid)
  fd.close()
    

subprocess.call("modprobe target_core_user", shell=True)
subprocess.call("mount -t configfs none /sys/kernel/config", shell=True)

subprocess.call(["killall", "longhorn", "ssync"])
subprocess.call("rm -rf /opt/*", shell=True)
subprocess.call("nohup ./bin/longhorn replica --listen localhost:9502 --size " + SIZE_STR + " /opt/volume > replica.out &", shell=True)
subprocess.call("nohup ./bin/longhorn replica --listen localhost:9505 --size " + SIZE_STR + " /opt/volume2 > replica2.out &", shell=True)
time.sleep(3)
subprocess.call("nohup ./bin/longhorn controller --frontend tcmu --replica tcp://localhost:9502 --replica tcp://localhost:9505 foo > controller.out &", shell=True)

time.sleep(20)

manager = Manager()
testdata = create_testdata()
snapshots = manager.dict()
snapshots["livedata"] = 0

workers = []

for i in range(10):
  p = Process(target = random_write, args = (snapshots, testdata, 10000))
  workers.append(p)
  p.start()

for i in range(10):
  p = Process(target = read_and_check, args = (snapshots, testdata, 10000))
  workers.append(p)
  p.start()

for p in workers:
  p.join()

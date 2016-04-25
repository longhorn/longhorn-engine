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
BATCH_SIZE = 1024
LIVE_DATA = binascii.crc32("livedata")

INIT_TIME = time.time()
MAX_SNAPSHOTS = 16

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

def random_write(snapshots, testdata):
  print "Starting random write in process " + str(multiprocessing.current_process())
  fd = open("/dev/longhorn/foo", "r+")
  maxoffset = SIZE / BLOCK_SIZE
  base = snapshots["livedata"]
  for n in range(10000):
    blockoffset = int(maxoffset * random.random())
    nblocks = int(100 * random.random())
    if nblocks + blockoffset > maxoffset:
      nblocks = maxoffset - blockoffset
    pattern = gen_pattern()
    for i in range(nblocks):
      testdata[base + blockoffset + i] = pattern
    fd.seek(blockoffset * BLOCK_SIZE)
    fd.write(gen_blockdata(blockoffset, nblocks, pattern))
  print "Completed random write in process " + str(multiprocessing.current_process())
  fd.close()

def read_and_check(snapshots, testdata):
  data_blocks = 0
  hole_blocks = 0
  percentage = 1
  print "Starting read and check"
  fd = open("/dev/longhorn/foo", "r+")
  fd.seek(0)
  base = snapshots["livedata"]
  for n in range(SIZE / BLOCK_SIZE / BATCH_SIZE):
    d = fd.read(BLOCK_SIZE * BATCH_SIZE)
    for i in range(BATCH_SIZE):
      blockoffset = ord(d[BLOCK_SIZE * i + 0]) + (ord(d[BLOCK_SIZE * i + 1]) << 8) + (ord(d[BLOCK_SIZE * i + 2]) << 16) + (ord(d[BLOCK_SIZE * i + 3]) << 24)
      pattern = ord(d[BLOCK_SIZE * i + 4]) + (ord(d[BLOCK_SIZE * i + 5]) << 8) + (ord(d[BLOCK_SIZE * i + 6]) << 16) + (ord(d[BLOCK_SIZE * i + 7]) << 24)
      try:
        if testdata[base + n * BATCH_SIZE + i] != 0:
          assert blockoffset == n * BATCH_SIZE + i
          pattern_mem = testdata[base + n * BATCH_SIZE + i]
          if pattern_mem > pattern:
            diff = pattern_mem - pattern
          else:
            diff = pattern - pattern_mem
#          if diff > 0:
#            print "Difference at " + str(n * BATCH_SIZE + i) + " is " + str(diff)
          assert diff < 3000
          data_blocks = data_blocks + 1
        else:
          assert blockoffset == 0
          assert pattern == 0
          hole_blocks = hole_blocks + 1
      except AssertionError:
        print "n = " + str(n) + " i = " + str(i) + " blockoffset = " + str(blockoffset) + " pattern = " + str(pattern) + " testdata[base + n * BATCH_SIZE + i] = " + str(testdata[base + n * BATCH_SIZE + i])
        raise
    if 100 * (n + 1) / (SIZE / BLOCK_SIZE / BATCH_SIZE) >= percentage:
      sys.stdout.write(" " + str(percentage) + "%")
      percentage = percentage + 1
      print " data_blocks: " + str(data_blocks) + " hole_blocks: " + str(hole_blocks)
  sys.stdout.write("\n")
  print "data_blocks: " + str(data_blocks) + " hole_blocks: " + str(hole_blocks)
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
  p = Process(target=random_write, args=(snapshots, testdata))
  workers.append(p)
  p.start()

for p in workers:
  p.join()

read_and_check(snapshots, testdata)


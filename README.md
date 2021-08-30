# zTree

## Dependencies
* gcc: 7.5.0
* cmake: 3.10.2
* C++ boost library

## Compile
```
    mkdir build
    cd build
    cmake ..
    make
```

## PM environment
```
    sh mount.sh
    sudo dd if=/dev/zero of=/home/bowen/mnt/pmem1/btree bs=1048576 count=num-MB
```

## RUN
### Options
```
    -b: Benchmark (0:Search 1:Insert 2:Update 3:Delete 4:YCSB(Update) 5:YCSB(Upsert), Default: 1)
    -n: Threads (Default: 1)
    -w: Key access distribution (0: Random, 1: Zipfian, Default: 0)
    -S: Skewness (Default: 0.99)
    -r: Read ratio (Default: 50)
    -d: Run time (s) (Default: 1)

```
### Single thread evaluation
```
    ./nbtree -b ${benchmark}
```

### Multi-threaded evaluation
```
    ./nbtree -b ${benchmark} -n ${num_thread}
```

### YCSB
```
    ./nbtree -b ${benchmark} -n ${num_thread} -w 1 -S ${skewness} -r ${read_ratio}
```



sudo mkfs.ext4 /dev/pmem1
sudo mount -t ext4 -o dax /dev/pmem1 /home/bowen/mnt/pmem1
# sudo mkfs.ext4 /dev/pmem13
# sudo mount -t ext4 -o dax /dev/pmem13 /home/bowen/mnt/pmem2
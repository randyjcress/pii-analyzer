#!/bin/bash
# Script to mount the 8TB data disk on Ubuntu 24.04
# This should be run as root or with sudo

set -e

echo "Setting up 8TB data disk for PII analysis..."

# Find the data disk
echo "Finding available disks..."
lsblk

# The disk is likely to be /dev/sdc on Azure VMs, but verify with lsblk output
DISK="/dev/sdc"
read -p "Enter the disk device to use (default: $DISK): " input_disk
DISK=${input_disk:-$DISK}

echo "Will use disk: $DISK"
echo "WARNING: All data on $DISK will be lost! Ctrl+C to cancel, or"
read -p "Press Enter to continue..."

# Create a GPT partition table and a single partition
echo "Creating partition table..."
sudo parted -s $DISK mklabel gpt
sudo parted -s $DISK mkpart primary ext4 0% 100%

# Wait for the partition to be recognized
echo "Waiting for partition to be recognized..."
sleep 2

# Get the partition name
PART="${DISK}1"
if [[ "$DISK" == *"nvme"* ]]; then
    # NVMe drives use p1, p2, etc. for partitions
    PART="${DISK}p1"
fi

# Format the partition with ext4
echo "Formatting partition with ext4..."
sudo mkfs.ext4 $PART

# Create mount point
echo "Creating mount point..."
sudo mkdir -p /mnt/data

# Mount the partition
echo "Mounting partition..."
sudo mount $PART /mnt/data

# Set appropriate permissions
echo "Setting permissions..."
sudo chown -R $USER:$USER /mnt/data
sudo chmod -R 755 /mnt/data

# Add to fstab for automatic mounting
echo "Configuring automatic mounting..."
PART_UUID=$(sudo blkid -s UUID -o value $PART)
echo "UUID=$PART_UUID /mnt/data ext4 defaults,discard,nofail 0 2" | sudo tee -a /etc/fstab

# Create directory structure for PII analysis
echo "Creating directory structure for PII processing..."
mkdir -p /mnt/data/input
mkdir -p /mnt/data/results
mkdir -p /mnt/data/db

# Link to home directory
echo "Creating symlinks in home directory..."
ln -sf /mnt/data/input ~/pii-data 
ln -sf /mnt/data/results ~/pii-results
ln -sf /mnt/data/db ~/pii-db

echo ""
echo "Data disk setup complete!"
echo "- Mount point: /mnt/data"
echo "- Available space:"
df -h /mnt/data
echo ""
echo "Directory structure:"
echo "- Input files: /mnt/data/input (linked as ~/pii-data)"
echo "- Results: /mnt/data/results (linked as ~/pii-results)"
echo "- Databases: /mnt/data/db (linked as ~/pii-db)"
echo ""
echo "To use the disk with the PII analyzer, reference these paths:"
echo "- For input: ~/pii-data"
echo "- For database: ~/pii-db/your_database.db" 
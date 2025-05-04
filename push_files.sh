#!/bin/bash
# Script to push files with processable extensions from the local server
# to the Azure VM while preserving directory structure

# Set source and destination
DEST_SERVER="piiadmin@20.169.240.64"  # Your Azure VM
SOURCE_DIR="/CoWS"                    # Your actual source directory 
DEST_DIR="/mnt/data/input"            # Destination directory on Azure VM
SSH_OPTS="-o ControlMaster=auto -o ControlPath=/tmp/ssh_mux_%h_%p_%r -o ControlPersist=1h"

# Default extensions that can be processed by PII analyzer
EXTENSIONS=(
  "csv"
  "docx"
  "jpeg" "jpg"
  "pdf"
  "png"
  "rtf"
  "tif" "tiff"
  "txt"
  "xlsx"
)

# Function to show help
show_help() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  -s, --server USER@HOST    Azure VM destination server (default: $DEST_SERVER)"
  echo "  -d, --source-dir DIR      Local source directory (default: $SOURCE_DIR)"
  echo "  -o, --dest-dir DIR        Destination directory on Azure VM (default: $DEST_DIR)"
  echo "  -e, --extensions LIST     Comma-separated list of extensions to transfer"
  echo "  -h, --help                Show this help message"
  echo ""
  echo "Example: $0 --server piiadmin@azure-vm-ip --source-dir /path/to/source --dest-dir /mnt/data/input"
  echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--server)
      DEST_SERVER="$2"
      shift 2
      ;;
    -d|--source-dir)
      SOURCE_DIR="$2"
      shift 2
      ;;
    -o|--dest-dir)
      DEST_DIR="$2"
      shift 2
      ;;
    -e|--extensions)
      IFS=',' read -ra EXTENSIONS <<< "$2"
      shift 2
      ;;
    -h|--help)
      show_help
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      show_help
      exit 1
      ;;
  esac
done

# Check if DEST_SERVER is set
if [ -z "$DEST_SERVER" ]; then
  echo "Error: Destination server not specified"
  show_help
  exit 1
fi

# Build the find command to get all processable files (run locally)
FIND_EXPR="find $SOURCE_DIR -type f"
for ((i=0; i<${#EXTENSIONS[@]}; i++)); do
  if [ $i -eq 0 ]; then
    FIND_EXPR+=" \( -name \"*.${EXTENSIONS[$i]}\""
  else
    FIND_EXPR+=" -o -name \"*.${EXTENSIONS[$i]}\""
  fi
done
FIND_EXPR+=" \)"

echo "===== File Push Script ====="
echo "Local source directory: $SOURCE_DIR"
echo "Destination server: $DEST_SERVER"
echo "Destination directory: $DEST_DIR"
echo "Extensions to transfer: ${EXTENSIONS[*]}"
echo "============================="

# Function to push files in batches
push_files() {
  echo "Getting list of extensions to process..."
  
  # Build rsync include/exclude patterns
  RSYNC_PATTERNS=""
  for ext in "${EXTENSIONS[@]}"; do
    RSYNC_PATTERNS+=" --include=*.${ext}"
  done
  
  # Create a timestamp for the transfer log
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  LOG_FILE="transfer_log_$TIMESTAMP.txt"
  
  # Create the base destination directory
  echo "Creating base destination directory..."
  ssh $SSH_OPTS "$DEST_SERVER" "mkdir -p $DEST_DIR"
  
  # Use rsync to transfer files - much more efficient than SCP for many files
  echo "Starting rsync transfer (this preserves directory structure)..."
  rsync -avz --progress --stats \
    $RSYNC_PATTERNS \
    --include='*/' \
    --exclude='*' \
    "$SOURCE_DIR/" "$DEST_SERVER:$DEST_DIR/" \
    | tee "$LOG_FILE"
    
  echo "Transfer complete. See $LOG_FILE for details."
}

# Confirm before proceeding
echo ""
echo "This will push files with extensions: ${EXTENSIONS[*]}"
echo "from local directory $SOURCE_DIR to $DEST_SERVER:$DEST_DIR"
echo ""
read -p "Continue? (y/n): " CONFIRM
if [[ $CONFIRM =~ ^[Yy]$ ]]; then
  push_files
else
  echo "Transfer cancelled"
  exit 0
fi 
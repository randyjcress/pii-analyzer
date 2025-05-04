#!/bin/bash
# Script to push files with processable extensions from the local server
# to the Azure VM while preserving directory structure

# Set source and destination
DEST_SERVER="user@azure-vm-ip"  # CHANGE THIS to the Azure VM's IP/hostname and user
SOURCE_DIR="/path/to/source"    # Local source directory
DEST_DIR="/mnt/data/input"      # Destination directory on Azure VM
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
  echo "Getting list of files to transfer..."
  # Create a temporary file to store the list of files
  TMP_FILE=$(mktemp)
  
  # Run the find command locally and save the output
  eval "$FIND_EXPR" > "$TMP_FILE"
  
  # Count total files
  TOTAL_FILES=$(wc -l < "$TMP_FILE")
  echo "Found $TOTAL_FILES files to transfer"
  
  # Create a timestamp for the transfer log
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  LOG_FILE="transfer_log_$TIMESTAMP.txt"
  
  # Extract unique directories to create on remote server
  echo "Extracting unique directories to create..."
  DIRS_FILE=$(mktemp)
  cat "$TMP_FILE" | while read FILE; do
    DIR=$(dirname "$FILE" | sed "s|$SOURCE_DIR||")
    echo "$DEST_DIR$DIR" >> "$DIRS_FILE"
  done
  sort -u "$DIRS_FILE" > "${DIRS_FILE}.sorted"
  
  # Create all directories in one SSH session
  echo "Creating directory structure on remote server (this may take a while)..."
  ssh $SSH_OPTS "$DEST_SERVER" "cat > /tmp/dirs_to_create.$$.txt" < "${DIRS_FILE}.sorted"
  ssh $SSH_OPTS "$DEST_SERVER" "xargs -I{} mkdir -p {} < /tmp/dirs_to_create.$$.txt && rm /tmp/dirs_to_create.$$.txt"
  
  # Transfer files
  echo "Starting file transfer..."
  BATCH_SIZE=1000
  BATCH=0
  TOTAL_BATCHES=$((($TOTAL_FILES + $BATCH_SIZE - 1) / $BATCH_SIZE))
  
  cat "$TMP_FILE" | while read -r FILE; do
    # Get file path relative to SOURCE_DIR
    REL_PATH="${FILE#$SOURCE_DIR/}"
    # Determine destination path
    DEST_PATH="$DEST_DIR/$REL_PATH"
    
    # Copy the file (push from local to remote)
    echo "Copying $FILE to $DEST_SERVER:$DEST_PATH"
    scp $SSH_OPTS "$FILE" "$DEST_SERVER:$DEST_PATH" >> "$LOG_FILE" 2>&1
    
    # Update batch counter
    BATCH=$((BATCH + 1))
    if [ $((BATCH % 100)) -eq 0 ]; then
      echo "Transferred $BATCH / $TOTAL_FILES files"
    fi
  done
  
  echo "Transfer complete. $BATCH files transferred."
  echo "Log file: $LOG_FILE"
  
  # Clean up
  rm "$TMP_FILE" "$DIRS_FILE" "${DIRS_FILE}.sorted"
  
  # Close SSH control connection
  ssh $SSH_OPTS -O exit "$DEST_SERVER" 2>/dev/null
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
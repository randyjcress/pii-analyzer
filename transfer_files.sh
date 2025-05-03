#!/bin/bash
# Script to transfer files with processable extensions from the original server
# to the new server while preserving directory structure

# Set source and destination
SOURCE_SERVER="user@original-server-ip"  # CHANGE THIS to the original server's IP/hostname and user
SOURCE_DIR="/CoWS"
DEST_DIR="/mnt/data/input"

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
  echo "  -s, --server USER@HOST  Source server (default: $SOURCE_SERVER)"
  echo "  -d, --source-dir DIR    Source directory on remote server (default: $SOURCE_DIR)"
  echo "  -o, --dest-dir DIR      Destination directory (default: $DEST_DIR)"
  echo "  -e, --extensions LIST   Comma-separated list of extensions to transfer"
  echo "  -h, --help              Show this help message"
  echo ""
  echo "Example: $0 --server user@192.168.1.100 --source-dir /CoWS --dest-dir /mnt/data/input"
  echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--server)
      SOURCE_SERVER="$2"
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

# Check if SOURCE_SERVER is set
if [ -z "$SOURCE_SERVER" ]; then
  echo "Error: Source server not specified"
  show_help
  exit 1
fi

# Create destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

# Build the find command to get all processable files
FIND_EXPR="find $SOURCE_DIR -type f"
for ((i=0; i<${#EXTENSIONS[@]}; i++)); do
  if [ $i -eq 0 ]; then
    FIND_EXPR+=" \( -name \"*.${EXTENSIONS[$i]}\""
  else
    FIND_EXPR+=" -o -name \"*.${EXTENSIONS[$i]}\""
  fi
done
FIND_EXPR+=" \)"

echo "===== File Transfer Script ====="
echo "Source server: $SOURCE_SERVER"
echo "Source directory: $SOURCE_DIR"
echo "Destination directory: $DEST_DIR"
echo "Extensions to transfer: ${EXTENSIONS[*]}"
echo "================================="

# Function to transfer files in batches
transfer_files() {
  echo "Getting list of files to transfer..."
  # Create a temporary file to store the list of files
  TMP_FILE=$(mktemp)
  
  # Run the find command on the remote server and save the output
  ssh "$SOURCE_SERVER" "$FIND_EXPR" > "$TMP_FILE"
  
  # Count total files
  TOTAL_FILES=$(wc -l < "$TMP_FILE")
  echo "Found $TOTAL_FILES files to transfer"
  
  # Create a timestamp for the transfer log
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  LOG_FILE="transfer_log_$TIMESTAMP.txt"
  
  # Create destination directory structure
  echo "Creating directory structure..."
  cat "$TMP_FILE" | while read FILE; do
    DIR=$(dirname "$FILE" | sed "s|$SOURCE_DIR||")
    mkdir -p "$DEST_DIR$DIR"
  done
  
  # Transfer files
  echo "Starting file transfer..."
  BATCH_SIZE=1000
  BATCH=0
  TOTAL_BATCHES=$((($TOTAL_FILES + $BATCH_SIZE - 1) / $BATCH_SIZE))
  
  cat "$TMP_FILE" | while read -r FILE; do
    # Get remote file path relative to SOURCE_DIR
    REL_PATH="${FILE#$SOURCE_DIR/}"
    # Determine destination path
    DEST_PATH="$DEST_DIR/$REL_PATH"
    DEST_DIR_PATH=$(dirname "$DEST_PATH")
    
    # Create destination directory if it doesn't exist
    mkdir -p "$DEST_DIR_PATH"
    
    # Copy the file
    echo "Copying $FILE to $DEST_PATH"
    scp "$SOURCE_SERVER:$FILE" "$DEST_PATH" >> "$LOG_FILE" 2>&1
    
    # Update batch counter
    BATCH=$((BATCH + 1))
    if [ $((BATCH % 100)) -eq 0 ]; then
      echo "Transferred $BATCH / $TOTAL_FILES files"
    fi
  done
  
  echo "Transfer complete. $BATCH files transferred."
  echo "Log file: $LOG_FILE"
  
  # Clean up
  rm "$TMP_FILE"
}

# Confirm before proceeding
echo ""
echo "This will transfer files with extensions: ${EXTENSIONS[*]}"
echo "from $SOURCE_SERVER:$SOURCE_DIR to $DEST_DIR"
echo ""
read -p "Continue? (y/n): " CONFIRM
if [[ $CONFIRM =~ ^[Yy]$ ]]; then
  transfer_files
else
  echo "Transfer cancelled"
  exit 0
fi 
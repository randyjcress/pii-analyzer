#!/usr/bin/env python3
"""
PII Analyzer Performance Monitor
Logs detailed system and process metrics every 5 seconds to help diagnose
performance issues and bottlenecks while the main process is running.
"""

import os
import sys
import time
import json
import datetime
import subprocess
import psutil
import signal
import socket
import threading
from collections import defaultdict, Counter

# Configuration
LOG_INTERVAL = 5  # seconds between log entries
OUTPUT_FILE = "performance_metrics.jsonl"
METRICS_HISTORY_LENGTH = 10  # Keep this many last readings for rate calculations
CONTINUE_ON_ERROR = True
MONITOR_TIKA = True
MONITOR_DB = True

class PerfMonitor:
    def __init__(self, log_interval=LOG_INTERVAL, output_file=OUTPUT_FILE):
        self.log_interval = log_interval
        self.output_file = output_file
        self.stop_event = threading.Event()
        self.metrics_history = []
        self.start_time = time.time()
        self.hostname = socket.gethostname()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        print(f"Starting performance monitor. Logging every {log_interval} seconds to {output_file}")
        print("Press Ctrl+C to stop monitoring")
    
    def handle_signal(self, sig, frame):
        """Handle termination signals gracefully"""
        print(f"\nReceived signal {sig}, shutting down...")
        self.stop_event.set()
    
    def get_system_metrics(self):
        """Gather system-wide metrics"""
        metrics = {}
        
        # Basic system info
        metrics["timestamp"] = datetime.datetime.now().isoformat()
        metrics["uptime"] = time.time() - psutil.boot_time()
        metrics["hostname"] = self.hostname
        
        # CPU metrics
        metrics["cpu"] = {
            "total_percent": psutil.cpu_percent(interval=None),
            "per_cpu_percent": psutil.cpu_percent(interval=None, percpu=True),
            "count_logical": psutil.cpu_count(logical=True),
            "count_physical": psutil.cpu_count(logical=False),
            "load_avg": os.getloadavg()
        }
        
        # Memory metrics
        memory = psutil.virtual_memory()
        metrics["memory"] = {
            "total_gb": memory.total / (1024**3),
            "available_gb": memory.available / (1024**3),
            "used_gb": memory.used / (1024**3),
            "percent": memory.percent
        }
        
        # Disk metrics
        disk_io = psutil.disk_io_counters()
        metrics["disk"] = {
            "read_bytes": disk_io.read_bytes if disk_io else 0,
            "write_bytes": disk_io.write_bytes if disk_io else 0,
            "read_count": disk_io.read_count if disk_io else 0,
            "write_count": disk_io.write_count if disk_io else 0
        }
        
        # Get disk usage for /mnt/data
        try:
            data_disk = psutil.disk_usage("/mnt/data")
            metrics["data_disk"] = {
                "total_gb": data_disk.total / (1024**3),
                "used_gb": data_disk.used / (1024**3),
                "free_gb": data_disk.free / (1024**3),
                "percent": data_disk.percent
            }
        except:
            metrics["data_disk"] = "not_found"
        
        # Network metrics
        net_io = psutil.net_io_counters()
        metrics["network"] = {
            "bytes_sent": net_io.bytes_sent,
            "bytes_recv": net_io.bytes_recv,
            "packets_sent": net_io.packets_sent,
            "packets_recv": net_io.packets_recv
        }
        
        return metrics
    
    def get_process_metrics(self):
        """Gather process-specific metrics"""
        metrics = {
            "total_process_count": 0,
            "python_process_count": 0,
            "worker_processes": [],
            "main_processes": [],
            "tika_processes": [],
            "ocr_processes": [],
            "by_type": defaultdict(int),
            "by_name": defaultdict(int)
        }
        
        # Get all processes
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent', 'memory_percent', 'create_time']):
            try:
                # Increment counters
                metrics["total_process_count"] += 1
                
                # Get process info
                proc_info = proc.info
                name = proc_info['name']
                metrics["by_name"][name] += 1
                
                # Skip kernel threads
                if name.startswith('['):
                    continue
                
                # Check if it's a Python process
                if 'python' in name:
                    metrics["python_process_count"] += 1
                
                # Check for PII worker processes
                cmdline = ' '.join(proc_info.get('cmdline', []))
                if 'pii-worker' in name or ('python' in name and 'analyze' in cmdline):
                    proc_detail = {
                        "pid": proc_info['pid'],
                        "name": name,
                        "cpu_percent": proc_info['cpu_percent'],
                        "memory_percent": proc_info['memory_percent'],
                        "age_seconds": time.time() - proc_info['create_time'],
                        "cmdline": cmdline[:200]  # Truncate long command lines
                    }
                    metrics["worker_processes"].append(proc_detail)
                    metrics["by_type"]["worker"] += 1
                
                # Check for PII main processes
                elif 'pii-main' in name or ('python' in name and 'process_files.py' in cmdline):
                    proc_detail = {
                        "pid": proc_info['pid'],
                        "name": name,
                        "cpu_percent": proc_info['cpu_percent'],
                        "memory_percent": proc_info['memory_percent'],
                        "age_seconds": time.time() - proc_info['create_time'],
                        "cmdline": cmdline[:200]
                    }
                    metrics["main_processes"].append(proc_detail)
                    metrics["by_type"]["main"] += 1
                
                # Check for Tika processes
                elif MONITOR_TIKA and ('java' in name or 'tika' in name.lower() or 'docker' in name):
                    proc_detail = {
                        "pid": proc_info['pid'],
                        "name": name,
                        "cpu_percent": proc_info['cpu_percent'],
                        "memory_percent": proc_info['memory_percent'],
                        "age_seconds": time.time() - proc_info['create_time']
                    }
                    metrics["tika_processes"].append(proc_detail)
                    metrics["by_type"]["tika"] += 1
                
                # Check for OCR processes (tesseract)
                elif 'tesseract' in name:
                    proc_detail = {
                        "pid": proc_info['pid'],
                        "name": name,
                        "cpu_percent": proc_info['cpu_percent'],
                        "memory_percent": proc_info['memory_percent'],
                        "age_seconds": time.time() - proc_info['create_time']
                    }
                    metrics["ocr_processes"].append(proc_detail)
                    metrics["by_type"]["ocr"] += 1
                
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        
        return metrics
    
    def get_db_metrics(self):
        """Attempt to get database metrics"""
        if not MONITOR_DB:
            return {"enabled": False}
        
        metrics = {"enabled": True}
        
        try:
            # Look for database files in the current directory
            db_files = [f for f in os.listdir() if f.endswith('.db')]
            
            if not db_files:
                return {"enabled": True, "db_files_found": False}
            
            metrics["db_files"] = db_files
            metrics["db_sizes"] = {}
            
            for db_file in db_files:
                file_stats = os.stat(db_file)
                metrics["db_sizes"][db_file] = {
                    "size_mb": file_stats.st_size / (1024**2),
                    "last_modified": datetime.datetime.fromtimestamp(file_stats.st_mtime).isoformat()
                }
                
                # Try to get table counts for the most recently modified DB
                if db_file == sorted(db_files, key=lambda f: os.stat(f).st_mtime, reverse=True)[0]:
                    try:
                        import sqlite3
                        conn = sqlite3.connect(db_file)
                        cursor = conn.cursor()
                        
                        # Get table names
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        tables = [row[0] for row in cursor.fetchall()]
                        
                        # Get count for each table
                        table_counts = {}
                        for table in tables:
                            try:
                                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                                table_counts[table] = cursor.fetchone()[0]
                            except:
                                table_counts[table] = "error"
                        
                        metrics["table_counts"] = table_counts
                        
                        # Get file status counts if available
                        try:
                            cursor.execute("SELECT status, COUNT(*) FROM files GROUP BY status")
                            metrics["file_status_counts"] = {status: count for status, count in cursor.fetchall()}
                        except:
                            pass
                        
                        conn.close()
                    except:
                        metrics["db_query_error"] = "Error querying database tables"
        
        except Exception as e:
            metrics["error"] = str(e)
        
        return metrics
    
    def get_tika_metrics(self):
        """Attempt to get Tika metrics"""
        if not MONITOR_TIKA:
            return {"enabled": False}
        
        metrics = {"enabled": True}
        
        try:
            # Check if Tika is running using docker
            result = subprocess.run(
                ["docker", "ps", "--filter", "name=tika", "--format", "{{.Names}} {{.Status}}"],
                capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0 and result.stdout.strip():
                containers = result.stdout.strip().split('\n')
                metrics["containers"] = [c.strip() for c in containers]
                
                # Get more detailed metrics for each container
                container_details = []
                for container_line in containers:
                    container_name = container_line.split()[0]
                    # Get container stats
                    stats_result = subprocess.run(
                        ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}|{{.MemUsage}}|{{.NetIO}}|{{.BlockIO}}", container_name],
                        capture_output=True, text=True, timeout=5
                    )
                    
                    if stats_result.returncode == 0:
                        stats = stats_result.stdout.strip().split('|')
                        if len(stats) >= 4:
                            container_details.append({
                                "name": container_name,
                                "cpu_percent": stats[0],
                                "memory_usage": stats[1],
                                "network_io": stats[2],
                                "block_io": stats[3]
                            })
                
                metrics["container_details"] = container_details
                
                # Try to get Tika's network port status
                for port in [9998, 9999, 10000]:  # Typical Tika ports
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.settimeout(1)
                            result = s.connect_ex(('localhost', port))
                            metrics[f"port_{port}_open"] = (result == 0)
                    except:
                        metrics[f"port_{port}_error"] = True
            else:
                metrics["running"] = False
        
        except Exception as e:
            metrics["error"] = str(e)
        
        return metrics
    
    def calculate_rates(self):
        """Calculate rate metrics based on history"""
        if len(self.metrics_history) < 2:
            return {}
        
        # Get the two most recent entries
        current = self.metrics_history[-1]
        previous = self.metrics_history[-2]
        
        # Calculate time difference
        time_diff = current.get("timestamp_epoch", 0) - previous.get("timestamp_epoch", 0)
        if time_diff <= 0:
            return {}
        
        rates = {}
        
        # Calculate disk IO rates
        try:
            current_disk = current.get("system", {}).get("disk", {})
            previous_disk = previous.get("system", {}).get("disk", {})
            
            rates["disk_read_mb_per_sec"] = (
                (current_disk.get("read_bytes", 0) - previous_disk.get("read_bytes", 0)) 
                / (1024**2) / time_diff
            )
            rates["disk_write_mb_per_sec"] = (
                (current_disk.get("write_bytes", 0) - previous_disk.get("write_bytes", 0)) 
                / (1024**2) / time_diff
            )
        except:
            pass
        
        # Calculate network rates
        try:
            current_net = current.get("system", {}).get("network", {})
            previous_net = previous.get("system", {}).get("network", {})
            
            rates["network_recv_mb_per_sec"] = (
                (current_net.get("bytes_recv", 0) - previous_net.get("bytes_recv", 0)) 
                / (1024**2) / time_diff
            )
            rates["network_sent_mb_per_sec"] = (
                (current_net.get("bytes_sent", 0) - previous_net.get("bytes_sent", 0)) 
                / (1024**2) / time_diff
            )
        except:
            pass
        
        # Calculate process creation/exit rates
        try:
            current_workers = len(current.get("processes", {}).get("worker_processes", []))
            previous_workers = len(previous.get("processes", {}).get("worker_processes", []))
            rates["worker_change_rate"] = (current_workers - previous_workers) / time_diff
        except:
            pass
        
        return rates
    
    def collect_and_log_metrics(self):
        """Collect all metrics and write to log file"""
        try:
            # Get metrics
            system_metrics = self.get_system_metrics()
            process_metrics = self.get_process_metrics()
            db_metrics = self.get_db_metrics()
            tika_metrics = self.get_tika_metrics()
            
            # Combine all metrics
            metrics = {
                "timestamp": system_metrics["timestamp"],
                "timestamp_epoch": time.time(),
                "monitor_runtime": time.time() - self.start_time,
                "system": system_metrics,
                "processes": process_metrics,
                "database": db_metrics,
                "tika": tika_metrics
            }
            
            # Add to history and trim if needed
            self.metrics_history.append(metrics)
            if len(self.metrics_history) > METRICS_HISTORY_LENGTH:
                self.metrics_history = self.metrics_history[-METRICS_HISTORY_LENGTH:]
            
            # Calculate rate metrics
            rate_metrics = self.calculate_rates()
            metrics["rates"] = rate_metrics
            
            # Write to log file
            with open(self.output_file, 'a') as f:
                f.write(json.dumps(metrics) + '\n')
            
            # Print stats to console
            worker_count = len(process_metrics.get("worker_processes", []))
            main_count = len(process_metrics.get("main_processes", []))
            ocr_count = len(process_metrics.get("ocr_processes", []))
            
            load_avg = system_metrics.get("cpu", {}).get("load_avg", [0, 0, 0])
            cpu_percent = system_metrics.get("cpu", {}).get("total_percent", 0)
            
            print(f"[{metrics['timestamp']}] CPU: {cpu_percent:.1f}%, Load: {load_avg[0]:.2f}, " 
                  f"Workers: {worker_count}, Main: {main_count}, OCR: {ocr_count}, "
                  f"Disk Write: {rate_metrics.get('disk_write_mb_per_sec', 0):.2f} MB/s")
            
            # Return key metrics for external consumers
            return {
                "worker_count": worker_count,
                "cpu_percent": cpu_percent,
                "load_avg": load_avg
            }
        
        except Exception as e:
            print(f"Error collecting metrics: {e}")
            if not CONTINUE_ON_ERROR:
                self.stop_event.set()
            return {}
    
    def run(self):
        """Main loop to collect metrics at intervals"""
        try:
            while not self.stop_event.is_set():
                self.collect_and_log_metrics()
                # Sleep for the interval, but check for stop event more frequently
                for _ in range(int(self.log_interval)):
                    if self.stop_event.is_set():
                        break
                    time.sleep(1)
            
            print("Performance monitoring stopped")
        
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
        except Exception as e:
            print(f"Error in monitoring loop: {e}")
        finally:
            print(f"Metrics have been saved to {self.output_file}")

if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Monitor performance of PII Analyzer")
    parser.add_argument("--interval", type=int, default=LOG_INTERVAL,
                        help=f"Logging interval in seconds (default: {LOG_INTERVAL})")
    parser.add_argument("--output", type=str, default=OUTPUT_FILE,
                        help=f"Output file path (default: {OUTPUT_FILE})")
    parser.add_argument("--no-tika", action="store_true",
                        help="Disable Tika monitoring")
    parser.add_argument("--no-db", action="store_true",
                        help="Disable database monitoring")
    
    args = parser.parse_args()
    
    # Update configuration
    if args.no_tika:
        MONITOR_TIKA = False
    if args.no_db:
        MONITOR_DB = False
    
    # Start monitoring
    monitor = PerfMonitor(log_interval=args.interval, output_file=args.output)
    monitor.run() 
#!/usr/bin/env python3
"""
Tika Load Balancer
Distributes requests across multiple Tika instances
"""

import os
import time
import random
import threading
import requests
from typing import List, Dict, Optional, Tuple

from ..utils.logger import app_logger as logger

class TikaLoadBalancer:
    """Load balancer for distributing requests across multiple Tika instances."""
    
    def __init__(self, tika_servers: Optional[List[str]] = None):
        """Initialize the load balancer with a list of Tika server URLs.
        
        Args:
            tika_servers: List of Tika server URLs
                          If None, will try to get from environment or use defaults
        """
        if tika_servers:
            self.tika_servers = tika_servers
        else:
            # Check for comma-separated list in environment variable
            env_servers = os.environ.get("TIKA_SERVER_ENDPOINTS")
            if env_servers:
                self.tika_servers = [s.strip() for s in env_servers.split(",")]
            else:
                # Default to three local instances on different ports
                self.tika_servers = [
                    "http://localhost:9998",
                    "http://localhost:9999",
                    "http://localhost:10000"
                ]
        
        # Server health status
        self.server_status = {server: True for server in self.tika_servers}
        self.last_checked = {server: 0 for server in self.tika_servers}
        
        # Request distribution tracking
        self.request_counts = {server: 0 for server in self.tika_servers}
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Health check interval (seconds)
        self.health_check_interval = 30
        
        logger.info(f"Tika load balancer initialized with {len(self.tika_servers)} servers: {self.tika_servers}")
    
    def get_available_servers(self) -> List[str]:
        """Get list of available Tika servers.
        
        Returns:
            List of available server URLs
        """
        with self.lock:
            return [server for server, status in self.server_status.items() if status]
    
    def get_server(self) -> Optional[str]:
        """Get next available Tika server using round-robin algorithm with health checks.
        
        Returns:
            Server URL or None if no servers are available
        """
        with self.lock:
            available_servers = self.get_available_servers()
            
            if not available_servers:
                # If no servers are available, try to recover one that hasn't been checked recently
                for server, last_time in self.last_checked.items():
                    if time.time() - last_time > self.health_check_interval:
                        if self._check_server_health(server):
                            available_servers = [server]
                            break
            
            if not available_servers:
                return None
            
            # Select the server with the lowest request count
            selected_server = min(available_servers, key=lambda s: self.request_counts[s])
            
            # Increment request count
            self.request_counts[selected_server] += 1
            
            return selected_server
    
    def _check_server_health(self, server: str) -> bool:
        """Check if a Tika server is healthy.
        
        Args:
            server: Server URL to check
            
        Returns:
            True if server is healthy, False otherwise
        """
        try:
            response = requests.get(f"{server}/tika", timeout=5)
            is_healthy = response.status_code == 200
            
            with self.lock:
                self.server_status[server] = is_healthy
                self.last_checked[server] = time.time()
            
            if not is_healthy:
                logger.warning(f"Tika server {server} is unhealthy: status code {response.status_code}")
            
            return is_healthy
        except Exception as e:
            with self.lock:
                self.server_status[server] = False
                self.last_checked[server] = time.time()
            
            logger.warning(f"Tika server {server} is unhealthy: {str(e)}")
            return False
    
    def mark_server_error(self, server: str) -> None:
        """Mark a server as having an error.
        
        Args:
            server: Server URL that experienced an error
        """
        with self.lock:
            self.server_status[server] = False
            self.last_checked[server] = time.time()
            logger.warning(f"Marked Tika server {server} as unhealthy due to processing error")
    
    def check_all_servers(self) -> Dict[str, bool]:
        """Check health of all Tika servers.
        
        Returns:
            Dictionary mapping server URLs to health status
        """
        results = {}
        for server in self.tika_servers:
            results[server] = self._check_server_health(server)
        
        return results
    
    def get_stats(self) -> Dict:
        """Get load balancer statistics.
        
        Returns:
            Dictionary with load balancer statistics
        """
        with self.lock:
            total_requests = sum(self.request_counts.values())
            return {
                "servers": len(self.tika_servers),
                "available_servers": len(self.get_available_servers()),
                "total_requests": total_requests,
                "requests_per_server": dict(self.request_counts),
                "server_status": dict(self.server_status),
                "last_checked": {server: time.time() - t for server, t in self.last_checked.items()}
            } 
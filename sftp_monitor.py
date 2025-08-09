#!/usr/bin/env python3
"""
SFTP Directory Monitor
Monitors a directory for new files and folders, automatically uploads them via SFTP
with verification, retry logic, and comprehensive logging.
"""

import os
import sys
import time
import json
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, Set, Optional, Tuple
from queue import Queue, Empty
from dataclasses import dataclass, asdict
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import paramiko
from paramiko import SSHClient, SFTPClient
import configparser

# Configuration data class
@dataclass
class Config:
    # Local directory settings
    watch_directory: str
    # SFTP settings
    sftp_host: str
    sftp_port: int
    sftp_username: str
    sftp_password: str
    sftp_remote_path: str
    # Application settings
    upload_queue_size: int = 1000
    retry_interval: int = 30
    max_retries: int = 5
    verify_uploads: bool = True
    log_level: str = "INFO"
    log_file: str = "sftp_monitor.log"
    state_file: str = "upload_state.json"

class UploadState:
    """Manages the state of uploaded files"""
    
    def __init__(self, state_file: str):
        self.state_file = state_file
        self.uploaded_files: Dict[str, dict] = {}
        self.failed_files: Dict[str, dict] = {}
        self.load_state()
    
    def load_state(self):
        """Load state from file"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    self.uploaded_files = data.get('uploaded_files', {})
                    self.failed_files = data.get('failed_files', {})
                logging.info(f"Loaded state: {len(self.uploaded_files)} uploaded, {len(self.failed_files)} failed")
        except Exception as e:
            logging.error(f"Error loading state file: {e}")
            self.uploaded_files = {}
            self.failed_files = {}
    
    def save_state(self):
        """Save state to file"""
        try:
            state_data = {
                'uploaded_files': self.uploaded_files,
                'failed_files': self.failed_files,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.state_file, 'w') as f:
                json.dump(state_data, f, indent=2)
        except Exception as e:
            logging.error(f"Error saving state file: {e}")
    
    def mark_uploaded(self, local_path: str, remote_path: str, file_hash: str, size: int):
        """Mark a file as successfully uploaded"""
        self.uploaded_files[local_path] = {
            'remote_path': remote_path,
            'hash': file_hash,
            'size': size,
            'upload_time': datetime.now().isoformat()
        }
        # Remove from failed files if it was there
        self.failed_files.pop(local_path, None)
        self.save_state()
    
    def mark_failed(self, local_path: str, error: str, retry_count: int = 0):
        """Mark a file as failed to upload"""
        self.failed_files[local_path] = {
            'error': error,
            'retry_count': retry_count,
            'last_attempt': datetime.now().isoformat()
        }
        self.save_state()
    
    def is_uploaded(self, local_path: str) -> bool:
        """Check if file has been uploaded"""
        return local_path in self.uploaded_files
    
    def get_failed_files(self) -> Dict[str, dict]:
        """Get list of failed files for retry"""
        return self.failed_files.copy()
    
    def clear_failed_file(self, local_path: str):
        """Remove file from failed list"""
        self.failed_files.pop(local_path, None)
        self.save_state()

class SFTPUploader:
    """Handles SFTP connections and file uploads"""
    
    def __init__(self, config: Config):
        self.config = config
        self.ssh_client: Optional[SSHClient] = None
        self.sftp_client: Optional[SFTPClient] = None
        self.connected = False
    
    def connect(self) -> bool:
        """Establish SFTP connection"""
        try:
            self.ssh_client = SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            self.ssh_client.connect(
                hostname=self.config.sftp_host,
                port=self.config.sftp_port,
                username=self.config.sftp_username,
                password=self.config.sftp_password,
                timeout=30
            )
            
            self.sftp_client = self.ssh_client.open_sftp()
            self.connected = True
            logging.info(f"Connected to SFTP server {self.config.sftp_host}:{self.config.sftp_port}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to connect to SFTP server: {e}")
            self.disconnect()
            return False
    
    def disconnect(self):
        """Close SFTP connection"""
        try:
            if self.sftp_client:
                self.sftp_client.close()
            if self.ssh_client:
                self.ssh_client.close()
        except Exception as e:
            logging.warning(f"Error during disconnect: {e}")
        finally:
            self.sftp_client = None
            self.ssh_client = None
            self.connected = False
    
    def ensure_remote_directory(self, remote_path: str) -> bool:
        """Ensure remote directory exists"""
        try:
            if not self.sftp_client:
                return False
            
            # Split path and create directories recursively
            parts = remote_path.strip('/').split('/')
            current_path = ''
            
            for part in parts:
                current_path += '/' + part
                try:
                    self.sftp_client.stat(current_path)
                except FileNotFoundError:
                    self.sftp_client.mkdir(current_path)
                    logging.debug(f"Created remote directory: {current_path}")
            
            return True
        except Exception as e:
            logging.error(f"Error creating remote directory {remote_path}: {e}")
            return False
    
    def upload_file(self, local_path: str, remote_path: str) -> Tuple[bool, str, Optional[str]]:
        """
        Upload a file via SFTP with verification
        Returns: (success, error_message, file_hash)
        """
        if not self.connected or not self.sftp_client:
            return False, "Not connected to SFTP server", None
        
        try:
            # Calculate local file hash
            local_hash = self.calculate_file_hash(local_path)
            if not local_hash:
                return False, "Could not calculate file hash", None
            
            # Ensure remote directory exists
            remote_dir = os.path.dirname(remote_path)
            if not self.ensure_remote_directory(remote_dir):
                return False, f"Could not create remote directory: {remote_dir}", None
            
            # Upload the file
            logging.debug(f"Uploading {local_path} -> {remote_path}")
            self.sftp_client.put(local_path, remote_path)
            
            # Verify upload if enabled
            if self.config.verify_uploads:
                if not self.verify_upload(local_path, remote_path, local_hash):
                    return False, "Upload verification failed", None
            
            logging.info(f"Successfully uploaded: {local_path}")
            return True, "", local_hash
            
        except Exception as e:
            error_msg = f"Upload failed: {e}"
            logging.error(error_msg)
            return False, error_msg, None
    
    def verify_upload(self, local_path: str, remote_path: str, local_hash: str) -> bool:
        """Verify uploaded file matches local file"""
        try:
            # Get remote file stats
            remote_stats = self.sftp_client.stat(remote_path)
            local_stats = os.stat(local_path)
            
            # Compare file sizes
            if remote_stats.st_size != local_stats.st_size:
                logging.error(f"Size mismatch for {remote_path}: local={local_stats.st_size}, remote={remote_stats.st_size}")
                return False
            
            # For small files, download and compare hashes
            if local_stats.st_size < 100 * 1024 * 1024:  # 100MB threshold
                temp_file = f"{local_path}.tmp_verify"
                try:
                    self.sftp_client.get(remote_path, temp_file)
                    remote_hash = self.calculate_file_hash(temp_file)
                    os.unlink(temp_file)
                    
                    if remote_hash != local_hash:
                        logging.error(f"Hash mismatch for {remote_path}")
                        return False
                except Exception as e:
                    logging.warning(f"Could not verify hash for {remote_path}: {e}")
                    if os.path.exists(temp_file):
                        os.unlink(temp_file)
            
            logging.debug(f"Upload verification successful: {remote_path}")
            return True
            
        except Exception as e:
            logging.error(f"Upload verification failed: {e}")
            return False
    
    @staticmethod
    def calculate_file_hash(file_path: str) -> Optional[str]:
        """Calculate SHA256 hash of a file"""
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logging.error(f"Error calculating hash for {file_path}: {e}")
            return None

class DirectoryEventHandler(FileSystemEventHandler):
    """Handles file system events"""
    
    def __init__(self, upload_queue: Queue, watch_directory: str):
        self.upload_queue = upload_queue
        self.watch_directory = Path(watch_directory).resolve()
        self.processed_events: Set[str] = set()
    
    def on_created(self, event):
        """Handle file/directory creation events"""
        if not event.is_directory:
            self.queue_file_upload(event.src_path)
    
    def on_moved(self, event):
        """Handle file/directory move events"""
        if not event.is_directory:
            self.queue_file_upload(event.dest_path)
    
    def queue_file_upload(self, file_path: str):
        """Queue a file for upload"""
        try:
            file_path = str(Path(file_path).resolve())
            
            # Avoid duplicate processing
            event_key = f"upload:{file_path}"
            if event_key in self.processed_events:
                return
            
            # Wait for file to be completely written
            time.sleep(1)
            if not os.path.exists(file_path):
                return
            
            # Check if file is still being written
            initial_size = os.path.getsize(file_path)
            time.sleep(0.5)
            if not os.path.exists(file_path):
                return
            
            final_size = os.path.getsize(file_path)
            if initial_size != final_size:
                logging.debug(f"File still being written: {file_path}")
                return
            
            self.processed_events.add(event_key)
            
            # Add to upload queue
            if not self.upload_queue.full():
                self.upload_queue.put(file_path)
                logging.debug(f"Queued for upload: {file_path}")
            else:
                logging.warning(f"Upload queue full, skipping: {file_path}")
                
        except Exception as e:
            logging.error(f"Error queuing file {file_path}: {e}")

class SFTPDirectoryMonitor:
    """Main application class"""
    
    def __init__(self, config_file: str = "config.ini"):
        self.config = self.load_config(config_file)
        self.setup_logging()
        
        self.upload_queue = Queue(maxsize=self.config.upload_queue_size)
        self.upload_state = UploadState(self.config.state_file)
        self.sftp_uploader = SFTPUploader(self.config)
        
        self.observer = Observer()
        self.event_handler = DirectoryEventHandler(self.upload_queue, self.config.watch_directory)
        
        self.running = False
        self.upload_thread: Optional[threading.Thread] = None
        self.retry_thread: Optional[threading.Thread] = None
    
    def load_config(self, config_file: str) -> Config:
        """Load configuration from file"""
        if not os.path.exists(config_file):
            self.create_sample_config(config_file)
            raise FileNotFoundError(f"Config file {config_file} not found. Sample created.")
        
        config = configparser.ConfigParser()
        config.read(config_file)
        
        return Config(
            watch_directory=config.get('local', 'watch_directory'),
            sftp_host=config.get('sftp', 'host'),
            sftp_port=config.getint('sftp', 'port', fallback=22),
            sftp_username=config.get('sftp', 'username'),
            sftp_password=config.get('sftp', 'password'),
            sftp_remote_path=config.get('sftp', 'remote_path'),
            upload_queue_size=config.getint('app', 'upload_queue_size', fallback=1000),
            retry_interval=config.getint('app', 'retry_interval', fallback=30),
            max_retries=config.getint('app', 'max_retries', fallback=5),
            verify_uploads=config.getboolean('app', 'verify_uploads', fallback=True),
            log_level=config.get('app', 'log_level', fallback='INFO'),
            log_file=config.get('app', 'log_file', fallback='sftp_monitor.log'),
            state_file=config.get('app', 'state_file', fallback='upload_state.json')
        )
    
    def create_sample_config(self, config_file: str):
        """Create a sample configuration file"""
        config = configparser.ConfigParser()
        
        config['local'] = {
            'watch_directory': 'C:/Users/YourUser/Documents/WatchFolder'
        }
        
        config['sftp'] = {
            'host': 'your-sftp-server.com',
            'port': '22',
            'username': 'your_username',
            'password': 'your_password',
            'remote_path': '/uploads'
        }
        
        config['app'] = {
            'upload_queue_size': '1000',
            'retry_interval': '30',
            'max_retries': '5',
            'verify_uploads': 'true',
            'log_level': 'INFO',
            'log_file': 'sftp_monitor.log',
            'state_file': 'upload_state.json'
        }
        
        with open(config_file, 'w') as f:
            config.write(f)
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log_level = getattr(logging, self.config.log_level.upper())
        
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(self.config.log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def scan_existing_files(self):
        """Scan for existing files in the watch directory"""
        logging.info("Scanning for existing files...")
        
        try:
            watch_path = Path(self.config.watch_directory)
            if not watch_path.exists():
                logging.error(f"Watch directory does not exist: {self.config.watch_directory}")
                return
            
            for file_path in watch_path.rglob('*'):
                if file_path.is_file():
                    file_str = str(file_path)
                    if not self.upload_state.is_uploaded(file_str):
                        if not self.upload_queue.full():
                            self.upload_queue.put(file_str)
                            logging.debug(f"Queued existing file: {file_str}")
            
            logging.info(f"Queued {self.upload_queue.qsize()} existing files for upload")
            
        except Exception as e:
            logging.error(f"Error scanning existing files: {e}")
    
    def upload_worker(self):
        """Worker thread for processing upload queue"""
        while self.running:
            try:
                # Get file from queue with timeout
                try:
                    file_path = self.upload_queue.get(timeout=1)
                except Empty:
                    continue
                
                # Skip if already uploaded
                if self.upload_state.is_uploaded(file_path):
                    self.upload_queue.task_done()
                    continue
                
                # Skip if file no longer exists
                if not os.path.exists(file_path):
                    logging.warning(f"File no longer exists: {file_path}")
                    self.upload_queue.task_done()
                    continue
                
                # Ensure SFTP connection
                if not self.sftp_uploader.connected:
                    if not self.sftp_uploader.connect():
                        # Re-queue file and wait before retry
                        self.upload_queue.put(file_path)
                        self.upload_queue.task_done()
                        time.sleep(self.config.retry_interval)
                        continue
                
                # Calculate relative path for remote upload
                relative_path = os.path.relpath(file_path, self.config.watch_directory)
                remote_path = f"{self.config.sftp_remote_path}/{relative_path}".replace('\\', '/')
                
                # Attempt upload
                success, error, file_hash = self.sftp_uploader.upload_file(file_path, remote_path)
                
                if success:
                    file_size = os.path.getsize(file_path)
                    self.upload_state.mark_uploaded(file_path, remote_path, file_hash, file_size)
                    logging.info(f"Upload completed: {file_path}")
                else:
                    self.upload_state.mark_failed(file_path, error)
                    logging.error(f"Upload failed: {file_path} - {error}")
                
                self.upload_queue.task_done()
                
            except Exception as e:
                logging.error(f"Error in upload worker: {e}")
                if not self.upload_queue.empty():
                    self.upload_queue.task_done()
    
    def retry_worker(self):
        """Worker thread for retrying failed uploads"""
        while self.running:
            try:
                time.sleep(self.config.retry_interval)
                
                failed_files = self.upload_state.get_failed_files()
                if not failed_files:
                    continue
                
                logging.info(f"Retrying {len(failed_files)} failed uploads...")
                
                for file_path, info in failed_files.items():
                    if not self.running:
                        break
                    
                    retry_count = info.get('retry_count', 0)
                    if retry_count >= self.config.max_retries:
                        logging.warning(f"Max retries exceeded for: {file_path}")
                        continue
                    
                    if os.path.exists(file_path):
                        if not self.upload_queue.full():
                            self.upload_queue.put(file_path)
                            # Update retry count
                            self.upload_state.mark_failed(
                                file_path, 
                                info.get('error', 'Unknown error'), 
                                retry_count + 1
                            )
                    else:
                        # File no longer exists, remove from failed list
                        self.upload_state.clear_failed_file(file_path)
                        logging.info(f"Removed non-existent file from retry list: {file_path}")
                
            except Exception as e:
                logging.error(f"Error in retry worker: {e}")
    
    def start(self):
        """Start the directory monitor"""
        logging.info("Starting SFTP Directory Monitor...")
        
        # Validate watch directory
        if not os.path.exists(self.config.watch_directory):
            logging.error(f"Watch directory does not exist: {self.config.watch_directory}")
            return False
        
        # Test SFTP connection
        if not self.sftp_uploader.connect():
            logging.error("Could not establish initial SFTP connection")
            return False
        
        self.sftp_uploader.disconnect()
        
        self.running = True
        
        # Start worker threads
        self.upload_thread = threading.Thread(target=self.upload_worker, daemon=True)
        self.retry_thread = threading.Thread(target=self.retry_worker, daemon=True)
        
        self.upload_thread.start()
        self.retry_thread.start()
        
        # Scan for existing files
        self.scan_existing_files()
        
        # Start directory monitoring
        self.observer.schedule(
            self.event_handler,
            self.config.watch_directory,
            recursive=True
        )
        self.observer.start()
        
        logging.info(f"Monitoring directory: {self.config.watch_directory}")
        logging.info("SFTP Directory Monitor started successfully")
        
        return True
    
    def stop(self):
        """Stop the directory monitor"""
        logging.info("Stopping SFTP Directory Monitor...")
        
        self.running = False
        
        # Stop directory observer
        self.observer.stop()
        self.observer.join()
        
        # Wait for upload queue to empty
        logging.info("Waiting for upload queue to empty...")
        self.upload_queue.join()
        
        # Wait for worker threads
        if self.upload_thread:
            self.upload_thread.join(timeout=10)
        if self.retry_thread:
            self.retry_thread.join(timeout=10)
        
        # Disconnect SFTP
        self.sftp_uploader.disconnect()
        
        logging.info("SFTP Directory Monitor stopped")
    
    def run(self):
        """Run the monitor until interrupted"""
        if not self.start():
            return
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Received interrupt signal")
        finally:
            self.stop()

def main():
    """Main entry point"""
    try:
        monitor = SFTPDirectoryMonitor()
        monitor.run()
    except FileNotFoundError as e:
        print(f"Configuration error: {e}")
        print("Please edit config.ini with your settings and restart.")
    except Exception as e:
        print(f"Fatal error: {e}")
        logging.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
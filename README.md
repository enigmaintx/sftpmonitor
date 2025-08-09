# sftpmonitor
Python script to monitor a directory for new files / folders and SFTP them to a remote server.
Features:

Directory Monitoring: Uses watchdog to monitor for new files and folders
SFTP Upload: Automatically uploads files using paramiko
File Tracking: Maintains state in JSON file to track uploaded files
File Verification: Compares file hashes and sizes to verify successful uploads
Error Handling & Retry: Automatically retries failed uploads when connectivity is restored
Comprehensive Logging: Detailed logging with configurable levels
Configuration Management: Easy-to-use INI configuration file

Required Dependencies: watchdog paramiko

Setup Instructions:

Create a directory to place the script in to run from.

First Run: The application will create a sample config.ini file on first run

Configure: Edit config.ini with your settings:

Local directory to monitor
SFTP server details (host, port, username, password)
Remote upload path
Application settings (retry intervals, logging level, etc.)


Run: Execute with python sftp_monitor.py

Architecture:

Main Thread: Handles directory monitoring and coordination
Upload Worker Thread: Processes the upload queue
Retry Worker Thread: Handles retry logic for failed uploads
Event Handler: Detects new files and queues them for upload

Key Components:

Config Class: Manages all configuration settings
UploadState: Tracks uploaded/failed files with persistent storage
SFTPUploader: Handles SFTP connections, uploads, and verification
DirectoryEventHandler: Processes file system events
SFTPDirectoryMonitor: Main orchestrator class

Features for Debugging:

Configurable logging levels (DEBUG, INFO, WARNING, ERROR)
Separate log file with timestamps
Console output for real-time monitoring
State persistence for tracking upload history
Detailed error messages and stack traces

The application is designed to be robust, with proper error handling, connection recovery, and the ability to resume operations after interruption. It will automatically scan for existing files on startup and handle various edge cases like files being written while monitoring, network interruptions, and server connectivity issues.

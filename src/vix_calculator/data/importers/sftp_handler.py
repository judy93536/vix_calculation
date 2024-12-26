import pysftp
import os
import hashlib
import logging
import zipfile
import shutil
from datetime import datetime
from typing import List, Dict, Set

class SafeSftpHandler:
    def __init__(self, hostname: str, username: str, password: str, port: int = 22):
        """
        Initialize SFTP handler with robust safety features
        """
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.connection = None
        
        # Setup logging
        self.setup_logging()
        
        # Directory structure
        self.base_dir = "/raid/Python/CBOE_VIX/SPX/spx_eod_1545"
        self.dirs = {
            'import': f"{self.base_dir}/import",
            'import_csv': f"{self.base_dir}/import_csv",
            'zip': f"{self.base_dir}/zip",
            'csv': f"{self.base_dir}/csv"
        }
        
        # Create directories if they don't exist
        for dir_path in self.dirs.values():
            os.makedirs(dir_path, exist_ok=True)

    def setup_logging(self):
        """Configure logging with timestamps and levels"""
        log_dir = "/raid/Python/CBOE_VIX/SPX/spx_eod_1545/logs"
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = f"{log_dir}/sftp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def calculate_checksum(self, file_path: str) -> str:
        """Calculate SHA-256 checksum of a file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def connect(self):
        """Establish SFTP connection with error handling"""
        try:
            self.connection = pysftp.Connection(
                host=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port
            )
            self.logger.info(f"Connected to {self.hostname} as {self.username}")
        except Exception as e:
            self.logger.error(f"SFTP connection failed: {str(e)}")
            raise

    def get_local_files(self) -> Set[str]:
        """Get set of files already in zip directory"""
        return set(os.listdir(self.dirs['zip']))

    def verify_zip_integrity(self, zip_path: str) -> bool:
        """Verify ZIP file integrity"""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                result = zip_ref.testzip()
                if result is not None:
                    self.logger.error(f"Corrupt file found in {zip_path}: {result}")
                    return False
                return True
        except Exception as e:
            self.logger.error(f"ZIP verification failed for {zip_path}: {str(e)}")
            return False

    def safe_download(self, remote_path: str, local_files: Set[str]) -> List[str]:
        """
        Download new files with validation and safety checks
        Returns list of successfully downloaded files
        """
        downloaded_files = []
        try:
            remote_files = self.connection.listdir_attr(remote_path)
            
            for remote_file in remote_files:
                if remote_file.filename not in local_files:
                    # Temporary download path
                    temp_path = os.path.join(self.dirs['import'], f"temp_{remote_file.filename}")
                    final_path = os.path.join(self.dirs['import'], remote_file.filename)
                    
                    # Download to temporary location
                    self.logger.info(f"Downloading {remote_file.filename}")
                    self.connection.get(
                        f"{remote_path}/{remote_file.filename}", 
                        temp_path
                    )
                    
                    # Verify file size
                    if os.path.getsize(temp_path) != remote_file.st_size:
                        raise ValueError(f"Size mismatch for {remote_file.filename}")
                    
                    # Verify ZIP integrity
                    if not self.verify_zip_integrity(temp_path):
                        raise ValueError(f"ZIP integrity check failed for {remote_file.filename}")
                    
                    # If all checks pass, move to final location
                    shutil.move(temp_path, final_path)
                    downloaded_files.append(remote_file.filename)
                    self.logger.info(f"Successfully downloaded and verified {remote_file.filename}")
                    
        except Exception as e:
            self.logger.error(f"Download process failed: {str(e)}")
            # Clean up any temporary files
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise
            
        return downloaded_files

    def extract_files(self, downloaded_files: List[str]) -> bool:
        """
        Safely extract downloaded ZIP files to import_csv directory
        """
        try:
            for filename in downloaded_files:
                zip_path = os.path.join(self.dirs['import'], filename)
                
                # Verify ZIP integrity again before extraction
                if not self.verify_zip_integrity(zip_path):
                    raise ValueError(f"ZIP integrity check failed before extraction: {filename}")
                
                # Extract files
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(self.dirs['import_csv'])
                
                # Move ZIP to archive directory
                shutil.move(
                    zip_path,
                    os.path.join(self.dirs['zip'], filename)
                )
                
                self.logger.info(f"Successfully extracted and archived {filename}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Extraction process failed: {str(e)}")
            return False

    def disconnect(self):
        """Safely disconnect from SFTP server"""
        if self.connection:
            self.connection.close()
            self.logger.info(f"Disconnected from {self.hostname}")
            self.connection = None

def main():
    handler = SafeSftpHandler(
        hostname='sftp.datashop.livevol.com',
        username='goodyear_judy_gmail_com',
        password='n734UU-93536'
    )
    
    remote_path = "/subscriptions/order_000036090/item_000042140/"
    
    try:
        # Connect to SFTP
        handler.connect()
        
        # Get existing files
        local_files = handler.get_local_files()
        
        # Download new files
        downloaded_files = handler.safe_download(remote_path, local_files)
        
        if downloaded_files:
            # Extract files
            if handler.extract_files(downloaded_files):
                print(f"Successfully processed {len(downloaded_files)} new files")
            else:
                print("Extraction process failed")
        else:
            print("No new files to download")
            
    except Exception as e:
        print(f"Process failed: {str(e)}")
        
    finally:
        handler.disconnect()

if __name__ == "__main__":
    main()


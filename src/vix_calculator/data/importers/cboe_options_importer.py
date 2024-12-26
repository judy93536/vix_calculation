from pathlib import Path
import pysftp
from .base_importer import BaseImporter

class CboeOptionsImporter(BaseImporter):
    """Handles SFTP downloading of CBOE options data"""
    
    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.sftp_config = self.config['sftp']['cboe']
        self.connection = None
        
        # Set up paths from config
        self.paths = {
            'import': Path(self.config['paths']['spx']['base']) / 'import',
            'zip': Path(self.config['paths']['spx']['base']) / 'zip'
        }
        
        # Create directories if they don't exist
        for path in self.paths.values():
            path.mkdir(parents=True, exist_ok=True)

            
    def connect(self):
        """Establishes SFTP connection"""
        try:
            self.logger.info(f"Attempting to connect to {self.sftp_config['hostname']} as {self.sftp_config['username']}")
            
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None  # For testing - in production we should use host keys
            
            self.connection = pysftp.Connection(
                host=self.sftp_config['hostname'],
                username=self.sftp_config['username'],
                password=self.sftp_config['password'],
                port=self.sftp_config.get('port', 22),
                cnopts=cnopts
            )
            self.logger.info(f"Successfully connected to {self.sftp_config['hostname']}")
            return True
            
        except Exception as e:
            self.logger.error(f"SFTP connection failed: {str(e)}")
            self.logger.error(f"Attempted connection with username: {self.sftp_config['username']}")
            self.logger.error(f"Host: {self.sftp_config['hostname']}")
            self.logger.error(f"Port: {self.sftp_config.get('port', 22)}")
            return False
        
    
    def disconnect(self):
        """Closes SFTP connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("SFTP connection closed")
            self.connection = None

    def get_local_files(self) -> set:
        """Get set of files already in zip directory"""
        return {f.name for f in self.paths['zip'].glob('*.zip')}

    def get_remote_files(self) -> set:
        """Get set of files on SFTP server"""
        try:
            remote_path = self.sftp_config['remote_path']
            files = set(self.connection.listdir(remote_path))
            self.logger.info(f"Found {len(files)} files on SFTP server")
            return files
        except Exception as e:
            self.logger.error(f"Failed to list remote files: {str(e)}")
            return set()

    def download_file(self, filename: str) -> bool:
        """
        Download a single file from SFTP
        
        Args:
            filename: Name of file to download
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            remote_path = f"{self.sftp_config['remote_path']}/{filename}"
            local_path = self.paths['import'] / filename
            
            self.logger.info(f"Downloading {filename}")
            self.connection.get(remote_path, str(local_path))
            self.logger.info(f"Successfully downloaded {filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to download {filename}: {str(e)}")
            return False

    def download_new_files(self) -> tuple[int, int]:
        """
        Download all new files from SFTP
        
        Returns:
            tuple: (number of files downloaded, number of files failed)
        """
        if not self.connection:
            if not self.connect():
                return 0, 0
                
        try:
            local_files = self.get_local_files()
            remote_files = self.get_remote_files()
            
            new_files = remote_files - local_files
            self.logger.info(f"Found {len(new_files)} new files to download")
            
            downloaded = 0
            failed = 0
            
            for filename in new_files:
                if self.download_file(filename):
                    downloaded += 1
                else:
                    failed += 1
            
            return downloaded, failed
            
        except Exception as e:
            self.logger.error(f"Download process failed: {str(e)}")
            return 0, 0
            
        finally:
            self.disconnect()

def main():
    """Test SFTP functionality"""
    config_path = 'config/config.yaml'
    importer = CboeOptionsImporter(config_path)
    
    downloaded, failed = importer.download_new_files()
    print(f"Download complete: {downloaded} successful, {failed} failed")

if __name__ == "__main__":
    main()
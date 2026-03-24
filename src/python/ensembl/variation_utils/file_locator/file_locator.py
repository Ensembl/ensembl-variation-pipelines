from abc import ABC, abstractmethod
from typing import Optional

class FileLocator(ABC):
    """Abstract base class for file locators"""
    
    def __init__(self):
        self.file: Optional[str] = None
    
    @abstractmethod
    def locate_file(self) -> str:
        """Locate a file and return its path"""
        pass
    
    @abstractmethod
    def copy_file(self, target_dir: str) -> bool:
        """Copy file to target directory"""
        pass  

# Factory Classes
class FileLocatorFactory(ABC):
    """Factory for creating file locators"""

    @abstractmethod
    def set_locator(self):
        pass
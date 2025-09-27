from abc import ABC, abstractmethod

class GenomeMatcher(ABC):
    @abstractmethod
    def match(self):
        pass

from abc import abstractmethod
from mobility.asset import Asset

class InMemoryAsset(Asset):
    
    def __init__(self, inputs):
        super().__init__(inputs)
    
    def get_cached_hash(self):
        return self.inputs_hash
    
    # @abstractmethod
    def get(self):
        pass
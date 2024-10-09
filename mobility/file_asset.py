import pathlib
import os

from mobility.asset import Asset
from typing import Any
from abc import abstractmethod

class FileAsset(Asset):
    """
    Abstract base class representing an Asset, with functionality for cache validation
    based on input hash comparison.

    Attributes:
        inputs (Dict): A dictionary of inputs used to generate the Asset.
        cache_path (pathlib.Path): The file path for storing the Asset.
        hash_path (pathlib.Path): The file path for storing the hash of the inputs.
        inputs_hash (str): The hash of the inputs.

    Methods:
        get_cached_asset: Abstract method to retrieve a cached Asset.
        create_and_get_asset: Abstract method to create and retrieve an Asset.
        get: Retrieves the cached Asset or creates a new one if needed.
        compute_inputs_hash: Computes a hash based on the inputs.
        is_update_needed: Checks if an update is needed based on the input hash.
        get_cached_hash: Retrieves the cached hash from the file system.
        update_hash: Updates the cached hash with a new hash value.
    """
    
    def __init__(self, inputs: dict, cache_path: pathlib.Path | dict[str, pathlib.Path]):
        """
        Initializes the Asset instance with given inputs and cache path.

        Args:
            inputs (Dict): The inputs used for creating or updating the Asset.
            cache_path (pathlib.Path): The path where the Asset is cached.
        """
        
        super().__init__(inputs)
        
        if isinstance(cache_path, dict):
            self.cache_path = {k: cp.parent / (self.inputs_hash + "-" + cp.name) for k, cp in cache_path.items()}
            self.hash_path = self.cache_path[list(self.cache_path.keys())[0]].with_suffix(".inputs-hash")
        else:
            cache_path = pathlib.Path(cache_path)
            filename = self.inputs_hash + "-" + cache_path.name
            cache_path = cache_path.parent / filename
            self.cache_path = cache_path.parent / filename
            self.hash_path = cache_path.with_suffix(".inputs-hash")
            if not cache_path.parent.exists():
                os.makedirs(cache_path.parent)
            
        self.update_hash(self.inputs_hash)
    
    @abstractmethod
    def get_cached_asset(self):
        pass

    @abstractmethod
    def create_and_get_asset(self):
        pass
    
    def get(self, *args, **kwargs) -> Any:
        """
        Retrieves the Asset, either from the cache or by creating a new one if the
        cache is outdated or non-existent.

        Returns:
            The retrieved or newly created Asset.
        """
        if self.is_update_needed():
            asset = self.create_and_get_asset(*args, **kwargs)
            self.update_hash(self.inputs_hash)
            return asset
        return self.get_cached_asset(*args, **kwargs)
        

    def is_update_needed(self) -> bool:
        """
        Checks if an update to the Asset is needed based on the current inputs hash,
        or the non existence of the output file.

        Returns:
            True if an update is needed, False otherwise.
        """
        same_hashes = self.get_cached_hash() == self.inputs_hash
        if isinstance(self.cache_path, dict):
            file_exists = all([cp.exists() for cp in self.cache_path.values()])
        else:
            file_exists = self.cache_path.exists()
        return same_hashes is False or file_exists is False
        
    def get_cached_hash(self) -> str:
        """
        Retrieves the cached hash of the Asset's inputs from the file system.

        Returns:
            The cached hash string if it exists, otherwise None.
        """
        if self.hash_path.exists():
            with open(self.hash_path, "r") as f:
                return f.read()
        return None
    
    def update_hash(self, new_hash: str) -> None:
        """
        Updates the cached hash of the Asset's inputs with a new hash.

        Args:
            new_hash (str): The new hash string to be cached.
        """
        self.inputs_hash = new_hash
        with open(self.hash_path, "w") as f:
            f.write(new_hash)

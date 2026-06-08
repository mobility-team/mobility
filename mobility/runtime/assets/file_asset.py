import pathlib
import os

from mobility.runtime.assets.asset import Asset
from mobility.runtime.assets.resolver import (
    asset_resolution_context,
    get_current_asset_resolver,
)
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
            basename = cache_path.name
            filename = self.inputs_hash + "-" + basename
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
        Retrieve the asset, ensuring that all upstream dependencies are up to date.
    
        This method first checks and rebuilds any ancestor FileAssets that are stale,
        then retrieves the current asset. If the asset itself is outdated or missing,
        it is rebuilt and its input hash is updated.
    
        Returns:
            Any: The cached or newly created asset.
        """
        
        resolver = get_current_asset_resolver()
        if resolver is not None:
            return resolver.get(self, *args, **kwargs)

        with asset_resolution_context() as resolver:
            return resolver.get(self, *args, **kwargs)
        

    def is_update_needed(self) -> bool:
        """
        Determine whether the asset requires an update.
    
        An update is needed if the recorded input hash differs from the current one
        or if the cached output file(s) are missing.
    
        Returns:
            bool: True if the asset is outdated or missing, False otherwise.
        """
        return self.inputs_changed() or self.assets_missing()
    
    def inputs_changed(self):
        """
        Check whether the asset's input hash differs from the cached version.
        
        Returns:
            bool: True if the cached hash does not match the current input hash.
        """
        return self.get_cached_hash() != self.inputs_hash
        
    def assets_missing(self):
        """
        Check whether the cached output file(s) exist.
        
        Returns:
            bool: True if any expected cache file is missing, False otherwise.
        """
        if isinstance(self.cache_path, dict):
            file_exists = all(cp.exists() for cp in self.cache_path.values())
        else:
            file_exists = self.cache_path.exists()
        return not file_exists
            
    def update_ancestors_if_needed(self):
        """
        Identify and rebuild stale ancestor FileAssets in dependency order.
        
        Builds a directed acyclic graph (DAG) of upstream FileAssets and determines
        which ones require updates. Those assets, along with all their descendants,
        are rebuilt in topological order. Each rebuilt asset also has its input hash
        refreshed after creation.
        
        Raises:
            RuntimeError: If a dependency cycle is detected among FileAssets.
        """
        
        resolver = get_current_asset_resolver()
        if resolver is not None:
            resolver.prepare_upstream_assets(self)
            return None

        with asset_resolution_context() as resolver:
            resolver.prepare_upstream_assets(self)
        return None
        
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
        self.hash_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.hash_path, "w") as f:
            f.write(new_hash)
            

    def remove(self):

        if isinstance(self.cache_path, dict):
            for k, v in self.cache_path.items():
                path = pathlib.Path(v)
                if path.exists():
                    path.unlink()
        else:
            path = pathlib.Path(self.cache_path)
            if path.exists():
                path.unlink()
            

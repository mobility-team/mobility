import json
import hashlib
import pathlib

import geopandas as gpd

from abc import ABC, abstractmethod
from dataclasses import is_dataclass, fields
from pandas.util import hash_pandas_object

class Asset(ABC):
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
    
    def __init__(self, inputs: dict):
        
        self.value = None
        self.inputs = inputs
        self.inputs_hash = self.compute_inputs_hash()
        
        for k, v in self.inputs.items():
            setattr(self, k, v)
    
    @abstractmethod
    def get(self):
        pass
        
    def compute_inputs_hash(self) -> str:
        """
        Computes a hash based on the current inputs of the Asset.
    
        Returns:
            A hash string representing the current state of the inputs.
        """
        def serialize(value):
            """
            Recursively serializes a value, handling nested dataclasses and sets.
            """

            if isinstance(value, Asset):
                return value.get_cached_hash()
            
            elif isinstance(value, list) and all(isinstance(v, Asset) for v in value):
                return {i: serialize(v) for i, v in enumerate(value)}
            
            elif is_dataclass(value):
                return {field.name: serialize(getattr(value, field.name)) for field in fields(value)}
            
            elif isinstance(value, dict):
               
               return {k: serialize(v) for k, v in value.items()}
            
            elif isinstance(value, set):
                return list(value)
            
            elif isinstance(value, pathlib.Path):
                return str(value)
            
            elif isinstance(value, gpd.GeoDataFrame):
                geom_hash = hashlib.sha256(b"".join(value.geometry.to_wkb())).hexdigest()
                attr_hash = hash_pandas_object(value.drop(columns="geometry")).sum()
                return hashlib.sha256((geom_hash + str(attr_hash)).encode()).hexdigest()
            
            else:
                return value
    
        hashable_inputs = {k: serialize(v) for k, v in self.inputs.items()}
        serialized_inputs = json.dumps(hashable_inputs, sort_keys=True).encode('utf-8')
        
        return hashlib.md5(serialized_inputs).hexdigest()
    
    
        
        

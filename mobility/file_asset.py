import pathlib
import os
import networkx as nx

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
            
            for k in self.cache_path.keys():
                
                self.update_ui_db(
                    cache_path[k].name,
                    self.inputs_hash,
                    self.cache_path[k]
                )
            
        else:
            
            cache_path = pathlib.Path(cache_path)
            basename = cache_path.name
            filename = self.inputs_hash + "-" + basename
            cache_path = cache_path.parent / filename
            self.cache_path = cache_path.parent / filename
            self.hash_path = cache_path.with_suffix(".inputs-hash")
            if not cache_path.parent.exists():
                os.makedirs(cache_path.parent)
                
            self.update_ui_db(
                basename,
                self.inputs_hash,
                cache_path
            )
            
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
            
        # Build a graph of input assets
        graph = nx.DiGraph()
        
        def add_upstream_deps(asset):
            graph.add_node(asset)
            for inp in asset.inputs.values():
                if isinstance(inp, FileAsset):
                    graph.add_node(inp)
                    graph.add_edge(inp, asset)
                    add_upstream_deps(inp)
        
        for inp in self.inputs.values():
            if isinstance(inp, FileAsset):
                add_upstream_deps(inp)
        
        # Find out which ones need to be updated and recompute them, as well
        # as all their descendants
        update_needed_assets = set()

        for asset in graph.nodes:
            if asset.is_update_needed():
                update_needed_assets.add(asset)
                for descendant in nx.descendants(graph, asset):
                    update_needed_assets.add(descendant)
    
        topo_order = list(nx.topological_sort(graph))
        
        assets = [asset for asset in topo_order if asset in update_needed_assets]
        
        for asset in assets:
            asset.get()
            
        upstream_updates = len(assets) > 0
        
        return same_hashes is False or file_exists is False or upstream_updates is True
        
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
            
            
    def update_ui_db(self, file_name: str, inputs_hash: str, cache_path: pathlib.Path) -> None:
        
        db_path = pathlib.Path(os.environ["MOBILITY_PROJECT_DATA_FOLDER"]) / "ui.sqlite"
        
        # with sqlite3.connect(db_path) as conn:
            
        #     cursor = conn.cursor()
    
        #     cursor.execute("""
        #         CREATE TABLE IF NOT EXISTS ui_cache (
        #             id INTEGER PRIMARY KEY AUTOINCREMENT,
        #             file_name TEXT NOT NULL,
        #             inputs_hash TEXT NOT NULL,
        #             cache_path TEXT NOT NULL,
        #             UNIQUE(file_name, inputs_hash)
        #         )
        #     """)
            
        #     cursor.execute("""
        #         INSERT INTO ui_cache (file_name, inputs_hash, cache_path)
        #         VALUES (?, ?, ?)
        #         ON CONFLICT(file_name, inputs_hash) 
        #         DO UPDATE SET cache_path = excluded.cache_path
        #     """, (file_name, inputs_hash, str(cache_path)))
        
        #     conn.commit()
            

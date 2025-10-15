import json
import hashlib
import pathlib

from abc import ABC, abstractmethod
from dataclasses import is_dataclass, fields


class Asset(ABC):
    """
    Abstract base class representing an Asset, with functionality for cache validation
    based on input hash comparison.
    """

    def __init__(self, inputs: dict, cache_path=None):
        """
        Compatible with subclasses calling super().__init__(inputs, cache_path).
        """
        self.value = None
        self.inputs = inputs

        # ✅ Safe handling of cache_path (can be None, str, Path, or even dict)
        if isinstance(cache_path, (str, pathlib.Path)):
            self.cache_path = pathlib.Path(cache_path)
        else:
            self.cache_path = None  # ignore invalid types safely

        self.inputs_hash = self.compute_inputs_hash()

        # Expose inputs as attributes
        for k, v in self.inputs.items():
            setattr(self, k, v)

    @abstractmethod
    def get(self):
        pass

    def get_cached_hash(self) -> str:
        """Return cached hash for nested serialization."""
        return getattr(self, "inputs_hash", "") or ""

    def compute_inputs_hash(self) -> str:
        """Compute deterministic hash of the inputs."""

        def serialize(value):
            if isinstance(value, Asset):
                return value.get_cached_hash()
            elif isinstance(value, list) and all(isinstance(v, Asset) for v in value):
                return {i: serialize(v) for i, v in enumerate(value)}
            elif is_dataclass(value):
                return {f.name: serialize(getattr(value, f.name)) for f in fields(value)}
            elif isinstance(value, dict):
                return {k: serialize(v) for k, v in value.items()}
            elif isinstance(value, set):
                return sorted(list(value))
            elif isinstance(value, pathlib.Path):
                return str(value)
            else:
                return value

        hashable_inputs = {k: serialize(v) for k, v in self.inputs.items()}
        serialized_inputs = json.dumps(hashable_inputs, sort_keys=True).encode("utf-8")
        return hashlib.md5(serialized_inputs).hexdigest()

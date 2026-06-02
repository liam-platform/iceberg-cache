from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pyarrow as pa


class CacheStrategy(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[pa.Table]:
        raise NotImplementedError

    @abstractmethod
    def put(self, key: str, table: pa.Table) -> List[str]:
        """Store table and return the keys of any entries that were evicted."""
        raise NotImplementedError

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        raise NotImplementedError

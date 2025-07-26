from abc import ABC, abstractmethod
from typing import Dict, List

import pyarrow as pa

from core.cache_data_model import PartitionInfo


class ObjectStoreProvider(ABC):
    """Abstract interface for object store operations"""
    
    @abstractmethod
    async def load_partition(self, partition_info: PartitionInfo) -> pa.Table:
        """Load a partition from object store"""
        pass
    
    @abstractmethod
    async def load_partitions_batch(self, partitions: List[PartitionInfo]) -> Dict[str, pa.Table]:
        """Load multiple partitions in parallel"""
        pass

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class DistributedCacheCoordinator(ABC):
    """Interface for coordinating with distributed cache nodes"""
    
    @abstractmethod
    async def get_partition_locations(self, partition_keys: List[str]) -> Dict[str, List[CacheLocation]]:
        """Get locations where partitions are cached"""
        pass
    
    @abstractmethod
    async def fetch_remote_partition(self, partition_key: str, source_node: str) -> pa.Table:
        """Fetch partition data from remote cache node"""
        pass
    
    @abstractmethod
    async def broadcast_partition_available(self, partition_key: str, node_id: str):
        """Notify other nodes that partition is now available locally"""
        pass
    
    @abstractmethod
    async def get_cluster_topology(self) -> Dict[str, Dict[str, Any]]:
        """Get current cluster topology and node capabilities"""
        pass
    
    @abstractmethod
    def get_local_node_id(self) -> str:
        """Get current node identifier"""
        pass

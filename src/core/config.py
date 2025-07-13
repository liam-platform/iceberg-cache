"""
Config stores configuration for the CacheNode, used for engineers to spin up the distributed
cache in their own system. 

Example:
{
    'max_cache_size': 1 * 1024 * 1024 * 1024,  # 1GB
    'iceberg_catalog': {
        'type': 'hive',
        'uri': 'thrift://localhost:9083'
    },
    'aws': {
        'region_name': 'us-west-2',
        # Add your AWS credentials here
    },
    'etcd': {
        'host': 'localhost',
        'port': 6379,
    }
}
"""

class CacheConfig:
    def __init__(self) -> None:
        pass

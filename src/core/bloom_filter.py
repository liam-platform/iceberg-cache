class BloomFilter:
    """Simple Bloom filter implementation for partition pruning"""
    
    def __init__(self, size: int = 1000000, hash_count: int = 5):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = [False] * size
        
    def _hash(self, value: str, seed: int) -> int:
        """Hash function with seed"""
        # TODO: Consider using a proper hash family like MurmurHash
        return abs(hash(f"{value}:{seed}")) % self.size
    
    def add(self, value: str):
        """Add value to bloom filter"""
        for i in range(self.hash_count):
            index = self._hash(value, i)
            self.bit_array[index] = True
    
    def might_contain(self, value: str) -> bool:
        """Check if value might be in the set"""
        for i in range(self.hash_count):
            index = self._hash(value, i)
            if not self.bit_array[index]:
                return False
        return True

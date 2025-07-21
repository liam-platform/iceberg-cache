import unittest
import random
from core.bloom_filter import BloomFilter  # Replace `your_module` with the actual module name


class TestBloomFilter(unittest.TestCase):
    def setUp(self):
        self.bf = BloomFilter(size=1000, hash_count=3)  # Use smaller size for tests

    def test_add_and_check_existing_value(self):
        self.bf.add("apple")
        self.assertTrue(self.bf.might_contain("apple"))

    def test_check_non_existing_value(self):
        self.bf.add("apple")
        self.assertFalse(self.bf.might_contain("banana"))

    def test_multiple_values(self):
        items = ["cat", "dog", "fish"]
        for item in items:
            self.bf.add(item)
        for item in items:
            self.assertTrue(self.bf.might_contain(item))

    def test_false_positive_rate(self):
        # Use a larger filter to make false positives unlikely, but possible
        bf = BloomFilter(size=10000, hash_count=4)
        inserted = set()
        not_inserted = set()

        # Insert 1000 items
        for _ in range(1000):
            val = str(random.randint(0, 100000))
            inserted.add(val)
            bf.add(val)

        # Generate 1000 values not added
        while len(not_inserted) < 1000:
            val = str(random.randint(100001, 200000))
            if val not in inserted:
                not_inserted.add(val)

        false_positives = sum(1 for val in not_inserted if bf.might_contain(val))
        false_positive_rate = false_positives / 1000

        # Acceptable FP rate for this setting should be <5%
        self.assertLess(false_positive_rate, 0.05)

    def test_empty_string(self):
        self.bf.add("")
        self.assertTrue(self.bf.might_contain(""))

    def test_case_sensitivity(self):
        self.bf.add("Test")
        self.assertFalse(self.bf.might_contain("test"))

    def test_collision_handling(self):
        # Even if hash collisions occur, filter should still work for inserted items
        values = [str(i) for i in range(100)]
        for v in values:
            self.bf.add(v)
        for v in values:
            self.assertTrue(self.bf.might_contain(v))

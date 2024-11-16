import hashlib

class ShardManager:
    def __init__(self, num_shards):
        self.num_shards = num_shards

    def get_shard_id(self, key):
        hash_digest = hashlib.sha256(key.encode()).hexdigest()
        shard_id = int(hash_digest, 16) % self.num_shards
        return shard_id
# dbms/distributed_dbms.py

from .shard_manager import ShardManager 
from .shard import Shard 
from nodes.database_node import DatabaseNode

class DistributedDBMS:
    def __init__(self, num_shards, replication_factor, node_ids):
        self.shard_manager = ShardManager(num_shards)
        self.nodes = {node_id: DatabaseNode(node_id) for node_id in node_ids}
        self.shards = {}
        self.setup_shards(num_shards, replication_factor)

    def setup_shards(self, num_shards, replication_factor):
        node_list = list(self.nodes.values())
        for shard_id in range(num_shards):
            shard_nodes = random.sample(node_list, replication_factor)
            shard = Shard(shard_id, replication_factor, shard_nodes)
            self.shards[shard_id] = shard

    def write(self, key, value):
        shard_id = self.shard_manager.get_shard_id(key)
        shard = self.shards[shard_id]
        success = shard.write(key, value)
        if not success:
            raise Exception("Write failed. Leader not available.")
        print(f"DBMS: Write {key} -> {value} in Shard {shard_id}")

    def read(self, key):
        shard_id = self.shard_manager.get_shard_id(key)
        shard = self.shards[shard_id]
        value = shard.read(key)
        return value
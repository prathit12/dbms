# dbms/shard.py

import time
import random

from raft.raft_node import RaftNode

class Shard:
    def __init__(self, shard_id, replication_factor, nodes):
        self.shard_id = shard_id
        self.replication_factor = replication_factor
        self.nodes = nodes  # List of DatabaseNode instances
        self.raft_nodes = []
        self.leader = None
        self.data = {}
        self.initialize_raft()

    def apply_command(self, command):
        key, value = command
        self.data[key] = value
        print(f"Shard {self.shard_id}: Applied {key} -> {value}")

    def initialize_raft(self):
        # Initialize RAFT nodes with the apply_command callback
        self.raft_nodes = [RaftNode(node_id=node.node_id, peers=[], apply_func=self.apply_command) for node in self.nodes]
        for node in self.raft_nodes:
            node.peers = [peer for peer in self.raft_nodes if peer != node]
        for node in self.raft_nodes:
            threading.Thread(target=node.run).start()
        # Allow time for leader election
        time.sleep(3)
        for node in self.raft_nodes:
            if node.state == State.LEADER:
                self.leader = node
                print(f"Shard {self.shard_id}: Leader is Node {node.node_id}")
                break

    def write(self, key, value):
        if self.leader:
            success = self.leader.receive_client_command((key, value))
            if success:
                self.replicate_to_followers(key, value)
                print(f"Shard {self.shard_id}: Write {key} -> {value} successful")
                return True
        print(f"Shard {self.shard_id}: Write {key} -> {value} failed")
        return False

    def replicate_to_followers(self, key, value):
        for node in self.raft_nodes:
            if node != self.leader:
                node.apply_func((key, value))

    def read(self, key):
        if key in self.data:
            print(f"Shard {self.shard_id}: Read {key} -> {self.data[key]}")
            return self.data[key]
        print(f"Shard {self.shard_id}: Read {key} -> Not Found")
        return None
import datetime
import redis
import uuid
import graphviz
import time
from collections import OrderedDict


redis = redis.Redis(host='localhost', port=6380, db=3)


class WorkflowNode:
    def __init__(self, func, args: list = [], kwargs: dict = {}):
        self.id = str(uuid.uuid4())
        self.title = func.__name__
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return self.title
    
    def exec(self):
        exec_log = WorkflowNodeExecutionLog(node=self)
        result = self.func(*self.args, **self.kwargs)
        exec_log.set()
        return result

    def get_execution_times(self):
        cursor, keys = redis.scan(match=f'{self.id}_*')
        data = redis.mget(keys)
        return data


class WorkflowNodeExecutionLog:
    def __init__(self, node: WorkflowNode):
        self.node = node
        self.id = str(uuid.uuid4())
        self.started_date_time = datetime.datetime.now()
        self.end_date_time = None
        self.cache_key = f'{self.node.id}_{self.id}'

    def set(self):
        self.end_date_time = datetime.datetime.now()
        redis.set(self.cache_key, (self.end_date_time - self.started_date_time).total_seconds())
    
    def get(self):
        return redis.get(self.cache_key)


class WorkflowDAG:
    def __init__(self):
        self.graph = {}
        self.reverse_graph = {}
        self.topological_order = None

    def add_node(self, node: WorkflowNode):
        if node not in self.graph:
            self.graph[node] = []
            self.reverse_graph[node] = []

    def add_edge(self, start_node, end_node):
        if start_node in self.graph and end_node in self.graph:
            self.graph[start_node].append(end_node)
            self.reverse_graph[end_node].append(start_node)

    def print(self):
        viz = graphviz.Digraph(comment='Workflow DAG')
        for v in self.graph.keys():
            viz.node(v.id, v.title)
            for d in self.graph[v]:
                viz.edge(v.id, d.id, constraint='true')
        viz.render(f'./{viz.name}')

    def analyze_graph(self):
        self.topological_order = self.topological_sort()
        node_levels = self.find_node_levels()
        parallel_chains = self.find_parallel_chains()
        min_containers_count = self.get_min_needed_containers_count(node_levels, parallel_chains)
        critical_path = self.critical_path()
        print(min_containers_count)
        return min_containers_count, critical_path

    def topological_sort(self):
        def dfs(v):
            visited.add(v)
            for neighbor in self.graph[v]:
                if neighbor not in visited:
                    dfs(neighbor)
            topological_order.append(v)

        visited = set()
        topological_order = []
        for node in self.graph:
            if node not in visited:
                dfs(node)
        topological_order.reverse()
        return topological_order

    def find_node_levels(self):
        node_levels = {}
        for node in self.topological_order:
            max_level = 0
            for dependency in self.reverse_graph[node]:
                max_level = max(max_level, node_levels.get(dependency, 0))
            node_levels[node] = max_level + 1

        return node_levels

    def find_chains(self):
        visited = set()
        chains = []

        def dfs(vertex, chain):
            visited.add(vertex)

            for neighbor in self.graph[vertex]:
                if neighbor not in visited:
                    dfs(neighbor, chain)

            chain.append(vertex)

        for vertex in self.graph:
            if vertex not in visited:
                chain = []
                dfs(vertex, chain)
                chains.append(chain[::-1])

        return chains

    def find_parallel_chains(self):
        chains = self.find_chains()

        parallel_chains = []
        for chain in chains:
            if len(chain) > 1:
                parallel_chains.append(chain)

        return parallel_chains

    def get_min_needed_containers_count(self, node_levels, parallel_chains):
        min_needed_containers_count = 1
        level_nodes = OrderedDict()
        for node, level in node_levels.items():
            if level not in level_nodes:
                level_nodes[level] = []
            level_nodes[level].append(node)
        level_nodes = OrderedDict(sorted(level_nodes.items(), key=lambda item: item[0]))
        for level in level_nodes:
            min_level_needed_containers_count = len(level_nodes[level]) + len(level_nodes.get(level + 1, []))
            if min_level_needed_containers_count >= min_needed_containers_count:
                min_needed_containers_count = min_level_needed_containers_count
        return min_needed_containers_count if len(parallel_chains) < min_needed_containers_count else\
            len(parallel_chains)

    def critical_path(self):
        return []
        earliest_start_time = {}
        latest_start_time = {}
        critical_path = []
        execution_times = {}

        for node in self.topological_order:
            execution_times[node] = node.get_execution_times()
            max_earliest_start_time = 0
            for neighbor in self.reverse_graph[node]:
                max_earliest_start_time = max(max_earliest_start_time, earliest_start_time.get(neighbor, 0))
            earliest_start_time[node] = max_earliest_start_time + max(execution_times[node])

        latest_start_time[self.topological_order[-1]] = earliest_start_time[self.topological_order[-1]]
        for node in reversed(self.topological_order[:-1]):
            min_latest_start_time = float('inf')
            for neighbor in self.graph[node]:
                min_latest_start_time = min(
                    min_latest_start_time, latest_start_time[neighbor] - min(execution_times[node])
                )
            latest_start_time[node] = min_latest_start_time

        # Find the critical path
        for node in self.topological_order:
            if earliest_start_time[node] == latest_start_time[node]:
                critical_path.append(node)

        return critical_path


def job1():
    time.sleep(1)


def job2():
    time.sleep(2)


def job3():
    time.sleep(3)


def job4():
    time.sleep(4)


def job5():
    time.sleep(5)


def warmup1():
    time.sleep(1)


def warmup2():
    time.sleep(2)


def warmup3():
    time.sleep(3)


def warmup4():
    time.sleep(4)


def warmup5():
    time.sleep(5)


dag = WorkflowDAG()
v1 = WorkflowNode(func=job1)
v2 = WorkflowNode(func=job2)
v3 = WorkflowNode(func=job3)
v4 = WorkflowNode(func=job4)
v5 = WorkflowNode(func=job5)
w1 = WorkflowNode(func=warmup1)
w2 = WorkflowNode(func=warmup2)
w3 = WorkflowNode(func=warmup3)
w4 = WorkflowNode(func=warmup4)
w5 = WorkflowNode(func=warmup5)

# Add vertices
dag.add_node(v1)
dag.add_node(v2)
dag.add_node(v3)
dag.add_node(v4)
dag.add_node(v5)
dag.add_node(w1)
dag.add_node(w2)
dag.add_node(w3)
dag.add_node(w4)
dag.add_node(w5)

# Add edges
dag.add_edge(v1, v4)
dag.add_edge(v2, v4)
dag.add_edge(v3, v4)
dag.add_edge(v4, v5)
dag.add_edge(w1, v1)
dag.add_edge(w2, v2)
dag.add_edge(w3, v3)
dag.add_edge(w4, v4)
dag.add_edge(w5, v5)

# Print the graph
dag.print()
dag.analyze_graph()

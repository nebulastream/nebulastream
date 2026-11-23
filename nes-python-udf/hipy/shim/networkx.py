"""Stand-in for the part of networkx that hipy.compiler uses (DiGraph, has_path, shortest_path)."""
from collections import deque


class DiGraph:
    def __init__(self):
        self.adj = {}

    def add_node(self, n):
        self.adj.setdefault(n, set())

    def add_edge(self, a, b):
        self.add_node(a)
        self.add_node(b)
        self.adj[a].add(b)

    def has_node(self, n):
        return n in self.adj


def shortest_path(g, source, target):
    prev = {source: None}
    queue = deque([source])
    while queue:
        n = queue.popleft()
        if n == target:
            path = []
            while n is not None:
                path.append(n)
                n = prev[n]
            return path[::-1]
        for m in g.adj.get(n, ()):
            if m not in prev:
                prev[m] = n
                queue.append(m)
    raise KeyError("no path")


def has_path(g, source, target):
    try:
        shortest_path(g, source, target)
        return True
    except KeyError:
        return False

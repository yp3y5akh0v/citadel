/// CSR (Compressed Sparse Row) graph for neighbor storage.
///
/// For n nodes, `offsets` has n+1 entries.
/// Neighbors of node i are `neighbors[offsets[i]..offsets[i+1]]`.
pub struct Graph {
    pub offsets: Vec<u32>,
    pub neighbors: Vec<u32>,
    pub n: usize,
}

impl Graph {
    /// Build from adjacency lists. Each entry in `adj` is the neighbor list for that node.
    pub fn from_adj(adj: &[Vec<u32>]) -> Self {
        let n = adj.len();
        let mut offsets = Vec::with_capacity(n + 1);
        let mut neighbors = Vec::new();
        let mut offset = 0u32;
        for list in adj {
            offsets.push(offset);
            neighbors.extend_from_slice(list);
            offset += list.len() as u32;
        }
        offsets.push(offset);
        Self {
            offsets,
            neighbors,
            n,
        }
    }

    /// Empty graph with n nodes and no edges.
    pub fn empty(n: usize) -> Self {
        Self {
            offsets: vec![0; n + 1],
            neighbors: Vec::new(),
            n,
        }
    }

    /// Degree of node i.
    #[inline]
    pub fn degree(&self, i: u32) -> usize {
        let i = i as usize;
        (self.offsets[i + 1] - self.offsets[i]) as usize
    }

    /// Neighbors of node i.
    #[inline]
    pub fn neighbors(&self, i: u32) -> &[u32] {
        let i = i as usize;
        let start = self.offsets[i] as usize;
        let end = self.offsets[i + 1] as usize;
        &self.neighbors[start..end]
    }

    /// Total number of edges (directed).
    pub fn num_edges(&self) -> usize {
        self.neighbors.len()
    }
}

/// Mutable adjacency list builder that converts to CSR.
pub struct AdjBuilder {
    adj: Vec<Vec<u32>>,
}

impl AdjBuilder {
    pub fn new(n: usize) -> Self {
        Self {
            adj: vec![Vec::new(); n],
        }
    }

    /// Add a directed edge from `src` to `dst`.
    #[inline]
    pub fn add_edge(&mut self, src: u32, dst: u32) {
        self.adj[src as usize].push(dst);
    }

    /// Add bidirectional edge.
    #[inline]
    pub fn add_undirected(&mut self, a: u32, b: u32) {
        self.adj[a as usize].push(b);
        self.adj[b as usize].push(a);
    }

    /// Get current neighbors (mutable).
    pub fn neighbors_mut(&mut self, i: u32) -> &mut Vec<u32> {
        &mut self.adj[i as usize]
    }

    /// Get current neighbors.
    pub fn neighbors(&self, i: u32) -> &[u32] {
        &self.adj[i as usize]
    }

    /// Total directed edges currently stored.
    pub fn total_edges(&self) -> usize {
        self.adj.iter().map(|v| v.len()).sum()
    }

    /// Create a read-only CSR graph snapshot without consuming the builder.
    pub fn snapshot(&self) -> Graph {
        Graph::from_adj(&self.adj)
    }

    /// Freeze into CSR graph, deduplicating edges.
    pub fn build(mut self) -> Graph {
        for list in &mut self.adj {
            list.sort_unstable();
            list.dedup();
        }
        Graph::from_adj(&self.adj)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_from_adj() {
        let adj = vec![vec![1, 2], vec![0], vec![0, 1]];
        let g = Graph::from_adj(&adj);
        assert_eq!(g.n, 3);
        assert_eq!(g.degree(0), 2);
        assert_eq!(g.degree(1), 1);
        assert_eq!(g.degree(2), 2);
        assert_eq!(g.neighbors(0), &[1, 2]);
        assert_eq!(g.neighbors(1), &[0]);
        assert_eq!(g.neighbors(2), &[0, 1]);
    }

    #[test]
    fn test_adj_builder() {
        let mut builder = AdjBuilder::new(3);
        builder.add_undirected(0, 1);
        builder.add_undirected(0, 2);
        let g = builder.build();
        assert_eq!(g.neighbors(0), &[1, 2]);
        assert_eq!(g.neighbors(1), &[0]);
        assert_eq!(g.neighbors(2), &[0]);
    }
}

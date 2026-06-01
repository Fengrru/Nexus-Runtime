use std::collections::BTreeSet;
use crate::types::*;

impl MemoryGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: MemoryNode) {
        self.nodes.insert(node.id.clone(), node);
    }

    pub fn add_edge(&mut self, edge: MemoryEdge) {
        self.edges.push(edge);
    }

    pub fn query_causal(
        &self,
        from: &str,
        edge_type: Option<MemoryEdgeType>,
        depth: usize,
    ) -> Vec<&MemoryNode> {
        let mut results = Vec::new();
        let mut visited = BTreeSet::new();
        let mut queue = vec![(from.to_string(), 0)];

        while let Some((current, current_depth)) = queue.pop() {
            if current_depth > depth || visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            if let Some(node) = self.nodes.get(&current) {
                results.push(node);
            }

            for edge in &self.edges {
                if edge.from == current {
                    if let Some(ref et) = edge_type {
                        if edge.edge_type != *et {
                            continue;
                        }
                    }
                    queue.push((edge.to.clone(), current_depth + 1));
                }
            }
        }
        results
    }

    pub fn compute_activation(
        &self,
        memory_id: &str,
        query_context: &QueryContext,
    ) -> u64 {
        let node = match self.nodes.get(memory_id) {
            Some(n) => n,
            None => return 0,
        };

        let relevance = match (&node.embedding, &query_context.embedding) {
            (Some(ne), Some(qe)) => cosine_similarity_u8(ne, qe),
            _ => 5000,
        };

        let importance = node.importance;

        let age_hours = if query_context.now > node.created_at {
            (query_context.now - node.created_at) / 3_600_000
        } else {
            0
        };
        let recency = if age_hours < 1 {
            10000
        } else {
            ((10000.0 / (1.0 + (age_hours as f64).ln())) as u64).min(10000)
        };

        let goal_alignment = if query_context
            .active_goals
            .iter()
            .any(|g| node.content.matches_goal(g))
        {
            8000
        } else {
            3000
        };

        let causal_proximity = query_context
            .recent_memories
            .iter()
            .map(|recent| self.graph_distance(memory_id, recent))
            .min()
            .map(|d| {
                if d == 0 {
                    10000
                } else if d == usize::MAX {
                    5000
                } else {
                    10000 / d as u64
                }
            })
            .unwrap_or(5000);

        (relevance * 3000
            + importance * 2500
            + recency * 2000
            + goal_alignment * 1500
            + causal_proximity * 1000)
            / 10000
    }

    fn graph_distance(&self, from: &str, to: &str) -> usize {
        let mut queue = std::collections::VecDeque::new();
        let mut visited = BTreeSet::new();
        queue.push_back((from.to_string(), 0));

        while let Some((current, distance)) = queue.pop_front() {
            if &current == to {
                return distance;
            }
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            for edge in &self.edges {
                if edge.from == current {
                    queue.push_back((edge.to.clone(), distance + 1));
                }
            }
        }
        usize::MAX
    }

    pub fn inherit_memories(
        &mut self,
        source: &MemoryGraph,
        source_session: SessionId,
        causal_vector: &CausalVector,
    ) -> Result<Vec<String>, String> {
        let mut imported = Vec::new();

        for (id, node) in &source.nodes {
            match node.causal_context.compare(causal_vector) {
                CausalRelation::Concurrent => {
                    continue;
                }
                _ => {}
            }

            let mut new_node = node.clone();
            new_node.session_lineage.push(source_session);
            new_node.causal_context.merge(causal_vector);

            let new_id = format!("{}:{}", source_session.to_hex(), id);
            self.nodes.insert(new_id.clone(), new_node);
            imported.push(new_id);
        }

        Ok(imported)
    }
}

fn cosine_similarity_u8(a: &[u8], b: &[u8]) -> u64 {
    if a.len() < 4 || b.len() < 4 || a.len() != b.len() {
        return 5000;
    }

    let a_f32: Vec<f32> = a
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    let b_f32: Vec<f32> = b
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();

    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;

    for i in 0..a_f32.len() {
        dot += a_f32[i] as f64 * b_f32[i] as f64;
        norm_a += (a_f32[i] as f64) * (a_f32[i] as f64);
        norm_b += (b_f32[i] as f64) * (b_f32[i] as f64);
    }

    if norm_a == 0.0 || norm_b == 0.0 {
        return 5000;
    }

    let sim = dot / (norm_a.sqrt() * norm_b.sqrt());
    ((sim.max(-1.0).min(1.0) + 1.0) * 5000.0) as u64
}

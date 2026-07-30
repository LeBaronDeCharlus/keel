#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScheduleError {
    #[error("no alive nodes available to schedule onto")]
    NoAvailableNodes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeResources {
    pub id: String,
    pub capacity_cpu: f64,
    pub capacity_memory: u64,
    pub committed_cpu: f64,
    pub committed_memory: u64,
}

pub fn pick_node(nodes: &[NodeResources]) -> Result<String, ScheduleError> {
    nodes
        .iter()
        .map(|n| (headroom_score(n), n.id.as_str()))
        .fold(None, |best: Option<(f64, &str)>, candidate| match best {
            None => Some(candidate),
            Some(current)
                if candidate.0 > current.0
                    || (candidate.0 == current.0 && candidate.1 < current.1) =>
            {
                Some(candidate)
            }
            _ => best,
        })
        .map(|(_, id)| id.to_string())
        .ok_or(ScheduleError::NoAvailableNodes)
}

fn headroom_score(n: &NodeResources) -> f64 {
    let cpu_frac = if n.capacity_cpu > 0.0 {
        (n.capacity_cpu - n.committed_cpu) / n.capacity_cpu
    } else {
        0.0
    };
    let mem_frac = if n.capacity_memory > 0 {
        (n.capacity_memory as f64 - n.committed_memory as f64) / n.capacity_memory as f64
    } else {
        0.0
    };
    cpu_frac.min(mem_frac)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(
        id: &str,
        capacity_cpu: f64,
        capacity_memory: u64,
        committed_cpu: f64,
        committed_memory: u64,
    ) -> NodeResources {
        NodeResources {
            id: id.to_string(),
            capacity_cpu,
            capacity_memory,
            committed_cpu,
            committed_memory,
        }
    }

    #[test]
    fn no_alive_nodes_returns_no_available_nodes_error() {
        let nodes: Vec<NodeResources> = vec![];
        assert_eq!(pick_node(&nodes), Err(ScheduleError::NoAvailableNodes));
    }

    #[test]
    fn a_single_alive_node_is_always_picked() {
        let nodes = vec![node("node-1", 4.0, 8 * 1024 * 1024 * 1024, 0.0, 0)];
        assert_eq!(pick_node(&nodes), Ok("node-1".to_string()));
    }

    #[test]
    fn the_node_with_more_headroom_in_its_most_constrained_resource_wins() {
        // node-1: 50% cpu headroom, 90% memory headroom -> min = 0.5
        // node-2: 90% cpu headroom, 50% memory headroom -> min = 0.5
        // node-3: 75% cpu headroom, 75% memory headroom -> min = 0.75, wins
        let nodes = vec![
            node("node-1", 4.0, 100, 2.0, 10),
            node("node-2", 4.0, 100, 0.4, 50),
            node("node-3", 4.0, 100, 1.0, 25),
        ];
        assert_eq!(pick_node(&nodes), Ok("node-3".to_string()));
    }

    #[test]
    fn ties_on_the_min_fraction_score_are_broken_by_ascending_node_id() {
        let nodes = vec![
            node("node-2", 4.0, 100, 2.0, 50),
            node("node-1", 4.0, 100, 2.0, 50),
        ];
        assert_eq!(pick_node(&nodes), Ok("node-1".to_string()));
    }

    #[test]
    fn an_over_committed_node_is_still_picked_when_it_is_the_only_alive_one() {
        // committed_cpu exceeds capacity_cpu: negative headroom, but still the only option.
        let nodes = vec![node("node-1", 4.0, 100, 6.0, 50)];
        assert_eq!(pick_node(&nodes), Ok("node-1".to_string()));
    }
}

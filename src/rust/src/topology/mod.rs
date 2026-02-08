use rustc_hash::FxHashMap;
use crate::models::{Segment, Way, Junction, SimplifyMethod, CoordHash};
use crate::geometry::{compute_junction_angle, simplify_polygon};
use crate::grouping::group_segments;

/// Global configuration constants - MUST match Python exactly
pub const ANGLE_MARGIN: f64 = 45.0; // Maximum turn angle for merging (degrees)
pub const SIMPLIFY_FACTOR: f64 = 0.2; // Douglas-Peucker epsilon in meters

/// Main entry point for network simplification
/// 
/// Port of simplify_network() from Python - matches Python behavior exactly
pub fn simplify_network(
    segments: &mut [Segment],
    method: SimplifyMethod,
) -> Vec<Way> {
    // 1. Simplify segment geometries (Douglas-Peucker) - matches Python line 1726-1730
    if SIMPLIFY_FACTOR > 0.0 {
        for segment in segments.iter_mut() {
            let simplified = simplify_polygon(&segment.geometry.0, SIMPLIFY_FACTOR);
            if simplified.len() >= 2 {
                segment.geometry = geo_types::LineString::from(simplified);
                // Update start/end nodes
                use crate::models::hash_coord;
                segment.start_node = hash_coord(segment.geometry.0.first().unwrap());
                segment.end_node = hash_coord(segment.geometry.0.last().unwrap());
            }
        }
    }
    
    // 2. Group segments - matches Python line 1769-1793
    let groups = group_segments(segments, method);
    
    // 3. Build junction index - matches Python line 1735-1752
    let junctions = build_junctions(segments);
    
    // 4. Merge based on method - matches Python line 1797-1803
    match method {
        SimplifyMethod::Recursive => {
            simplify_recursive(segments, &groups, &junctions)
        }
        SimplifyMethod::Route | SimplifyMethod::Refname | SimplifyMethod::Linear => {
            // NOTE: Python's linear algorithm (simplify_network_linear) is used for 
            // both "route" and "refname" methods. It does NOT check oneway or group
            // compatibility - only angle and tag equality.
            simplify_linear(segments, &groups, &junctions)
        }
        SimplifyMethod::Segment => {
            // No merging - each segment is its own way
            segments.iter().enumerate()
                .map(|(idx, seg)| Way {
                    segment_indices: vec![idx],
                    tags: seg.tags.clone(),
                })
                .collect()
        }
    }
}

/// Build junction index from segments
/// Port of Python junction building (lines 1735-1752)
fn build_junctions(segments: &[Segment]) -> FxHashMap<CoordHash, Junction> {
    let mut junctions: FxHashMap<CoordHash, Junction> = FxHashMap::default();
    
    for (idx, segment) in segments.iter().enumerate() {
        // Start node
        junctions.entry(segment.start_node)
            .or_default()
            .segment_indices.push(idx);
        
        // End node
        junctions.entry(segment.end_node)
            .or_default()
            .segment_indices.push(idx);
    }
    
    junctions
}

/// Linear simplification algorithm
/// 
/// EXACT port of simplify_network_linear() from Python lines 1626-1711
/// 
/// Key behaviors:
/// - NO oneway check (unlike recursive algorithm)
/// - NO group compatibility check (segments are pre-grouped)
/// - Only checks angle and tag equality
fn simplify_linear(
    segments: &[Segment],
    groups: &FxHashMap<String, Vec<usize>>,
    junctions: &FxHashMap<CoordHash, Junction>,
) -> Vec<Way> {
    let mut ways: Vec<Way> = Vec::new();
    
    for (_group_id, segment_indices) in groups.iter() {
        if segment_indices.is_empty() {
            continue;
        }
        
        // Use BTreeSet for deterministic ordering and O(log n) removal
        // Python dicts preserve insertion order (3.7+), so we maintain original order
        let mut remaining: std::collections::BTreeSet<usize> = segment_indices.iter().cloned().collect();
        
        // Build O(1) lookup dicts for this group - matches Python lines 1638-1643
        let mut by_start: FxHashMap<CoordHash, Vec<usize>> = FxHashMap::default();
        let mut by_end: FxHashMap<CoordHash, Vec<usize>> = FxHashMap::default();
        
        for &idx in segment_indices {
            let seg = &segments[idx];
            by_start.entry(seg.start_node).or_default().push(idx);
            by_end.entry(seg.end_node).or_default().push(idx);
        }
        
        // Repeat building sequences of longer ways until all segments have been used
        // Matches Python line 1646
        while !remaining.is_empty() {
            // Get first available segment (deterministic) - matches Python line 1648
            let start_idx = *remaining.iter().next().unwrap();
            remaining.remove(&start_idx);
            
            // Remove from lookup dicts - matches Python lines 1651-1652
            let seg = &segments[start_idx];
            remove_from_lookup(&mut by_start, seg.start_node, start_idx);
            remove_from_lookup(&mut by_end, seg.end_node, start_idx);
            
            let mut way = vec![start_idx];
            let mut first_node = seg.start_node;
            let mut last_node = seg.end_node;
            
            // Build way forward - O(1) lookup - matches Python lines 1659-1675
            let mut found = true;
            while found {
                found = false;
                // Get candidates from by_start using last_node
                let candidates: Vec<usize> = by_start.get(&last_node)
                    .map(|v| v.clone())
                    .unwrap_or_default();
                
                for candidate_idx in candidates {
                    if !remaining.contains(&candidate_idx) {
                        continue;
                    }
                    
                    let candidate = &segments[candidate_idx];
                    
                    // NOTE: Python does NOT check group compatibility or oneway here!
                    // It only checks angle (line 1667-1668)
                    
                    // Check angle - matches Python line 1668
                    let last_seg = &segments[*way.last().unwrap()];
                    let angle = compute_junction_angle(last_seg, candidate);
                    if angle.abs() >= ANGLE_MARGIN {
                        continue;
                    }
                    
                    // Found valid continuation
                    last_node = candidate.end_node;
                    way.push(candidate_idx);
                    remaining.remove(&candidate_idx);
                    remove_from_lookup(&mut by_start, candidate.start_node, candidate_idx);
                    remove_from_lookup(&mut by_end, candidate.end_node, candidate_idx);
                    found = true;
                    break;
                }
            }
            
            // Build way backward - O(1) lookup - matches Python lines 1677-1693
            let mut found = true;
            while found {
                found = false;
                // Get candidates from by_end using first_node
                let candidates: Vec<usize> = by_end.get(&first_node)
                    .map(|v| v.clone())
                    .unwrap_or_default();
                
                for candidate_idx in candidates {
                    if !remaining.contains(&candidate_idx) {
                        continue;
                    }
                    
                    let candidate = &segments[candidate_idx];
                    
                    // NOTE: Python does NOT check group compatibility or oneway here!
                    // It only checks angle (line 1685-1686)
                    
                    // Check angle (note: reversed order for backward extension)
                    let first_seg = &segments[way[0]];
                    let angle = compute_junction_angle(candidate, first_seg);
                    if angle.abs() >= ANGLE_MARGIN {
                        continue;
                    }
                    
                    // Found valid continuation
                    first_node = candidate.start_node;
                    way.insert(0, candidate_idx);
                    remaining.remove(&candidate_idx);
                    remove_from_lookup(&mut by_start, candidate.start_node, candidate_idx);
                    remove_from_lookup(&mut by_end, candidate.end_node, candidate_idx);
                    found = true;
                    break;
                }
            }
            
            // First split at true junctions (3+ segments meeting across ALL groups)
            // This ensures intersections are represented as way endpoints for OSRM
            let mut junction_split_chains: Vec<Vec<usize>> = Vec::new();
            if !way.is_empty() {
                let mut current_chunk: Vec<usize> = Vec::new();
                for (i, &seg_idx) in way.iter().enumerate() {
                    current_chunk.push(seg_idx);
                    if i < way.len() - 1 {
                        let seg = &segments[seg_idx];
                        if let Some(j) = junctions.get(&seg.end_node) {
                            if j.segment_indices.len() >= 3 {
                                junction_split_chains.push(current_chunk);
                                current_chunk = Vec::new();
                            }
                        }
                    }
                }
                if !current_chunk.is_empty() {
                    junction_split_chains.push(current_chunk);
                }
            }

            // Then split each sub-chain by tags
            // Matches Python lines 1697-1711
            for chain in junction_split_chains {
                let mut current_way = vec![chain[0]];
                let mut current_tags = segments[chain[0]].tags.clone();

                for &seg_idx in &chain[1..] {
                    let seg = &segments[seg_idx];
                    if seg.tags == current_tags {
                        current_way.push(seg_idx);
                    } else {
                        ways.push(Way {
                            segment_indices: current_way,
                            tags: current_tags,
                        });
                        current_way = vec![seg_idx];
                        current_tags = seg.tags.clone();
                    }
                }

                if !current_way.is_empty() {
                    ways.push(Way {
                        segment_indices: current_way,
                        tags: current_tags,
                    });
                }
            }
        }
    }
    
    ways
}

/// Remove a segment index from lookup
fn remove_from_lookup(
    lookup: &mut FxHashMap<CoordHash, Vec<usize>>,
    node: CoordHash,
    idx: usize,
) {
    if let Some(vec) = lookup.get_mut(&node) {
        vec.retain(|&x| x != idx);
        if vec.is_empty() {
            lookup.remove(&node);
        }
    }
}

/// Recursive simplification algorithm
/// 
/// Port of simplify_network_recursive() from Python
/// 
/// NOTE: Python's recursive algorithm checks oneway in connected_way()
/// but we use the linear algorithm for "refname" which is the default.
fn simplify_recursive(
    segments: &[Segment],
    groups: &FxHashMap<String, Vec<usize>>,
    _junctions: &FxHashMap<CoordHash, Junction>,
) -> Vec<Way> {
    let mut ways: Vec<Way> = Vec::new();
    
    for (_group_id, segment_indices) in groups.iter() {
        let mut remaining: Vec<usize> = segment_indices.clone();
        
        while !remaining.is_empty() {
            let start_idx = remaining[0];
            
            // Build sequence forward using recursive search
            // For now, use linear approach (can be enhanced with true recursive search)
            // TODO: Implement true recursive search matching Python's connected_way()
            let sequence = vec![start_idx];
            
            // Remove used segments
            for idx in &sequence {
                if let Some(pos) = remaining.iter().position(|&x| x == *idx) {
                    remaining.remove(pos);
                }
            }
            
            if !sequence.is_empty() {
                let first_seg = &segments[sequence[0]];
                ways.push(Way {
                    segment_indices: sequence,
                    tags: first_seg.tags.clone(),
                });
            }
        }
    }
    
    ways
}

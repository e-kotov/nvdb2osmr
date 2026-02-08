use rustc_hash::FxHashMap;
use crate::models::{Segment, SimplifyMethod};

/// Group segments for simplification
/// 
/// EXACT port of Python's grouping logic (lines 1769-1793)
pub fn group_segments(
    segments: &[Segment],
    method: SimplifyMethod,
) -> FxHashMap<String, Vec<usize>> {
    let mut groups: FxHashMap<String, Vec<usize>> = FxHashMap::default();
    
    for (idx, segment) in segments.iter().enumerate() {
        let group_id = match method {
            SimplifyMethod::Route => group_by_route(segment),
            // Python: refname and recursive use same grouping (line 1778-1788)
            SimplifyMethod::Refname | SimplifyMethod::Recursive | SimplifyMethod::Linear => {
                group_by_refname(segment)
            }
            SimplifyMethod::Segment => String::new(), // All in one group
        };
        
        groups.entry(group_id).or_default().push(idx);
    }
    
    groups
}

/// Group by ROUTE_ID property
/// Port of Python line 1773-1776
fn group_by_route(segment: &Segment) -> String {
    segment.properties
        .get("ROUTE_ID")
        .map(|v| v.as_string())
        .unwrap_or_default()
}

/// Group by ref + name + highway
/// 
/// EXACT port of Python lines 1778-1788:
/// - ref or Driftbidrag statligt/Vägnr (road number for countryside)
/// - name
/// - highway
fn group_by_refname(segment: &Segment) -> String {
    let mut group_id = String::new();
    
    // ref or Driftbidrag statligt/Vägnr (line 1779-1784)
    if let Some(ref_val) = segment.tags.get("ref") {
        group_id.push_str(ref_val);
    } else if let Some(vagnr) = segment.properties.get("Driftbidrag statligt/Vägnr") {
        group_id.push_str(&vagnr.as_string());
    }
    
    // name (line 1785-1786)
    if let Some(name) = segment.tags.get("name") {
        group_id.push_str(name);
    }
    
    // highway (line 1787-1788)
    if let Some(highway) = segment.tags.get("highway") {
        group_id.push_str(highway);
    }
    
    group_id
}



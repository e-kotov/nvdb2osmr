use geo_types::Coord;
use crate::models::Segment;

/// Compute bearing between two coordinates (0-360 degrees)
/// 
/// Bearing is the direction from `from` to `to` in degrees,
/// where 0 = North, 90 = East, 180 = South, 270 = West
pub fn compute_bearing(from: &Coord, to: &Coord) -> f64 {
    let lat1 = from.y.to_radians();
    let lat2 = to.y.to_radians();
    let dlon = (to.x - from.x).to_radians();
    
    let y = dlon.sin() * lat2.cos();
    let x = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * dlon.cos();
    
    let bearing = y.atan2(x).to_degrees();
    (bearing + 360.0) % 360.0
}

/// Compute junction angle between two segments
/// 
/// Returns the angle difference in degrees (-180 to 180).
/// Positive = left turn, Negative = right turn
/// 
/// # Arguments
/// * `seg1` - First segment
/// * `seg2` - Second segment
/// * `connection_type` - How segments connect (start-to-end, etc.)
pub fn compute_junction_angle(seg1: &Segment, seg2: &Segment) -> f64 {
    // Determine how segments connect
    let (bearing1, bearing2) = if seg1.end_node == seg2.start_node {
        // seg1 -> seg2 (normal forward connection)
        let b1 = compute_bearing(
            seg1.geometry.0.get(seg1.geometry.0.len().saturating_sub(2)).unwrap_or(seg1.start_coord()),
            seg1.end_coord()
        );
        let b2 = compute_bearing(
            seg2.start_coord(),
            seg2.geometry.0.get(1).unwrap_or(seg2.end_coord())
        );
        (b1, b2)
    } else if seg1.start_node == seg2.end_node {
        // seg1 <- seg2 (reverse connection)
        let b1 = compute_bearing(
            seg1.geometry.0.get(1).unwrap_or(seg1.end_coord()),
            seg1.start_coord()
        );
        let b2 = compute_bearing(
            seg2.end_coord(),
            seg2.geometry.0.get(seg2.geometry.0.len().saturating_sub(2)).unwrap_or(seg2.start_coord())
        );
        (b1, b2)
    } else if seg1.start_node == seg2.start_node {
        // seg1 starts at same point as seg2
        let b1 = compute_bearing(
            seg1.geometry.0.get(1).unwrap_or(seg1.end_coord()),
            seg1.start_coord()
        );
        let b2 = compute_bearing(
            seg2.start_coord(),
            seg2.geometry.0.get(1).unwrap_or(seg2.end_coord())
        );
        (b1, b2)
    } else {
        // seg1.end_node == seg2.end_node
        let b1 = compute_bearing(
            seg1.geometry.0.get(seg1.geometry.0.len().saturating_sub(2)).unwrap_or(seg1.start_coord()),
            seg1.end_coord()
        );
        let b2 = compute_bearing(
            seg2.end_coord(),
            seg2.geometry.0.get(seg2.geometry.0.len().saturating_sub(2)).unwrap_or(seg2.start_coord())
        );
        (b1, b2)
    };
    
    let mut delta = bearing2 - bearing1;
    delta = (delta + 360.0) % 360.0;
    if delta > 180.0 {
        delta -= 360.0;
    }
    delta
}

/// Douglas-Peucker polygon simplification
/// 
/// Removes points that are within `epsilon` meters of the line
/// connecting their neighbors.
pub fn simplify_polygon(coords: &[Coord], epsilon: f64) -> Vec<Coord> {
    if coords.len() <= 2 {
        return coords.to_vec();
    }
    
    // Find point with maximum distance from line between first and last
    let first = &coords[0];
    let last = &coords[coords.len() - 1];
    
    let mut max_dist = 0.0;
    let mut max_idx = 0;
    
    for (i, point) in coords.iter().enumerate().skip(1).take(coords.len() - 2) {
        let dist = point_to_line_distance(first, last, point);
        if dist > max_dist {
            max_dist = dist;
            max_idx = i;
        }
    }
    
    // If max distance is >= epsilon, recursively simplify (matches Python)
    if max_dist >= epsilon {
        let left = simplify_polygon(&coords[..=max_idx], epsilon);
        let right = simplify_polygon(&coords[max_idx..], epsilon);
        
        let mut result = left;
        result.pop(); // Remove duplicate point
        result.extend(right);
        result
    } else {
        vec![*first, *last]
    }
}

/// Compute distance from point p3 to line segment [s1, s2]
/// 
/// Uses simplified reprojection for short distances
fn point_to_line_distance(s1: &Coord, s2: &Coord, p3: &Coord) -> f64 {
    // Convert to radians
    let x1 = s1.x.to_radians();
    let y1 = s1.y.to_radians();
    let x2 = s2.x.to_radians();
    let y2 = s2.y.to_radians();
    let x3 = p3.x.to_radians();
    let y3 = p3.y.to_radians();
    
    // Simplified reprojection of latitude
    let x1 = x1 * y1.cos();
    let x2 = x2 * y2.cos();
    let x3 = x3 * y3.cos();
    
    let a = x3 - x1;
    let b = y3 - y1;
    let c = x2 - x1;
    let d = y2 - y1;
    
    let dot = a * c + b * d;
    let len_sq = c * c + d * d;
    
    let param = if len_sq != 0.0 { dot / len_sq } else { -1.0 };
    
    let (xx, yy) = if param < 0.0 {
        (x1, y1)
    } else if param > 1.0 {
        (x2, y2)
    } else {
        (x1 + param * c, y1 + param * d)
    };
    
    let dx = x3 - xx;
    let dy = y3 - yy;
    
    // Convert back to meters (approximate)
    (dx * dx + dy * dy).sqrt() * 6_371_000.0 // Earth's radius in meters
}



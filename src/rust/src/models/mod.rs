use rustc_hash::FxHashMap;
use geo_types::{Coord, LineString};
use serde::{Deserialize, Serialize};

/// Coordinate hash for fast lookups (8 bytes)
pub type CoordHash = u64;

/// Hash a coordinate to u64 for use as map keys
pub fn hash_coord(coord: &Coord) -> CoordHash {
    let lat = (coord.y * 10_000_000.0).round() as i64;
    let lon = (coord.x * 10_000_000.0).round() as i64;
    ((lat as u64) << 32) | (lon as u64)
}

/// NVDB Property value (can be int, float, or string)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PropertyValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

impl PropertyValue {
    pub fn as_string(&self) -> String {
        match self {
            PropertyValue::Integer(i) => i.to_string(),
            PropertyValue::Float(f) => f.to_string(),
            PropertyValue::String(s) => s.clone(),
            PropertyValue::Boolean(b) => b.to_string(),
            PropertyValue::Null => String::new(),
        }
    }
    
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            PropertyValue::Integer(i) => Some(*i),
            PropertyValue::Float(f) => Some(*f as i64),
            _ => None,
        }
    }
    
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            PropertyValue::Integer(i) => Some(*i as f64),
            PropertyValue::Float(f) => Some(*f),
            _ => None,
        }
    }
}

/// Road segment from NVDB
#[derive(Debug, Clone)]
pub struct Segment {
    pub start_node: CoordHash,
    pub end_node: CoordHash,
    pub geometry: LineString<f64>,
    pub tags: FxHashMap<String, String>,
    pub properties: FxHashMap<String, PropertyValue>,
    pub shape_length: f64,
    // Node IDs for internal coordinates (for PBF output)
    pub internal_node_ids: Vec<i64>,
}

impl Segment {
    pub fn new(_id: String, geometry: LineString<f64>) -> Self {
        let shape_length = geometry_length(&geometry);
        let start_node = hash_coord(geometry.0.first().unwrap());
        let end_node = hash_coord(geometry.0.last().unwrap());
        
        Self {
            start_node,
            end_node,
            geometry,
            tags: FxHashMap::default(),
            properties: FxHashMap::default(),
            shape_length,
            internal_node_ids: Vec::new(),
        }
    }
    
    /// Get the coordinate at start or end
    pub fn start_coord(&self) -> &Coord {
        self.geometry.0.first().unwrap()
    }
    
    pub fn end_coord(&self) -> &Coord {
        self.geometry.0.last().unwrap()
    }
    
    /// Get internal coordinates (excluding start and end)
    pub fn internal_coords(&self) -> &[Coord] {
        let coords = &self.geometry.0;
        if coords.len() <= 2 {
            &[]
        } else {
            &coords[1..coords.len()-1]
        }
    }
}

/// Junction node where segments connect
#[derive(Debug, Clone, Default)]
pub struct Junction {
    pub segment_indices: Vec<usize>,
}

/// Merged way (collection of connected segments)
#[derive(Debug, Clone)]
pub struct Way {
    pub segment_indices: Vec<usize>,
    pub tags: FxHashMap<String, String>,
}

/// Bridge/tunnel structure
#[derive(Debug, Clone)]
pub struct Bridge {
    pub car_count: i32,
    pub cycle_count: i32,
    pub length: f64,
    pub layer: String,
}

/// Simplification method
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SimplifyMethod {
    Recursive,
    Route,
    Refname,
    Linear,
    Segment, // No simplification
}

impl From<&str> for SimplifyMethod {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "recursive" => SimplifyMethod::Recursive,
            "route" => SimplifyMethod::Route,
            "refname" => SimplifyMethod::Refname,
            "linear" => SimplifyMethod::Linear,
            "segment" => SimplifyMethod::Segment,
            _ => SimplifyMethod::Refname, // Default
        }
    }
}

/// Compute length of a LineString in meters
fn geometry_length(geometry: &LineString<f64>) -> f64 {
    use geo::algorithm::euclidean_length::EuclideanLength;
    geometry.euclidean_length()
}

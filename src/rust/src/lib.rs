use extendr_api::*;
use rustc_hash::FxHashMap;
use geo_types::{Coord, LineString};

// Module imports
mod models;
mod geometry;
mod grouping;
mod tag_mapper;
mod topology;

use models::{Segment, Way, SimplifyMethod, CoordHash, PropertyValue};
use pbf_craft::models::{Bound, Element, Node, Way as PbfWay, Tag, WayNode};
use pbf_craft::writers::PbfWriter;

/// Container for pre-processed column data
struct PreprocessedColumns {
    names: Vec<String>,
    // Store data as owned vectors to avoid lifetime issues
    string_cols: Vec<(usize, Vec<String>)>,
    int_cols: Vec<(usize, Vec<i32>)>,
    real_cols: Vec<(usize, Vec<f64>)>,
    logical_cols: Vec<(usize, Vec<i32>)>,
}

impl PreprocessedColumns {
    fn new(col_names: Vec<String>, col_data: &[Robj]) -> Self {
        let mut string_cols = Vec::new();
        let mut int_cols = Vec::new();
        let mut real_cols = Vec::new();
        let mut logical_cols = Vec::new();
        
        for (i, col) in col_data.iter().enumerate() {
            if i >= col_names.len() {
                break;
            }
            
            // Try to extract data based on type
            if let Some(chars) = col.as_str_vector() {
                // Convert to owned Strings
                let strings: Vec<String> = chars.iter().map(|s| s.to_string()).collect();
                string_cols.push((i, strings));
            } else if let Some(ints) = col.as_integer_slice() {
                // Check if it's actually a logical vector
                // R logical values: 0=FALSE, 1=TRUE, NA=INT_MIN
                if col.is_logical() {
                    let logicals: Vec<i32> = ints.to_vec();
                    logical_cols.push((i, logicals));
                } else {
                    let ints_vec: Vec<i32> = ints.to_vec();
                    int_cols.push((i, ints_vec));
                }
            } else if let Some(reals) = col.as_real_slice() {
                let reals_vec: Vec<f64> = reals.to_vec();
                real_cols.push((i, reals_vec));
            }
            // Unknown types are skipped
        }
        
        Self {
            names: col_names,
            string_cols,
            int_cols,
            real_cols,
            logical_cols,
        }
    }
    
    fn build_properties(&self, row_idx: usize) -> FxHashMap<String, PropertyValue> {
        let mut props = FxHashMap::default();
        
        // Process string columns
        for (col_idx, values) in &self.string_cols {
            if row_idx < values.len() {
                let s = &values[row_idx];
                if !s.is_empty() {
                    props.insert(self.names[*col_idx].clone(), PropertyValue::String(s.clone()));
                }
            }
        }
        
        // Process integer columns
        for (col_idx, values) in &self.int_cols {
            if row_idx < values.len() {
                let val = values[row_idx];
                // Check for NA (R uses INT_MIN for NA_INTEGER)
                if val != i32::MIN {
                    props.insert(self.names[*col_idx].clone(), PropertyValue::Integer(val as i64));
                }
            }
        }
        
        // Process real columns
        for (col_idx, values) in &self.real_cols {
            if row_idx < values.len() {
                let val = values[row_idx];
                // Check for NA (NaN or a special value)
                if !val.is_nan() {
                    let pv = if val == val.floor() {
                        PropertyValue::Integer(val as i64)
                    } else {
                        PropertyValue::Float(val)
                    };
                    props.insert(self.names[*col_idx].clone(), pv);
                }
            }
        }
        
        // Process logical columns
        for (col_idx, values) in &self.logical_cols {
            if row_idx < values.len() {
                let val = values[row_idx];
                // R: 0=FALSE, 1=TRUE, NA=INT_MIN
                if val != i32::MIN {
                    props.insert(self.names[*col_idx].clone(), PropertyValue::Boolean(val != 0));
                }
            }
        }
        
        props
    }
}

/// Parse WKB (Well-Known Binary) geometry
/// Handles 2D, 3D (Z), and 4D (ZM) coordinate types
fn parse_wkb(wkb: &[u8]) -> Option<LineString<f64>> {
    if wkb.len() < 9 {
        return None;
    }
    
    let little_endian = wkb[0] == 1;
    
    let geom_type = if little_endian {
        u32::from_le_bytes([wkb[1], wkb[2], wkb[3], wkb[4]])
    } else {
        u32::from_be_bytes([wkb[1], wkb[2], wkb[3], wkb[4]])
    };
    
    // WKB geometry types (base types without Z/M flags)
    // 2D: 1-7, Z: 1001-1007, M: 2001-2007, ZM: 3001-3007
    let base_type = geom_type % 1000;
    let has_z = (geom_type / 1000) == 1 || (geom_type / 1000) == 3;
    let has_m = (geom_type / 1000) == 2 || (geom_type / 1000) == 3;
    let coord_size = 16 + if has_z { 8 } else { 0 } + if has_m { 8 } else { 0 };
    
    match base_type {
        2 => parse_linestring_wkb(wkb, 5, little_endian, coord_size),
        5 => parse_multilinestring_wkb(wkb, little_endian, coord_size),
        _ => None,
    }
}

fn parse_linestring_wkb(wkb: &[u8], offset: usize, little_endian: bool, coord_size: usize) -> Option<LineString<f64>> {
    if wkb.len() < offset + 4 {
        return None;
    }
    
    let num_points = if little_endian {
        u32::from_le_bytes([wkb[offset], wkb[offset+1], wkb[offset+2], wkb[offset+3]]) as usize
    } else {
        u32::from_be_bytes([wkb[offset], wkb[offset+1], wkb[offset+2], wkb[offset+3]]) as usize
    };
    
    let point_offset = offset + 4;
    let expected_len = point_offset + num_points * coord_size;
    
    if wkb.len() < expected_len {
        return None;
    }
    
    let mut coords = Vec::with_capacity(num_points);
    
    for i in 0..num_points {
        let base = point_offset + i * coord_size;
        let x = read_f64(&wkb[base..base+8], little_endian);
        let y = read_f64(&wkb[base+8..base+16], little_endian);
        // Skip Z and M coordinates if present (we only need X,Y for OSM)
        coords.push(Coord { x, y });
    }
    
    Some(LineString::from(coords))
}

fn parse_multilinestring_wkb(wkb: &[u8], little_endian: bool, _coord_size: usize) -> Option<LineString<f64>> {
    if wkb.len() < 14 {
        return None;
    }
    
    // MultiLineString has a num_geoms field at offset 5, then each geometry
    let num_geoms = if little_endian {
        u32::from_le_bytes([wkb[5], wkb[6], wkb[7], wkb[8]]) as usize
    } else {
        u32::from_be_bytes([wkb[5], wkb[6], wkb[7], wkb[8]]) as usize
    };
    
    if num_geoms == 0 {
        return None;
    }
    
    // For simplicity, parse just the first LineString
    // Each geometry in MultiLineString is: byte_order (1) + type (4) + num_points (4) + points
    // Skip to first geometry: offset 9 (after num_geoms)
    let geom_start = 9;
    if wkb.len() < geom_start + 9 {
        return None;
    }
    
    // Verify it's a LineString
    let geom_byte_order = wkb[geom_start];
    let geom_little_endian = geom_byte_order == 1;
    let geom_type = if geom_little_endian {
        u32::from_le_bytes([wkb[geom_start+1], wkb[geom_start+2], wkb[geom_start+3], wkb[geom_start+4]])
    } else {
        u32::from_be_bytes([wkb[geom_start+1], wkb[geom_start+2], wkb[geom_start+3], wkb[geom_start+4]])
    };
    
    // Accept LineString (2) or LineString Z/M variants
    let base_geom_type = geom_type % 1000;
    if base_geom_type != 2 {
        return None;
    }
    
    // Check if the inner geometry has different Z/M flags
    let inner_has_z = (geom_type / 1000) == 1 || (geom_type / 1000) == 3;
    let inner_has_m = (geom_type / 1000) == 2 || (geom_type / 1000) == 3;
    let inner_coord_size = 16 + if inner_has_z { 8 } else { 0 } + if inner_has_m { 8 } else { 0 };
    
    parse_linestring_wkb(wkb, geom_start + 5, geom_little_endian, inner_coord_size)
}

fn read_f64(bytes: &[u8], little_endian: bool) -> f64 {
    let arr: [u8; 8] = [bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]];
    if little_endian {
        f64::from_le_bytes(arr)
    } else {
        f64::from_be_bytes(arr)
    }
}

/// Process NVDB data with WKB geometries and direct R property columns
/// 
/// # Arguments
/// * `wkb_geoms` - List of raw WKB bytes (one per geometry)
/// * `col_names` - Vector of column names for properties
/// * `col_data` - List of vectors (one per column), each vector has same length as wkb_geoms
/// * `output_path` - Path to write the PBF file
/// * `simplify_method` - Simplification method name
/// * `node_id_start` - Starting ID for nodes
/// * `way_id_start` - Starting ID for ways
#[extendr]
fn process_nvdb_wkb(
    wkb_geoms: List,
    col_names: Vec<String>,
    col_data: List,
    output_path: String,
    simplify_method: String,
    node_id_start: i64,
    way_id_start: i64,
) -> bool {
    let n = wkb_geoms.len();
    
    if n == 0 {
        eprintln!("No geometries provided");
        return false;
    }
    
    if col_data.len() != col_names.len() {
        eprintln!("Column names and data length mismatch: {} vs {}", col_data.len(), col_names.len());
        return false;
    }
    
    // Convert List to Vec<Robj> for easier access
    let col_data_vec: Vec<Robj> = col_data.into_iter().map(|(_, v)| v).collect();
    
    // Pre-process columns for efficient access
    let preprocessed = PreprocessedColumns::new(col_names, &col_data_vec);
    
    // Parse geometries and build segments
    let mut segments: Vec<Segment> = Vec::with_capacity(n);
    
    // Iterate over the wkb_geoms list
    for (i, (_, wkb_robj)) in wkb_geoms.into_iter().enumerate() {
        // Extract raw bytes from Robj
        let wkb_bytes: Vec<u8> = if let Some(raw_slice) = wkb_robj.as_raw_slice() {
            raw_slice.to_vec()
        } else {
            eprintln!("Geometry {} is not raw bytes", i);
            continue;
        };
        
        // Parse WKB
        let geometry = match parse_wkb(&wkb_bytes) {
            Some(geom) => geom,
            None => {
                eprintln!("Failed to parse WKB for geometry {}", i);
                continue;
            }
        };
        
        // Build segment
        let mut seg = Segment::new(format!("seg_{}", i), geometry);
        seg.properties = preprocessed.build_properties(i);
        
        segments.push(seg);
    }
    
    if segments.is_empty() {
        eprintln!("No valid geometries parsed");
        return false;
    }
    
    // Apply tags
    tag_mapper::tag_network(&mut segments);
    
    // Simplify network
    let method = SimplifyMethod::from(simplify_method.as_str());
    let ways = topology::simplify_network(&mut segments, method);
    
    // Write PBF using three-pass approach (nodes first, then ways)
    match write_pbf_three_pass(&ways, &mut segments, &output_path, node_id_start, way_id_start) {
        Ok(_) => true,
        Err(e) => {
            eprintln!("Failed to write PBF: {}", e);
            false
        }
    }
}

/// Write ways to PBF file using three-pass approach (nodes first, then ways)
/// This matches Python's behavior and ensures Osmium compatibility
fn write_pbf_three_pass(
    ways: &[Way],
    segments: &mut [Segment],
    output_path: &str,
    node_id_start: i64,
    way_id_start: i64,
) -> std::result::Result<(), String> {
    let mut writer = PbfWriter::from_path(output_path, true)
        .map_err(|e| format!("Failed to create writer: {}", e))?;

    // Compute bounding box from all segment geometries
    let (mut min_lat, mut max_lat) = (f64::MAX, f64::MIN);
    let (mut min_lon, mut max_lon) = (f64::MAX, f64::MIN);
    for seg in segments.iter() {
        for coord in &seg.geometry.0 {
            min_lat = min_lat.min(coord.y);
            max_lat = max_lat.max(coord.y);
            min_lon = min_lon.min(coord.x);
            max_lon = max_lon.max(coord.x);
        }
    }
    writer.set_bbox(Bound {
        left: deg_to_nanodeg(min_lon),
        right: deg_to_nanodeg(max_lon),
        top: deg_to_nanodeg(max_lat),
        bottom: deg_to_nanodeg(min_lat),
        origin: "nvdb2osmr".to_string(),
    });

    let mut node_id = node_id_start;
    let mut way_id = way_id_start;
    
    // Build junction index and assign junction node IDs
    let mut junction_ids: FxHashMap<CoordHash, i64> = FxHashMap::default();
    
    // Pass 1: Identify all junction nodes (start/end of segments that are used in ways)
    // and assign them IDs
    for way in ways {
        if !way.segment_indices.is_empty() {
            let first_seg = &segments[way.segment_indices[0]];
            let last_seg = &segments[way.segment_indices[way.segment_indices.len() - 1]];
            
            // Start junction of the way
            let start_hash = first_seg.start_node;
            if !junction_ids.contains_key(&start_hash) {
                let coord = first_seg.start_coord();
                let id = node_id;
                node_id += 1;
                junction_ids.insert(start_hash, id);
                
                let node = Node {
                    id,
                    latitude: deg_to_nanodeg(coord.y),
                    longitude: deg_to_nanodeg(coord.x),
                    tags: vec![],
                    version: 0,
                    timestamp: None,
                    user: None,
                    changeset_id: 0,
                    visible: true,
                };
                let _ = writer.write(Element::Node(node));
            }
            
            // End junction of the way
            let end_hash = last_seg.end_node;
            if !junction_ids.contains_key(&end_hash) {
                let coord = last_seg.end_coord();
                let id = node_id;
                node_id += 1;
                junction_ids.insert(end_hash, id);
                
                let node = Node {
                    id,
                    latitude: deg_to_nanodeg(coord.y),
                    longitude: deg_to_nanodeg(coord.x),
                    tags: vec![],
                    version: 0,
                    timestamp: None,
                    user: None,
                    changeset_id: 0,
                    visible: true,
                };
                let _ = writer.write(Element::Node(node));
            }
        }
        
        // Also need internal junctions (where segments connect within a way)
        for seg_indices in way.segment_indices.windows(2) {
            let seg1 = &segments[seg_indices[0]];
            let _seg2 = &segments[seg_indices[1]];
            
            // The junction between segments
            let junction_hash = seg1.end_node; // or seg2.start_node
            if !junction_ids.contains_key(&junction_hash) {
                let coord = seg1.end_coord();
                let id = node_id;
                node_id += 1;
                junction_ids.insert(junction_hash, id);
                
                let node = Node {
                    id,
                    latitude: deg_to_nanodeg(coord.y),
                    longitude: deg_to_nanodeg(coord.x),
                    tags: vec![],
                    version: 0,
                    timestamp: None,
                    user: None,
                    changeset_id: 0,
                    visible: true,
                };
                let _ = writer.write(Element::Node(node));
            }
        }
    }
    
    // Pass 2: Write internal nodes for each segment
    // Internal nodes are all coordinates except start and end
    // First, collect all (seg_idx, coord) pairs that need nodes
    let mut internal_node_data: Vec<(usize, Vec<Coord>)> = Vec::new();
    for way in ways {
        for &seg_idx in &way.segment_indices {
            let seg = &segments[seg_idx];
            let coords: Vec<Coord> = seg.internal_coords().iter().copied().collect();
            internal_node_data.push((seg_idx, coords));
        }
    }
    
    // Now process each segment's internal nodes
    for (seg_idx, coords) in internal_node_data {
        let seg = &mut segments[seg_idx];
        seg.internal_node_ids.clear();
        
        for coord in coords {
            let id = node_id;
            node_id += 1;
            seg.internal_node_ids.push(id);
            
            let node = Node {
                id,
                latitude: deg_to_nanodeg(coord.y),
                longitude: deg_to_nanodeg(coord.x),
                tags: vec![],
                version: 0,
                timestamp: None,
                user: None,
                changeset_id: 0,
                visible: true,
            };
            let _ = writer.write(Element::Node(node));
        }
    }
    
    // Pass 3: Write all ways
    for way in ways {
        let mut way_node_ids: Vec<i64> = Vec::new();
        
        if !way.segment_indices.is_empty() {
            // Start with first segment's start junction
            let first_seg = &segments[way.segment_indices[0]];
            let start_id = junction_ids.get(&first_seg.start_node)
                .copied()
                .unwrap_or_else(|| {
                    // Fallback: create new node
                    let id = node_id;
                    node_id += 1;
                    id
                });
            way_node_ids.push(start_id);
            
            // Add internal nodes and end junctions for each segment
            for &seg_idx in &way.segment_indices {
                let seg = &segments[seg_idx];
                
                // Add internal nodes
                for &internal_id in &seg.internal_node_ids {
                    way_node_ids.push(internal_id);
                }
                
                // Add end junction
                let end_id = junction_ids.get(&seg.end_node)
                    .copied()
                    .unwrap_or_else(|| {
                        let id = node_id;
                        node_id += 1;
                        id
                    });
                way_node_ids.push(end_id);
            }
        }
        
        // Deduplicate consecutive nodes (in case junctions overlap)
        way_node_ids.dedup();
        
        let way_nodes: Vec<WayNode> = way_node_ids
            .iter()
            .map(|&id| WayNode::new_without_coords(id))
            .collect();
        
        let tags: Vec<Tag> = way.tags
            .iter()
            .map(|(k, v)| Tag {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
        
        let pbf_way = PbfWay {
            id: way_id,
            way_nodes,
            tags,
            version: 0,
            timestamp: None,
            user: None,
            changeset_id: 0,
            visible: true,
        };
        
        let _ = writer.write(Element::Way(pbf_way));
        way_id += 1;
    }
    
    writer.finish().map_err(|e| format!("Failed to finish: {}", e))?;
    Ok(())
}

/// Convert degrees to nanodegrees (for PBF format)
fn deg_to_nanodeg(deg: f64) -> i64 {
    (deg * 1_000_000_000.0) as i64
}

extendr_module! {
    mod nvdb2osmr;
    fn process_nvdb_wkb;
}

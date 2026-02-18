//! Node generation for NVDB point features
//! 
//! This module handles generation of OSM nodes (POIs) from NVDB segment data.
//! Ported from Python create_node() function (py-script.py lines 1006-1027).

use rustc_hash::FxHashMap;
use crate::models::{Segment, NodeFeature};

/// Container for all generated nodes during tagging
#[derive(Debug, Default)]
pub struct NodeCollection {
    nodes: Vec<NodeFeature>,
    next_id: i64,
}

impl NodeCollection {
    pub fn new(starting_id: i64) -> Self {
        Self {
            nodes: Vec::new(),
            next_id: starting_id,
        }
    }
    
    pub fn add_node(&mut self, lat: f64, lon: f64, tags: FxHashMap<String, String>) -> i64 {
        let mut node = NodeFeature::new(self.next_id, lat, lon);
        node.tags = tags;
        let id = self.next_id;
        self.nodes.push(node);
        self.next_id += 1;
        id
    }
    
    pub fn into_nodes(self) -> Vec<NodeFeature> {
        self.nodes
    }
    
    pub fn nodes(&self) -> &[NodeFeature] {
        &self.nodes
    }
}

/// Generate nodes for a segment based on NVDB properties
/// 
/// This function checks various NVDB properties and creates appropriate
/// OSM nodes (crossings, cameras, barriers, etc.)
/// 
/// Python equivalent: osm_tags() lines 319-446
pub fn generate_nodes_for_segment(segment: &Segment, next_id: i64) -> (Vec<NodeFeature>, i64) {
    let mut nodes = Vec::new();
    let mut id = next_id;
    
    // Get the first coordinate of the segment (used for node position)
    // Python uses: way["geometry"]["coordinates"][0][0]
    let coord = segment.geometry.0.first();
    if coord.is_none() {
        return (nodes, id);
    }
    let coord = coord.unwrap();
    let (lon, lat) = (coord.x, coord.y);
    
    // 1. Pedestrian/Cycle Crossings (GCM-passage)
    // Python lines 321-336
    if let Some(passage_type) = segment.properties.get("Passa_85").and_then(|v| v.as_i64()) {
        let mut tags = FxHashMap::default();
        
        match passage_type {
            3 => {
                // övergångsställe och/eller cykelpassage
                tags.insert("highway".to_string(), "crossing".to_string());
            }
            4 => {
                // signalreglerat övergångsställe
                tags.insert("highway".to_string(), "crossing".to_string());
                tags.insert("crossing".to_string(), "traffic_signals".to_string());
            }
            5 => {
                // annan ordnad passage
                tags.insert("highway".to_string(), "crossing".to_string());
            }
            _ => {}
        }
        
        if !tags.is_empty() {
            nodes.push(NodeFeature { id, lat, lon, tags });
            id += 1;
        }
    }
    
    // 2. Railway Crossings (Järnvägskorsning)
    // Python lines 338-354
    if let Some(skydd) = segment.properties.get("Vagsk_100").and_then(|v| v.as_i64()) {
        let mut tags = FxHashMap::default();
        
        // Determine railway tag based on network type
        let net_type = segment.properties.get("Vagtr_474").and_then(|v| v.as_i64()).unwrap_or(0);
        if net_type == 1 {
            tags.insert("railway".to_string(), "level_crossing".to_string());
        } else {
            tags.insert("railway".to_string(), "crossing".to_string());
        }
        
        // Add protection details
        match skydd {
            1 => { tags.insert("crossing:barrier".to_string(), "full".to_string()); }    // Helbom
            2 => { tags.insert("crossing:barrier".to_string(), "half".to_string()); }    // Halvbom
            3 => {
                tags.insert("crossing:bell".to_string(), "yes".to_string());
                tags.insert("crossing:light".to_string(), "yes".to_string());
            }
            4 => { tags.insert("crossing:light".to_string(), "yes".to_string()); }       // Ljussignal
            5 => { tags.insert("crossing:bell".to_string(), "yes".to_string()); }        // Ljudsignal
            6 => { tags.insert("crossing:saltire".to_string(), "yes".to_string()); }      // Kryssmärke
            7 => { tags.insert("crossing".to_string(), "uncontrolled".to_string()); }     // Utan skydd
            _ => {}
        }
        
        if tags.len() > 1 || tags.contains_key("railway") {
            nodes.push(NodeFeature { id, lat, lon, tags });
            id += 1;
        }
    }
    
    // 3. Traffic Calming (Farthinder)
    // Python lines 356-372
    if let Some(farthinder_typ) = segment.properties.get("TypAv_82").and_then(|v| v.as_i64()) {
        let mut tags = FxHashMap::default();
        
        let calming_type = match farthinder_typ {
            1 => "choker",     // avsmalning till ett körfält
            2 => "hump",       // gupp
            3 => "chicane",    // sidoförskjutning
            4 => "island",     // sidoförskjutning - refug
            5 => "dip",        // väghåla
            6 => "cushion",    // vägkudde
            7 => "table",      // förhöjd genomgående gcm-passage
            8 => "table",      // förhöjd korsning
            9 => "yes",        // övrigt farthinder
            _ => "",
        };
        
        if !calming_type.is_empty() {
            tags.insert("traffic_calming".to_string(), calming_type.to_string());
            nodes.push(NodeFeature { id, lat, lon, tags });
            id += 1;
        }
    }
    
    // 4. Barriers (Väghinder)
    // Python lines 374-388
    if let Some(hinder_typ) = segment.properties.get("Hinde_72").and_then(|v| v.as_i64()) {
        let mut tags = FxHashMap::default();
        
        let barrier_type = match hinder_typ {
            1 => "bollard",       // pollare
            2 => "swing_gate",    // eftergivlig grind
            3 => "cycle_barrier", // cykelfålla
            4 => "lift_gate",     // låst grind/bom
            5 => "jersey_barrier",// betonghinder
            6 => "bus_trap",      // spårviddshinder
            99 => "yes",          // övrigt
            _ => "",
        };
        
        if !barrier_type.is_empty() {
            tags.insert("barrier".to_string(), barrier_type.to_string());
            
            // Add maxwidth:physical if available
            if let Some(pass_width) = segment.properties.get("Passe_73").and_then(|v| v.as_f64()) {
                if pass_width > 0.0 {
                    tags.insert("maxwidth:physical".to_string(), format!("{:.1}", pass_width));
                }
            }
            
            nodes.push(NodeFeature { id, lat, lon, tags });
            id += 1;
        }
    }
    
    // 5. Speed Cameras (ATK-Mätplats)
    // Python lines 390-415
    let f_atk = segment.properties.get("F_ATK_Matplats").or_else(|| segment.properties.get("F_ATK_Matplats_117"))
        .and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    let b_atk = segment.properties.get("B_ATK_Matplats").or_else(|| segment.properties.get("B_ATK_Matplats_117"))
        .and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    
    if f_atk || b_atk {
        let mut tags = FxHashMap::default();
        tags.insert("highway".to_string(), "speed_camera".to_string());
        
        // Add maxspeed from the corresponding direction
        if f_atk {
            if let Some(speed) = segment.properties.get("F_Hogst_225").and_then(|v| v.as_i64()) {
                if speed > 0 && speed <= 120 {
                    tags.insert("maxspeed".to_string(), speed.to_string());
                }
            }
        } else if b_atk {
            if let Some(speed) = segment.properties.get("B_Hogst_225").and_then(|v| v.as_i64()) {
                if speed > 0 && speed <= 120 {
                    tags.insert("maxspeed".to_string(), speed.to_string());
                }
            }
        }
        
        nodes.push(NodeFeature { id, lat, lon, tags });
        id += 1;
    }
    
    // 6. Rest Areas (Rastplats)
    // Python lines 417-440
    if let Some(rastplats) = segment.properties.get("Rastplats").and_then(|v| v.as_i64()) {
        if rastplats > 0 {
            let mut tags = FxHashMap::default();
            tags.insert("highway".to_string(), "rest_area".to_string());
            
            // Add name if available
            if let Some(name) = segment.properties.get("Rastp_118") {
                let name_str = name.as_string().trim().to_string();
                if !name_str.is_empty() && name_str != "NA" {
                    tags.insert("name".to_string(), name_str);
                }
            }
            
            // Add capacity for cars
            if let Some(cap) = segment.properties.get("Antal_119").and_then(|v| v.as_i64()) {
                if cap > 0 {
                    tags.insert("capacity".to_string(), cap.to_string());
                }
            }
            
            // Add capacity for HGVs
            if let Some(cap_hgv) = segment.properties.get("Antal_122").and_then(|v| v.as_i64()) {
                if cap_hgv > 0 {
                    tags.insert("capacity:hgv".to_string(), cap_hgv.to_string());
                }
            }
            
            nodes.push(NodeFeature { id, lat, lon, tags });
            id += 1;
        }
    }
    
    // 7. Parking Along Highway (Rastficka)
    // Python lines 442-446
    let l_rastficka = segment.properties.get("L_Rastficka_2").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    let r_rastficka = segment.properties.get("R_Rastficka_2").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    
    if l_rastficka || r_rastficka {
        let mut tags = FxHashMap::default();
        tags.insert("amenity".to_string(), "parking".to_string());
        
        // Add parking type if we know which side
        if l_rastficka && !r_rastficka {
            tags.insert("parking:lane:left".to_string(), "yes".to_string());
        } else if r_rastficka && !l_rastficka {
            tags.insert("parking:lane:right".to_string(), "yes".to_string());
        }
        
        nodes.push(NodeFeature { id, lat, lon, tags });
        id += 1;
    }
    
    (nodes, id)
}

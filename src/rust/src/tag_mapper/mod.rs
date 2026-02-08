use rustc_hash::FxHashMap;
use crate::models::{Segment, Bridge};
use std::sync::OnceLock;

// Static lookup tables for tag mapping
static HIGHWAY_CLASSES: OnceLock<FxHashMap<i64, &'static str>> = OnceLock::new();

fn init_highway_classes() -> FxHashMap<i64, &'static str> {
    let mut map = FxHashMap::default();
    // Funktionell vägklass/Klass
    map.insert(1, "trunk");        // Europeisk väg, Nationell väg
    map.insert(2, "trunk");        // Primär länsväg
    map.insert(3, "primary");      // Sekundär länsväg
    map.insert(4, "secondary");    // Primär kommunal väg
    map.insert(5, "tertiary");     // Sekundär kommunal väg
    map.insert(6, "unclassified"); // Tertiär kommunal väg
    map.insert(7, "residential");  // Övriga kommunala vägar
    map
}

/// Main entry point for tagging network
/// 
/// Port of tag_network() from Python
pub fn tag_network(segments: &mut [Segment]) {
    // 1. Detect bridges and tunnels
    let bridges = detect_bridges(segments);
    
    // 2. Handle missing bridge segments
    detect_missing_bridges(segments, &bridges);
    
    // 3. Main tagging loop
    for segment in segments.iter_mut() {
        map_highway(segment);
        map_surface(segment);
        map_maxspeed(segment);
        map_oneway(segment);
        map_bridge_tunnel(segment, &bridges);
        map_name(segment);
        map_ref(segment);
        map_lanes(segment);
        map_width(segment);
        map_layer(segment);
        // ... more mappings
    }
    
    // 4. Post-processing
    tag_isolated_tracks(segments);
    tag_urban_vs_rural(segments);
}

/// Detect bridges and build bridge dictionary
fn detect_bridges(segments: &[Segment]) -> FxHashMap<String, Bridge> {
    let mut bridges: FxHashMap<String, Bridge> = FxHashMap::default();
    
    for segment in segments {
        // Check for bridge/tunnel identity and construction
        if let (Some(id_prop), Some(constr_prop)) = (
            segment.properties.get("Ident_191"),
            segment.properties.get("Konst_190")
        ) {
            let bridge_id = id_prop.as_string();
            let construction = constr_prop.as_i64().unwrap_or(0);
            
            let bridge = bridges.entry(bridge_id).or_insert(Bridge {
                car_count: 0,
                cycle_count: 0,
                length: 0.0,
                layer: "1".to_string(),
            });
            
            // Construction codes:
            // 1 = Over bridge (väg på bro)
            // 2 = Under bridge, car traffic (väg under bro, bil)
            // 3 = Under bridge, cycle traffic (väg under bro, cykel)
            // 4 = Middle layer bridge
            match construction {
                2 | 4 => {
                    // Under bridge
                    if let Some(net_type) = segment.properties.get("Vagtr_474").and_then(|v| v.as_i64()) {
                        if net_type == 1 {
                            bridge.car_count += 1;
                        } else {
                            bridge.cycle_count += 1;
                        }
                    }
                }
                1 => {
                    // Over bridge - track length
                    if segment.shape_length > bridge.length {
                        bridge.length = segment.shape_length;
                    }
                }
                _ => {}
            }
        }
    }
    
    bridges
}

/// Detect missing bridge segments
fn detect_missing_bridges(_segments: &mut [Segment], _bridges: &FxHashMap<String, Bridge>) {
    // TODO: Implement intersection-based bridge detection
    // This requires spatial index for efficiency
}

/// Map highway class from NVDB
fn map_highway(segment: &mut Segment) {
    let highway = if segment.properties.get("Motorvag").and_then(|v| v.as_i64()) == Some(1) {
        "motorway"
    } else if segment.properties.get("Motortrafikled").and_then(|v| v.as_i64()) == Some(1) {
        "trunk"
    } else {
        let classes = HIGHWAY_CLASSES.get_or_init(init_highway_classes);
        segment.properties
            .get("Klass_181") // Funktionell vägklass
            .and_then(|v| v.as_i64())
            .and_then(|class| classes.get(&class).copied())
            .unwrap_or("residential")
    };
    
    segment.tags.insert("highway".to_string(), highway.to_string());
    
    // Add motorroad tag for trunk roads
    if highway == "trunk" {
        segment.tags.insert("motorroad".to_string(), "yes".to_string());
    }
}

/// Map surface type
fn map_surface(segment: &mut Segment) {
    if let Some(surface_code) = segment.properties.get("Slitl_152").and_then(|v| v.as_i64()) {
        let surface = match surface_code {
            1 => "paved",
            2 => "unpaved",
            3 => "gravel",
            _ => return, // Don't add unknown surfaces
        };
        segment.tags.insert("surface".to_string(), surface.to_string());
    }
}

/// Map maxspeed
fn map_maxspeed(segment: &mut Segment) {
    // Check various speed limits
    let speed = segment.properties
        .get("Hogst_36") // Generell hastighetsbegränsning
        .or_else(|| segment.properties.get("Hogst_46")) // Hastighetsbegränsning
        .or_else(|| segment.properties.get("F_Hogst_24")) // Hastighetsbegränsning(F)
        .and_then(|v| v.as_i64());
    
    if let Some(speed) = speed {
        if speed > 0 && speed <= 120 {
            segment.tags.insert("maxspeed".to_string(), format!("{}", speed));
        }
    }
}

/// Map oneway status
///
/// Matches Python behavior (lines 514-520):
/// - F_ForbjudenFardriktning=1: forward direction forbidden → reverse geometry, set oneway=yes
/// - B_ForbjudenFardriktning=1: backward direction forbidden → set oneway=yes (geometry correct)
fn map_oneway(segment: &mut Segment) {
    use crate::models::hash_coord;

    // Check direction of travel restrictions first (takes priority)
    let f_forbidden = segment.properties.get("F_ForbjudenFardriktning")
        .and_then(|v| v.as_i64()) == Some(1);
    let b_forbidden = segment.properties.get("B_ForbjudenFardriktning")
        .and_then(|v| v.as_i64()) == Some(1);

    if f_forbidden && !b_forbidden {
        // Forward direction forbidden → reverse geometry so oneway=yes follows correct direction
        segment.geometry.0.reverse();
        segment.start_node = hash_coord(segment.geometry.0.first().unwrap());
        segment.end_node = hash_coord(segment.geometry.0.last().unwrap());
        segment.tags.insert("oneway".to_string(), "yes".to_string());
    } else if b_forbidden && !f_forbidden {
        // Backward direction forbidden → geometry already points in correct direction
        segment.tags.insert("oneway".to_string(), "yes".to_string());
    }

    // Check Korfa_524 (Körfältsanvändning) only if oneway not already set
    if !segment.tags.contains_key("oneway") {
        if let Some(korfa) = segment.properties.get("Korfa_524").and_then(|v| v.as_i64()) {
            if korfa == 1 {
                segment.tags.insert("oneway".to_string(), "yes".to_string());
            }
        }
    }
}

/// Map bridge and tunnel tags
fn map_bridge_tunnel(segment: &mut Segment, bridges: &FxHashMap<String, Bridge>) {
    if let (Some(id_prop), Some(constr_prop)) = (
        segment.properties.get("Ident_191"),
        segment.properties.get("Konst_190")
    ) {
        let bridge_id = id_prop.as_string();
        let construction = constr_prop.as_i64().unwrap_or(0);
        
        match construction {
            1 | 4 => {
                // Over bridge or middle layer
                segment.tags.insert("bridge".to_string(), "yes".to_string());
                
                // Add layer tag
                if let Some(bridge) = bridges.get(&bridge_id) {
                    segment.tags.insert("layer".to_string(), bridge.layer.clone());
                }
            }
            2 | 3 => {
                // Under bridge
                if let Some(bridge) = bridges.get(&bridge_id) {
                    // Mark as tunnel only if significant
                    if bridge.length > 50.0 {
                        segment.tags.insert("tunnel".to_string(), "yes".to_string());
                        segment.tags.insert("layer".to_string(), "-1".to_string());
                    }
                }
            }
            _ => {}
        }
    }
    
    // Check for explicit tunnel attribute
    if segment.properties.get("Farjeled").and_then(|v| v.as_i64()) == Some(1) {
        segment.tags.insert("tunnel".to_string(), "yes".to_string());
    }
}

/// Map name (street name)
fn map_name(segment: &mut Segment) {
    if let Some(name) = segment.properties.get("Namn_130") {
        let name_str = name.as_string();
        if !name_str.is_empty() {
            segment.tags.insert("name".to_string(), name_str);
        }
    }
}

/// Map reference number
fn map_ref(segment: &mut Segment) {
    // Primary: Driftbidrag statligt/Vägnr (road number for countryside)
    if let Some(vagnr) = segment.properties.get("Vagnr_10370") {
        let ref_str = vagnr.as_string();
        if !ref_str.is_empty() && ref_str != "0" {
            segment.tags.insert("ref".to_string(), ref_str);
            return;
        }
    }
    
    // Alternative: National road number
    if let Some(road_num) = segment.properties.get("Evag_555") {
        let ref_str = road_num.as_string();
        if !ref_str.is_empty() && ref_str != "0" {
            segment.tags.insert("ref".to_string(), ref_str);
        }
    }
}

/// Map number of lanes
fn map_lanes(segment: &mut Segment) {
    // Check Korfa_524 for number of lanes
    if let Some(korfa) = segment.properties.get("Korfa_524").and_then(|v| v.as_i64()) {
        let lanes = match korfa {
            1 => 1, // One-way traffic
            2 => 2, // Two-way with lane markings
            3 => 2, // Two-way without lane markings
            4 => 3, // Three lanes
            5 => 4, // Four lanes
            _ => return,
        };
        segment.tags.insert("lanes".to_string(), lanes.to_string());
    }
}

/// Map width
fn map_width(segment: &mut Segment) {
    if let Some(width) = segment.properties.get("Bredd_156").and_then(|v| v.as_f64()) {
        if width > 0.0 && width < 50.0 {
            segment.tags.insert("width".to_string(), format!("{:.1}", width));
        }
    }
}

/// Map layer (for bridges/tunnels)
fn map_layer(segment: &mut Segment) {
    // Already handled in bridge/tunnel mapping
    // But check for additional layer info
    if segment.tags.contains_key("bridge") && !segment.tags.contains_key("layer") {
        segment.tags.insert("layer".to_string(), "1".to_string());
    }
}

/// Tag isolated service roads as tracks
fn tag_isolated_tracks(segments: &mut [Segment]) {
    for segment in segments.iter_mut() {
        if let Some(highway) = segment.tags.get("highway") {
            if highway == "service" {
                // Check if isolated (no connected segments would need spatial index)
                // For now, simple heuristic based on length
                if segment.shape_length < 100.0 {
                    // Could be isolated
                }
            }
        }
    }
}

/// Tag urban vs rural streets
fn tag_urban_vs_rural(_segments: &mut [Segment]) {
    // TODO: Implement based on TätbebyggtOmrade attribute
}

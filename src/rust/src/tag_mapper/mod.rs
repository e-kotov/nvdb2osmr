use rustc_hash::FxHashMap;
use crate::models::{Segment, Bridge, OnewayDirection};
use std::sync::OnceLock;

pub mod nodes;

// Static lookup tables for tag mapping
static HIGHWAY_CLASSES: OnceLock<FxHashMap<i64, &'static str>> = OnceLock::new();
static COUNTY_CODES: OnceLock<FxHashMap<i64, &'static str>> = OnceLock::new();
static VEHICLE_TYPE_MAP: OnceLock<FxHashMap<i64, &'static str>> = OnceLock::new();

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

/// Swedish county codes for road references
/// Maps county number (Kommunnr // 100) to county letter code
fn init_county_codes() -> FxHashMap<i64, &'static str> {
    let mut map = FxHashMap::default();
    map.insert(1, "AB");   // Stockholms län
    map.insert(3, "C");    // Uppsala län
    map.insert(4, "D");    // Södermanlands län
    map.insert(5, "E");    // Östergötlands län
    map.insert(6, "F");    // Jönköpings län
    map.insert(7, "G");    // Kronobergs län
    map.insert(8, "H");    // Kalmar län
    map.insert(9, "I");    // Gotlands län
    map.insert(10, "K");   // Blekinge län
    map.insert(11, "L");   // Kristianstads län (f.d.)
    map.insert(12, "M");   // Skåne län
    map.insert(13, "N");   // Hallands län
    map.insert(14, "O");   // Västra Götalands län
    map.insert(15, "P");   // Älvsborgs län (f.d.)
    map.insert(16, "R");   // Skaraborgs län (f.d.)
    map.insert(17, "S");   // Värmlands län
    map.insert(18, "T");   // Örebro län
    map.insert(19, "U");   // Västmanlands län
    map.insert(20, "W");   // Dalarnas län
    map.insert(21, "X");   // Gävleborgs län
    map.insert(22, "Y");   // Västernorrlands län
    map.insert(23, "Z");   // Jämtlands län
    map.insert(24, "AC");  // Västerbottens län
    map.insert(25, "BD");  // Norrbottens län
    map
}

/// NVDB vehicle type codes to OSM access tags
/// From "Förbud mot trafik/Gäller fordon"
fn init_vehicle_type_map() -> FxHashMap<i64, &'static str> {
    let mut map = FxHashMap::default();
    map.insert(10, "motorcar");       // bil
    map.insert(20, "bus");            // buss
    map.insert(30, "bicycle");        // cykel
    map.insert(40, "vehicle");        // fordon (all vehicles)
    map.insert(90, "hgv");            // lastbil (heavy goods vehicle)
    map.insert(100, "goods");         // lätt lastbil (light truck)
    map.insert(120, "moped");         // moped
    map.insert(130, "moped");         // moped klass I
    map.insert(140, "moped");         // moped klass II
    map.insert(150, "motorcycle");    // motorcykel
    map.insert(170, "motor_vehicle"); // motordrivna fordon
    map.insert(180, "motor_vehicle"); // motorredskap
    map.insert(210, "motorcar");      // personbil (passenger car)
    map.insert(230, "atv");           // terrängmotorfordon
    map.insert(270, "tractor");       // traktor
    map.insert(280, "hgv");           // tung lastbil (heavy truck)
    map
}

/// Main entry point for tagging network
/// 
/// Port of tag_network() from Python
pub fn tag_network(segments: &mut [Segment]) {
    // Initialize lookup tables
    let _ = HIGHWAY_CLASSES.get_or_init(init_highway_classes);
    let _ = COUNTY_CODES.get_or_init(init_county_codes);
    let _ = VEHICLE_TYPE_MAP.get_or_init(init_vehicle_type_map);
    
    // 1. Detect bridges and tunnels
    let bridges = detect_bridges(segments);
    
    // 2. Handle missing bridge segments
    detect_missing_bridges(segments, &bridges);
    
    // 2b. Build street_names set for cycleway name logic (Python lines 1190-1203)
    let street_names = build_street_names(segments);

    // 3. Main tagging loop — order matches Python osm_tags() function
    for segment in segments.iter_mut() {
        // Bridge/tunnel must come before highway (Python line 486 before 528)
        map_bridge_tunnel(segment, &bridges);

        // Oneway MUST be determined before any directional tags (Python lines 514-524)
        map_oneway(segment);

        // Highway classification (Python lines 528-680)
        map_highway(segment, &street_names);

        // Motorway/motorroad override AFTER category (Python lines 684-688)
        map_motorway_override(segment);

        // Highway links (Python lines 693-701)
        map_highway_links(segment);

        // Road references (Python lines 732-745)
        map_ref(segment);

        // Roundabout (Python lines 749-756) — uses tag_direction
        map_roundabout(segment);

        // Maxspeed (Python lines 758-770) — uses tag_direction
        map_maxspeed(segment);

        // Motor vehicle access (Python lines 772-779) — uses tag_direction
        map_motor_vehicle_access(segment);

        // Vehicle type restrictions (Python lines 781-845)
        map_vehicle_restrictions(segment);

        // Hazmat (Python lines 846-860)
        map_hazmat(segment);

        // Overtaking (Python lines 862-869) — uses tag_direction
        map_overtaking_restrictions(segment);

        // Lanes (Python lines 873-905)
        map_lanes(segment);

        // Surface (Python lines 909-912)
        map_surface(segment);

        // Width (Python line 914-915)
        map_width(segment);

        // Priority road (Python line 917-918)
        map_priority_road(segment);

        // Bicycle designated (Python line 920-921)
        map_bicycle_designated(segment);

        // Low emission zone (Python lines 923-927)
        map_low_emission_zone(segment);

        // Names (Python lines 929-948)
        map_name(segment);
        map_bridge_tunnel_names(segment);

        // Restrictions (Python lines 950-998)
        // (maxheight/maxlength/maxwidth/maxaxleload already in map_vehicle_restrictions)

        // Lit (from GCM_belyst, Python line 598-599)
        map_lit(segment);

        // Layer fallback
        map_layer(segment);
    }
    
    // 4. Post-processing
    tag_isolated_tracks(segments);
    tag_urban_vs_rural(segments);
}

/// Detect bridges and build bridge dictionary
/// 
/// Python logic (lines 1088-1183):
/// 1. Collect all bridges with car/cycle counts
/// 2. Decide tag: "bridge" if car>0 or long, else "tunnel" if cycle>0, else "bridge"
fn detect_bridges(segments: &[Segment]) -> FxHashMap<String, Bridge> {
    let mut bridges: FxHashMap<String, Bridge> = FxHashMap::default();
    
    // First pass: collect bridge info (lines 1091-1132)
    for segment in segments {
        // Check for bridge/tunnel identity and construction
        if let (Some(id_prop), Some(constr_prop)) = (
            segment.properties.get("Ident_191"),
            segment.properties.get("Konst_190")
        ) {
            let bridge_id = id_prop.as_string();
            let construction = constr_prop.as_i64().unwrap_or(0);
            
            let bridge = bridges.entry(bridge_id.clone()).or_insert(Bridge {
                car_count: 0,
                cycle_count: 0,
                length: 0.0,
                layer: "1".to_string(),
                tag: "bridge".to_string(),  // Default
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
                        // Line 1113-1118: net_type 1 and not construction 3 = car
                        if net_type == 1 && construction != 3 {
                            bridge.car_count += 1;
                        } else {
                            bridge.cycle_count += 1;
                        }
                    }
                }
                1 => {
                    // Over bridge - track length (line 1120-1125)
                    if segment.shape_length > bridge.length {
                        bridge.length = segment.shape_length;
                    }
                }
                _ => {}
            }
        }
    }
    
    // Second pass: decide tag for each bridge (lines 1173-1183)
    let bridge_margin = 50.0;
    for (_, bridge) in bridges.iter_mut() {
        if bridge.car_count > 0 || bridge.length > bridge_margin {
            // Always tag bridge if highway underneath is for cars or long enough
            bridge.tag = "bridge".to_string();
        } else if bridge.cycle_count > 0 {
            // Tag tunnel if only foot/cycleway underneath
            bridge.tag = "tunnel".to_string();
        } else {
            // Catch all
            bridge.tag = "bridge".to_string();
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
///
/// Follows official Swedish categories as used by Trafikverket and Lantmäteriet.
/// Ported from Python osm_tags() function.
///
/// CRITICAL: The order of checks matches Python:
/// 1. Ferry (with full classification, ref, name — P3 fix)
/// 2. Cycleway/footway (BEFORE motor vehicle highways)
/// 3. Motor vehicle highways by category
/// 4. Private roads / Service / Track
fn map_highway(segment: &mut Segment, street_names: &std::collections::HashSet<String>) {
    // STEP 0: Check for ferry first (Python lines 452-480)
    if segment.properties.get("Farjeled").and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
        segment.tags.insert("route".to_string(), "ferry".to_string());
        segment.tags.insert("foot".to_string(), "yes".to_string());
        // Motor vehicle access depends on network type
        let net_type = segment.properties.get("Vagtr_474").and_then(|v| v.as_i64()).unwrap_or(0);
        if net_type == 1 {
            segment.tags.insert("motor_vehicle".to_string(), "yes".to_string());
        } else {
            segment.tags.insert("motor_vehicle".to_string(), "no".to_string());
        }

        // P3 FIX: Ferry category (Python lines 461-469)
        if let Some(kateg) = segment.properties.get("Kateg_380").and_then(|v| v.as_i64()) {
            let ferry_cat = match kateg {
                1 | 2 => "trunk",
                3 => "primary",
                4 => "secondary",
                _ => "",
            };
            if !ferry_cat.is_empty() {
                segment.tags.insert("ferry".to_string(), ferry_cat.to_string());
            }
        }

        // P3 FIX: Ferry ref (Python lines 471-475)
        if let Some(huvnr) = segment.properties.get("Huvnr_556_1") {
            let huvnr_str = huvnr.as_string();
            if !huvnr_str.is_empty() && huvnr_str != "0" {
                let kateg = segment.properties.get("Kateg_380").and_then(|v| v.as_i64()).unwrap_or(0);
                if kateg == 1 {
                    segment.tags.insert("ref".to_string(), format!("E {}", huvnr_str));
                } else {
                    segment.tags.insert("ref".to_string(), huvnr_str);
                }
            }
        }

        // P3 FIX: Ferry name (Python lines 477-478)
        if let Some(name) = segment.properties.get("Farje_139") {
            let name_str = name.as_string();
            let name_str = name_str.trim();
            if !name_str.is_empty() && name_str != "NA" {
                segment.tags.insert("name".to_string(), name_str.to_string());
            }
        }

        return;
    }

    // Get network type - needed for cycleway/footway detection
    let net_type = segment.properties.get("Vagtr_474").and_then(|v| v.as_i64()).unwrap_or(1);

    // STEP 1: Check for cycleway/footway based on network type (Python lines 528-619)
    if net_type == 2 || net_type == 4 {
        // Check for sidewalk FIRST (Python lines 563-570) — overrides GCM type
        let l_separ = segment.properties.get("L_Separ_500").and_then(|v| v.as_i64()).unwrap_or(0);
        let r_separ = segment.properties.get("R_Separ_500").and_then(|v| v.as_i64()).unwrap_or(0);
        if l_separ == 1 || r_separ == 1 {
            segment.tags.insert("highway".to_string(), "footway".to_string());
            segment.tags.insert("footway".to_string(), "sidewalk".to_string());
        } else if let Some(gcm_typ) = segment.properties.get("GCM_t_502").and_then(|v| v.as_i64()) {
            // P6 FIX: Full GCM type mapping (Python lines 529-561)
            match gcm_typ {
                1 | 2 | 3 | 5 | 8 | 9 | 13 | 15 => {
                    segment.tags.insert("highway".to_string(), "cycleway".to_string());
                }
                4 | 10 | 11 => {
                    segment.tags.insert("highway".to_string(), "footway".to_string());
                }
                12 => {
                    segment.tags.insert("highway".to_string(), "footway".to_string());
                    segment.tags.insert("footway".to_string(), "sidewalk".to_string());
                }
                14 => {
                    segment.tags.insert("highway".to_string(), "footway".to_string());
                    segment.tags.insert("covered".to_string(), "yes".to_string());
                }
                16 => {
                    segment.tags.insert("highway".to_string(), "platform".to_string());
                }
                17 => {
                    segment.tags.insert("highway".to_string(), "steps".to_string());
                }
                18 | 19 => {
                    segment.tags.insert("highway".to_string(), "footway".to_string());
                    segment.tags.insert("conveying".to_string(), "yes".to_string());
                }
                20 | 21 => {
                    segment.tags.insert("highway".to_string(), "elevator".to_string());
                }
                22 => { // P6 FIX: linbana (cable car)
                    segment.tags.insert("aerialway".to_string(), "cable_car".to_string());
                }
                23 => { // P6 FIX: bergbana (funicular)
                    segment.tags.insert("railway".to_string(), "furnicular".to_string());
                }
                24 | 26 => {
                    segment.tags.insert("highway".to_string(), "pedestrian".to_string());
                }
                25 => { // P6 FIX: kaj (quay)
                    segment.tags.insert("highway".to_string(), "footway".to_string());
                }
                27 => { // P6 FIX: färja (GCM ferry)
                    segment.tags.insert("route".to_string(), "ferry".to_string());
                    segment.tags.insert("foot".to_string(), "yes".to_string());
                    segment.tags.insert("motor_vehicle".to_string(), "no".to_string());
                }
                28 => {
                    segment.tags.insert("highway".to_string(), "cycleway".to_string());
                }
                29 => { // P6 FIX: cykelbana ej lämplig för gång
                    segment.tags.insert("highway".to_string(), "cycleway".to_string());
                    segment.tags.insert("foot".to_string(), "no".to_string());
                }
                _ => {
                    // Default based on network type
                    if net_type == 2 {
                        segment.tags.insert("highway".to_string(), "cycleway".to_string());
                    } else {
                        segment.tags.insert("highway".to_string(), "footway".to_string());
                    }
                }
            }
        } else {
            // No GCM type, use default based on network type
            if net_type == 2 {
                segment.tags.insert("highway".to_string(), "cycleway".to_string());
            } else {
                segment.tags.insert("highway".to_string(), "footway".to_string());
            }
        }

        // P12 FIX: Swap cycleway to footway if footway network (Python lines 577-585)
        if net_type == 4 {
            if let Some(hw) = segment.tags.get("highway").cloned() {
                if hw == "cycleway" {
                    segment.tags.insert("highway".to_string(), "footway".to_string());
                    // Move cycleway sub-tag to footway sub-tag
                    if let Some(sub) = segment.tags.remove("cycleway") {
                        segment.tags.insert("footway".to_string(), sub);
                    }
                }
            }
        }

        // P11 FIX: Cycleway/footway name logic (Python lines 587-607)
        if let Some(name) = segment.properties.get("Namn_130") {
            let name_str = name.as_string();
            let name_str = name_str.trim();
            if !name_str.is_empty() && name_str != "NA" {
                let highway = segment.tags.get("highway").map(|s| s.as_str()).unwrap_or("");
                let name_lower = name_str.to_lowercase();
                // Python: include name if pedestrian, or name contains stig/gång/park,
                // or name is not a motor vehicle street name
                if highway == "pedestrian"
                    || name_lower.contains("stig")
                    || name_lower.contains("gång")
                    || name_lower.contains("park")
                    || !street_names.contains(name_str)
                {
                    segment.tags.insert("name".to_string(), name_str.to_string());
                }
            }
        }

        // GCM-belyst → lit=yes (Python line 598-599)
        if segment.properties.get("GCM_belyst").and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
            if segment.tags.contains_key("highway") {
                segment.tags.insert("lit".to_string(), "yes".to_string());
            }
        }

        // Cycleway route name (Python lines 602-607)
        if let Some(cykel_namn) = segment.properties.get("Namn_457") {
            let s = cykel_namn.as_string();
            let s = s.trim();
            if !s.is_empty() && s != "NA" {
                if segment.tags.get("highway").map(|s| s.as_str()) == Some("cycleway") {
                    segment.tags.insert("cycleway:name".to_string(), s.to_string());
                }
            }
        }

        // Bridge name for cycleways (Python lines 609-617)
        if segment.tags.contains_key("bridge") {
            if let Some(namn_132) = segment.properties.get("Namn_132") {
                let s = namn_132.as_string();
                if !s.is_empty() && s.contains("bron") {
                    segment.tags.insert("bridge:name".to_string(), s.trim().to_string());
                }
            }
            if let Some(namn_193) = segment.properties.get("Namn_193") {
                let s = namn_193.as_string();
                if !s.is_empty() {
                    segment.tags.insert("description".to_string(), s.trim().to_string());
                }
            }
        }

        return;  // Exit early - cycleways/footways handled separately
    }

    // STEP 2: Check Vägkategori/Kategori (Kateg_380) for motor vehicle roads
    // NOTE: No early return here — motorway/motorroad override comes after (in map_motorway_override)
    if let Some(kateg) = segment.properties.get("Kateg_380").and_then(|v| v.as_i64()) {
        match kateg {
            1 => { // E road
                segment.tags.insert("highway".to_string(), "trunk".to_string());
                return;
            }
            2 => { // National road
                segment.tags.insert("highway".to_string(), "trunk".to_string());
                return;
            }
            3 => { // Primary county road
                segment.tags.insert("highway".to_string(), "primary".to_string());
                return;
            }
            4 => { // Other county road
                segment.tags.insert("highway".to_string(), "secondary".to_string());
                return;
            }
            _ => {} // Fall through to other checks
        }
    }

    // STEP 3: Check pedestrian streets and living streets
    let l_gagata = segment.properties.get("L_Gagata").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    let r_gagata = segment.properties.get("R_Gagata").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    if l_gagata || r_gagata {
        segment.tags.insert("highway".to_string(), "pedestrian".to_string());
        return;
    }

    let l_gangfart = segment.properties.get("L_Gangfartsomrade").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    let r_gangfart = segment.properties.get("R_Gangfartsomrade").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    if l_gangfart || r_gangfart {
        segment.tags.insert("highway".to_string(), "living_street".to_string());
        return;
    }

    // STEP 4: Check Funktionell vägklass < 6 → tertiary
    if let Some(klass) = segment.properties.get("Klass_181").and_then(|v| v.as_i64()) {
        if klass > 0 && klass < 6 {
            segment.tags.insert("highway".to_string(), "tertiary".to_string());
            return;
        }
    }

    // STEP 5: Private road checks, service, and track classification
    let vagha = segment.properties.get("Vagha_6").and_then(|v| v.as_i64()).unwrap_or(0);
    let klass = segment.properties.get("Klass_181").and_then(|v| v.as_i64()).unwrap_or(0);
    let tillg = segment.properties.get("Tillg_169").and_then(|v| v.as_i64()).unwrap_or(0);
    let has_namn = segment.properties.get("Namn_130")
        .map(|v| {
            let s = v.as_string();
            !s.is_empty() && s != "NA"
        })
        .unwrap_or(false);
    let slitl = segment.properties.get("Slitl_152").and_then(|v| v.as_i64()).unwrap_or(0);
    let tatt = segment.properties.get("TattbebyggtOmrade").and_then(|v| v.as_i64()).unwrap_or(0);
    // P4 FIX: Check Driftbidrag statligt/Vägnr (Python line 658)
    let has_vagnr = segment.properties.get("Vagnr_10370")
        .map(|v| {
            let s = v.as_string();
            !s.is_empty() && s != "0" && s != "NA"
        })
        .unwrap_or(false);

    if vagha == 3 { // Private road owner
        // P4 FIX: Python line 655-661: klass < 8 OR Driftbidrag OR (klass==8 AND no tillg)
        if (klass > 0 && klass < 8) || has_vagnr || (klass == 8 && tillg == 0) {
            if tatt == 1 || tatt == -1 { // Urban area
                segment.tags.insert("highway".to_string(), "residential".to_string());
            } else {
                segment.tags.insert("highway".to_string(), "unclassified".to_string());
            }
            return;
        } else if tillg > 0 && !has_namn && slitl != 1 {
            // Track for inaccessible roads
            segment.tags.insert("highway".to_string(), "track".to_string());
            return;
        } else {
            segment.tags.insert("highway".to_string(), "service".to_string());
            return;
        }
    }

    // For non-private roads: check if track or service
    if tillg > 0 && !has_namn && slitl != 1 {
        segment.tags.insert("highway".to_string(), "track".to_string());
        return;
    }

    // Check if service road (functional class 9)
    if klass == 9 || (klass == 8 && tillg > 0) {
        segment.tags.insert("highway".to_string(), "service".to_string());
        return;
    }

    // STEP 6: Default to residential or unclassified (Python lines 678-680)
    if tatt == 1 || tatt == -1 {
        segment.tags.insert("highway".to_string(), "residential".to_string());
    } else {
        segment.tags.insert("highway".to_string(), "unclassified".to_string());
    }
}

/// P1 FIX: Motorway/motorroad override (Python lines 684-688)
/// Must run AFTER map_highway — overrides the category-based classification
fn map_motorway_override(segment: &mut Segment) {
    if segment.properties.get("Motorvag").and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
        segment.tags.insert("highway".to_string(), "motorway".to_string());
    } else if segment.properties.get("Motortrafikled").and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
        segment.tags.insert("motorroad".to_string(), "yes".to_string());
    }
}

/// P7 FIX: bicycle=designated for recommended cycle roads (Python line 920-921)
/// Separate function so it applies to ALL motor vehicle highways, not just default
fn map_bicycle_designated(segment: &mut Segment) {
    // Only for motor vehicle highways (not cycleways/footways)
    let net_type = segment.properties.get("Vagtr_474").and_then(|v| v.as_i64()).unwrap_or(1);
    if net_type == 2 || net_type == 4 {
        return;
    }
    if segment.properties.get("C_Rekbilvagcykeltrafik").and_then(|v| v.as_i64()) == Some(1) {
        segment.tags.insert("bicycle".to_string(), "designated".to_string());
    }
}

/// P13 FIX: Roundabout via tag_direction (Python lines 749-756)
fn map_roundabout(segment: &mut Segment) {
    let f_cirk = segment.properties.get("F_Cirkulationsplats").and_then(|v| v.as_i64());
    let b_cirk = segment.properties.get("B_Cirkulationsplats").and_then(|v| v.as_i64());
    tag_direction(
        &mut segment.tags,
        segment.oneway_direction,
        "junction",
        Some("roundabout"),
        f_cirk,
        b_cirk,
    );
}

/// Map highway links (_link suffix for ramps/slip roads)
/// 
/// Python logic (lines 690-701):
/// Highway links are recognized by:
/// - highway in [motorway, trunk, primary]
/// - FPV class is None (not on functional priority road network)
/// - Delivery quality class < 4
/// - Not a roundabout
fn map_highway_links(segment: &mut Segment) {
    // Only apply to certain highway types
    let highway = segment.tags.get("highway").map(|s| s.as_str()).unwrap_or("");
    if !matches!(highway, "motorway" | "trunk" | "primary") {
        return;
    }
    
    // Check FPV class - must be None (not on priority network)
    let fpv_class = segment.properties.get("FPV_k_309").and_then(|v| v.as_i64());
    if fpv_class.is_some() {
        return;
    }
    
    // Check delivery quality class - must be < 4
    let delivery_class = segment.properties.get("Lever_292").and_then(|v| v.as_i64());
    if let Some(dc) = delivery_class {
        if dc >= 4 {
            return;
        }
    } else {
        return; // No delivery class info
    }
    
    // Check not a roundabout
    let f_cirk = segment.properties.get("F_Cirkulationsplats").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    let b_cirk = segment.properties.get("B_Cirkulationsplats").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    if f_cirk || b_cirk {
        return;
    }
    
    // All conditions met - add _link suffix
    segment.tags.insert("highway".to_string(), format!("{}_link", highway));
}

/// Map surface type (Python lines 909-912)
///
/// P8 FIX: Python applies surface to ALL motor vehicle highways (no highway type filter).
/// Only cycleways/footways are excluded (they return early in Python's osm_tags).
fn map_surface(segment: &mut Segment) {
    // Cycleways/footways already returned in Python — they never reach this code
    let net_type = segment.properties.get("Vagtr_474").and_then(|v| v.as_i64()).unwrap_or(1);
    if net_type == 2 || net_type == 4 {
        return;  // Already handled in cycleway section
    }
    // Ferry doesn't get surface either
    if segment.tags.contains_key("route") {
        return;
    }

    if let Some(surface_code) = segment.properties.get("Slitl_152").and_then(|v| v.as_i64()) {
        let surface = match surface_code {
            1 => "paved",
            2 => "unpaved",
            _ => return,
        };
        segment.tags.insert("surface".to_string(), surface.to_string());
    }
}

/// Map maxspeed using tag_direction() (Python lines 758-770)
///
/// P2 FIX: Now uses shared tag_direction() with proper oneway semantics
fn map_maxspeed(segment: &mut Segment) {
    // Check if this is a track with 70/70 speeds (excluded in Python, lines 758-762)
    let highway = segment.tags.get("highway").map(|s| s.as_str()).unwrap_or("");
    let speed_f = segment.properties.get("F_Hogst_225").and_then(|v| v.as_i64());
    let speed_b = segment.properties.get("B_Hogst_225").and_then(|v| v.as_i64());

    if highway == "track" && speed_f == Some(70) && speed_b == Some(70) {
        return;
    }

    // Use tag_direction for maxspeed — value=None means use the property value directly
    tag_direction(
        &mut segment.tags,
        segment.oneway_direction,
        "maxspeed",
        None,  // Use property values directly (speeds)
        speed_f.filter(|&v| v > 0 && v <= 120),
        speed_b.filter(|&v| v > 0 && v <= 120),
    );
}

/// Map oneway status and set segment.oneway_direction
///
/// Python behavior (lines 514-524):
/// - B_ForbjudenFardriktning=1: backward forbidden → oneway="forward", tags oneway=yes
/// - F_ForbjudenFardriktning=1: forward forbidden → reverse geometry, oneway="backward"
///
/// CRITICAL: Must run BEFORE any directional tags (maxspeed, motor_vehicle, etc.)
/// because they all depend on segment.oneway_direction via tag_direction()
fn map_oneway(segment: &mut Segment) {
    use crate::models::hash_coord;

    // Check direction of travel restrictions (takes priority)
    // Python: if prop["Förbjuden färdriktning(B)"]: oneway = "forward"
    //         elif prop["Förbjuden färdriktning(F)"]: oneway = "backward"; reverse_segment
    let f_forbidden = segment.properties.get("F_ForbjudenFardriktning")
        .and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    let b_forbidden = segment.properties.get("B_ForbjudenFardriktning")
        .and_then(|v| v.as_i64()).unwrap_or(0) > 0;

    if b_forbidden && !f_forbidden {
        // Backward direction forbidden → traffic flows forward → geometry correct
        segment.tags.insert("oneway".to_string(), "yes".to_string());
        segment.oneway_direction = OnewayDirection::Forward;
    } else if f_forbidden && !b_forbidden {
        // Forward direction forbidden → reverse geometry, traffic flows in original "backward" direction
        segment.geometry.0.reverse();
        segment.start_node = hash_coord(segment.geometry.0.first().unwrap());
        segment.end_node = hash_coord(segment.geometry.0.last().unwrap());
        segment.tags.insert("oneway".to_string(), "yes".to_string());
        segment.oneway_direction = OnewayDirection::Backward;
    }

    // Check Korfa_524 (Körfältsanvändning) only if oneway not already set
    if segment.oneway_direction == OnewayDirection::None {
        if let Some(korfa) = segment.properties.get("Korfa_524").and_then(|v| v.as_i64()) {
            if korfa == 1 {
                segment.tags.insert("oneway".to_string(), "yes".to_string());
                segment.oneway_direction = OnewayDirection::Forward;
            }
        }
    }
}

/// Port of Python tag_direction() helper (lines 1040-1072)
///
/// Handles forward/backward directional tag application with oneway awareness.
/// Parameters:
/// - segment: the road segment (for oneway_direction)
/// - tag: the OSM tag key (e.g., "maxspeed", "motor_vehicle")
/// - value: if Some, use this value instead of prop values when prop == 1
/// - prop_forward: forward property value (None means absent)
/// - prop_backward: backward property value (None means absent)
fn tag_direction(
    tags: &mut FxHashMap<String, String>,
    oneway: OnewayDirection,
    tag: &str,
    value: Option<&str>,
    prop_forward: Option<i64>,
    prop_backward: Option<i64>,
) {
    // Python: if prop_forward or prop_backward:
    let pf = prop_forward.filter(|&v| v != 0);
    let pb = prop_backward.filter(|&v| v != 0);

    if pf.is_none() && pb.is_none() {
        return;
    }

    // Convert property values to tag values
    // Python: if value and prop_forward == 1: prop_forward = value
    let val_f = pf.map(|v| {
        if let Some(fixed_val) = value {
            if v == 1 { fixed_val.to_string() } else { v.to_string() }
        } else {
            v.to_string()
        }
    });
    let val_b = pb.map(|v| {
        if let Some(fixed_val) = value {
            if v == 1 { fixed_val.to_string() } else { v.to_string() }
        } else {
            v.to_string()
        }
    });

    // Python: if prop_forward == prop_backward: tags[tag] = str(prop_forward)
    if val_f.is_some() && val_b.is_some() && val_f == val_b {
        tags.insert(tag.to_string(), val_f.unwrap());
        return;
    }

    // Forward direction
    if let Some(vf) = val_f {
        match oneway {
            OnewayDirection::Backward => {
                // Python: if prop_forward and oneway != "backward" — skip
                // oneway is backward, so skip forward property
            }
            OnewayDirection::Forward => {
                // Python: if oneway == "forward": tags[tag] = str(prop_forward)
                tags.insert(tag.to_string(), vf);
            }
            OnewayDirection::None => {
                // Python: tags[tag + ":forward"] = str(prop_forward)
                tags.insert(format!("{}:forward", tag), vf);
            }
        }
    }

    // Backward direction
    if let Some(vb) = val_b {
        match oneway {
            OnewayDirection::Forward => {
                // Python: if prop_backward and oneway != "forward" — skip
                // oneway is forward, so skip backward property
            }
            OnewayDirection::Backward => {
                // Python: if oneway == "backward": tags[tag] = str(prop_backward)
                tags.insert(tag.to_string(), vb);
            }
            OnewayDirection::None => {
                // Python: tags[tag + ":backward"] = str(prop_backward)
                tags.insert(format!("{}:backward", tag), vb);
            }
        }
    }
}

/// Build set of motor vehicle street names (Python lines 1190-1203)
/// Used to determine if a cycleway name is shared with a motor road
fn build_street_names(segments: &[Segment]) -> std::collections::HashSet<String> {
    let mut names = std::collections::HashSet::new();
    for segment in segments {
        let net_type = segment.properties.get("Vagtr_474").and_then(|v| v.as_i64()).unwrap_or(0);
        if net_type == 1 {
            if let Some(name) = segment.properties.get("Namn_130") {
                let s = name.as_string();
                let s = s.trim();
                if !s.is_empty() && s != "NA" {
                    names.insert(s.to_string());
                }
            }
        }
    }
    names
}

/// Map bridge and tunnel tags
/// 
/// Python logic (lines 486-510):
/// - Construction 1,4: bridge (over bridge or middle layer)
/// - Construction 2,3: tunnel IF bridge tag is "tunnel" or no bridge ID + conditions
fn map_bridge_tunnel(segment: &mut Segment, bridges: &FxHashMap<String, Bridge>) {
    // Check for bridge/tunnel by construction type (Konst_190)
    if let Some(constr_prop) = segment.properties.get("Konst_190") {
        let construction = constr_prop.as_i64().unwrap_or(0);
        let bridge_margin = 50.0;
        
        match construction {
            1 | 4 => {
                // Over bridge or middle layer → always bridge (lines 486-495)
                segment.tags.insert("bridge".to_string(), "yes".to_string());
                
                // Add layer tag if we have bridge ID
                if let Some(id_prop) = segment.properties.get("Ident_191") {
                    let bridge_id = id_prop.as_string();
                    if let Some(bridge) = bridges.get(&bridge_id) {
                        segment.tags.insert("layer".to_string(), bridge.layer.clone());
                    }
                }
            }
            2 | 3 => {
                // Under bridge - check if should be marked as tunnel (lines 497-510)
                let net_type = segment.properties.get("Vagtr_474").and_then(|v| v.as_i64()).unwrap_or(0);
                let is_long = segment.shape_length > bridge_margin;
                
                // Check if we have a bridge ID and what its tag is
                let bridge_tag = segment.properties.get("Ident_191")
                    .and_then(|id| bridges.get(&id.as_string()))
                    .map(|b| b.tag.as_str());
                
                // Python logic:
                // - Construction 3 → tunnel (cycle path under bridge)
                // - Construction 2 AND (bridge tag is "tunnel" OR no bridge ID AND (net_type != 1 OR is_long))
                let should_be_tunnel = if construction == 3 {
                    true  // Always tunnel for construction 3
                } else {
                    // Construction 2
                    match bridge_tag {
                        Some("tunnel") => true,
                        None => net_type != 1 || is_long,  // No bridge ID
                        _ => false,  // bridge tag is "bridge"
                    }
                };
                
                if should_be_tunnel {
                    segment.tags.insert("tunnel".to_string(), "yes".to_string());
                    segment.tags.insert("layer".to_string(), "-1".to_string());
                }
            }
            _ => {}
        }
    }
}

/// Map name for motor vehicle highways (Python lines 929-937)
///
/// P9 FIX: Python applies names to ALL motor vehicle highways (not restricted to specific types)
/// Cycleways/footways already handled in map_highway cycleway section.
/// Uses Namn_130 with Namn_132 fallback.
fn map_name(segment: &mut Segment) {
    // Cycleways/footways already got their names in map_highway
    let net_type = segment.properties.get("Vagtr_474").and_then(|v| v.as_i64()).unwrap_or(1);
    if net_type == 2 || net_type == 4 {
        return;
    }
    // Ferry names handled in map_highway ferry section
    if segment.tags.contains_key("route") {
        return;
    }

    // Skip if roundabout (Python lines 931-932)
    let f_cirk = segment.properties.get("F_Cirkulationsplats").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    let b_cirk = segment.properties.get("B_Cirkulationsplats").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
    if f_cirk || b_cirk {
        return;
    }

    // Python: Gatunamn/Namn first, then Övrigt vägnamn/Namn fallback (lines 934-937)
    let name_str = segment.properties.get("Namn_130")
        .map(|v| v.as_string())
        .filter(|s| {
            let t = s.trim();
            !t.is_empty() && t != "NA" && t != "-1"
        })
        .or_else(|| {
            segment.properties.get("Namn_132")
                .map(|v| v.as_string())
                .filter(|s| {
                    let t = s.trim();
                    !t.is_empty() && t != "NA" && t != "-1"
                })
        });

    if let Some(name) = name_str {
        segment.tags.insert("name".to_string(), name.trim().to_string());
    }
}

/// Map reference number
/// 
/// Python logic (lines 732-745):
/// - Category 1 (E road): "E " + Huvudnummer
/// - Category 2,3 (Trunk, Primary): Huvudnummer
/// - Category 4 (Secondary): County letter + " " + Huvudnummer
fn map_ref(segment: &mut Segment) {
    let kateg = segment.properties.get("Kateg_380").and_then(|v| v.as_i64());
    let huvnr = segment.properties.get("Huvnr_556_1");
    
    if let (Some(kat), Some(huvnr_val)) = (kateg, huvnr) {
        let huvnr_str = huvnr_val.as_string();
        if huvnr_str.is_empty() || huvnr_str == "0" || huvnr_str == "-1" {
            return;
        }
        
        match kat {
            1 => {
                // E road: "E " + number
                segment.tags.insert("ref".to_string(), format!("E {}", huvnr_str));
            }
            2 | 3 => {
                // National/Primary county road: just the number
                segment.tags.insert("ref".to_string(), huvnr_str);
            }
            4 => {
                // Secondary county road: county letter + number
                if let Some(kommun) = segment.properties.get("Kommu_141").and_then(|v| v.as_i64()) {
                    let county_num = kommun / 100;
                    let county_codes = COUNTY_CODES.get_or_init(init_county_codes);
                    
                    if let Some(&county_letter) = county_codes.get(&county_num) {
                        segment.tags.insert("ref".to_string(), format!("{} {}", county_letter, huvnr_str));
                    }
                }
            }
            _ => {}
        }
    }
}

/// Map number of lanes and PSV lanes (Python lines 873-905)
///
/// P5 FIX: Python uses Korfa_497 (Antal körfält/Körfältsantal) for lane count,
/// NOT Korfa_524. Only includes lanes > 2 (or > 1 on oneway).
/// PSV lanes use tag_direction via F/B_Korfa_517.
fn map_lanes(segment: &mut Segment) {
    // P5 FIX: Use Korfa_497 for lane count (Python line 873-878)
    if let Some(lane_count) = segment.properties.get("Korfa_497").and_then(|v| v.as_i64()) {
        let is_oneway = segment.oneway_direction != OnewayDirection::None;
        // Python: only tag if > 2, or oneway and > 1
        if lane_count > 2 || (is_oneway && lane_count > 1) {
            segment.tags.insert("lanes".to_string(), lane_count.to_string());
        }
    }

    // PSV lanes via tag_direction (Python lines 880-905)
    let f_psv = segment.properties.get("F_Korfa_517").and_then(|v| v.as_i64()).unwrap_or(0);
    let b_psv = segment.properties.get("B_Korfa_517").and_then(|v| v.as_i64()).unwrap_or(0);

    // PSV=yes + motor_vehicle=no for bus-only lanes (value 2)
    let f_bus = if f_psv == 2 { Some(1i64) } else { None };
    let b_bus = if b_psv == 2 { Some(1i64) } else { None };
    tag_direction(&mut segment.tags, segment.oneway_direction, "psv", Some("yes"), f_bus, b_bus);
    tag_direction(&mut segment.tags, segment.oneway_direction, "motor_vehicle", Some("no"), f_bus, b_bus);

    // lanes:psv=1 for dedicated PSV lanes (value 1)
    let f_psv_lane = if f_psv == 1 { Some(1i64) } else { None };
    let b_psv_lane = if b_psv == 1 { Some(1i64) } else { None };
    tag_direction(&mut segment.tags, segment.oneway_direction, "lanes:psv", Some("1"), f_psv_lane, b_psv_lane);
}

/// Map width (Python line 914-915)
///
/// P8 FIX: Python applies width to ALL motor vehicle highways (no type filter)
fn map_width(segment: &mut Segment) {
    // Cycleways/footways already returned in Python
    let net_type = segment.properties.get("Vagtr_474").and_then(|v| v.as_i64()).unwrap_or(1);
    if net_type == 2 || net_type == 4 {
        return;
    }
    if segment.tags.contains_key("route") {
        return;  // No width for ferries
    }

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
/// 
/// UPDATED: Better implementation based on Python logic
fn tag_isolated_tracks(segments: &mut [Segment]) {
    for segment in segments.iter_mut() {
        if let Some(highway) = segment.tags.get("highway") {
            if highway == "service" {
                // Python logic for isolated tracks (lines 655-674):
                // - Tillgänglighet/Tillgänglighetsklass exists (> 0)
                // - No street name
                // - Unpaved surface
                let tillg = segment.properties.get("Tillg_169").and_then(|v| v.as_i64()).unwrap_or(0);
                let has_namn = segment.properties.get("Namn_130")
                    .map(|v| {
                        let s = v.as_string();
                        !s.is_empty() && s != "NA"
                    })
                    .unwrap_or(false);
                let slitl = segment.properties.get("Slitl_152").and_then(|v| v.as_i64()).unwrap_or(0);
                
                if tillg > 0 && !has_namn && slitl != 1 {
                    segment.tags.insert("highway".to_string(), "track".to_string());
                }
            }
        }
    }
}

/// Tag urban vs rural streets
fn tag_urban_vs_rural(_segments: &mut [Segment]) {
    // TODO: Implement based on TätbebyggtOmrade attribute
}

/// Map priority_road tag
/// Set for roads with official road numbers
fn map_priority_road(segment: &mut Segment) {
    // Check if road has an official number (Vägnummer/Huvudnummer = Huvnr_556_1)
    if let Some(huvnr) = segment.properties.get("Huvnr_556_1") {
        let huvnr_str = huvnr.as_string();
        if !huvnr_str.is_empty() && huvnr_str != "0" {
            segment.tags.insert("priority_road".to_string(), "designated".to_string());
        }
    }
}

/// Map lit tag (street lighting)
/// GCM-belyst = 1 means lit
fn map_lit(segment: &mut Segment) {
    if let Some(belyst) = segment.properties.get("GCM_belyst") {
        if belyst.as_i64() == Some(1) {
            segment.tags.insert("lit".to_string(), "yes".to_string());
        }
    }
}

/// Motor vehicle access restriction — Python lines 772-779
/// tag_direction(tags, "motor_vehicle", "no", F_ForbudTrafik, B_ForbudTrafik, oneway)
fn map_motor_vehicle_access(segment: &mut Segment) {
    let f = segment.properties.get("F_ForbudTrafik").and_then(|v| v.as_i64());
    let b = segment.properties.get("B_ForbudTrafik").and_then(|v| v.as_i64());
    tag_direction(&mut segment.tags, segment.oneway_direction, "motor_vehicle", Some("no"), f, b);
}

/// Map hazmat tags (Python lines 846-860)
///
/// Now uses tag_direction for proper oneway handling
fn map_hazmat(segment: &mut Segment) {
    // Check if recommended for hazardous goods (Python line 847-848)
    if segment.properties.get("Rekom_185").and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
        segment.tags.insert("hazmat".to_string(), "designated".to_string());
    }

    // Check for restrictions (Python lines 850-860)
    let hazmat_f = segment.properties.get("F_Beskr_124").and_then(|v| v.as_i64()).unwrap_or(0);
    let hazmat_b = segment.properties.get("B_Beskr_124").and_then(|v| v.as_i64()).unwrap_or(0);
    // Python converts truthy to 1 for tag_direction: `1 if hazmat_f else None`
    let hf = if hazmat_f > 0 { Some(1i64) } else { None };
    let hb = if hazmat_b > 0 { Some(1i64) } else { None };
    tag_direction(&mut segment.tags, segment.oneway_direction, "hazmat", Some("no"), hf, hb);
}

/// Map vehicle size and weight restrictions
/// 
/// UPDATED: Added full vehicle type restrictions from "Förbud mot trafik"
/// Python lines 781-845
fn map_vehicle_restrictions(segment: &mut Segment) {
    // Max height (Höjdhinder upp till 4,5 m/Fri höjd)
    if let Some(height) = segment.properties.get("Fri_h_143").and_then(|v| v.as_f64()) {
        if height > 0.0 && height < 10.0 {
            segment.tags.insert("maxheight".to_string(), format!("{:.1}", height));
        }
    }
    
    // Max length (Begränsad fordonslängd)
    if let Some(length) = segment.properties.get("Hogst_46").and_then(|v| v.as_f64()) {
        if length > 0.0 && length < 50.0 {
            segment.tags.insert("maxlength".to_string(), format!("{:.1}", length));
        }
    }
    
    // Max width (Begränsad fordonsbredd)
    if let Some(width) = segment.properties.get("Hogst_36").and_then(|v| v.as_f64()) {
        if width > 0.0 && width < 10.0 {
            segment.tags.insert("maxwidth".to_string(), format!("{:.1}", width));
        }
    }
    
    // Max axle load (Begränsat axel-boggitryck)
    if let Some(axleload) = segment.properties.get("Hogst_55_30").and_then(|v| v.as_f64()) {
        if axleload > 0.0 && axleload < 100.0 {
            segment.tags.insert("maxaxleload".to_string(), format!("{:.1}", axleload));
        }
    }
    
    // Max weight - directional (Begränsad bruttovikt)
    let weight_f = segment.properties.get("F_Hogst_24").and_then(|v| v.as_f64());
    let weight_b = segment.properties.get("B_Hogst_24").and_then(|v| v.as_f64());
    
    let wf = weight_f.filter(|&v| v > 0.0 && v < 100.0);
    let wb = weight_b.filter(|&v| v > 0.0 && v < 100.0);
    
    if let (Some(wf_val), Some(wb_val)) = (wf, wb) {
        if (wf_val - wb_val).abs() < 0.1 {
            segment.tags.insert("maxweight".to_string(), format!("{:.1}", wf_val));
        } else {
            segment.tags.insert("maxweight:forward".to_string(), format!("{:.1}", wf_val));
            segment.tags.insert("maxweight:backward".to_string(), format!("{:.1}", wb_val));
        }
    } else if let Some(wf_val) = wf {
        segment.tags.insert("maxweight:forward".to_string(), format!("{:.1}", wf_val));
    } else if let Some(wb_val) = wb {
        segment.tags.insert("maxweight:backward".to_string(), format!("{:.1}", wb_val));
    }
    
    // HGV restriction for forest roads (Framkomlighetsklass = 4)
    if let Some(framk) = segment.properties.get("Framk_161").and_then(|v| v.as_i64()) {
        if framk == 4 {
            segment.tags.insert("hgv".to_string(), "no".to_string());
        }
    }
    
    // Bridge weight limit fallback (Python lines 994-998)
    if segment.tags.contains_key("bridge") && !segment.tags.contains_key("maxweight") {
        if let Some(barig) = segment.properties.get("Barig_64").and_then(|v| v.as_i64()) {
            let maxweight = match barig {
                1 => "64.0",  // BK1
                2 => "51.4",  // BK2
                3 => "37.5",  // BK3
                4 => "74.0",  // BK4
                5 => "74.0",  // BK4 särskilda villkor
                _ => "",
            };
            if !maxweight.is_empty() {
                segment.tags.insert("maxweight".to_string(), maxweight.to_string());
            }
        }
    }
    
    // Vehicle type restrictions from "Förbud mot trafik/Gäller fordon"
    // Python lines 781-845 — uses manual direction logic, not tag_direction()
    let vehicle_type_map = VEHICLE_TYPE_MAP.get_or_init(init_vehicle_type_map);
    let oneway = segment.oneway_direction;

    // Collect restrictions to avoid borrow issues with segment.properties + segment.tags
    struct VehicleRestriction {
        is_forward: bool,
        osm_tag: &'static str,
        weight_limit: Option<f64>,
    }
    let mut restrictions: Vec<VehicleRestriction> = Vec::new();

    for is_forward in [true, false] {
        let gallar_key = if is_forward { "F_Gallar_135" } else { "B_Gallar_135" };
        let total_key = if is_forward { "F_Total_136" } else { "B_Total_136" };

        if let Some(vehicle_type) = segment.properties.get(gallar_key).and_then(|v| v.as_i64()) {
            if let Some(&osm_tag) = vehicle_type_map.get(&vehicle_type) {
                let weight_limit = segment.properties.get(total_key)
                    .and_then(|v| v.as_f64())
                    .filter(|&w| w > 0.0);
                restrictions.push(VehicleRestriction { is_forward, osm_tag, weight_limit });
            }
        }
    }

    // Apply restrictions — exact port of Python lines 802-844
    for r in &restrictions {
        if let Some(weight) = r.weight_limit {
            if r.osm_tag == "hgv" {
                // Python line 812: maxweight:(F)/(B) — use :forward/:backward
                let suffix = if r.is_forward { ":forward" } else { ":backward" };
                segment.tags.insert(format!("maxweight{}", suffix), format!("{}", weight));
            } else {
                // Python lines 817-830: conditional restriction with direction handling
                let tag_value = format!("no @ (weight>{})", weight);
                if r.is_forward {
                    // Python line 820: if oneway != "backward"
                    if oneway != OnewayDirection::Backward {
                        if oneway == OnewayDirection::Forward {
                            // Python line 822: tags[tag_key] = tag_value (no direction suffix)
                            segment.tags.insert(format!("{}:conditional", r.osm_tag), tag_value);
                        } else {
                            // Python line 824
                            segment.tags.insert(format!("{}:forward:conditional", r.osm_tag), tag_value);
                        }
                    }
                } else {
                    // Python line 826: if oneway != "forward"
                    if oneway != OnewayDirection::Forward {
                        if oneway == OnewayDirection::Backward {
                            // Python line 828
                            segment.tags.insert(format!("{}:conditional", r.osm_tag), tag_value);
                        } else {
                            // Python line 830
                            segment.tags.insert(format!("{}:backward:conditional", r.osm_tag), tag_value);
                        }
                    }
                }
            }
        } else {
            // Python lines 831-844: simple vehicle restriction, no weight
            if r.is_forward {
                // Python line 834: if oneway != "backward"
                if oneway != OnewayDirection::Backward {
                    if oneway == OnewayDirection::Forward {
                        // Python line 836: tags[osm_tag] = "no"
                        segment.tags.insert(r.osm_tag.to_string(), "no".to_string());
                    } else {
                        // Python line 838
                        segment.tags.insert(format!("{}:forward", r.osm_tag), "no".to_string());
                    }
                }
            } else {
                // Python line 840: if oneway != "forward"
                if oneway != OnewayDirection::Forward {
                    if oneway == OnewayDirection::Backward {
                        // Python line 842: tags[osm_tag] = "no"
                        segment.tags.insert(r.osm_tag.to_string(), "no".to_string());
                    } else {
                        // Python line 844
                        segment.tags.insert(format!("{}:backward", r.osm_tag), "no".to_string());
                    }
                }
            }
        }
    }
}

/// Map overtaking restrictions — Python lines 862-869
/// Uses tag_direction() for proper oneway handling
fn map_overtaking_restrictions(segment: &mut Segment) {
    let f = segment.properties.get("F_Omkorningsforbud").and_then(|v| v.as_i64());
    let b = segment.properties.get("B_Omkorningsforbud").and_then(|v| v.as_i64());
    tag_direction(&mut segment.tags, segment.oneway_direction, "overtaking", Some("no"), f, b);
}

/// Map low emission zone
fn map_low_emission_zone(segment: &mut Segment) {
    if let Some(miljozon) = segment.properties.get("Miljozon").and_then(|v| v.as_i64()) {
        if miljozon == 1 {
            segment.tags.insert("low_emission_zone".to_string(), "yes".to_string());
        } else if miljozon > 1 {
            segment.tags.insert("low_emission_zone".to_string(), miljozon.to_string());
        }
    }
}

/// Map bridge and tunnel names — Python lines 939-948
fn map_bridge_tunnel_names(segment: &mut Segment) {
    let is_bridge = segment.tags.contains_key("bridge");
    let is_tunnel = segment.tags.contains_key("tunnel");

    // P10 FIX: Check Namn_132 (Övrigt vägnamn/Namn) for bridge/tunnel name substrings
    // Python lines 939-943
    if let Some(namn_132) = segment.properties.get("Namn_132") {
        let name_str = namn_132.as_string();
        let name_str = name_str.trim();
        if !name_str.is_empty() {
            if is_tunnel && name_str.to_lowercase().contains("tunneln") {
                segment.tags.insert("tunnel:name".to_string(), name_str.to_string());
            } else if is_bridge && name_str.to_lowercase().contains("bron") {
                segment.tags.insert("bridge:name".to_string(), name_str.to_string());
            }
        }
    }

    if !is_bridge && !is_tunnel {
        return;
    }

    // Bridge/tunnel description from Namn_193 (Bro och tunnel/Namn) — Python lines 945-948
    if let Some(name) = segment.properties.get("Namn_193") {
        let name_str = name.as_string();
        let name_str = name_str.trim();
        if !name_str.is_empty() {
            segment.tags.insert("description".to_string(), name_str.to_string());
        }
    }
}

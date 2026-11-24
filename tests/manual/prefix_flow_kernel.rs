//! Experimental flat-array prefix-volume propagation for the manual benchmark.
//! Build: rustc -O --crate-type cdylib prefix_flow_kernel.rs -o PATH_TO_DLL

use std::slice;

#[no_mangle]
pub unsafe extern "C" fn propagate_prefix_flows(
    parents_ptr: *const u32,
    tokens_ptr: *const u64,
    node_count: usize,
    leaves_ptr: *const u32,
    section_count: usize,
    section_ids_ptr: *const u32,
    volumes_ptr: *const f64,
    allocated_section_count: usize,
    token_volumes_ptr: *mut f64,
    token_count: usize,
) -> i32 {
    if parents_ptr.is_null() || tokens_ptr.is_null() || leaves_ptr.is_null()
        || section_ids_ptr.is_null() || volumes_ptr.is_null() || token_volumes_ptr.is_null()
        || node_count == 0
    {
        return 1;
    }
    let parents = slice::from_raw_parts(parents_ptr, node_count);
    let tokens = slice::from_raw_parts(tokens_ptr, node_count);
    let leaves = slice::from_raw_parts(leaves_ptr, section_count);
    let section_ids = slice::from_raw_parts(section_ids_ptr, allocated_section_count);
    let volumes = slice::from_raw_parts(volumes_ptr, allocated_section_count);
    let token_volumes = slice::from_raw_parts_mut(token_volumes_ptr, token_count);
    let mut nodes = vec![0.0_f64; node_count];

    for (&section, &volume) in section_ids.iter().zip(volumes) {
        let Some(&leaf) = leaves.get(section as usize) else { return 2 };
        let Some(node) = nodes.get_mut(leaf as usize) else { return 3 };
        *node += volume;
    }
    // IDs are assigned by increasing depth, so every parent precedes its
    // children. Processing in reverse performs one pass over the graph.
    for node in (1..node_count).rev() {
        let parent = parents[node] as usize;
        let token = tokens[node] as usize;
        if parent >= node || token >= token_count { return 4; }
        let volume = nodes[node];
        nodes[parent] += volume;
        token_volumes[token] += volume;
    }
    0
}

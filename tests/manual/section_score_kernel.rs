//! Manual experiment: sum already-scored atoms into sections and plans.
//! Build with: pixi run rustc -O --crate-type cdylib tests/manual/section_score_kernel.rs -o <path>
use std::slice;

#[no_mangle]
pub unsafe extern "C" fn score_sections_and_plans(
    selected_sections: *const u32, section_count: usize,
    offsets: *const i64, tokens: *const i64, token_utility: *const f64,
    token_feasible: *const u8, token_known: *const u8,
    section_utility: *mut f64, section_feasible: *mut u8, section_known: *mut u8,
    selected_plans: *const u32, plan_count: usize,
    plan_offsets: *const i64, plan_sections: *const u32,
    plan_utility: *mut f64, plan_feasible: *mut u8,
) {
    // Inputs are validated and indexed by the Python benchmark. Each list of
    // offsets describes an ordered sequence of atom or section references.
    for &section in slice::from_raw_parts(selected_sections, section_count) {
        let i = section as usize;
        let mut utility = 0.0;
        let mut feasible = 1u8;
        let mut known = 1u8;
        for pos in *offsets.add(i)..*offsets.add(i + 1) {
            let token = *tokens.add(pos as usize) as usize;
            utility += *token_utility.add(token);
            feasible &= *token_feasible.add(token);
            known &= *token_known.add(token);
        }
        *section_utility.add(i) = utility;
        *section_feasible.add(i) = feasible;
        *section_known.add(i) = known;
    }
    for &plan in slice::from_raw_parts(selected_plans, plan_count) {
        let i = plan as usize;
        let mut utility = 0.0;
        let mut feasible = 1u8;
        for pos in *plan_offsets.add(i)..*plan_offsets.add(i + 1) {
            let section = *plan_sections.add(pos as usize) as usize;
            utility += *section_utility.add(section);
            feasible &= *section_feasible.add(section);
        }
        *plan_utility.add(i) = utility;
        *plan_feasible.add(i) = feasible;
    }
}

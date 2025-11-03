pub fn bind_to_core(preferred_index: usize) {
    if let Some(cores) = core_affinity::get_core_ids() {
        if !cores.is_empty() {
            let target = cores
                .iter()
                .find(|c| c.id == preferred_index)
                .cloned()
                .or_else(|| cores.get(preferred_index).cloned());
            if let Some(core_id) = target {
                core_affinity::set_for_current(core_id);
            }
        }
    }
}

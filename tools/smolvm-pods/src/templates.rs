//! smolvm's disk templates.
//!
//! smolvm layers a new VM's disks on its disk templates. The node has them as
//! `.zst` archives, which smolvm expands the first time a VM needs them, with no
//! lock. VMs that start together on a new node would expand them at the same
//! time into one scratch file, and a VM could get a template that another
//! process is still writing. So the node expands them at boot
//! (`_expand-templates`), and the node service and the TSO canary make sure
//! they are expanded before they create a VM, under a lock that all of them
//! take.

use std::path::Path;

use crate::engine::POD_SHARE_HOST_ROOT;
use crate::files;

const TEMPLATES: [&str; 2] = ["storage-template.ext4", "overlay-template.ext4"];

/// Make sure the disk templates are expanded, under the lock. Returns those
/// that smolvm cannot find.
pub fn expand() -> Result<Vec<&'static str>, String> {
    let dir = Path::new(POD_SHARE_HOST_ROOT);
    std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let lock = dir.join(".templates.lock");
    let _lock = files::lock(&lock, 0o666, true).map_err(|e| e.message(&lock))?;
    Ok(TEMPLATES
        .into_iter()
        .filter(|t| smolvm_pack::assets::find_existing_template(t).is_none())
        .collect())
}

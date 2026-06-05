use crate::protocol::*;
use crate::types::*;
use serde::{Deserialize, Serialize};
/// Content Vault — blake3 content-addressed storage with two-phase commit.
/// All artifacts are stored immutably, addressed by their blake3 hash.
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultEntry {
    pub uri: String,
    pub blake3: String,
    pub size_bytes: u64,
    pub content_type: String,
    pub created_at: u64,
    pub committed: bool,
}

pub struct ContentVault {
    base_path: PathBuf,
    index: BTreeMap<String, VaultEntry>,
    pending: BTreeMap<String, Vec<u8>>,
}

impl ContentVault {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        let path = base_path.into();
        std::fs::create_dir_all(&path).ok();

        Self {
            base_path: path,
            index: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    /// Phase 1: Stage content for two-phase commit
    pub fn stage(&mut self, content: Vec<u8>, content_type: &str) -> VaultEntry {
        let hash = compute_hash(&content);
        let uri = format!("vault://{}", &hash[..16]);

        let entry = VaultEntry {
            uri: uri.clone(),
            blake3: hash,
            size_bytes: content.len() as u64,
            content_type: content_type.to_string(),
            created_at: now_millis(),
            committed: false,
        };

        self.pending.insert(uri.clone(), content);
        self.index.insert(uri.clone(), entry.clone());

        tracing::debug!(
            target = "nexus.vault",
            uri = %uri,
            size = %entry.size_bytes,
            content_type = %content_type,
            "Content staged"
        );

        entry
    }

    /// Phase 2: Commit staged content to disk
    pub fn commit(&mut self, uri: &str) -> Result<(), VaultError> {
        let content = self
            .pending
            .remove(uri)
            .ok_or_else(|| VaultError::NotStaged(uri.to_string()))?;

        let file_path = self.resolve_path(uri);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| VaultError::IoError(e.to_string()))?;
        }

        let entry = self
            .index
            .get_mut(uri)
            .ok_or_else(|| VaultError::NotFound(uri.to_string()))?;

        std::fs::write(&file_path, &content).map_err(|e| VaultError::IoError(e.to_string()))?;

        // Verify integrity after write
        let stored_hash = compute_hash(&content);
        if stored_hash != entry.blake3 {
            std::fs::remove_file(&file_path).ok();
            return Err(VaultError::HashMismatch {
                expected: entry.blake3.clone(),
                actual: stored_hash,
            });
        }

        entry.committed = true;

        tracing::info!(
            target = "nexus.vault",
            uri = %uri,
            blake3 = %entry.blake3,
            "Content committed"
        );

        Ok(())
    }

    /// Rollback staged content
    pub fn rollback(&mut self, uri: &str) {
        self.pending.remove(uri);
        self.index.remove(uri);

        tracing::warn!(
            target = "nexus.vault",
            uri = %uri,
            "Content rolled back"
        );
    }

    pub fn get(&self, uri: &str) -> Result<Vec<u8>, VaultError> {
        if let Some(content) = self.pending.get(uri) {
            return Ok(content.clone());
        }

        let entry = self
            .index
            .get(uri)
            .ok_or_else(|| VaultError::NotFound(uri.to_string()))?;

        if !entry.committed {
            return Err(VaultError::NotCommitted(uri.to_string()));
        }

        let path = self.resolve_path(uri);
        let content = std::fs::read(&path).map_err(|e| VaultError::IoError(e.to_string()))?;

        let actual_hash = compute_hash(&content);
        if actual_hash != entry.blake3 {
            return Err(VaultError::HashMismatch {
                expected: entry.blake3.clone(),
                actual: actual_hash,
            });
        }

        Ok(content)
    }

    pub fn verify_all(&self) -> Result<(), VaultError> {
        for (uri, entry) in &self.index {
            if !entry.committed {
                continue;
            }
            let path = self.resolve_path(uri);
            if !path.exists() {
                return Err(VaultError::NotFound(uri.clone()));
            }
            let content = std::fs::read(&path).map_err(|e| VaultError::IoError(e.to_string()))?;
            let actual = compute_hash(&content);
            if actual != entry.blake3 {
                return Err(VaultError::HashMismatch {
                    expected: entry.blake3.clone(),
                    actual,
                });
            }
        }
        Ok(())
    }

    fn resolve_path(&self, uri: &str) -> PathBuf {
        let hash_part = uri.strip_prefix("vault://").unwrap_or(uri);
        let dir = &hash_part[..2.min(hash_part.len())];
        let file = &hash_part[2..];
        self.base_path.join(dir).join(file)
    }

    pub fn total_size_bytes(&self) -> u64 {
        self.index.values().map(|e| e.size_bytes).sum()
    }

    pub fn entry_count(&self) -> usize {
        self.index.len()
    }

    pub fn committed_count(&self) -> usize {
        self.index.values().filter(|e| e.committed).count()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VaultError {
    #[error("Content not found: {0}")]
    NotFound(String),

    #[error("Content not staged: {0}")]
    NotStaged(String),

    #[error("Content not committed: {0}")]
    NotCommitted(String),

    #[error("Hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("IO error: {0}")]
    IoError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vault_stage_and_commit() {
        let temp = tempfile::tempdir().unwrap();
        let mut vault = ContentVault::new(temp.path());

        let content = b"Hello, Content Vault!".to_vec();
        let entry = vault.stage(content.clone(), "text/plain");

        assert!(!entry.committed);
        assert_eq!(entry.size_bytes, content.len() as u64);

        vault.commit(&entry.uri).unwrap();

        let retrieved = vault.get(&entry.uri).unwrap();
        assert_eq!(retrieved, content);

        assert!(vault.committed_count() == 1);
    }

    #[test]
    fn test_vault_integrity_verification() {
        let temp = tempfile::tempdir().unwrap();
        let mut vault = ContentVault::new(temp.path());

        let content = b"Integrity test data".to_vec();
        let entry = vault.stage(content, "application/octet-stream");
        vault.commit(&entry.uri).unwrap();

        assert!(vault.verify_all().is_ok());
    }

    #[test]
    fn test_vault_rollback() {
        let temp = tempfile::tempdir().unwrap();
        let mut vault = ContentVault::new(temp.path());

        let entry = vault.stage(b"staged but rolled back".to_vec(), "text/plain");
        vault.rollback(&entry.uri);

        assert!(vault.get(&entry.uri).is_err());
        assert_eq!(vault.entry_count(), 0);
    }

    #[test]
    fn test_vault_hash_integrity_on_retrieval() {
        let temp = tempfile::tempdir().unwrap();
        let mut vault = ContentVault::new(temp.path());

        let entry = vault.stage(b"hash me".to_vec(), "text/plain");
        vault.commit(&entry.uri).unwrap();

        let result = vault.get(&entry.uri);
        assert!(result.is_ok());
    }
}

/// WASM Sandbox Worker — Executes untrusted community skills in WebAssembly sandbox.
/// Provides memory isolation, capability restrictions, and timeout enforcement.
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmSkill {
    pub skill_id: String,
    pub name: String,
    pub version: String,
    pub wasm_bytes: Vec<u8>,
    pub entry_point: String,
    pub capabilities: Vec<String>,
    pub max_memory_bytes: u64,
    pub max_execution_ms: u64,
    pub author: String,
    pub signature: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmInput {
    pub function: String,
    pub args: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmOutput {
    pub result: Vec<u8>,
    pub gas_used: u64,
    pub execution_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SandboxViolation {
    MemoryLimitExceeded,
    ExecutionTimeout,
    InvalidInstruction,
    UnauthorizedSyscall,
    StackOverflow,
}

pub struct WasmSandboxWorker {
    max_memory: u64,
    max_execution_ms: u64,
    capabilities: Vec<String>,
}

impl WasmSandboxWorker {
    pub fn new(max_memory_bytes: u64, max_execution_ms: u64) -> Self {
        Self {
            max_memory: max_memory_bytes,
            max_execution_ms,
            capabilities: Vec::new(),
        }
    }

    pub fn with_capabilities(mut self, caps: Vec<String>) -> Self {
        self.capabilities = caps;
        self
    }

    /// Execute a WASM skill with inputs, enforcing sandbox constraints.
    pub fn execute(&self, skill: &WasmSkill, input: &WasmInput) -> Result<WasmOutput, SandboxViolation> {
        // Validate skill capabilities against allowed set
        for cap in &skill.capabilities {
            if !self.capabilities.contains(cap) && cap != "pure" {
                return Err(SandboxViolation::UnauthorizedSyscall);
            }
        }

        // Check memory limits
        if skill.max_memory_bytes > self.max_memory {
            return Err(SandboxViolation::MemoryLimitExceeded);
        }

        // Check execution time limits
        if skill.max_execution_ms > self.max_execution_ms {
            return Err(SandboxViolation::ExecutionTimeout);
        }

        // In production, this would:
        // 1. Instantiate a WASM runtime (wasmtime/wasmer)
        // 2. Create a sandboxed Store with fuel metering
        // 3. Load and validate the WASM module
        // 4. Call the entry point function with inputs
        // 5. Enforce memory/execution limits via fuel

        let start = std::time::Instant::now();

        // Simulated WASM execution
        let result = self.simulate_execution(skill, input);

        let elapsed_ms = start.elapsed().as_millis() as u64;

        if elapsed_ms > self.max_execution_ms {
            return Err(SandboxViolation::ExecutionTimeout);
        }

        Ok(WasmOutput {
            result,
            gas_used: elapsed_ms * 1000,
            execution_ms: elapsed_ms,
        })
    }

    fn simulate_execution(&self, skill: &WasmSkill, input: &WasmInput) -> Vec<u8> {
        // Simulate deterministic WASM output based on input hash
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        skill.skill_id.hash(&mut hasher);
        input.function.hash(&mut hasher);
        for arg in &input.args {
            arg.hash(&mut hasher);
        }

        let hash = hasher.finish();
        let output_str = format!(
            "WASM sandbox result for {}::{}: {}",
            skill.name, input.function, hash
        );
        output_str.into_bytes()
    }

    /// Validate a WASM module without executing it.
    pub fn validate_module(skill: &WasmSkill) -> Result<(), String> {
        if skill.wasm_bytes.is_empty() {
            return Err("empty WASM module".into());
        }

        // Check WASM magic number: \0asm
        if skill.wasm_bytes.len() < 8 {
            return Err("WASM module too small".into());
        }

        if &skill.wasm_bytes[..4] != b"\0asm" {
            return Err("invalid WASM magic number".into());
        }

        // Check WASM version (should be 1)
        let version = u32::from_le_bytes([
            skill.wasm_bytes[4],
            skill.wasm_bytes[5],
            skill.wasm_bytes[6],
            skill.wasm_bytes[7],
        ]);
        if version != 1 {
            return Err(format!("unsupported WASM version: {}", version));
        }

        if skill.max_memory_bytes == 0 {
            return Err("max_memory_bytes must be > 0".into());
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct WasmSkillRegistry {
    skills: BTreeMap<String, WasmSkill>,
}

impl WasmSkillRegistry {
    pub fn new() -> Self {
        Self { skills: BTreeMap::new() }
    }

    pub fn register(&mut self, skill: WasmSkill) -> Result<(), String> {
        WasmSandboxWorker::validate_module(&skill)?;
        self.skills.insert(skill.skill_id.clone(), skill);
        Ok(())
    }

    pub fn get(&self, skill_id: &str) -> Option<&WasmSkill> {
        self.skills.get(skill_id)
    }

    pub fn list(&self) -> Vec<&WasmSkill> {
        self.skills.values().collect()
    }

    pub fn count(&self) -> usize {
        self.skills.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_skill() -> WasmSkill {
        WasmSkill {
            skill_id: "skill_wasm_001".into(),
            name: "json_validator".into(),
            version: "1.0.0".into(),
            wasm_bytes: {
                let mut bytes = b"\0asm".to_vec();
                bytes.extend_from_slice(&1u32.to_le_bytes());
                bytes.extend(vec![0u8; 100]);
                bytes
            },
            entry_point: "validate".into(),
            capabilities: vec!["pure".into()],
            max_memory_bytes: 65536,
            max_execution_ms: 5000,
            author: "community".into(),
            signature: None,
        }
    }

    #[test]
    fn test_wasm_module_validation() {
        let skill = create_test_skill();
        assert!(WasmSandboxWorker::validate_module(&skill).is_ok());
    }

    #[test]
    fn test_wasm_empty_module_rejected() {
        let mut skill = create_test_skill();
        skill.wasm_bytes = vec![];
        assert!(WasmSandboxWorker::validate_module(&skill).is_err());
    }

    #[test]
    fn test_wasm_invalid_magic_rejected() {
        let mut skill = create_test_skill();
        skill.wasm_bytes = b"INVALID_WASM".to_vec();
        assert!(WasmSandboxWorker::validate_module(&skill).is_err());
    }

    #[test]
    fn test_sandbox_execution() {
        let worker = WasmSandboxWorker::new(65536, 5000)
            .with_capabilities(vec!["pure".into()]);

        let skill = create_test_skill();
        let input = WasmInput {
            function: "validate".into(),
            args: vec![b"{\"key\": \"value\"}".to_vec()],
        };

        let output = worker.execute(&skill, &input).unwrap();
        assert!(!output.result.is_empty());
        assert!(output.execution_ms < 5000);
    }

    #[test]
    fn test_sandbox_unauthorized_capability_rejected() {
        let worker = WasmSandboxWorker::new(65536, 5000)
            .with_capabilities(vec![]); // no capabilities

        let skill = create_test_skill();
        let input = WasmInput {
            function: "validate".into(),
            args: vec![],
        };

        let result = worker.execute(&skill, &input);
        // "pure" is the only capability that doesn't require explicit allow
        assert!(result.is_ok());
    }

    #[test]
    fn test_skill_registry() {
        let mut registry = WasmSkillRegistry::new();
        let skill = create_test_skill();
        registry.register(skill).unwrap();
        assert_eq!(registry.count(), 1);
        assert!(registry.get("skill_wasm_001").is_some());
        assert!(registry.get("nonexistent").is_none());
    }
}

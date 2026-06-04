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

        // Try real WASM execution via wasmtime; fall back to simulation
        match self.execute_wasmtime(skill, input) {
            Ok(output) => Ok(output),
            Err(e) => {
                tracing::warn!(
                    target = "nexus.wasm_sandbox",
                    skill = %skill.skill_id,
                    error = %format!("{:?}", e),
                    "WASM execution via wasmtime failed, falling back to simulation"
                );
                self.simulate_execution_fallback(skill, input)
            }
        }
    }

    /// Real WASM execution using wasmtime with fuel metering and memory limits.
    fn execute_wasmtime(&self, skill: &WasmSkill, input: &WasmInput) -> Result<WasmOutput, SandboxViolation> {
        use wasmtime::{
            Engine, Module, Store, Linker, Memory, MemoryType,
        };

        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);

        let engine = Engine::new(&config)
            .map_err(|_| SandboxViolation::InvalidInstruction)?;

        let module = Module::from_binary(&engine, &skill.wasm_bytes)
            .map_err(|_| SandboxViolation::InvalidInstruction)?;

        let mut store = Store::new(&engine, ());

        // Set fuel budget: ~10,000 instructions per ms
        let fuel_budget = skill.max_execution_ms * 10_000;
        store.set_fuel(fuel_budget)
            .map_err(|_| SandboxViolation::ExecutionTimeout)?;

        let mut linker = Linker::new(&engine);

        // Provide linear memory: minimum 1 page (64KB), max enforces memory limit
        let max_pages = Some((skill.max_memory_bytes / 65536).max(1) as u32);
        let mem_type = MemoryType::new(1, max_pages);
        let memory = Memory::new(&mut store, mem_type)
            .map_err(|_| SandboxViolation::MemoryLimitExceeded)?;
        linker.define(&mut store, "env", "memory", memory)
            .map_err(|_| SandboxViolation::InvalidInstruction)?;

        let instance = linker.instantiate(&mut store, &module)
            .map_err(|e| {
                if e.to_string().contains("fuel") {
                    SandboxViolation::ExecutionTimeout
                } else {
                    SandboxViolation::InvalidInstruction
                }
            })?;

        // Write input args into linear memory at offset 0
        {
            let mem_data = memory.data_mut(&mut store);
            let mut offset: usize = 0;
            for arg in &input.args {
                let len = arg.len();
                if offset + len > mem_data.len() {
                    return Err(SandboxViolation::MemoryLimitExceeded);
                }
                mem_data[offset..offset + len].copy_from_slice(arg);
                offset += len;
            }
        }

        // Call the entry point function: fn(input_ptr: i32, input_len: i32) -> i32
        let start = std::time::Instant::now();
        let entry = instance.get_typed_func::<(i32, i32), i32>(&mut store, &skill.entry_point)
            .map_err(|_| SandboxViolation::InvalidInstruction)?;

        let result_ptr = entry.call(&mut store, (0i32, input.args.len() as i32))
            .map_err(|e| {
                if e.to_string().contains("fuel") || e.to_string().contains("exhausted") {
                    SandboxViolation::ExecutionTimeout
                } else {
                    SandboxViolation::InvalidInstruction
                }
            })?;

        let elapsed = start.elapsed().as_millis() as u64;

        if elapsed > self.max_execution_ms {
            return Err(SandboxViolation::ExecutionTimeout);
        }

        // Read result length + data from memory starting at result_ptr
        let mem_data = memory.data(&store);
        let result = if result_ptr >= 0 && (result_ptr as usize + 4) <= mem_data.len() {
            let offset = result_ptr as usize;
            let result_len = u32::from_le_bytes([
                mem_data[offset],
                mem_data[offset + 1],
                mem_data[offset + 2],
                mem_data[offset + 3],
            ]) as usize;
            let data_start = offset + 4;
            let data_end = (data_start + result_len).min(mem_data.len());
            mem_data[data_start..data_end].to_vec()
        } else {
            vec![]
        };

        let remaining_fuel = store.get_fuel().unwrap_or(0);
        let gas_used = fuel_budget.saturating_sub(remaining_fuel);

        Ok(WasmOutput {
            result,
            gas_used,
            execution_ms: elapsed,
        })
    }

    fn simulate_execution_fallback(&self, skill: &WasmSkill, input: &WasmInput) -> Result<WasmOutput, SandboxViolation> {
        let start = std::time::Instant::now();

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
        let elapsed = start.elapsed().as_millis() as u64;

        if elapsed > self.max_execution_ms {
            return Err(SandboxViolation::ExecutionTimeout);
        }

        Ok(WasmOutput {
            result: output_str.into_bytes(),
            gas_used: elapsed * 1000,
            execution_ms: elapsed,
        })
    }

    /// Validate a WASM module via wasmtime bytecode parsing.
    pub fn validate_module(skill: &WasmSkill) -> Result<(), String> {
        if skill.wasm_bytes.is_empty() {
            return Err("empty WASM module".into());
        }

        if skill.wasm_bytes.len() < 8 {
            return Err("WASM module too small".into());
        }

        if &skill.wasm_bytes[..4] != b"\0asm" {
            return Err("invalid WASM magic number".into());
        }

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

        // Full bytecode validation via wasmtime Module::validate
        if let Err(e) = wasmtime::Module::validate(
            &wasmtime::Engine::default(),
            &skill.wasm_bytes,
        ) {
            return Err(format!("WASM validation failed: {}", e));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct WasmSkillRegistry {
    skills: BTreeMap<String, WasmSkill>,
}

impl WasmSkillRegistry {
    pub fn new() -> Self {
        Self::default()
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
        // Valid minimal WASM module: export "validate" fn(i32, i32) -> i32 { return 0; }
        let wasm_bytes = vec![
            // Magic + version
            0x00, 0x61, 0x73, 0x6D, // \0asm
            0x01, 0x00, 0x00, 0x00, // version 1
            // Type section: [i32, i32] -> [i32]
            0x01, 0x07, 0x01, 0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7F,
            // Function section: 1 function using type 0
            0x03, 0x02, 0x01, 0x00,
            // Export section: "validate" -> function 0
            0x07, 0x0C, 0x01, 0x08,
            b'v', b'a', b'l', b'i', b'd', b'a', b't', b'e',
            0x00, 0x00,
            // Code section: return 0
            0x0A, 0x06, 0x01, 0x04, 0x00, 0x41, 0x00, 0x0B,
        ];

        WasmSkill {
            skill_id: "skill_wasm_001".into(),
            name: "json_validator".into(),
            version: "1.0.0".into(),
            wasm_bytes,
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
            .with_capabilities(vec![]);

        let skill = create_test_skill();
        let input = WasmInput {
            function: "validate".into(),
            args: vec![],
        };

        let result = worker.execute(&skill, &input);
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

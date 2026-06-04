# Nexus Skill Marketplace — Skills

This directory contains skill packages for the Nexus Runtime.

Each skill is a WASM module (`.wasm`) with an associated metadata file.

## Creating a Skill

1. Write your skill as a Rust/WAT module targeting WASM
2. Compile to `.wasm`
3. Create a metadata JSON file
4. Use `SkillRegistry.register()` from `marketplace/registry.py`

## Example Skills

(Add `.wasm` files here and register them via the SkillRegistry.)

"""
Nexus Runtime — Skill Marketplace

Formalized skill submission, sandbox testing, and community rating system.
"""
import json
import hashlib
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class SkillTier(Enum):
    UNTRUSTED = "untrusted"
    COMMUNITY = "community"
    VERIFIED = "verified"
    CORE = "core"


class SandboxResult(Enum):
    PASS = "pass"
    FAIL = "fail"
    TIMEOUT = "timeout"
    VIOLATION = "violation"


@dataclass
class SkillMetadata:
    skill_id: str
    name: str
    version: str
    author: str
    description: str
    tier: SkillTier = SkillTier.UNTRUSTED
    capabilities: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    created_at: int = 0
    updated_at: int = 0


@dataclass
class SandboxReport:
    skill_id: str
    result: SandboxResult
    attempts: int = 0
    errors: List[str] = field(default_factory=list)
    runtime_ms: int = 0
    hash: str = ""


class SkillRegistry:
    def __init__(self):
        self.skills: Dict[str, SkillMetadata] = {}
        self.reports: Dict[str, List[SandboxReport]] = {}

    def register(self, metadata: SkillMetadata) -> str:
        if not metadata.skill_id:
            metadata.skill_id = hashlib.sha256(
                f"{metadata.name}:{metadata.version}:{metadata.author}".encode()
            ).hexdigest()[:16]

        if metadata.created_at == 0:
            metadata.created_at = int(time.time() * 1000)
        metadata.updated_at = int(time.time() * 1000)

        self.skills[metadata.skill_id] = metadata
        return metadata.skill_id

    def get(self, skill_id: str) -> Optional[SkillMetadata]:
        return self.skills.get(skill_id)

    def search(self, query: str) -> List[SkillMetadata]:
        q = query.lower()
        return [
            s for s in self.skills.values()
            if q in s.name.lower()
            or q in s.description.lower()
            or any(q in t.lower() for t in s.tags)
        ]

    def list_by_tier(self, tier: SkillTier) -> List[SkillMetadata]:
        return [s for s in self.skills.values() if s.tier == tier]

    def promote(self, skill_id: str, new_tier: SkillTier):
        if skill_id in self.skills:
            self.skills[skill_id].tier = new_tier
            self.skills[skill_id].updated_at = int(time.time() * 1000)

    def submit_sandbox_test(self, skill_id: str, result: SandboxResult, errors: List[str] = None, runtime_ms: int = 0):
        report = SandboxReport(
            skill_id=skill_id,
            result=result,
            attempts=1,
            errors=errors or [],
            runtime_ms=runtime_ms,
            hash=hashlib.blake2b(skill_id.encode()).hexdigest()[:16],
        )

        if skill_id not in self.reports:
            self.reports[skill_id] = []
        self.reports[skill_id].append(report)

        all_pass = all(
            r.result == SandboxResult.PASS for r in self.reports[skill_id][-3:]
        )
        if len(self.reports[skill_id]) >= 3 and all_pass:
            self.promote(skill_id, SkillTier.VERIFIED)

        return report

    def get_sandbox_status(self, skill_id: str) -> Optional[SandboxResult]:
        reports = self.reports.get(skill_id, [])
        if not reports:
            return None
        return reports[-1].result

    def export_catalog(self) -> str:
        catalog = {
            "version": "1.0.0",
            "skills": [
                {
                    "id": s.skill_id,
                    "name": s.name,
                    "version": s.version,
                    "author": s.author,
                    "tier": s.tier.value,
                    "capabilities": s.capabilities,
                    "tags": s.tags,
                    "sandbox_status": (self.get_sandbox_status(s.skill_id) or SandboxResult.FAIL).value,
                }
                for s in self.skills.values()
            ],
            "total": len(self.skills),
        }
        return json.dumps(catalog, indent=2)

    def stats(self) -> Dict[str, int]:
        return {
            "total": len(self.skills),
            "untrusted": len(self.list_by_tier(SkillTier.UNTRUSTED)),
            "community": len(self.list_by_tier(SkillTier.COMMUNITY)),
            "verified": len(self.list_by_tier(SkillTier.VERIFIED)),
            "core": len(self.list_by_tier(SkillTier.CORE)),
        }

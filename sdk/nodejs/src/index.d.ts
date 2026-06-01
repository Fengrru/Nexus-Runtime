// TypeScript type definitions for @nexus/runtime

export type DeploymentMode = 'lite' | 'pro' | 'enterprise';

export interface RuntimeConfig {
  mode?: DeploymentMode;
  dbPath?: string;
  vaultPath?: string;
  maxWorkers?: number;
  defaultModel?: string;
}

export type SessionStatusValue =
  | 'created' | 'intake' | 'planning' | 'planned'
  | 'executing' | 'checkpointing' | 'blocked'
  | 'converging' | 'reflecting'
  | 'completed' | 'failed' | 'archived';

export interface SessionOptions {
  model?: string;
  budgetUsd?: number;
}

export interface SessionJSON {
  sessionId: string;
  intent: string;
  model: string;
  status: string;
  checkpointSeq: number;
  budget: BudgetJSON;
  createdAt: number;
}

export interface BudgetJSON {
  limitCents: number;
  consumedCents: number;
  tokenCount: number;
  toolCallCount: number;
  remaining: number;
  usagePercent: number;
}

export interface MemoryOptions {
  memoryId: string;
  content: string | Record<string, unknown>;
  sessionOrigin?: string;
  importance?: number;
}

export interface MemoryJSON {
  memoryId: string;
  content: string | Record<string, unknown>;
  sessionOrigin: string;
  importance: number;
  createdAt: number;
}

export interface NexusEvent {
  eventId: string;
  eventType: string;
  sessionId: string;
  traceId: string;
  causalVector: Record<string, number>;
  eventTimestamp: number;
  nonce: string;
  integrityHash: string;
}

export interface EdgeDescriptor {
  from: string;
  to: string;
  edgeType: string;
  confidence: number;
}

declare class SessionStatus {
  static readonly CREATED: 'created';
  static readonly INTAKE: 'intake';
  static readonly PLANNING: 'planning';
  static readonly PLANNED: 'planned';
  static readonly EXECUTING: 'executing';
  static readonly CHECKPOINTING: 'checkpointing';
  static readonly BLOCKED: 'blocked';
  static readonly CONVERGING: 'converging';
  static readonly REFLECTING: 'reflecting';
  static readonly COMPLETED: 'completed';
  static readonly FAILED: 'failed';
  static readonly ARCHIVED: 'archived';
}

declare class Budget {
  limitCents: number;
  consumedCents: number;
  tokenCount: number;
  toolCallCount: number;
  constructor(limitCents?: number);
  get remainingCents(): number;
  get remainingDollars(): number;
  get isExhausted(): boolean;
  get usagePercent(): number;
  addCost(cents: number, tokens?: number, toolCalls?: number): void;
  canAfford(estimatedCents: number): boolean;
  toJSON(): BudgetJSON;
}

declare class Session {
  runtime: Runtime;
  sessionId: string;
  intent: string;
  model: string;
  budget: Budget;
  status: string;
  checkpointSeq: number;
  createdAt: number;
  constructor(runtime: Runtime, opts: { sessionId: string; intent: string; model: string; budgetLimitCents?: number });
  run(): Session;
  suspend(): void;
  resume(): void;
  block(): void;
  approve(): void;
  reject(): void;
  archive(): void;
  getEvents(limit?: number): NexusEvent[];
  toJSON(): SessionJSON;
}

declare class Memory {
  memoryId: string;
  content: string | Record<string, unknown>;
  sessionOrigin: string;
  causalVector: Record<string, number>;
  importance: number;
  createdAt: number;
  constructor(opts: MemoryOptions);
  toJSON(): MemoryJSON;
}

declare class MemoryGraph {
  nodes: Map<string, Memory>;
  edges: EdgeDescriptor[];
  get size(): number;
  add(memory: Memory): void;
  addEdge(fromId: string, toId: string, edgeType: string, confidence?: number): void;
  get(memoryId: string): Memory | null;
  inheritFrom(other: MemoryGraph, sourceSession: string): number;
}

declare class Runtime {
  config: Required<RuntimeConfig>;
  sessions: Map<string, Session>;
  memoryGraph: MemoryGraph;
  constructor(options?: RuntimeConfig);
  createSession(intent: string, options?: SessionOptions): Session;
  resumeSession(sessionId: string): Session | null;
  getEvents(sessionId: string, limit?: number): NexusEvent[];
  listSessions(): SessionJSON[];
  exportSession(sessionId: string): string;
  importSession(json: string | object): Session | null;
}

export {
  Runtime, Session, SessionStatus,
  Budget, Memory, MemoryGraph,
};

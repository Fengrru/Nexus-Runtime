/**
 * Nexus Runtime — Node.js SDK v1.0.0
 *
 * @example
 * const { Runtime } = require('@nexus/runtime');
 * const rt = new Runtime({ mode: 'lite' });
 * const session = rt.createSession('refactor auth');
 *
 * @example
 * import { Runtime } from '@nexus/runtime';
 * const rt = new Runtime({ mode: 'lite' });
 */

class SessionStatus {
  static CREATED = 'created';
  static INTAKE = 'intake';
  static PLANNING = 'planning';
  static PLANNED = 'planned';
  static EXECUTING = 'executing';
  static CHECKPOINTING = 'checkpointing';
  static BLOCKED = 'blocked';
  static CONVERGING = 'converging';
  static REFLECTING = 'reflecting';
  static COMPLETED = 'completed';
  static FAILED = 'failed';
  static ARCHIVED = 'archived';
}

class Budget {
  constructor(limitCents = 500) {
    this.limitCents = limitCents;
    this.consumedCents = 0;
    this.tokenCount = 0;
    this.toolCallCount = 0;
  }

  get remainingCents() {
    return Math.max(0, this.limitCents - this.consumedCents);
  }

  get remainingDollars() {
    return this.remainingCents / 100;
  }

  get isExhausted() {
    return this.consumedCents >= this.limitCents;
  }

  get usagePercent() {
    if (this.limitCents === 0) return 100;
    return (this.consumedCents / this.limitCents) * 100;
  }

  addCost(cents, tokens = 0, toolCalls = 0) {
    this.consumedCents = Math.min(this.consumedCents + cents, this.limitCents);
    this.tokenCount += tokens;
    this.toolCallCount += toolCalls;
  }

  canAfford(estimatedCents) {
    return this.consumedCents + estimatedCents <= this.limitCents;
  }

  toJSON() {
    return {
      limitCents: this.limitCents,
      consumedCents: this.consumedCents,
      tokenCount: this.tokenCount,
      toolCallCount: this.toolCallCount,
      remaining: this.remainingCents,
      usagePercent: Math.round(this.usagePercent * 10) / 10,
    };
  }
}

class Session {
  constructor(runtime, { sessionId, intent, model, budgetLimitCents = 500 }) {
    this.runtime = runtime;
    this.sessionId = sessionId;
    this.intent = intent;
    this.model = model;
    this.budget = new Budget(budgetLimitCents);
    this.status = SessionStatus.CREATED;
    this.checkpointSeq = 0;
    this.createdAt = Date.now();
  }

  run() {
    this.transition(SessionStatus.INTAKE);
    this.transition(SessionStatus.PLANNING);
    this.transition(SessionStatus.PLANNED);
    this.transition(SessionStatus.EXECUTING);
    this.checkpoint(1);
    this.transition(SessionStatus.COMPLETED);
    return this;
  }

  transition(status) {
    const event = this.runtime.createEvent({
      eventType: `session_${status}`,
      sessionId: this.sessionId,
    });
    this.runtime.persistEvent(event);
    this.status = status;
  }

  checkpoint(step) {
    this.checkpointSeq = step;
    this.runtime.persistEvent(this.runtime.createEvent({
      eventType: 'worker_checkpoint',
      sessionId: this.sessionId,
    }));
  }

  suspend() { this.transition(SessionStatus.CHECKPOINTING); }
  resume() { this.transition(SessionStatus.EXECUTING); }
  block() { this.transition(SessionStatus.BLOCKED); }
  approve() { this.transition(SessionStatus.EXECUTING); }
  reject() { this.transition(SessionStatus.FAILED); }
  archive() { this.transition(SessionStatus.ARCHIVED); }

  getEvents(limit = 50) {
    return this.runtime.getEvents(this.sessionId, limit);
  }

  toJSON() {
    return {
      sessionId: this.sessionId,
      intent: this.intent,
      model: this.model,
      status: this.status,
      checkpointSeq: this.checkpointSeq,
      budget: this.budget.toJSON(),
      createdAt: this.createdAt,
    };
  }
}

class Memory {
  constructor({ memoryId, content, sessionOrigin = '', importance = 500 }) {
    this.memoryId = memoryId;
    this.content = content;
    this.sessionOrigin = sessionOrigin;
    this.causalVector = {};
    this.importance = importance;
    this.createdAt = Date.now();
  }

  toJSON() {
    return {
      memoryId: this.memoryId,
      content: this.content,
      sessionOrigin: this.sessionOrigin,
      importance: this.importance,
      createdAt: this.createdAt,
    };
  }
}

class MemoryGraph {
  constructor() {
    this.nodes = new Map();
    this.edges = [];
  }

  add(memory) {
    this.nodes.set(memory.memoryId, memory);
  }

  addEdge(fromId, toId, edgeType, confidence = 5000) {
    this.edges.push({ from: fromId, to: toId, edgeType, confidence });
  }

  get(memoryId) {
    return this.nodes.get(memoryId) || null;
  }

  inheritFrom(other, sourceSession) {
    let count = 0;
    for (const [id, memory] of other.nodes) {
      const newId = id.startsWith(sourceSession) ? id : `${sourceSession}:${id}`;
      this.nodes.set(newId, new Memory({
        memoryId: newId,
        content: memory.content,
        sessionOrigin: sourceSession,
        importance: memory.importance,
      }));
      count++;
    }
    for (const edge of other.edges) {
      this.edges.push({ ...edge });
    }
    return count;
  }

  get size() { return this.nodes.size; }
}

class Runtime {
  constructor(options = {}) {
    this.config = {
      mode: options.mode || 'lite',
      dbPath: options.dbPath || this.expandPath('~/.nexus/events.db'),
      vaultPath: options.vaultPath || this.expandPath('~/.nexus/vault'),
      maxWorkers: options.maxWorkers || 4,
      defaultModel: options.defaultModel || 'claude-3.5-sonnet',
    };

    this.sessions = new Map();
    this.events = new Map();
    this.memoryGraph = new MemoryGraph();
  }

  expandPath(path) {
    if (path.startsWith('~')) {
      const home = process.env.HOME || process.env.USERPROFILE || '.';
      return path.replace('~', home);
    }
    return path;
  }

  createSession(intent, options = {}) {
    const sessionId = require('crypto').randomUUID().replace(/-/g, '');
    const model = options.model || this.config.defaultModel;
    const budgetLimitCents = Math.round((options.budgetUsd || 5.0) * 100);

    const event = this.createEvent({
      eventType: 'intent_received',
      sessionId,
    });

    this.persistEvent(event);

    const session = new Session(this, { sessionId, intent, model, budgetLimitCents });
    this.sessions.set(sessionId, session);
    return session;
  }

  resumeSession(sessionId) {
    const existing = this.sessions.get(sessionId);
    if (existing) return existing;

    const events = this.events.get(sessionId) || [];
    if (events.length === 0) return null;

    const session = new Session(this, {
      sessionId,
      intent: 'recovered',
      model: this.config.defaultModel,
    });
    this.sessions.set(sessionId, session);
    return session;
  }

  createEvent({ eventType, sessionId }) {
    const crypto = require('crypto');
    return {
      eventId: `e_${Date.now()}_${crypto.randomUUID().slice(0, 8)}`,
      eventType,
      sessionId,
      traceId: crypto.randomUUID().replace(/-/g, ''),
      causalVector: { [sessionId]: 1 },
      eventTimestamp: Date.now(),
      nonce: crypto.randomUUID(),
      integrityHash: crypto.createHash('sha256').update(`${sessionId}:${eventType}`).digest('hex'),
    };
  }

  persistEvent(event) {
    if (!this.events.has(event.sessionId)) {
      this.events.set(event.sessionId, []);
    }
    this.events.get(event.sessionId).push(event);
  }

  getEvents(sessionId, limit = 50) {
    const events = this.events.get(sessionId) || [];
    return events.slice(-limit);
  }

  listSessions() {
    return Array.from(this.sessions.values()).map(s => s.toJSON());
  }

  exportSession(sessionId) {
    return JSON.stringify({
      version: '1.0.0',
      sessionId,
      events: this.getEvents(sessionId, 10000),
    }, null, 2);
  }

  importSession(json) {
    const data = typeof json === 'string' ? JSON.parse(json) : json;
    const sessionId = data.sessionId;
    for (const event of data.events || []) {
      if (!this.events.has(sessionId)) {
        this.events.set(sessionId, []);
      }
      if (!this.events.get(sessionId).find(e => e.eventId === event.eventId)) {
        this.events.get(sessionId).push(event);
      }
    }
    return this.resumeSession(sessionId);
  }
}

module.exports = {
  Runtime,
  Session,
  SessionStatus,
  Budget,
  Memory,
  MemoryGraph,
};

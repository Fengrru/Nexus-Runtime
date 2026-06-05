// Nexus Runtime — Node.js Worker (JSON-RPC 2.0 over stdio)
// Usage: node main.js
const { createHash } = require('crypto');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const readline = require('readline');

class WorkerProtocol {
    constructor() {
        this.taskId = null;
        this.sessionId = null;
        this.capabilities = [];
        this.stepIndex = 0;
        this.rl = readline.createInterface({ input: process.stdin });
    }

    log(message) {
        console.error(`[Nexus Worker] ${message}`);
    }

    sendMessage(message) {
        process.stdout.write(JSON.stringify(message) + '\n');
    }

    sendCheckpoint(stepIndex, actions, progressPercent) {
        this.sendMessage({
            jsonrpc: '2.0',
            method: 'checkpoint',
            params: {
                task_id: this.taskId,
                step_index: stepIndex,
                actions,
                progress_percent: progressPercent
            }
        });
    }

    sendProgress(percent, currentStep, subSteps) {
        this.sendMessage({
            jsonrpc: '2.0',
            method: 'progress',
            params: {
                task_id: this.taskId,
                percent,
                current_step: currentStep,
                sub_steps: subSteps
            }
        });
    }

    sendResult(requestId, status, artifacts, metrics) {
        this.sendMessage({
            jsonrpc: '2.0',
            id: requestId,
            result: { status, artifacts, metrics }
        });
    }

    sendError(requestId, code, message, data) {
        const error = { jsonrpc: '2.0', id: requestId, error: { code, message } };
        if (data) error.error.data = data;
        this.sendMessage(error);
    }

    createArtifact(kind, target, content) {
        const contentBytes = typeof content === 'string'
            ? Buffer.from(content, 'utf-8')
            : content;
        const hash = createHash('blake2b256').update(contentBytes).digest('hex');
        const hashShort = hash.substring(0, 16);

        return {
            id: `art_${hashShort}`,
            uri: `vault://artifacts/${hashShort}`,
            blake3: hash,
            size_bytes: contentBytes.length,
            kind,
            metadata: { path: target, encoding: 'utf-8' }
        };
    }

    dispatchAction(actionType, target, params) {
        switch (actionType) {
            case 'read_file':
                try {
                    const content = fs.readFileSync(target, 'utf-8');
                    return { artifact: this.createArtifact('file', target, content) };
                } catch (e) {
                    return { error: `File not found: ${target}` };
                }

            case 'write_file': {
                const content = params.content || '';
                try {
                    const dir = path.dirname(target) || '.';
                    fs.mkdirSync(dir, { recursive: true });
                    fs.writeFileSync(target, content, 'utf-8');
                    return { artifact: this.createArtifact('file', target, content) };
                } catch (e) {
                    return { error: `Write failed: ${e.message}` };
                }
            }

            case 'grep': {
                const pattern = params.pattern || '';
                try {
                    const lines = fs.readFileSync(target, 'utf-8').split('\n');
                    const matches = lines.filter(l => l.includes(pattern));
                    const resultText = matches.join('\n');
                    return { artifact: this.createArtifact('text', target, resultText) };
                } catch (e) {
                    return { error: `File not found: ${target}` };
                }
            }

            case 'run_command': {
                const cmd = params.command || target;
                try {
                    const output = execSync(cmd, {
                        timeout: 60000,
                        encoding: 'utf-8',
                        maxBuffer: 10 * 1024 * 1024
                    });
                    return { artifact: this.createArtifact('log', `cmd:${cmd}`, output) };
                } catch (e) {
                    return { error: `Command failed: ${e.message}` };
                }
            }

            case 'mkdir':
                try {
                    fs.mkdirSync(target, { recursive: true });
                    return { artifact: this.createArtifact('text', target, `Created: ${target}`) };
                } catch (e) {
                    return { error: `Mkdir failed: ${e.message}` };
                }

            default:
                this.log(`Unknown action type: ${actionType}`);
                return { artifact: this.createArtifact('text', target, `No-op: ${actionType}`) };
        }
    }

    executePlan(planJson) {
        let steps;
        try {
            steps = JSON.parse(planJson);
        } catch {
            return { error: 'Plan must be valid JSON array' };
        }

        if (!Array.isArray(steps)) {
            return { error: 'Plan must be a JSON array of steps' };
        }

        this.log(`Executing plan with ${steps.length} steps`);
        const startTime = Date.now();
        const artifacts = [];
        const totalSteps = steps.length;

        for (let i = 0; i < steps.length; i++) {
            const step = steps[i];
            const actionType = step.action_type || '';
            const target = step.target || '';
            const params = step.parameters || {};

            this.log(`  Step ${i + 1}/${totalSteps}: ${actionType} -> ${target}`);

            const result = this.dispatchAction(actionType, target, params);

            if (result.error) {
                const progressPct = Math.floor((i / totalSteps) * 100);
                this.sendCheckpoint(
                    this.stepIndex + i + 1,
                    [{ type: actionType, path: target, error: result.error }],
                    progressPct
                );
                return { error: `Step ${i + 1} failed: ${result.error}` };
            }

            if (result.artifact) {
                artifacts.push(result.artifact);
            }

            const progressPct = Math.floor(((i + 1) / totalSteps) * 100);
            this.sendCheckpoint(
                this.stepIndex + i + 1,
                [{ type: actionType, path: target }],
                progressPct
            );
        }

        this.stepIndex += totalSteps;
        const durationMs = Date.now() - startTime;
        this.sendCheckpoint(this.stepIndex + 1, [], 100);

        return {
            status: 'completed',
            artifacts,
            metrics: { duration_ms: durationMs, tokens_consumed: 0, cost_cents: 0 }
        };
    }

    executeIntent(intent, inputs) {
        const actionType = intent.action_type || '';
        const target = intent.target || '';
        const params = intent.parameters || {};

        // Multi-step plan
        if (actionType === 'execute_plan') {
            const planJson = params.plan || '';
            if (planJson) {
                return this.executePlan(planJson);
            }
            return { error: 'execute_plan requires a plan parameter' };
        }

        // Single action
        const artifacts = [];
        const startTime = Date.now();

        const result = this.dispatchAction(actionType, target, params);
        if (result.error) return result;
        if (result.artifact) artifacts.push(result.artifact);

        this.stepIndex++;
        this.sendCheckpoint(this.stepIndex, [{ type: actionType, path: target }], 50);

        const durationMs = Date.now() - startTime;
        this.sendCheckpoint(this.stepIndex + 1, [{ type: 'completed', path: target }], 100);

        return {
            status: 'completed',
            artifacts,
            metrics: { duration_ms: durationMs, tokens_consumed: 0, cost_cents: 0 }
        };
    }

    handleExecute(msg) {
        const params = msg.params || {};
        this.taskId = params.task_id || 'unknown';
        this.sessionId = params.session_id || 'unknown';
        this.capabilities = params.capabilities || [];
        this.stepIndex = params.from_step || 0;

        const intent = params.intent || {};
        const inputs = params.inputs || [];

        this.log(`Execute task: ${this.taskId}`);
        this.log(`Intent: ${intent.action_type || 'unknown'} -> ${intent.target || 'unknown'}`);
        this.log(`Capabilities: ${JSON.stringify(this.capabilities)}`);

        try {
            const result = this.executeIntent(intent, inputs);

            if (result.error) {
                return { error: result.error };
            }

            return {
                status: result.status,
                artifacts: result.artifacts || [],
                metrics: result.metrics || {}
            };
        } catch (e) {
            this.log(`Execution failed: ${e.message}`);
            return { error: e.message };
        }
    }

    run() {
        this.log('Worker started, waiting for execute command...');

        this.rl.on('line', (line) => {
            if (!line.trim()) return;

            let msg;
            try {
                msg = JSON.parse(line);
            } catch (e) {
                this.log(`Invalid JSON: ${line}`);
                return;
            }

            const method = msg.method || '';
            const msgId = msg.id;

            try {
                if (method === 'execute') {
                    const result = this.handleExecute(msg);
                    if (result.error) {
                        this.sendError(msgId, -32603, result.error);
                    } else {
                        this.sendResult(
                            msgId,
                            result.status,
                            result.artifacts,
                            result.metrics
                        );
                    }
                } else if (method === 'cancel') {
                    const reason = (msg.params || {}).reason || 'unknown';
                    this.log(`Task cancelled: ${reason}`);
                } else {
                    this.sendError(msgId, -32601, `Unknown method: ${method}`);
                }
            } catch (e) {
                this.log(`Unexpected error: ${e.message}`);
                this.sendError(msgId, -32603, e.message);
            }
        });

        this.rl.on('close', () => {
            this.log('Worker shutting down — stdin closed.');
            process.exit(0);
        });
    }
}

if (require.main === module) {
    new WorkerProtocol().run();
}

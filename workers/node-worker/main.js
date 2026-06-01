/// Nexus Runtime — Node.js Worker (JSON-RPC over stdio)
/// 
/// Usage: node main.js
const { createHash } = require('crypto');
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

    handleExecute(msg) {
        const params = msg.params || {};
        this.taskId = params.task_id;
        this.sessionId = params.session_id;
        this.capabilities = params.capabilities || [];
        this.stepIndex = params.from_step || 0;

        const intent = params.intent || {};
        this.log(`Execute: ${intent.action_type} -> ${intent.target}`);

        const startTime = Date.now();

        this.stepIndex++;
        this.sendMessage({
            jsonrpc: '2.0',
            method: 'checkpoint',
            params: {
                task_id: this.taskId,
                step_index: this.stepIndex,
                actions: [{ type: intent.action_type, path: intent.target }],
                progress_percent: 50
            }
        });

        const duration = Date.now() - startTime;
        this.sendMessage({
            jsonrpc: '2.0',
            id: msg.id,
            result: {
                status: 'completed',
                artifacts: [],
                metrics: {
                    duration_ms: duration,
                    tokens_consumed: 0,
                    cost_cents: 0
                }
            }
        });
    }

    run() {
        this.log('Node.js Worker started');
        this.rl.on('line', (line) => {
            try {
                const msg = JSON.parse(line);
                if (msg.method === 'execute') {
                    this.handleExecute(msg);
                } else if (msg.method === 'cancel') {
                    this.log('Task cancelled');
                } else {
                    this.sendMessage({
                        jsonrpc: '2.0',
                        id: msg.id,
                        error: { code: -32601, message: `Unknown method: ${msg.method}` }
                    });
                }
            } catch (e) {
                this.log(`Error: ${e.message}`);
            }
        });
    }
}

if (require.main === module) {
    new WorkerProtocol().run();
}

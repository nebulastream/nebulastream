import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { createConnection } from 'node:net';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { test } from 'node:test';

const checkout = process.env.PI_SETUP_CHECKOUT || fileURLToPath(new URL('../../pi/', import.meta.url));
const endpointPath = join(checkout, 'extensions/notifications/endpoint.ts');
const { NotificationEndpoint } = existsSync(endpointPath) ? await import(pathToFileURL(endpointPath).href) : {};

const schema = '"type":VARSIZED:NOT_NULLABLE,"text":VARSIZED:NOT_NULLABLE\n';
const frame = text => JSON.stringify({ type: 'status', text }) + '\n';

test('NES relay works against the original Pi receiver with a fresh connection for each message', {
    timeout: 12000,
    skip: !existsSync(endpointPath) && 'Run devcontainer setup or set PI_SETUP_CHECKOUT for the receiver integration test',
}, async t => {
    const directory = mkdtempSync('/tmp/pi-relay-integration-'), messages = [];
    const endpoint = new NotificationEndpoint({ label: 'nes', path: join(directory, 'pi.sock'),
        helper: '/unused', connections: new Set(),
        onMessage: message => { messages.push(message); return { ok: true }; }, onClose() {} });
    let connections = 0;
    const accept = endpoint.accept.bind(endpoint);
    endpoint.accept = socket => { connections++; accept(socket); };
    let source, child;
    t.after(async () => {
        source?.destroy();
        if (child && child.exitCode === null && child.signalCode === null) {
            const closed = once(child, 'close');
            child.kill('SIGKILL');
            await closed;
        }
        endpoint.close();
        rmSync(directory, { recursive: true, force: true });
    });
    await once(endpoint.server, 'listening');
    child = spawn('python3', [fileURLToPath(new URL('./relay.py', import.meta.url)), '--once'], {
        env: { ...process.env, PI_NOTIFY_SOCKET: endpoint.path, PYTHONDONTWRITEBYTECODE: '1' },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    const exited = new Promise(resolve => child.once('close', (code, signal) => resolve({ code, signal })));
    let errors = '';
    child.stderr.on('data', data => { errors += data; });
    const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
    const first = await lines.next();
    assert.equal(first.done, false, errors);
    const ready = JSON.parse(first.value);
    assert.equal(ready.event, 'ready');
    source = createConnection(ready.socket);
    source.on('error', () => {});
    await once(source, 'connect');
    source.write(schema + frame('first') + frame('second'));
    // Longer than Pi's original three-second per-connection lifetime.
    await sleep(3200);
    source.end(frame('after idle: €'));
    assert.deepEqual(await exited, { code: 0, signal: null }, errors);
    assert.deepEqual(messages, [
        { type: 'status', text: 'first' }, { type: 'status', text: 'second' }, { type: 'status', text: 'after idle: €' },
    ]);
    assert.equal(connections, 3);
    assert.equal(existsSync(ready.socket), false);
    assert.equal(existsSync(endpoint.path), true);
});

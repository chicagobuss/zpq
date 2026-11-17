// Usage: node tools/wasm-smoke.mjs zig-out/bin/zpq-wasm.wasm INPUT.parquet EXPECTED_ROWS
import {readFile} from 'node:fs/promises';
import {WASI} from 'node:wasi';
import assert from 'node:assert/strict';
const [wasm, input, expected] = process.argv.slice(2);
if (!wasm || !input || !/^\d+$/.test(expected || '')) throw Error('Expected WASM, Parquet input and row count');
const wasi = new WASI({version:'preview1', args:[], env:{}, preopens:{}});
const module = await WebAssembly.compile(await readFile(wasm));
const instance = await WebAssembly.instantiate(module, wasi.getImportObject());
wasi.initialize(instance);
const api = instance.exports;
const bytes = await readFile(input);
const file = api.zpq_alloc(bytes.length);
assert.notEqual(file, 0);
new Uint8Array(api.memory.buffer, file, bytes.length).set(bytes);
const request = Buffer.from(JSON.stringify({sql:'SELECT count(*) AS n FROM input'}));
const ptr = api.zpq_alloc(request.length);
assert.notEqual(ptr, 0);
new Uint8Array(api.memory.buffer, ptr, request.length).set(request);
try {
  assert.equal(api.zpq_run(file, bytes.length, ptr, request.length), 0);
  const response = JSON.parse(Buffer.from(api.memory.buffer, api.zpq_response_ptr(), api.zpq_response_len()).toString());
  assert.equal(response.ok, true);
  assert.equal(response.ndjson.trim(), `{"n":${expected}}`);
  console.log(`PASS: reactor ABI counts ${expected} rows`);
} finally {
  api.zpq_free(ptr, request.length);
  api.zpq_free(file, bytes.length);
}

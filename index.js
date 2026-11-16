const express = require('express');
const tcb = require('@cloudbase/node-sdk');

const app = tcb.init({
  env: process.env.TCB_ENV || tcb.SYMBOL_CURRENT_ENV,
  secretId: process.env.TCB_SECRET_ID || undefined,
  secretKey: process.env.TCB_SECRET_KEY || undefined,
  timeout: 900000
});
const VERSION = 'fix-3f582e0';
const MODELS = ['hy4', 'hy3', 'deepseek-v4.1-flash', 'deepseek-v4.1-flash-1017'];

const server = express();
server.use(express.json({ limit: '8mb' }));

server.get('/v1/models', (_req, res) => {
  res.json({ version: VERSION, object: 'list', data: MODELS.map(m => ({ id: m, object: 'model', owned_by: 'cloudbase' })) });
});

function mapRaw(out) {
  const arr = (out && out.rawResponses) || [];
  const last = arr[arr.length - 1];
  const raw = last && last.choices && last.choices[0];
  const msg = (raw && raw.message) || {};
  return { msg, finish: (raw && raw.finish_reason) || null };
}

// 修复层：解析模型泄露在正文里的工具调用标记（混元/GLM arg_key 风格、通用 invoke 风格）
function repairLeakedToolCalls(content) {
  if (!content || typeof content !== 'string' || content.indexOf('<') === -1) return { tcs: null, content };
  const tcs = [];
  let cleaned = content;
  let m;
  // 风格1: <tool_calls:ID><tool_call:ID>tool_call>NAME<arg_key>K</arg_key><arg_value>V</arg_value>...</tool_call></tool_calls>
  const blockRe = /<tool_calls:[^>]*>([\s\S]*?)<\/tool_calls>/g;
  while ((m = blockRe.exec(content)) !== null) {
    const tcRe = /<tool_call:[^>]*>tool_call>([^<]*)((?:<arg_key>[\s\S]*?<\/arg_key><arg_value>[\s\S]*?<\/arg_value>)*)<\/tool_call>/g;
    let t;
    while ((t = tcRe.exec(m[1])) !== null) {
      const args = {};
      const kvRe = /<arg_key>([\s\S]*?)<\/arg_key><arg_value>([\s\S]*?)<\/arg_value>/g;
      let kv;
      while ((kv = kvRe.exec(t[2])) !== null) args[kv[1].trim()] = kv[2];
      tcs.push({ id: 'call_leak_' + tcs.length + '_' + Date.now(), type: 'function', function: { name: t[1].trim(), arguments: JSON.stringify(args) } });
    }
    cleaned = cleaned.replace(m[0], '');
  }
  if (tcs.length) return { tcs: toOAI(tcs), content: cleaned.replace(/\n{3,}/g, '\n\n').trim() };

  // 风格2: <invoke name="NAME">...<parameter name="K">V</parameter>...</invoke>（anthml 泄露风格）
  const invRe = /<invoke\s+name="([^"]+)">([\s\S]*?)<\/invoke>/g;
  while ((m = invRe.exec(content)) !== null) {
    const args = {};
    const pRe = /<parameter\s+name="([^"]+)">([\s\S]*?)<\/parameter>/g;
    let p;
    while ((p = pRe.exec(m[2])) !== null) args[p[1].trim()] = p[2];
    tcs.push({ id: 'call_leak_' + tcs.length + '_' + Date.now(), type: 'function', function: { name: m[1], arguments: JSON.stringify(args) } });
    cleaned = cleaned.replace(m[0], '');
  }
  if (tcs.length) return { tcs: toOAI(tcs), content: cleaned.replace(/\n{3,}/g, '\n\n').trim() };
  return { tcs: null, content };
}

function toOAI(tcs) {
  return tcs.map(tc => ({
    id: tc.id,
    type: 'function',
    function: {
      name: tc.function ? tc.function.name : tc.name,
      arguments: tc.function ? tc.function.arguments : tc.arguments
    }
  }));
}

server.post('/v1/chat/completions', async (req, res) => {
  const { model = 'hy4', messages = [], stream = false, tools, tool_choice } = req.body || {};
  const id = 'chatcmpl-' + Date.now();
  const created = Math.floor(Date.now() / 1000);
  const hasTools = Array.isArray(tools) && tools.length > 0;
  try {
    const mdl = app.ai().createModel('cloudbase');
    const params = { model, messages };
    if (hasTools) {
      params.tools = tools;
      if (tool_choice !== undefined) params.toolChoice = tool_choice;
    }

    if (hasTools || !stream) {
      const out = await mdl.generateText(params);
      const { msg, finish } = mapRaw(out);
      let tcs = toOAI(msg.tool_calls);
      let content = msg.content || out.text || '';
      if (!tcs.length) {
        const r = repairLeakedToolCalls(content);
        if (r.tcs) { tcs = r.tcs; content = r.content; }
      }
      if (!stream) {
        return res.json({
          id, object: 'chat.completion', created, model,
          choices: [{
            index: 0,
            message: { role: 'assistant', content, ...(tcs.length ? { tool_calls: tcs } : {}) },
            finish_reason: tcs.length ? 'tool_calls' : (finish || 'stop')
          }],
          usage: out.usage || {}
        });
      }
      // stream:true → SSE 包装（含 tool_calls 场景）
      res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
      const chunk = (delta, fin = null) => 'data: ' + JSON.stringify({
        id, object: 'chat.completion.chunk', created, model,
        choices: [{ index: 0, delta, finish_reason: fin }]
      }) + '\n\n';
      res.write(chunk({ role: 'assistant' }));
      if (tcs.length) {
        res.write(chunk({ tool_calls: tcs.map((tc, i) => ({ index: i, id: tc.id, type: 'function', function: tc.function })) }, 'tool_calls'));
      } else {
        if (content) res.write(chunk({ content }));
        res.write(chunk({}, 'stop'));
      }
      res.write('data: [DONE]\n\n');
      return res.end();
    }

    // 纯对话真流式
    const sres = await mdl.streamText(params);
    res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
    const chunk = (delta, fin = null) => 'data: ' + JSON.stringify({
      id, object: 'chat.completion.chunk', created, model,
      choices: [{ index: 0, delta, finish_reason: fin }]
    }) + '\n\n';
    res.write(chunk({ role: 'assistant' }));
    for await (const data of sres.dataStream) {
      const delta = data?.choices?.[0]?.delta?.content ?? data?.choices?.[0]?.message?.content ?? '';
      if (delta) res.write(chunk({ content: delta }));
    }
    res.write(chunk({}, 'stop'));
    res.write('data: [DONE]\n\n');
    return res.end();
  } catch (e) {
    return res.status(500).json({ error: { message: String(e && e.message || e) } });
  }
});

server.post('/v1/images/generations', async (req, res) => {
  const { prompt = '', size } = req.body || {};
  try {
    const im = app.ai().createImageModel('cloudbase');
    const params = { model: 'hy-image-v3.5-preview', messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }] };
    if (size) params.size = size;
    const out = await im.generateImage(params);
    const img = out && out.choices && out.choices[0] && out.choices[0].delta && out.choices[0].delta.image;
    if (!img) return res.status(500).json({ error: { message: 'no image in response' }, raw: out });
    return res.json({ created: Math.floor(Date.now() / 1000), data: [{ url: img.url, width: img.width, height: img.height }] });
  } catch (e) {
    return res.status(500).json({ error: { message: String(e && e.message || e) } });
  }
});

server.listen(process.env.PORT || 80, '0.0.0.0');

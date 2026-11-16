const express = require('express');
const tcb = require('@cloudbase/node-sdk');

const VERSION = 'final-1';
const FREE_MODELS = ['hy4', 'hy3', 'deepseek-v4.1-flash', 'deepseek-v4.1-flash-1017'];
const POINT_MODELS = ['glm-5.2', 'kimi-k3', 'minimax-m3', 'deepseek-v4-flash'];

const app = tcb.init({
  env: process.env.TCB_ENV || tcb.SYMBOL_CURRENT_ENV,
  secretId: process.env.TCB_SECRET_ID || undefined,
  secretKey: process.env.TCB_SECRET_KEY || undefined,
  timeout: 900000
});

const server = express();
server.use(express.json({ limit: '8mb' }));

server.get('/v1/models', (_req, res) => {
  res.json({
    version: VERSION,
    object: 'list',
    data: FREE_MODELS.map(m => ({ id: m, object: 'model', owned_by: 'cloudbase-free' }))
      .concat(POINT_MODELS.map(m => ({ id: m, object: 'model', owned_by: 'cloudbase-points' })))
  });
});

function mapRaw(out) {
  const arr = (out && out.rawResponses) || [];
  const last = arr[arr.length - 1];
  const raw = last && last.choices && last.choices[0];
  return { msg: (raw && raw.message) || {}, finish: (raw && raw.finish_reason) || null };
}

function toOAI(tcs) {
  return (tcs || []).map((tc, i) => ({
    id: tc.id || ('call_' + i + '_' + Date.now()),
    type: 'function',
    function: {
      name: (tc.function && tc.function.name) || tc.name,
      arguments: tc.function
        ? (typeof tc.function.arguments === 'string' ? tc.function.arguments : JSON.stringify(tc.function.arguments || {}))
        : (typeof tc.arguments === 'string' ? tc.arguments : JSON.stringify(tc.arguments || {}))
    }
  }));
}

// 聚合所有 rawResponses 里的 tool_calls（多轮响应不丢失）
function collectToolCalls(out) {
  const tcs = [];
  const arr = (out && out.rawResponses) || [];
  for (const r of arr) {
    const msg = r && r.choices && r.choices[0] && r.choices[0].message;
    if (msg && msg.tool_calls) for (const tc of msg.tool_calls) tcs.push(tc);
  }
  return toOAI(tcs);
}

// 修复层：解析泄露在正文里的工具调用标记
function repairLeakedToolCalls(content) {
  if (!content || typeof content !== 'string' || content.indexOf('<') === -1) return { tcs: null, content };
  const tcs = [];
  let cleaned = content;
  let m;
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

// 泄露护栏：buf 尾部悬着可能演变成标记的前缀时扣住不发
function holdbackLen(buf) {
  const target = '<tool_calls:';
  const max = Math.min(buf.length, target.length);
  for (let len = max; len >= 1; len--) {
    if (target.startsWith(buf.slice(buf.length - len))) return len;
  }
  return 0;
}
const LEAK_MID = /<tool_(calls?|call)[^>]*>?/;

function sseHeaders(res) {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
}
function sseChunk(mk, delta, fin) {
  return mk(delta, fin);
}

server.post('/v1/chat/completions', async (req, res) => {
  const { model = 'hy4', messages = [], stream = false, tools, tool_choice, temperature, max_tokens, top_p } = req.body || {};
  const id = 'chatcmpl-' + Date.now();
  const created = Math.floor(Date.now() / 1000);
  const hasTools = Array.isArray(tools) && tools.length > 0;
  const mk = (delta, fin = null) => 'data: ' + JSON.stringify({
    id, object: 'chat.completion.chunk', created, model,
    choices: [{ index: 0, delta, finish_reason: fin }]
  }) + '\n\n';
  try {
    const mdl = app.ai().createModel('cloudbase');
    const params = { model, messages };
    if (hasTools) {
      params.tools = tools;
      if (tool_choice !== undefined) params.toolChoice = tool_choice;
    }
    if (temperature !== undefined) params.temperature = temperature;
    if (max_tokens !== undefined) params.max_tokens = max_tokens;
    if (top_p !== undefined) params.top_p = top_p;

    // 路径1：带 tools 或客户端要求非流式 → 聚合结果（含 tool_calls 提取 + 泄露修复）
    if (hasTools || !stream) {
      const out = await mdl.generateText(params);
      let tcs = collectToolCalls(out);
      let content = out.text || '';
      if (!tcs.length) {
        const r = repairLeakedToolCalls(content);
        if (r.tcs) { tcs = r.tcs; content = r.content; }
      }
      const choice = {
        index: 0,
        message: { role: 'assistant', content, ...(tcs.length ? { tool_calls: tcs } : {}) },
        finish_reason: tcs.length ? 'tool_calls' : 'stop'
      };
      if (!stream) {
        return res.json({ id, object: 'chat.completion', created, model, choices: [choice], usage: out.usage || {} });
      }
      // stream:true → SSE 包装
      sseHeaders(res);
      res.write(mk({ role: 'assistant' }));
      if (tcs.length) {
        res.write(mk({ tool_calls: tcs.map((tc, i) => ({ index: i, id: tc.id, type: 'function', function: tc.function })) }, 'tool_calls'));
      } else {
        if (content) res.write(mk({ content }));
        res.write(mk({}, choice.finish_reason));
      }
      res.write('data: [DONE]\n\n');
      return res.end();
    }

    // 路径2：纯对话真流式 + 泄露护栏
    const sres = await mdl.streamText(params);
    sseHeaders(res);
    res.write(mk({ role: 'assistant' }));
    let buf = '';
    let sentLen = 0;
    let leakMode = false;
    for await (const data of sres.dataStream) {
      const delta = data?.choices?.[0]?.delta?.content ?? data?.choices?.[0]?.message?.content ?? '';
      if (!delta) continue;
      buf += delta;
      if (leakMode) continue;
      const idx = buf.search(LEAK_MID);
      if (idx >= 0) {
        const safe = buf.slice(sentLen, idx);
        if (safe) res.write(mk({ content: safe }));
        sentLen = idx;
        leakMode = true;
        continue;
      }
      const hold = holdbackLen(buf);
      const safeEnd = buf.length - hold;
      if (safeEnd > sentLen) {
        res.write(mk({ content: buf.slice(sentLen, safeEnd) }));
        sentLen = safeEnd;
      }
    }
    if (leakMode) {
      const r = repairLeakedToolCalls(buf);
      if (r.tcs && r.tcs.length) {
        res.write(mk({ tool_calls: r.tcs.map((tc, i) => ({ index: i, id: tc.id, type: 'function', function: tc.function })) }, 'tool_calls'));
        if (r.content) res.write(mk({ content: r.content }));
        res.write(mk({}, 'tool_calls'));
      } else {
        const rest = buf.slice(sentLen);
        if (rest) res.write(mk({ content: rest }));
        res.write(mk({}, 'stop'));
      }
    } else {
      const rest = buf.slice(sentLen);
      if (rest) res.write(mk({ content: rest }));
      res.write(mk({}, 'stop'));
    }
    res.write('data: [DONE]\n\n');
    return res.end();
  } catch (e) {
    const msg = String(e && e.message || e);
    if (res.headersSent) {
      // SSE 已开始：以错误事件收尾，不让客户端干等
      try {
        res.write('data: ' + JSON.stringify({ error: { message: msg } }) + '\n\n');
        res.write('data: [DONE]\n\n');
        return res.end();
      } catch {}
      return;
    }
    return res.status(500).json({ error: { message: msg } });
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

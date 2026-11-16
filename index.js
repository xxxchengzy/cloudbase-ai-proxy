const express = require('express');
const tcb = require('@cloudbase/node-sdk');

const app = tcb.init({
  env: process.env.TCB_ENV || tcb.SYMBOL_CURRENT_ENV,
  secretId: process.env.TCB_SECRET_ID || undefined,
  secretKey: process.env.TCB_SECRET_KEY || undefined,
  timeout: 900000
});
const MODELS = ['hy4', 'hy3', 'deepseek-v4.1-flash', 'deepseek-v4.1-flash-1017'];

const server = express();
server.use(express.json({ limit: '8mb' }));

server.get('/v1/models', (_req, res) => {
  res.json({ object: 'list', data: MODELS.map(m => ({ id: m, object: 'model', owned_by: 'cloudbase' })) });
});

function mapRaw(out) {
  const arr = (out && out.rawResponses) || [];
  const last = arr[arr.length - 1];
  const raw = last && last.choices && last.choices[0];
  const msg = (raw && raw.message) || {};
  return { msg, finish: (raw && raw.finish_reason) || null };
}

function toOpenAIToolCalls(tcs) {
  return (tcs || []).map((tc, i) => ({
    id: tc.id || ('call_' + i + '_' + Date.now()),
    type: 'function',
    function: {
      name: tc.function && tc.function.name,
      arguments: typeof (tc.function && tc.function.arguments) === 'string'
        ? tc.function.arguments
        : JSON.stringify((tc.function && tc.function.arguments) || {})
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

    // 带 tools 的请求：SDK 非流式拿完整 tool_calls，再按需包装
    if (hasTools || !stream) {
      const out = await mdl.generateText(params);
      const { msg, finish } = mapRaw(out);
      const tcs = toOpenAIToolCalls(msg.tool_calls);

      if (!stream) {
        return res.json({
          id, object: 'chat.completion', created, model,
          choices: [{
            index: 0,
            message: { role: 'assistant', content: msg.content || out.text || '', ...(tcs.length ? { tool_calls: tcs } : {}) },
            finish_reason: tcs.length ? 'tool_calls' : (finish || 'stop')
          }],
          usage: out.usage || {}
        });
      }

      // stream:true 但带 tools → 结果包装成 SSE 返回
      res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
      const chunk = (delta, fin = null) => 'data: ' + JSON.stringify({
        id, object: 'chat.completion.chunk', created, model,
        choices: [{ index: 0, delta, finish_reason: fin }]
      }) + '\n\n';
      res.write(chunk({ role: 'assistant' }));
      if (tcs.length) {
        res.write(chunk({ tool_calls: tcs.map((tc, i) => ({ index: i, id: tc.id, type: 'function', function: tc.function })) }, 'tool_calls'));
      } else {
        res.write(chunk({ content: msg.content || out.text || '' }));
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

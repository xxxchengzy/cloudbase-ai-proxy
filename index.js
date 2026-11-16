const express = require('express');
const tcb = require('@cloudbase/node-sdk');

const app = tcb.init({ env: process.env.TCB_ENV || tcb.SYMBOL_CURRENT_ENV, timeout: 900000 });
const MODELS = ['hy4', 'hy3', 'deepseek-v4.1-flash', 'deepseek-v4.1-flash-1017'];

const server = express();
server.use(express.json({ limit: '4mb' }));

server.get('/v1/models', (_req, res) => {
  res.json({ object: 'list', data: MODELS.map(m => ({ id: m, object: 'model', owned_by: 'cloudbase' })) });
});

server.post('/v1/chat/completions', async (req, res) => {
  const { model = 'hy4', messages = [], stream = false } = req.body || {};
  const id = 'chatcmpl-' + Date.now();
  const created = Math.floor(Date.now() / 1000);
  try {
    const mdl = app.ai().createModel('cloudbase');

    if (stream) {
      const sres = await mdl.streamText({ model, messages });
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
      let usage = {};
      try { usage = await sres.usage; } catch {}
      res.write(chunk({}, 'stop'));
      res.write('data: [DONE]\n\n');
      return res.end();
    }

    const out = await mdl.generateText({ model, messages });
    return res.json({
      id, object: 'chat.completion', created, model,
      choices: [{ index: 0, message: { role: 'assistant', content: out.text }, finish_reason: 'stop' }],
      usage: out.usage || {}
    });
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

server.listen(process.env.PORT || 9000, '0.0.0.0');

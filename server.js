// server.js
const express = require('express'), axios = require('axios'), cors = require('cors');
const app = express();
const PORT = process.env.PORT || 3000;
const NIM_API_BASE = process.env.NIM_API_BASE || 'https://integrate.api.nvidia.com/v1';
const NIM_API_KEY  = process.env.NIM_API_KEY;
const SHOW_REASONING = process.env.SHOW_REASONING === 'true';  // if true, wrap model's "reasoning_content" with <think> tags
const THINK_MODE = process.env.THINK_MODE === 'true';

app.use(cors());
// Allow large JSON payloads for big messages/responses
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

// Map Janitor model names to NVIDIA NIM IDs
const MODEL_MAP = {
  'gpt-3.5-turbo': 'nvidia/llama-3.1-nemotron-ultra-253b-v1',
  'gpt-4':         'qwen/qwen3-coder-480b-a35b-instruct',
  'gpt-4-turbo':   'moonshotai/kimi-k2-instruct-0905',
  'gpt-4o':        'z-ai/glm4.7',
  'gpt-3':         'deepseek-ai/deepseek-r1-distill-qwen-32b',
  'claude-3-opus': 'openai/gpt-oss-120b',
  'claude-3-sonnet': 'openai/gpt-oss-20b',
  'gemini-pro':      'deepseek-ai/deepseek-v4-pro',
  'deepseek-3':      'minimaxai/minimax-m2.7',
  // ... add more as needed
};

// Health check (for monitoring)
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'OpenAI-NIM Proxy', reasoning: SHOW_REASONING, thinking: THINK_MODE });
});

// List models endpoint (matches OpenAI's /v1/models format)
app.get('/v1/models', (req, res) => {
  const data = Object.keys(MODEL_MAP).map(key => ({
    id: key, object: 'model', created: Date.now(), owned_by: 'nvidia-nim-proxy'
  }));
  res.json({ object: 'list', data });
});

// Chat completions proxy
app.post('/v1/chat/completions', async (req, res) => {

  try {

    const {
      model,
      messages,
      stream,
      temperature,
      max_tokens
    } = req.body;

    const nimModel =
      MODEL_MAP[model] ||
      'meta/llama-3.1-70b-instruct';

    const upstream = await axios({
      method: 'post',
      url: `${NIM_API_BASE}/chat/completions`,
      responseType: 'stream',
      timeout: 0,
      headers: {
        Authorization: `Bearer ${NIM_API_KEY}`,
        'Content-Type': 'application/json'
      },
      data: {
        model: nimModel,
        messages,
        stream: true,
        temperature: temperature ?? 0.7,
        max_tokens: max_tokens ?? 512
      }
    });

    res.status(200);

    res.set({
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no'
    });

    res.flushHeaders?.();

    // IMPORTANT:
    // Send immediate chunk so Render knows stream is alive

    res.write(': connected\n\n');

    // heartbeat every 10s

    const heartbeat = setInterval(() => {
      res.write(': ping\n\n');
    }, 10000);

    upstream.data.on('data', chunk => {

      // flush immediately

      res.write(chunk);

    });

    upstream.data.on('end', () => {

      clearInterval(heartbeat);

      res.write('data: [DONE]\n\n');

      res.end();

    });

    upstream.data.on('error', err => {

      console.error('STREAM ERROR', err);

      clearInterval(heartbeat);

      res.end();

    });

    req.on('close', () => {

      clearInterval(heartbeat);

      upstream.data.destroy?.();

    });

  } catch (err) {

    console.error(err?.response?.data || err);

    if (!res.headersSent) {

      res.status(500).json({
        error: {
          message: err.message
        }
      });

    } else {

      res.end();

    }

  }

});
    // Trim message history if too long
    const recentMsgs = messages.slice(-12);

    // Determine NIM model ID
    let nimModel = MODEL_MAP[model];
    if (!nimModel) {
      // Try using the same name (if NIM has that model ID)
      try {
        const test = await axios.post(
          `${NIM_API_BASE}/chat/completions`,
          { model, messages: [{role: 'user', content: 'test'}], max_tokens: 1 },
          { headers: { 'Authorization': `Bearer ${NIM_API_KEY}` }, validateStatus: s => s < 500 }
        );
        if (test.status < 300) nimModel = model;
      } catch {}
    }
    // Fallback default if still unknown
    if (!nimModel) {
      const name = model.toLowerCase();
      if (name.includes('gpt-4') || name.includes('claude-opus') || name.includes('405b')) nimModel = 'meta/llama-3.1-405b-instruct';
      else if (name.includes('claude') || name.includes('gemini') || name.includes('70b')) nimModel = 'meta/llama-3.1-70b-instruct';
      else nimModel = 'meta/llama-3.1-8b-instruct';
    }

    // Build NIM request
    const nimReq = {
      model: nimModel,
      messages: recentMsgs,
      temperature: temperature ?? 0.6,
      max_tokens: Math.min(max_tokens ?? 512, 1024),
      stream: stream === true
    };
    if (THINK_MODE) nimReq.chat_template_kwargs = { thinking: true };

    const nimResp = await axios.post(
      `${NIM_API_BASE}/chat/completions`,
      nimReq,
      {
        headers: { 'Authorization': `Bearer ${NIM_API_KEY}`, 'Content-Type': 'application/json' },
        responseType: stream ? 'stream' : 'json',
        timeout: 30 * 60 * 1000  // 30m
      }
    );

    if (stream) {
      // Stream mode: pipe SSE to client
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      res.flushHeaders?.();

      // Send a heartbeat ping every 15s to avoid timeouts
      const ping = setInterval(() => { res.write(':ping\n\n'); }, 15000);

      nimResp.data.on('data', chunk => {
        const text = chunk.toString();
        for (const line of text.split('\n')) {
          if (!line.trim()) continue;
          if (line.startsWith('data:')) {
            const payload = line.replace(/^data: ?/, '');
            if (payload === '[DONE]') {
              res.write('data: [DONE]\n\n');
              continue;
            }
            try {
              const obj = JSON.parse(payload);
              // Handle reasoning display if enabled
              const delta = obj.choices?.[0]?.delta;
              if (SHOW_REASONING && delta?.reasoning_content) {
                const reasoning = delta.reasoning_content || '';
                const content = delta.content || '';
                // Insert <think> blocks around reasoning
                if (reasoning && !content) {
                  res.write(`data: ${JSON.stringify({choices:[{delta:{content:"<think>\\n"+reasoning}}]})}\n\n`);
                } else if (reasoning && content) {
                  res.write(`data: ${JSON.stringify({choices:[{delta:{content:reasoning+"</think>\\n\\n"+content}}]})}\n\n`);
                } else {
                  // no special handling if no reasoning
                  res.write(`data: ${JSON.stringify(obj)}\n\n`);
                }
              } else {
                // Normal chunk
                res.write(`data: ${JSON.stringify(obj)}\n\n`);
              }
            } catch (e) {
              // If JSON.parse fails, just forward raw
              res.write(`${line}\n`);
            }
          }
        }
      });
      nimResp.data.on('end', () => { clearInterval(ping); res.end(); });
      nimResp.data.on('error', err => { clearInterval(ping); console.error(err); res.end(); });
    } 
    else {
      // Non-streaming: wrap NIM response in OpenAI format
      const data = nimResp.data;
      const choices = data.choices.map(c => {
        let content = c.message?.content || '';
        if (SHOW_REASONING && c.message?.reasoning_content) {
          content = "<think>\n" + c.message.reasoning_content + "</think>\n\n" + content;
        }
        return { index: c.index, finish_reason: c.finish_reason, message: { role: c.message.role, content }};
      });
      res.json({
        id: `chatcmpl-${Date.now()}`,
        object: 'chat.completion',
        created: Math.floor(Date.now()/1000),
        model,
        usage: data.usage || {prompt_tokens:0, completion_tokens:0, total_tokens:0},
        choices
      });
    }
  } catch (err) {
    console.error('Proxy error:', err.message, err.response?.data);
    res.status(err.response?.status || 500).json({
      error: { message: err.message || 'Internal error', type: 'api_error', code: err.response?.status || 500 }
    });
  }
});

// Fallback for unknown endpoints
app.all('*', (req, res) => res.status(404).json({ error:{ message:`Endpoint ${req.path} not found`, code:404 } }));

// Start server
const server = app.listen(PORT, () => {
  console.log(`Proxy running on port ${PORT}`);
});
server.setTimeout(0);  // Disable default 2min timeout for long streams
server.timeout = 0;
server.keepAliveTimeout = 0;
server.headersTimeout = 0;

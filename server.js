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
        max_tokens: max_tokens ?? 1024
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

// Start server
const server = app.listen(PORT, () => {
  console.log(`Proxy running on port ${PORT}`);
});
server.setTimeout(0);  // Disable default 2min timeout for long streams
server.timeout = 0;
server.keepAliveTimeout = 0;
server.headersTimeout = 0;

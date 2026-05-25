const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

const NIM_API_BASE = process.env.NIM_API_BASE || 'https://integrate.api.nvidia.com/v1';
const NIM_API_KEY = process.env.NIM_API_KEY;

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));

const MODEL_MAP = {
  'gpt-3.5-turbo': 'nvidia/llama-3.1-nemotron-ultra-253b-v1',
  'gpt-4': 'qwen/qwen3-coder-480b-a35b-instruct',
  'gpt-4-turbo': 'moonshotai/kimi-k2-instruct-0905',
  'gpt-4o': 'z-ai/glm4.7',
  'gpt-3': 'deepseek-ai/deepseek-r1-distill-qwen-32b',
  'claude-3-opus': 'openai/gpt-oss-120b',
  'claude-3-sonnet': 'openai/gpt-oss-20b',
  'gemini-pro': 'deepseek-ai/deepseek-v4-pro'
};

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/v1/models', (req, res) => {
  res.json({
    object: 'list',
    data: Object.keys(MODEL_MAP).map(id => ({
      id,
      object: 'model',
      created: Date.now(),
      owned_by: 'nim-proxy'
    }))
  });
});

app.post('/v1/chat/completions', async (req, res) => {
  try {
    const { model, messages, temperature, max_tokens } = req.body;

    const nimModel = MODEL_MAP[model] || 'meta/llama-3.1-70b-instruct';

    const upstream = await axios.post(
      `${NIM_API_BASE}/chat/completions`,
      {
        model: nimModel,
        messages,
        stream: true,
        temperature: temperature ?? 0.7,
        max_tokens: max_tokens ?? 1024
      },
      {
        headers: {
          Authorization: `Bearer ${NIM_API_KEY}`,
          'Content-Type': 'application/json'
        },
        responseType: 'stream',
        timeout: 0
      }
    );

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders?.();

    res.write(': connected\n\n');

    // KEEP ALIVE (VERY IMPORTANT FOR RENDER + JANITOR)
    const heartbeat = setInterval(() => {
      res.write(': ping\n\n');
    }, 15000);

    upstream.data.on('data', (chunk) => {
      const text = chunk.toString();

      for (const line of text.split('\n')) {
        if (!line.startsWith('data:')) continue;

        const payload = line.replace('data: ', '').trim();

        if (payload === '[DONE]') continue;

        try {
          const json = JSON.parse(payload);
          res.write(`data: ${JSON.stringify(json)}\n\n`);
        } catch (e) {
          // ignore bad chunks
        }
      }
    });

    upstream.data.on('end', () => {
      clearInterval(heartbeat);
      res.write('data: [DONE]\n\n');
      res.end();
    });

    upstream.data.on('error', () => {
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
          message: err.message,
          type: 'proxy_error'
        }
      });
    } else {
      res.end();
    }
  }
});

app.listen(PORT, () => {
  console.log(`Proxy running on ${PORT}`);
});

// server.js
const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

const NIM_API_BASE =
  process.env.NIM_API_BASE || 'https://integrate.api.nvidia.com/v1';

const NIM_API_KEY = process.env.NIM_API_KEY;

if (!NIM_API_KEY) {
  console.error("❌ Missing NIM_API_KEY");
  process.exit(1);
}

const MODEL_MAP = {
  'gpt-3.5-turbo': 'meta/llama-3.1-8b-instruct',
  'gpt-4': 'meta/llama-3.1-70b-instruct',
  'gpt-4-turbo': 'meta/llama-3.1-70b-instruct',
  'gpt-4o': 'meta/llama-3.1-70b-instruct',
  'gpt-3': 'meta/llama-3.1-8b-instruct',
  'claude-3-opus': 'meta/llama-3.1-70b-instruct',
  'claude-3-sonnet': 'meta/llama-3.1-70b-instruct',
  'gemini-pro': 'meta/llama-3.1-8b-instruct',
  'deepseek-3': 'meta/llama-3.1-8b-instruct'
};

app.use(cors());
app.use(express.json({ limit: '50mb' }));

// Health
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

// Models
app.get('/v1/models', (req, res) => {
  res.json({
    object: 'list',
    data: Object.keys(MODEL_MAP).map(id => ({
      id,
      object: 'model'
    }))
  });
});

// Chat completions
app.post('/v1/chat/completions', async (req, res) => {
  try {
    const {
      model,
      messages,
      stream = true,
      temperature,
      max_tokens
    } = req.body;

    const nimModel =
      MODEL_MAP[model] || 'meta/llama-3.1-8b-instruct';

    const trimmedMessages = (messages || []).slice(-12);

    const response = await axios({
      method: 'post',
      url: `${NIM_API_BASE}/chat/completions`,
      responseType: stream ? 'stream' : 'json',
      timeout: 120000,
      headers: {
        Authorization: `Bearer ${NIM_API_KEY}`,
        'Content-Type': 'application/json'
      },
      data: {
        model: nimModel,
        messages: trimmedMessages,
        stream: true,
        temperature: temperature ?? 0.7,
        max_tokens: Math.min(max_tokens ?? 512, 1024)
      }
    });

    // STREAM MODE (JanitorAI safe)
    if (stream) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache, no-transform');
      res.setHeader('Connection', 'keep-alive');

      response.data.on('data', chunk => {
        res.write(chunk);
      });

      response.data.on('end', () => {
        res.write('data: [DONE]\n\n');
        res.end();
      });

      response.data.on('error', err => {
        console.error("STREAM ERROR:", err.message);
        res.end();
      });

      req.on('close', () => {
        response.data.destroy?.();
      });

      return;
    }

    // NON-STREAM MODE
    const data = response.data;

    res.json({
      id: `chatcmpl-${Date.now()}`,
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model: nimModel,
      choices: data.choices || []
    });

  } catch (err) {
    console.error("PROXY ERROR:", err.response?.data || err.message);

    res.status(err.response?.status || 500).json({
      error: {
        message: err.message,
        type: 'api_error',
        code: err.response?.status || 500
      }
    });
  }
});

app.all('*', (req, res) => {
  res.status(404).json({
    error: { message: 'Not found', code: 404 }
  });
});

app.listen(PORT, () => {
  console.log(`Proxy running on port ${PORT}`);
});

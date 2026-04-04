// ─────────────────────────────────────────────────────────────────────────────
// LogiLake SQL Agent — Netlify Function
// POST { question: string } → { sql: string }
// Requiere: OPENAI_API_KEY en variables de entorno de Netlify
// ─────────────────────────────────────────────────────────────────────────────

const SCHEMA_PROMPT = `
You are a SQL expert for a Brazilian e-commerce analytics dashboard (Olist dataset, 2016–2018).
Users ask questions in Spanish or English. Respond ONLY with a valid DuckDB SQL query.
No markdown, no code fences, no explanation — just the raw SQL.

Available tables:

kpi_global (1 row — global totals):
  total_orders INT, total_delivered INT, total_canceled INT,
  otif_rate_pct DOUBLE, avg_delivery_days_actual DOUBLE,
  total_revenue_brl DOUBLE, avg_order_value_brl DOUBLE,
  avg_review_score DOUBLE, avg_freight_ratio_pct DOUBLE

kpi_monthly (1 row per month, format YYYY-MM):
  order_month VARCHAR, orders INT, delivered INT, canceled INT,
  otif_rate_pct DOUBLE, avg_delivery_days DOUBLE, avg_delay_days DOUBLE,
  revenue_brl DOUBLE, avg_review_score DOUBLE, cancellation_rate_pct DOUBLE

kpi_category (1 row per product category):
  category VARCHAR, orders INT, revenue_brl DOUBLE,
  avg_order_value DOUBLE, avg_review_score DOUBLE, otif_rate_pct DOUBLE

kpi_nps (1 row per star rating 1–5):
  review_score_int INT, orders INT, avg_payment_value DOUBLE,
  avg_delivery_days DOUBLE, pct_of_total DOUBLE

kpi_seller_state (1 row per Brazilian state, 2-letter code):
  seller_state VARCHAR, orders INT, avg_delivery_days DOUBLE,
  avg_delay_days DOUBLE, otif_rate_pct DOUBLE,
  revenue_brl DOUBLE, avg_review_score DOUBLE

Rules:
- Return ONLY the raw SQL query, nothing else
- Use DuckDB syntax
- ROUND all numeric results to 2 decimal places
- For year from order_month: CAST(LEFT(order_month, 4) AS INT)
- Limit to 20 rows unless the user specifies otherwise
- Order results meaningfully (DESC for revenue/score/otif, ASC for delivery days)
`.trim();

const CORS_HEADERS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Content-Type': 'application/json',
};

exports.handler = async (event) => {
  // CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS_HEADERS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  try {
    const { question } = JSON.parse(event.body || '{}');

    if (!question?.trim()) {
      return { statusCode: 400, headers: CORS_HEADERS, body: JSON.stringify({ error: 'question is required' }) };
    }

    if (!process.env.OPENAI_API_KEY) {
      return {
        statusCode: 500, headers: CORS_HEADERS,
        body: JSON.stringify({ error: 'OPENAI_API_KEY no está configurada en Netlify. Ve a Site settings → Environment variables.' }),
      };
    }

    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model:       'gpt-4o',
        temperature: 0,
        max_tokens:  400,
        messages: [
          { role: 'system', content: SCHEMA_PROMPT },
          { role: 'user',   content: question },
        ],
      }),
    });

    if (!response.ok) {
      const err = await response.json().catch(() => ({}));
      throw new Error(err.error?.message || `OpenAI error ${response.status}`);
    }

    const data = await response.json();
    let sql = data.choices[0].message.content.trim();

    // Limpiar code fences si el modelo los incluye
    sql = sql.replace(/^```(?:sql)?\s*/i, '').replace(/\s*```$/, '').trim();

    return {
      statusCode: 200,
      headers:    CORS_HEADERS,
      body:       JSON.stringify({ sql }),
    };

  } catch (err) {
    return {
      statusCode: 500,
      headers:    CORS_HEADERS,
      body:       JSON.stringify({ error: err.message }),
    };
  }
};

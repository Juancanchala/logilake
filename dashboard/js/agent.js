// ─────────────────────────────────────────────────────────────────────────────
// LogiLake SQL Agent
// DuckDB WASM (motor SQL en el browser) + GPT-4o via Netlify Function
// ─────────────────────────────────────────────────────────────────────────────

const FUNCTION_URL = '/.netlify/functions/ask';
const DUCKDB_CDN   = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/+esm';

const CSV_TABLES = [
  'kpi_global', 'kpi_monthly', 'kpi_nps', 'kpi_category', 'kpi_seller_state',
];

const EXAMPLES = [
  '¿Qué categoría tuvo más revenue?',
  '¿Cuál es el OTIF promedio por año?',
  '¿Qué estado tiene mejor review score?',
  'Top 5 meses con más órdenes',
  '¿En qué mes hubo más cancelaciones?',
  '¿Cuántas órdenes se entregaron tarde?',
];

// ─── State ────────────────────────────────────────────────────────────────────
let conn    = null;
let dbReady = false;

// ─── UI helpers ───────────────────────────────────────────────────────────────
const $  = id => document.getElementById(id);

function setStatus(text, color = '#b4b2a9') {
  const el = $('agent-status');
  el.textContent = text;
  el.style.color  = color;
}

function setReady(ready) {
  $('agent-btn').disabled    = !ready;
  $('agent-input').disabled  = !ready;
  $('agent-btn').style.opacity = ready ? '1' : '0.4';
  $('agent-btn').style.cursor  = ready ? 'pointer' : 'default';
}

function showError(msg) {
  $('agent-result').style.display = 'none';
  $('agent-error').style.display  = 'block';
  $('agent-error').textContent    = msg;
}

function showResult(sql, rows, cols) {
  $('agent-error').style.display  = 'none';
  $('agent-result').style.display = 'block';
  $('agent-sql').textContent      = sql;

  if (!rows.length) {
    $('agent-table-wrap').innerHTML =
      '<p style="font-size:12px;color:#888780;margin-top:4px">Sin resultados.</p>';
    return;
  }

  const thStyle = 'text-align:left;padding:6px 10px;border-bottom:2px solid #e8e6e3;' +
                  'color:#888780;font-size:10px;text-transform:uppercase;white-space:nowrap';
  const tdStyle = 'padding:6px 10px;border-bottom:1px solid #f4f3f1;font-size:12px;color:#1a1a18';

  const thead = `<thead><tr>${cols.map(c =>
    `<th style="${thStyle}">${c}</th>`).join('')}</tr></thead>`;
  const tbody = `<tbody>${rows.map(row =>
    `<tr>${cols.map(c =>
      `<td style="${tdStyle}">${row[c] ?? '–'}</td>`).join('')}</tr>`
  ).join('')}</tbody>`;

  $('agent-table-wrap').innerHTML =
    `<table style="width:100%;border-collapse:collapse">${thead}${tbody}</table>`;
}

// ─── DuckDB WASM init ─────────────────────────────────────────────────────────
async function initDuckDB() {
  setStatus('Iniciando motor SQL...');

  const duckdb = await import(DUCKDB_CDN);

  const bundles = duckdb.getJsDelivrBundles();
  const bundle  = await duckdb.selectBundle(bundles);

  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}")`], { type: 'text/javascript' })
  );

  const worker = new Worker(workerUrl);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  const db     = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  conn = await db.connect();

  // Registrar y cargar los 5 CSVs como tablas SQL
  const base = window.location.origin;
  setStatus('Cargando tablas...');

  for (const name of CSV_TABLES) {
    const url = `${base}/data/${name}.csv`;
    await db.registerFileURL(
      `${name}.csv`, url,
      duckdb.DuckDBDataProtocol.HTTP, false
    );
    await conn.query(
      `CREATE TABLE "${name}" AS SELECT * FROM read_csv_auto('${name}.csv', header=true)`
    );
  }

  dbReady = true;
  setStatus('Motor SQL activo', '#3b6d11');
  setReady(true);
}

// ─── Ask ──────────────────────────────────────────────────────────────────────
async function ask(question) {
  question = question.trim();
  if (!question) return;
  if (!dbReady) { showError('El motor SQL aún está cargando, espera un momento.'); return; }

  setReady(false);
  $('agent-btn').textContent = '...';
  $('agent-error').style.display  = 'none';
  $('agent-result').style.display = 'none';

  try {
    // 1. GPT-4o genera el SQL via Netlify Function
    const res = await fetch(FUNCTION_URL, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ question }),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || `Error ${res.status} del servidor`);
    }

    const { sql } = await res.json();

    // 2. DuckDB ejecuta el SQL en el browser
    const result = await conn.query(sql);

    // 3. Convertir Arrow Table → rows planas
    const cols = result.schema.fields.map(f => f.name);
    const rows = result.toArray().map(row => {
      const obj = {};
      cols.forEach(c => { obj[c] = row[c] != null ? String(row[c]) : null; });
      return obj;
    });

    showResult(sql, rows, cols);

  } catch (err) {
    showError('Error: ' + err.message);
  } finally {
    $('agent-btn').textContent = 'Preguntar';
    setReady(true);
  }
}

// ─── Example chips ────────────────────────────────────────────────────────────
function buildChips() {
  const container = $('agent-chips');
  EXAMPLES.forEach(ex => {
    const btn = document.createElement('button');
    btn.textContent   = ex;
    btn.style.cssText =
      'font-size:11px;color:#5f5e5a;background:#f4f3f1;border:1px solid #e8e6e3;' +
      'border-radius:6px;padding:5px 10px;cursor:pointer;transition:all 0.15s';
    btn.addEventListener('mouseover', () => { btn.style.background = '#1a1a18'; btn.style.color = '#fff'; });
    btn.addEventListener('mouseout',  () => { btn.style.background = '#f4f3f1'; btn.style.color = '#5f5e5a'; });
    btn.addEventListener('click', () => {
      $('agent-input').value = ex;
      ask(ex);
    });
    container.appendChild(btn);
  });
}

// ─── Bind events & boot ───────────────────────────────────────────────────────
$('agent-btn').addEventListener('click', () => ask($('agent-input').value));
$('agent-input').addEventListener('keydown', e => {
  if (e.key === 'Enter') ask($('agent-input').value);
});

buildChips();
initDuckDB().catch(err => {
  setStatus('Error al cargar DuckDB: ' + err.message, '#e24b4a');
  console.error(err);
});

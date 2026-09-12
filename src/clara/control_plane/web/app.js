/* Clara console.
 *
 * Vanilla JS on purpose: no build step, no bundler, no node_modules. The whole
 * console is three static files served by the same process as the API, so
 * `clara serve` is the entire deployment.
 *
 * Everything here goes through the public REST API — there is no private
 * endpoint the UI uses that a script could not. */

'use strict';

const API = '/api/v1';

/* ── state ──────────────────────────────────────────────────────────────── */

const store = {
  apiKey: localStorage.getItem('clara.apiKey') || '',
  connectors: [],
  selected: null,          // chosen connector spec
  config: {},              // its configuration
  streams: [],             // discovered streams
  chosen: new Map(),       // stream name -> options
  models: [],              // authored models
  step: 1,
  runPoll: null,
  currentRun: null,
};

/* ── helpers ────────────────────────────────────────────────────────────── */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

/** Escape text for HTML interpolation. Connector output is untrusted. */
function esc(value) {
  if (value === null || value === undefined) return '';
  return String(value).replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
}

function fmtNum(n) {
  if (n === null || n === undefined) return '—';
  return Number(n).toLocaleString();
}

function fmtBytes(n) {
  if (!n) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0, v = Number(n);
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function fmtUsd(n) {
  const v = Number(n || 0);
  return v > 0 && v < 0.01 ? `$${v.toFixed(5)}` : `$${v.toFixed(2)}`;
}

function toast(message, kind = '') {
  const el = $('#toast');
  el.textContent = message;
  el.className = `toast${kind ? ' is-' + kind : ''}`;
  el.hidden = false;
  clearTimeout(el._timer);
  el._timer = setTimeout(() => { el.hidden = true; }, 4200);
}

function statusBadge(status) {
  return `<span class="status status-${esc(status)}">${esc(status)}</span>`;
}

function setResult(selector, message, kind) {
  const el = $(selector);
  el.textContent = message;
  el.className = `result${kind ? ' is-' + kind : ''}`;
}

/** Render an array of row objects as a table. */
function renderTable(columns, rows) {
  if (!rows || !rows.length) return '<div class="empty">No rows.</div>';
  const head = columns.map((c) => `<th>${esc(c)}</th>`).join('');
  const body = rows.map((row) => {
    const cells = columns.map((c) => {
      const v = Array.isArray(row) ? row[columns.indexOf(c)] : row[c];
      const numeric = typeof v === 'number';
      return `<td class="${numeric ? 'num' : ''}">${esc(v)}</td>`;
    }).join('');
    return `<tr>${cells}</tr>`;
  }).join('');
  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

/* ── API client ─────────────────────────────────────────────────────────── */

async function api(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
  if (store.apiKey) headers.Authorization = `Bearer ${store.apiKey}`;

  const response = await fetch(`${API}${path}`, { ...options, headers });
  const text = await response.text();
  let payload = null;
  try { payload = text ? JSON.parse(text) : null; } catch { payload = { raw: text }; }

  if (!response.ok) {
    if (response.status === 401) {
      openKeyDialog();
      throw new Error('API key required');
    }
    const detail = payload?.error?.message || payload?.detail?.message
      || payload?.detail || response.statusText;
    throw new Error(typeof detail === 'string' ? detail : JSON.stringify(detail));
  }
  return payload;
}

const get = (path) => api(path);
const post = (path, body) => api(path, { method: 'POST', body: JSON.stringify(body ?? {}) });
const put = (path, body) => api(path, { method: 'PUT', body: JSON.stringify(body ?? {}) });

/* ── navigation ─────────────────────────────────────────────────────────── */

const loaders = {
  dashboard: loadDashboard,
  builder: loadBuilder,
  data: loadData,
  sql: () => {},
  runs: loadRuns,
  cost: loadCost,
};

function show(view) {
  $$('.nav-item').forEach((b) => b.classList.toggle('is-active', b.dataset.view === view));
  $$('.view').forEach((v) => v.classList.toggle('is-active', v.id === `view-${view}`));
  location.hash = view;
  (loaders[view] || (() => {}))();
}

function gotoStep(n) {
  store.step = n;
  $$('.step').forEach((el) => {
    const s = Number(el.dataset.step);
    el.classList.toggle('is-active', s === n);
    el.classList.toggle('is-done', s < n);
  });
  $$('.panel').forEach((el) => el.classList.toggle('is-active', Number(el.dataset.panel) === n));
  if (n === 3) renderTablePlan();
  if (n === 6) renderReview();
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

/* ── overview ───────────────────────────────────────────────────────────── */

async function loadDashboard() {
  let data;
  try { data = await get('/dashboard'); }
  catch (err) { toast(err.message, 'bad'); return; }

  $('#env-pill').textContent =
    `${data.health.environment} · ${data.health.provider} · ${data.health.engines.join('+')}`;
  $('#brand-sub').textContent = `v${data.health.version} · ${data.health.plan} plan`;
  $('#dash-project').textContent = data.spec.project || '';

  const usage = data.usage;
  $('#stat-grid').innerHTML = [
    stat('Tables', fmtNum(data.table_count), `${fmtNum(data.total_rows)} rows`),
    stat('Stored', fmtBytes(data.total_bytes), 'Iceberg / Parquet', 'green'),
    stat('Compute used', `${fmtNum(usage.compute_ccu_minutes)}`, 'CCU-minutes this month'),
    stat('Infra cost', fmtUsd(usage.infra_cost_usd), 'at provider rates', 'yellow'),
    stat('Projected', fmtUsd(data.forecast.projected_usd), 'month-end total', 'yellow'),
  ].join('');

  $('#dash-lineage').innerHTML = renderBatches(data.spec.execution_batches);
  $('#dash-runs').innerHTML = data.runs.length
    ? data.runs.map(runRow).join('')
    : '<div class="empty">No runs yet. Build a pipeline, then run it.</div>';
  $$('#dash-runs .list-item').forEach((el) =>
    el.addEventListener('click', () => { show('runs'); selectRun(el.dataset.id); }));

  $('#dash-table-note').textContent = data.table_count
    ? `${data.table_count} tables` : 'none yet';
  $('#dash-tables').innerHTML = data.tables.length
    ? `<div class="table-scroll">${renderTable(
        ['table', 'format', 'rows', 'size', 'columns'],
        data.tables.map((t) => ({
          table: t.fqn, format: t.format,
          rows: t.row_count === null ? '—' : t.row_count,
          size: fmtBytes(t.size_bytes), columns: t.columns,
        })))}</div>`
    : '<div class="empty">No tables yet.</div>';
}

function stat(label, value, sub, kind) {
  return `<div class="stat${kind ? ' is-' + kind : ''}">
    <div class="stat-label">${esc(label)}</div>
    <div class="stat-value">${esc(value)}</div>
    <div class="stat-sub">${esc(sub)}</div></div>`;
}

function runRow(run) {
  return `<div class="list-item" data-id="${esc(run.id)}">
    <div class="list-item-main">
      <div class="list-item-title">${esc(run.pipeline)} · ${esc(run.trigger)}</div>
      <div class="list-item-sub">${esc(run.started_at || 'queued')} · ${esc(run.duration)}
        · ${fmtNum(run.records_synced)} rows · ${run.models_built} models</div>
    </div>${statusBadge(run.status)}</div>`;
}

function renderBatches(batches) {
  if (!batches || !batches.length) {
    return '<div class="empty">No pipeline defined yet.</div>';
  }
  return batches.map((batch, i) => `<div class="lineage-row">
    <span class="lineage-label">step ${i + 1}</span>
    ${batch.map((n) => `<span class="node ${nodeClass(n)}">${esc(n)}</span>`).join('')}
  </div>`).join('');
}

function nodeClass(name) {
  if (name.startsWith('ingest:')) return 'node-ingest';
  if (name.startsWith('model:')) return 'node-model';
  return 'node-maint';
}

/* ── builder: step 1, connectors ────────────────────────────────────────── */

async function loadBuilder() {
  if (store.connectors.length) return;
  try {
    const data = await get('/connectors');
    store.connectors = data.sources;
    $('#connector-grid').innerHTML = store.connectors.map((c) => `
      <button type="button" class="connector" data-name="${esc(c.name)}">
        <div class="connector-name">${esc(c.title)}</div>
        <div class="connector-meta">${esc(c.name)}</div>
        ${c.supports_incremental ? '<span class="tag">INCREMENTAL</span>' : ''}
      </button>`).join('');
    $$('#connector-grid .connector').forEach((el) =>
      el.addEventListener('click', () => selectConnector(el.dataset.name)));
  } catch (err) { toast(err.message, 'bad'); }
}

function selectConnector(name) {
  const spec = store.connectors.find((c) => c.name === name);
  if (!spec) return;
  store.selected = spec;
  store.config = {};
  store.streams = [];
  store.chosen.clear();

  $$('#connector-grid .connector').forEach((el) =>
    el.classList.toggle('is-selected', el.dataset.name === name));

  $('#config-card').hidden = false;
  $('#config-title').textContent = `Configure ${spec.title}`;
  $('#config-form').innerHTML = renderConfigForm(spec);
  setResult('#check-result', '', '');
  $('#to-2').disabled = true;
}

/** Build a form from the connector's JSON-schema config — no per-connector UI code. */
function renderConfigForm(spec) {
  const properties = spec.config_schema?.properties || {};
  const required = spec.config_schema?.required || [];
  const keys = Object.keys(properties);
  if (!keys.length) return '<p class="note">This connector needs no configuration.</p>';

  return keys.map((key) => {
    const field = properties[key];
    const isRequired = required.includes(key);
    const secret = spec.secret_fields.includes(key) || field.airbyte_secret;
    const label = `${esc(field.title || key)}${isRequired ? ' *' : ''}`;
    const value = field.default !== undefined ? esc(field.default) : '';

    let input;
    if (field.enum) {
      input = `<select name="${esc(key)}">${field.enum.map((o) =>
        `<option value="${esc(o)}"${o === field.default ? ' selected' : ''}>${esc(o)}</option>`
      ).join('')}</select>`;
    } else if (field.type === 'integer' || field.type === 'number') {
      input = `<input type="number" name="${esc(key)}" value="${value}">`;
    } else if (field.type === 'array' || field.type === 'object') {
      const placeholder = field.type === 'array' ? 'comma,separated' : '{"key": "value"}';
      const dflt = Array.isArray(field.default) ? field.default.join(',') : '';
      input = `<input type="text" name="${esc(key)}" value="${esc(dflt)}"
                 placeholder="${esc(placeholder)}" data-kind="${esc(field.type)}">`;
    } else {
      input = `<input type="${secret ? 'password' : 'text'}" name="${esc(key)}" value="${value}">`;
    }
    const hint = field.description ? `<span class="field-hint">${esc(field.description)}</span>` : '';
    return `<label>${label}${input}${hint}</label>`;
  }).join('');
}

/** Read the config form, coercing values to the schema's types. */
function readConfig() {
  const spec = store.selected;
  const properties = spec.config_schema?.properties || {};
  const config = {};

  $$('#config-form [name]').forEach((el) => {
    const key = el.name;
    const raw = el.value.trim();
    if (raw === '') return;
    const field = properties[key] || {};
    if (field.type === 'integer') config[key] = parseInt(raw, 10);
    else if (field.type === 'number') config[key] = parseFloat(raw);
    else if (field.type === 'array') config[key] = raw.split(',').map((s) => s.trim()).filter(Boolean);
    else if (field.type === 'object') { try { config[key] = JSON.parse(raw); } catch { config[key] = {}; } }
    else config[key] = raw;
  });
  store.config = config;
  return config;
}

async function checkConnection() {
  readConfig();
  setResult('#check-result', 'testing…', 'busy');
  try {
    const result = await post(`/connectors/${store.selected.name}/check`, { config: store.config });
    if (result.succeeded) {
      const streams = result.detected_streams != null ? ` · ${result.detected_streams} streams` : '';
      setResult('#check-result', `connected — ${result.message}${streams}`, 'ok');
      $('#to-2').disabled = false;
    } else {
      setResult('#check-result', result.message || 'connection failed', 'bad');
      $('#to-2').disabled = true;
    }
  } catch (err) {
    setResult('#check-result', err.message, 'bad');
    $('#to-2').disabled = true;
  }
}

/* ── builder: step 2, streams ───────────────────────────────────────────── */

async function discoverStreams() {
  $('#stream-list').innerHTML = '<div class="empty">Discovering streams…</div>';
  gotoStep(2);
  try {
    const data = await post(`/connectors/${store.selected.name}/discover`, { config: store.config });
    store.streams = data.streams;
    renderStreams();
  } catch (err) {
    $('#stream-list').innerHTML = `<div class="empty">${esc(err.message)}</div>`;
    toast(err.message, 'bad');
  }
}

function renderStreams() {
  if (!store.streams.length) {
    $('#stream-list').innerHTML = '<div class="empty">This source exposed no streams.</div>';
    return;
  }
  $('#stream-list').innerHTML = store.streams.map((s) => {
    const keys = (s.source_defined_primary_key || []).flat();
    const cursors = s.default_cursor_field || [];
    const columns = (s.columns || []).map((c) => c.name);
    return `<div class="stream" data-name="${esc(s.name)}">
      <div class="stream-top">
        <label class="checkbox"><input type="checkbox" class="stream-on"> </label>
        <span class="stream-name">${esc(s.name)}</span>
        <span class="muted">${columns.length} columns</span>
        ${s.supports_incremental ? '<span class="tag">INCREMENTAL</span>' : ''}
        <span style="flex:1"></span>
        <button type="button" class="btn btn-sm btn-ghost stream-preview">Preview rows</button>
      </div>
      <div class="stream-opts">
        <label>Sync mode
          <select class="stream-mode">
            <option value="full_refresh">full refresh — reload every run</option>
            ${s.supports_incremental
              ? '<option value="incremental" selected>incremental — only new rows</option>' : ''}
          </select></label>
        <label>Cursor column
          <select class="stream-cursor">
            <option value="">(none)</option>
            ${columns.map((c) => `<option value="${esc(c)}"${cursors.includes(c) ? ' selected' : ''}>${esc(c)}</option>`).join('')}
          </select></label>
        <label>Primary key
          <select class="stream-key">
            <option value="">(none)</option>
            ${columns.map((c) => `<option value="${esc(c)}"${keys.includes(c) ? ' selected' : ''}>${esc(c)}</option>`).join('')}
          </select></label>
        <label>Destination table
          <input type="text" class="stream-table" value="${esc(s.name)}"></label>
      </div></div>`;
  }).join('');

  $$('#stream-list .stream').forEach((el) => {
    const toggle = $('.stream-on', el);
    toggle.addEventListener('change', () => {
      el.classList.toggle('is-on', toggle.checked);
      syncChosen();
    });
    $$('select, input', el).forEach((input) =>
      input.addEventListener('change', syncChosen));
    $('.stream-preview', el).addEventListener('click', () => previewStream(el.dataset.name));
    // Preselect everything: the common case is "sync all of it".
    toggle.checked = true;
    el.classList.add('is-on');
  });
  syncChosen();
}

function syncChosen() {
  store.chosen.clear();
  $$('#stream-list .stream').forEach((el) => {
    if (!$('.stream-on', el).checked) return;
    const mode = $('.stream-mode', el).value;
    store.chosen.set(el.dataset.name, {
      name: el.dataset.name,
      sync_mode: mode,
      cursor_field: mode === 'incremental' ? ($('.stream-cursor', el).value || null) : null,
      primary_key: $('.stream-key', el).value || null,
      table: $('.stream-table', el).value.trim() || el.dataset.name,
    });
  });
  $('#to-3').disabled = store.chosen.size === 0;
}

async function previewStream(name) {
  $('#preview-card').hidden = false;
  $('#preview-note').textContent = `${name} — loading…`;
  $('#stream-preview').innerHTML = '';
  try {
    const data = await post(`/connectors/${store.selected.name}/preview`,
      { config: store.config, stream: name, limit: 10 });
    $('#preview-note').textContent = `${name} — ${data.row_count} sample rows`;
    $('#stream-preview').innerHTML = renderTable(data.columns, data.rows);
  } catch (err) {
    $('#preview-note').textContent = err.message;
    toast(err.message, 'bad');
  }
}

/* ── builder: step 3, raw tables ────────────────────────────────────────── */

function renderTablePlan() {
  const ns = $('#raw-namespace').value.trim() || 'raw';
  const rows = Array.from(store.chosen.values()).map((s) => ({
    stream: s.name,
    table: `${ns}.${s.table}`,
    mode: s.sync_mode,
    write: s.sync_mode === 'incremental' && s.primary_key ? 'merge on key' : (s.sync_mode === 'incremental' ? 'append' : 'replace'),
    key: s.primary_key || '—',
    cursor: s.cursor_field || '—',
  }));
  $('#table-plan').innerHTML = rows.length
    ? `<div class="table-scroll">${renderTable(
        ['stream', 'table', 'mode', 'write', 'key', 'cursor'], rows)}</div>`
    : '<div class="empty">No streams selected.</div>';
}

/* ── builder: step 4, transform ─────────────────────────────────────────── */

function suggestModel() {
  const first = Array.from(store.chosen.values())[0];
  if (!first) { toast('Select a stream first', 'bad'); return; }
  const ns = $('#raw-namespace').value.trim() || 'raw';
  const stream = store.streams.find((s) => s.name === first.name);
  const columns = (stream?.columns || []).map((c) => c.name);

  const numeric = (stream?.columns || [])
    .filter((c) => ['long', 'int', 'double', 'float', 'decimal'].includes(c.type))
    .map((c) => c.name);
  const groupable = columns.find((c) => !numeric.includes(c)) || columns[0] || '*';
  const measure = numeric.find((c) => !/id$/i.test(c)) || null;

  $('#model-name').value = `${first.table}_summary`;
  $('#model-sql').value = measure
    ? `SELECT\n  ${groupable},\n  count(*) AS records,\n  round(sum(${measure}), 2) AS total_${measure}\nFROM {{ source('${ns}', '${first.table}') }}\nGROUP BY ${groupable}\nORDER BY total_${measure} DESC`
    : `SELECT\n  ${groupable},\n  count(*) AS records\nFROM {{ source('${ns}', '${first.table}') }}\nGROUP BY ${groupable}\nORDER BY records DESC`;
  toast('Draft model written — preview it, then add', 'ok');
}

async function previewModelSql() {
  const sql = $('#model-sql').value.trim();
  if (!sql) { toast('Write some SQL first', 'bad'); return; }
  setResult('#sql-result', 'running…', 'busy');
  $('#sql-preview').innerHTML = '';
  try {
    const data = await post('/query/preview', {
      sql, limit: 20,
      raw_namespace: $('#raw-namespace').value.trim() || 'raw',
      analytics_namespace: $('#analytics-namespace').value.trim() || 'analytics',
    });
    setResult('#sql-result', `${data.row_count} rows via ${data.engine}`, 'ok');
    $('#sql-preview').innerHTML = renderTable(data.columns, data.rows);
  } catch (err) {
    setResult('#sql-result', err.message, 'bad');
  }
}

function addModel() {
  const name = $('#model-name').value.trim();
  const sql = $('#model-sql').value.trim();
  if (!name || !sql) { toast('Model needs a name and SQL', 'bad'); return; }

  const model = {
    name,
    sql,
    materialization: $('#model-mat').value,
    unique_key: $('#model-key').value.trim() || null,
    description: '',
    tests: [],
  };
  const existing = store.models.findIndex((m) => m.name === name);
  if (existing >= 0) store.models[existing] = model; else store.models.push(model);

  renderModels();
  $('#model-name').value = '';
  $('#model-sql').value = '';
  $('#model-key').value = '';
  $('#sql-preview').innerHTML = '';
  setResult('#sql-result', '', '');
  toast(`Model "${name}" added`, 'ok');
}

function renderModels() {
  $('#model-count').textContent = store.models.length
    ? `${store.models.length} model(s)` : 'none yet';
  $('#model-list').innerHTML = store.models.length
    ? store.models.map((m, i) => `<div class="model-row">
        <div class="model-row-main">
          <div class="list-item-title">${esc(m.name)}
            <span class="muted">· ${esc(m.materialization)}</span></div>
          <div class="list-item-sub"><code>${esc(m.sql.split('\n')[0].slice(0, 90))}…</code></div>
        </div>
        <button class="btn btn-sm" data-edit="${i}">Edit</button>
        <button class="btn btn-sm btn-ghost" data-del="${i}">Remove</button></div>`).join('')
    : '<div class="empty">No models yet. A pipeline can also be ingest-only.</div>';

  $$('#model-list [data-del]').forEach((b) => b.addEventListener('click', () => {
    store.models.splice(Number(b.dataset.del), 1); renderModels();
  }));
  $$('#model-list [data-edit]').forEach((b) => b.addEventListener('click', () => {
    const m = store.models[Number(b.dataset.edit)];
    $('#model-name').value = m.name;
    $('#model-sql').value = m.sql;
    $('#model-mat').value = m.materialization;
    $('#model-key').value = m.unique_key || '';
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }));
}

/* ── builder: step 6, review & save ─────────────────────────────────────── */

function draftPayload() {
  return {
    project: $('#project-name').value.trim() || 'my_pipeline',
    description: 'Created in the Clara console',
    sources: store.selected ? [{
      name: store.selected.name,
      connector: store.selected.name,
      config: store.config,
      namespace: $('#raw-namespace').value.trim() || 'raw',
      streams: Array.from(store.chosen.values()).map((s) => ({
        name: s.name,
        sync_mode: s.sync_mode,
        ...(s.cursor_field ? { cursor_field: s.cursor_field } : {}),
        ...(s.primary_key ? { primary_key: s.primary_key } : {}),
        ...(s.table ? { table: s.table } : {}),
      })),
    }] : [],
    models: store.models,
    schedule: $('#schedule-select').value || null,
    warehouse_size: $('#warehouse-size').value,
    raw_namespace: $('#raw-namespace').value.trim() || 'raw',
    analytics_namespace: $('#analytics-namespace').value.trim() || 'analytics',
  };
}

function renderReview() {
  const draft = draftPayload();
  const ns = draft.raw_namespace;
  const rows = [
    ['Project', draft.project],
    ['Source', store.selected ? store.selected.title : '(none)'],
    ['Streams', Array.from(store.chosen.values()).map((s) => `${ns}.${s.table} (${s.sync_mode})`).join(', ') || '—'],
    ['Models', draft.models.map((m) => `${m.name} (${m.materialization})`).join(', ') || '—'],
    ['Schedule', draft.schedule || 'manual only'],
    ['Warehouse', draft.warehouse_size.toUpperCase()],
  ];
  $('#review-summary').innerHTML = `<table><tbody>${rows.map(([k, v]) =>
    `<th style="width:150px">${esc(k)}</th><td style="white-space:normal">${esc(v)}</td></tr>`
      .replace('<th', '<tr><th')).join('')}</tbody></table>`;
  $('#review-plan').innerHTML = '<div class="empty">Save the pipeline to compute its plan.</div>';
}

async function savePipeline(andRun) {
  setResult('#save-result', 'saving…', 'busy');
  try {
    const result = await post('/pipeline/build', draftPayload());
    $('#review-plan').innerHTML = renderBatches(result.plan.batches);
    setResult('#save-result', `saved to ${result.path || 'clara.yaml'}`, 'ok');
    toast('Pipeline saved', 'ok');

    if (andRun) {
      const run = await post('/runs', { trigger: 'console' });
      toast('Run started', 'ok');
      show('runs');
      selectRun(run.id);
      startRunPolling(run.id);
    }
  } catch (err) {
    setResult('#save-result', err.message, 'bad');
    toast(err.message, 'bad');
  }
}

/* ── data browser ───────────────────────────────────────────────────────── */

async function loadData() {
  try {
    const data = await get('/catalog/tables');
    $('#data-tables').innerHTML = data.tables.length
      ? data.tables.map((t) => `<div class="list-item" data-ns="${esc(t.namespace)}"
            data-name="${esc(t.name)}">
          <div class="list-item-main">
            <div class="list-item-title">${esc(t.fqn)}</div>
            <div class="list-item-sub">${t.row_count === null ? t.format : fmtNum(t.row_count) + ' rows'}
              · ${fmtBytes(t.size_bytes)} · ${t.columns} cols</div>
          </div></div>`).join('')
      : '<div class="empty">No tables yet.</div>';

    $$('#data-tables .list-item').forEach((el) => el.addEventListener('click', () => {
      $$('#data-tables .list-item').forEach((o) => o.classList.remove('is-selected'));
      el.classList.add('is-selected');
      openTable(el.dataset.ns, el.dataset.name);
    }));
  } catch (err) { toast(err.message, 'bad'); }
}

async function openTable(namespace, name) {
  $('#data-detail-title').textContent = `${namespace}.${name}`;
  $('#data-detail-note').textContent = 'loading…';
  try {
    const [detail, preview] = await Promise.all([
      get(`/catalog/tables/${namespace}/${name}`),
      get(`/catalog/tables/${namespace}/${name}/preview?limit=50`),
    ]);
    $('#data-detail-note').textContent =
      `${detail.format} · ${detail.row_count === null ? '—' : fmtNum(detail.row_count)} rows · ${fmtBytes(detail.size_bytes)}`;
    $('#data-schema').innerHTML = `<div class="table-scroll">${renderTable(
      ['column', 'type', 'nullable'],
      detail.schema.fields.map((f) => ({ column: f.name, type: f.type, nullable: f.nullable })))}</div>`;
    $('#data-rows').innerHTML = `<h2 style="margin:14px 0 8px">Rows</h2>${
      renderTable(preview.columns, preview.rows)}`;
  } catch (err) {
    $('#data-detail-note').textContent = err.message;
  }
}

/* ── SQL editor ─────────────────────────────────────────────────────────── */

async function runSql(explainOnly) {
  const sql = $('#sql-input').value.trim();
  if (!sql) return;
  setResult('#sql-meta', explainOnly ? 'planning…' : 'running…', 'busy');
  try {
    const data = await post('/query', { sql, max_rows: 500, explain_only: explainOnly });
    if (explainOnly) {
      setResult('#sql-meta',
        `${data.routing.engine} — ${data.routing.reason} · est. ${fmtUsd(data.cost.total_usd)}`, 'ok');
      $('#sql-output').innerHTML = `<pre class="logs">${esc(data.plan)}</pre>`;
      return;
    }
    setResult('#sql-meta',
      `${fmtNum(data.row_count)} rows · ${data.stats.duration} · ${data.routing.engine}`
      + ` (${data.routing.reason}) · ${fmtUsd(data.cost.infrastructure_usd)}`, 'ok');
    $('#sql-output').innerHTML = renderTable(data.columns, data.rows);
  } catch (err) {
    setResult('#sql-meta', err.message, 'bad');
    $('#sql-output').innerHTML = '';
  }
}

/* ── runs ───────────────────────────────────────────────────────────────── */

async function loadRuns() {
  try {
    const data = await get('/runs?limit=30');
    $('#runs-list').innerHTML = data.runs.length
      ? data.runs.map(runRow).join('')
      : '<div class="empty">No runs yet.</div>';
    $$('#runs-list .list-item').forEach((el) =>
      el.addEventListener('click', () => selectRun(el.dataset.id)));
    if (store.currentRun) markSelectedRun(store.currentRun);
  } catch (err) { toast(err.message, 'bad'); }
}

function markSelectedRun(id) {
  $$('#runs-list .list-item').forEach((el) =>
    el.classList.toggle('is-selected', el.dataset.id === id));
}

async function selectRun(id) {
  store.currentRun = id;
  markSelectedRun(id);
  try {
    const run = await get(`/runs/${id}`);
    $('#run-detail-title').textContent = `${run.pipeline} · ${run.trigger}`;
    $('#run-detail-note').textContent =
      `${run.status} · ${run.duration} · ${fmtNum(run.records_synced)} rows · ${run.models_built} models`;

    $('#run-tasks').innerHTML = run.tasks.length
      ? `<div class="table-scroll">${renderTable(
          ['task', 'kind', 'status', 'duration', 'detail'],
          run.tasks.map((t) => ({
            task: t.name, kind: t.kind, status: t.status, duration: t.duration,
            detail: t.error || describeOutput(t.output),
          })))}</div>`
      : '<div class="empty">No tasks recorded yet.</div>';

    const logs = run.tasks.flatMap((t) => (t.logs || []).map((l) => `[${t.name}] ${l}`));
    $('#run-logs').hidden = !logs.length;
    $('#run-logs').textContent = logs.join('\n');

    if (!['succeeded', 'failed', 'cancelled', 'skipped'].includes(run.status)) {
      startRunPolling(id);
    } else {
      stopRunPolling();
      loadRuns();
    }
  } catch (err) { toast(err.message, 'bad'); }
}

function describeOutput(output) {
  if (!output) return '';
  if (output.records != null) return `${fmtNum(output.records)} rows → ${(output.tables || []).join(', ')}`;
  if (output.rows != null) return `${fmtNum(output.rows)} rows → ${output.table || ''}`;
  if (output.optimized) return `compacted ${output.optimized.length} table(s)`;
  return '';
}

function startRunPolling(id) {
  stopRunPolling();
  store.runPoll = setInterval(() => selectRun(id), 1200);
}

function stopRunPolling() {
  if (store.runPoll) { clearInterval(store.runPoll); store.runPoll = null; }
}

async function triggerRun() {
  try {
    const run = await post('/runs', {
      trigger: 'console',
      full_refresh: $('#runs-full-refresh')?.checked || false,
    });
    toast('Run started', 'ok');
    show('runs');
    selectRun(run.id);
  } catch (err) { toast(err.message, 'bad'); }
}

/* ── cost ───────────────────────────────────────────────────────────────── */

async function loadCost() {
  try {
    const [forecast, usage, providers, compare] = await Promise.all([
      get('/billing/forecast'), get('/usage'),
      get('/providers/compare'), post('/billing/estimate', { monthly_ccu_hours: 1000 }),
    ]);

    $('#cost-stats').innerHTML = [
      stat('Month to date', fmtUsd(forecast.to_date_usd), 'all layers'),
      stat('Projected', fmtUsd(forecast.projected_usd), forecast.projection_method, 'yellow'),
      stat('Infrastructure', fmtUsd(forecast.infrastructure_to_date_usd), 'at cost, no markup', 'green'),
      stat('Platform fee', fmtUsd(forecast.platform_fee_to_date_usd), "Clara's share"),
    ].join('');
    $('#cost-period').textContent = `${forecast.period_start?.slice(0, 10)} → ${forecast.period_end?.slice(0, 10)}`;

    const quantities = usage.quantities || {};
    const costs = usage.infra_cost_usd || {};
    $('#cost-usage').innerHTML = Object.keys(quantities).length
      ? `<div class="table-scroll">${renderTable(['meter', 'quantity', 'infra cost'],
          Object.entries(quantities).map(([meter, q]) => ({
            meter, quantity: fmtNum(Number(q).toFixed(2)),
            'infra cost': fmtUsd(costs[meter] || 0),
          })))}</div>`
      : '<div class="empty">No usage recorded yet.</div>';

    $('#cost-providers').innerHTML = `<div class="table-scroll">${renderTable(
      ['provider', 'medium warehouse / hour', 'storage $/GB-mo', 'egress $/GB', 'free CCU-min/mo'],
      providers.map((p) => ({
        provider: p.display_name,
        'medium warehouse / hour': fmtUsd(p.medium_warehouse_hour),
        'storage $/GB-mo': p.rates.storage_gb_month,
        'egress $/GB': p.rates.egress_gb,
        'free CCU-min/mo': fmtNum(p.free_tier.ccu_minutes_month),
      })))}</div>`;

    $('#cost-compare').innerHTML = `
      <p class="note" style="margin-bottom:10px">Clara on <strong>${esc(compare.provider)}</strong>:
        ${fmtUsd(compare.clara.all_in_usd_per_ccu_hour)} per CCU-hour all in
        (${fmtUsd(compare.clara.monthly_total_usd)}/month).</p>
      <div class="table-scroll">${renderTable(
        ['platform', '$ / CCU-hour', '$ / month', 'Clara cheaper by'],
        compare.competitors.map((c) => ({
          platform: c.platform, '$ / CCU-hour': fmtUsd(c.usd_per_ccu_hour),
          '$ / month': fmtUsd(c.monthly_usd),
          'Clara cheaper by': c.clara_is_cheaper_by ? `${c.clara_is_cheaper_by}×` : '—',
        })))}</div>
      <p class="note">${esc(compare.assumptions)}</p>`;

    loadInvoice();
  } catch (err) { toast(err.message, 'bad'); }
}

async function loadInvoice() {
  try {
    const data = await get('/billing/invoice');
    $('#cost-invoice').textContent = data.text;
  } catch (err) { $('#cost-invoice').textContent = err.message; }
}

/* ── API key dialog ─────────────────────────────────────────────────────── */

function openKeyDialog() {
  $('#key-input').value = store.apiKey;
  $('#key-dialog').showModal();
}

/* ── wiring ─────────────────────────────────────────────────────────────── */

function init() {
  $$('.nav-item').forEach((b) => b.addEventListener('click', () => show(b.dataset.view)));
  $$('[data-goto]').forEach((b) =>
    b.addEventListener('click', () => gotoStep(Number(b.dataset.goto))));

  $('#btn-check').addEventListener('click', (e) => { e.preventDefault(); checkConnection(); });
  $('#to-2').addEventListener('click', discoverStreams);
  $('#to-3').addEventListener('click', () => gotoStep(3));
  $('#btn-suggest').addEventListener('click', suggestModel);
  $('#btn-preview-sql').addEventListener('click', previewModelSql);
  $('#btn-add-model').addEventListener('click', addModel);
  $('#btn-save').addEventListener('click', () => savePipeline(false));
  $('#btn-save-run').addEventListener('click', () => savePipeline(true));

  $('#dash-run').addEventListener('click', triggerRun);
  $('#dash-refresh').addEventListener('click', loadDashboard);
  $('#data-refresh').addEventListener('click', loadData);
  $('#btn-run-sql').addEventListener('click', () => runSql(false));
  $('#btn-explain-sql').addEventListener('click', () => runSql(true));
  $('#runs-trigger').addEventListener('click', triggerRun);
  $('#runs-refresh').addEventListener('click', loadRuns);
  $('#cost-invoice-refresh').addEventListener('click', loadInvoice);

  $('#raw-namespace').addEventListener('change', renderTablePlan);

  $('#key-btn').addEventListener('click', openKeyDialog);
  $('#key-save').addEventListener('click', () => {
    store.apiKey = $('#key-input').value.trim();
    localStorage.setItem('clara.apiKey', store.apiKey);
    toast('API key saved', 'ok');
    setTimeout(() => show(location.hash.slice(1) || 'dashboard'), 100);
  });

  // Theme: follow the OS by default, remember an explicit choice.
  const saved = localStorage.getItem('clara.theme');
  const preferDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  document.documentElement.dataset.theme = saved || (preferDark ? 'dark' : 'light');
  $('#theme-toggle').addEventListener('click', () => {
    const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = next;
    localStorage.setItem('clara.theme', next);
  });

  // Ctrl/Cmd+Enter runs the SQL editor.
  $('#sql-input').addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); runSql(false); }
  });

  renderModels();
  show(location.hash.slice(1) || 'dashboard');
}

document.addEventListener('DOMContentLoaded', init);

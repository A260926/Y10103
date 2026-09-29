let currentId = null;
let timer = null;

async function api(path, opts = {}) {
  const res = await fetch('/api' + path, {
    method: opts.method || 'GET',
    headers: { 'Content-Type': 'application/json' },
    body: opts.body ? JSON.stringify(opts.body) : undefined
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data.message || res.statusText);
    err.code = data.error;
    err.status = res.status;
    throw err;
  }
  return data;
}

function toast(msg, isErr = false) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast' + (isErr ? ' err' : '');
  t.style.display = 'block';
  clearTimeout(t._h);
  t._h = setTimeout(() => { t.style.display = 'none'; }, 4200);
}

function esc(v) {
  return String(v === null || v === undefined ? '' : v)
    .replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

function statusBadge(status) {
  return `<span class="badge b-${status.toLowerCase()}">${status}</span>`;
}

async function loadAll() {
  const list = await api('/instances');
  const box = document.getElementById('instances');
  box.innerHTML = '';
  for (const ins of list.instances) {
    const a = document.createElement('a');
    a.className = currentId === ins.id ? 'active' : '';
    a.innerHTML = `#${ins.id} def#${ins.def_id} ${esc(ins.status)}${statusBadge(ins.status)}
      <div class="muted">${esc(ins.idem_key || '')} 代次 ${ins.lease_generation}</div>`;
    a.onclick = () => { currentId = ins.id; loadAll(); };
    box.appendChild(a);
  }
  if (currentId === null && list.instances.length) {
    currentId = list.instances[list.instances.length - 1].id;
  }
  if (currentId !== null) {
    loadDetail();
  }
}

function renderDag(def, ins) {
  const byId = Object.fromEntries(def.steps.map(s => [s.id, s]));
  const stepViews = Object.fromEntries(ins.steps.map(s => [s.step_id, s]));
  const levels = [];
  const placed = {};
  const levelOf = (id) => {
    if (placed[id] !== undefined) return placed[id];
    const step = byId[id];
    const lvl = !step.dependsOn.length ? 0 : Math.max(...step.dependsOn.map(levelOf)) + 1;
    placed[id] = lvl;
    return lvl;
  };
  for (const s of def.steps) levels[levelOf(s.id)] ??= [];
  for (const s of def.steps) levels[levelOf(s.id)].push(s.id);

  const dag = document.getElementById('dag');
  dag.innerHTML = '';
  levels.forEach((ids, lvl) => {
    const row = document.createElement('div');
    row.className = 'dagrow';
    ids.forEach((id, idx) => {
      if (idx > 0) {
        const par = document.createElement('span');
        par.className = 'arrow'; par.textContent = '∥';
        row.appendChild(par);
      }
      row.appendChild(stepCard(byId[id], stepViews[id]));
    });
    dag.appendChild(row);
    if (lvl < levels.length - 1) {
      const arrow = document.createElement('div');
      arrow.className = 'arrow'; arrow.textContent = '↓ 依赖';
      dag.appendChild(arrow);
    }
  });
  document.getElementById('dtitle').textContent =
    `DAG：${esc(def.defKey)} v${def.version} · adapter ${esc(def.adapterVersion)} · 摘要 ${esc(def.digest)}`;
}

function stepCard(step, sv) {
  const div = document.createElement('div');
  div.className = 'step';
  const status = sv ? sv.status : 'PENDING';
  let rows = '';
  if (sv) {
    for (const it of sv.intents || []) {
      rows += `<li><b>intent</b>#${it.attempt} fence=<span class="chip">g${it.fencing_token}</span>
        <pre>${esc(pretty(it.payload_json))}</pre></li>`;
    }
    for (const r of sv.receipts || []) {
      rows += `<li><b>receipt</b>#${r.attempt} ${esc(r.verdict)}
        ${r.accepted ? '' : '<span class="pill s-FAILED">旧代次拒收</span>'}
        ref=<span class="chip">${esc(r.external_ref || '-')}</span> fence=<span class="chip">g${r.fencing_token}</span>
        <pre>${esc(pretty(r.payload_json))}</pre></li>`;
    }
    for (const c of sv.confirms || []) {
      rows += `<li><b>confirm</b>#${c.attempt} ${esc(c.verdict)}
        ${c.manual ? '<span class="pill s-AWAIT_CONFIRM">人工 ' + esc(c.operator || '') + '</span>' : ''}
        fence=<span class="chip">g${c.fencing_token}</span> <span class="muted">v${sv.step_version}</span></li>`;
    }
    for (const c of sv.compensations || []) {
      rows += `<li><b>compensate</b>#${c.attempt} ${esc(c.verdict)} fence=<span class="chip">g${c.fencing_token}</span></li>`;
    }
  }
  let actions = '';
  if (sv && status === 'AWAIT_CONFIRM') {
    actions += `<button onclick="manual('${step.id}','SUCCESS')">人工确认成功</button>`;
    actions += `<button class="danger" onclick="manual('${step.id}','FAILURE')">判定失败</button>`;
  }
  div.innerHTML = `
    <div class="stitle">${esc(step.name)} <span class="muted">(${esc(step.id)})</span></div>
    <div class="smeta">依赖: ${step.dependsOn.length ? esc(step.dependsOn.join(', ')) : '∅'}
      状态: <span class="pill s-${status}">${status}</span> v${sv ? sv.step_version : 0}</div>
    ${actions}
    <ul>${rows}</ul>`;
  return div;
}

function pretty(v) {
  try { return JSON.stringify(JSON.parse(v), null, 2); } catch { return esc(v); }
}

async function loadDetail() {
  const ins = await api(`/instances/${currentId}`);
  const defs = await api('/definitions');
  const def = defs.definitions.find(d => d.id === ins.defId);
  renderDag(def, ins);
  document.getElementById('raw').textContent = JSON.stringify(ins, null, 2);
  const ev = document.getElementById('events');
  ev.innerHTML = (ins.events || []).map(e =>
    `<div><span class="chip">${e.id}</span> <b>${esc(e.kind)}</b>
     ${e.step_id ? '· ' + esc(e.step_id) : ''}
     <span class="muted">${e.detail ? esc(e.detail) : ''}</span></div>`).join('');

  const l = ins.lease;
  document.getElementById('lease').innerHTML =
    `owner=<span class="chip">${esc(l.owner || '∅')}</span>
     代次 generation=<span class="chip">${l.generation}</span>
     fencing token=<span class="chip">${l.fencingToken}</span><br>
     过期时间=<span class="chip">${l.expiresAtMs}</span>`;
  const order = ins.compensationOrder || [];
  document.getElementById('comporder').innerHTML =
    order.map((id, i) => `<span ${i === ins.compensationCursor && ins.status === 'FAILED_COMPENSATION'
      ? 'style="outline:1px solid var(--err)"' : ''}>${i}:${esc(id)}</span>`).join(' → ');
  document.getElementById('compcursor').textContent =
    ins.status === 'FAILED_COMPENSATION'
      ? `failed_compensation：游标停在 #${ins.compensationCursor}，下一次从这里继续，不会粗暴回退`
      : `游标 ${ins.compensationCursor}/${order.length}`;

  const actions = document.getElementById('actions');
  let html = `<button class="ghost" onclick="tick()">worker tick</button>`;
  if (ins.status === 'FAILED_COMPENSATION' || ins.status === 'COMPENSATING') {
    html += `<button onclick="resume()">继续补偿（游标续跑）</button>`;
  }
  if (['COMPENSATED', 'FAILED_COMPENSATION', 'AWAIT_MANUAL', 'COMPENSATING'].includes(ins.status)) {
    html += `<button onclick="derive()">派生重试（复用已确认步骤）</button>`;
  }
  actions.innerHTML = html;
}

async function tick() {
  try { await api(`/instances/${currentId}/tick`, { method: 'POST', body: { worker: 'ui-worker' } }); }
  catch (e) { toast('tick: ' + e.message, true); }
  await loadAll();
}

async function resume() {
  try {
    const r = await api(`/instances/${currentId}/resume-compensation`,
      { method: 'POST', body: { worker: 'ui-worker' } });
    toast('继续补偿完成：' + r.status);
  } catch (e) { toast('resume: ' + e.message, true); }
  await loadAll();
}

async function derive() {
  const key = 'derive-' + currentId + '-' + Date.now();
  try {
    const r = await api(`/instances/${currentId}/derive`,
      { method: 'POST', body: { idemKey: key } });
    currentId = r.instanceId;
    toast('已派生新实例 #' + r.instanceId + '，旧实例锁定');
  } catch (e) { toast('derive: ' + e.message, true); }
  await loadAll();
}

async function manual(stepId, verdict) {
  const ins = await api(`/instances/${currentId}`);
  const sv = ins.steps.find(s => s.step_id === stepId);
  try {
    await api(`/instances/${currentId}/confirm/${stepId}`, {
      method: 'POST',
      body: { verdict, operator: 'operator-A', expectedVersion: sv.step_version }
    });
    toast(`已提交人工${verdict === 'SUCCESS' ? '确认' : '失败判定'}`);
  } catch (e) {
    toast(`${e.status === 409 ? '版本冲突（409）' : '人工判定失败'}: ${e.message}`, true);
  }
  await loadAll();
}

loadAll();
timer = setInterval(loadAll, 2500);

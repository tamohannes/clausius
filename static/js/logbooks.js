// ── Logbooks ──

let _lbCurrentLogbook = '';
let _lbEditingIndex = -1;

async function loadLogbookPanel(project) {
  const sel = document.getElementById('logbook-select');
  const entries = document.getElementById('logbook-entries');
  if (!sel || !entries) return;

  try {
    const res = await fetch(`/api/logbooks/${encodeURIComponent(project)}`);
    const logbooks = await res.json();

    sel.innerHTML = logbooks.length
      ? logbooks.map(lb => `<option value="${lb.name}">${lb.name} (${lb.entry_count})</option>`).join('')
      : '<option value="">no logbooks</option>';

    if (logbooks.length) {
      if (_lbCurrentLogbook && logbooks.some(lb => lb.name === _lbCurrentLogbook)) {
        sel.value = _lbCurrentLogbook;
      } else {
        _lbCurrentLogbook = logbooks[0].name;
      }
      await renderLogbook(project, _lbCurrentLogbook);
    } else {
      _lbCurrentLogbook = '';
      entries.innerHTML = '<div class="logbook-empty">Create a logbook to start taking notes.</div>';
    }
  } catch (e) {
    entries.innerHTML = `<div class="logbook-empty" style="color:var(--red)">Failed to load logbooks</div>`;
  }
}

async function switchLogbook() {
  const sel = document.getElementById('logbook-select');
  _lbCurrentLogbook = sel.value;
  _lbEditingIndex = -1;
  if (_projCurrentName && _lbCurrentLogbook) {
    await renderLogbook(_projCurrentName, _lbCurrentLogbook);
  }
}

async function renderLogbook(project, name) {
  const el = document.getElementById('logbook-entries');
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(project)}/${encodeURIComponent(name)}`);
    const data = await res.json();
    if (data.error) {
      el.innerHTML = `<div class="logbook-empty">${data.error}</div>`;
      return;
    }
    const entries = data.entries || [];
    if (!entries.length) {
      el.innerHTML = '<div class="logbook-empty">No entries yet. Add your first note above.</div>';
      return;
    }
    el.innerHTML = entries.map((entry, i) => {
      const rendered = _renderLogbookMarkdown(entry);
      return `<div class="logbook-entry" data-index="${i}">
        <div class="logbook-entry-content">${rendered}</div>
        <div class="logbook-entry-actions">
          <button class="logbook-entry-btn" onclick="editLogbookEntry(${i})" title="edit">edit</button>
        </div>
      </div>`;
    }).join('<div class="logbook-separator"></div>');
  } catch (e) {
    el.innerHTML = `<div class="logbook-empty" style="color:var(--red)">Failed: ${e}</div>`;
  }
}

function _renderLogbookMarkdown(raw) {
  let html = markdownToHtml(raw);
  html = html.replace(/@([\w_-]+)/g, (match, name) => {
    return `<span class="run-ref" onclick="openLogByName('${name}')">${match}</span>`;
  });
  return html;
}

async function openLogByName(runName) {
  if (!_projCurrentName) return;
  try {
    const res = await fetch(`/api/history?project=${encodeURIComponent(_projCurrentName)}&limit=500`);
    const rows = await res.json();
    const match = rows.find(r => (r.job_name || '').includes(runName));
    if (match) {
      openLog(match.cluster, match.job_id, match.job_name);
    } else {
      toast(`No job found matching "${runName}"`, 'error');
    }
  } catch (_) {
    toast('Failed to search for run', 'error');
  }
}

async function addLogbookEntry() {
  const textarea = document.getElementById('logbook-new-entry');
  const content = textarea.value.trim();
  if (!content || !_projCurrentName) return;

  let name = _lbCurrentLogbook;
  if (!name) {
    name = 'notes';
    _lbCurrentLogbook = name;
  }

  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}/${encodeURIComponent(name)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      textarea.value = '';
      await loadLogbookPanel(_projCurrentName);
      toast('Entry added');
    } else {
      toast(d.error || 'Failed', 'error');
    }
  } catch (e) {
    toast('Failed to add entry', 'error');
  }
}

function editLogbookEntry(index) {
  const el = document.querySelector(`.logbook-entry[data-index="${index}"]`);
  if (!el) return;
  const contentEl = el.querySelector('.logbook-entry-content');
  const actionsEl = el.querySelector('.logbook-entry-actions');

  // Fetch raw content
  fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}/${encodeURIComponent(_lbCurrentLogbook)}`)
    .then(r => r.json())
    .then(data => {
      const raw = (data.entries || [])[index] || '';
      contentEl.innerHTML = `<textarea class="logbook-edit-area" rows="6">${raw.replace(/</g, '&lt;')}</textarea>`;
      actionsEl.innerHTML = `
        <button class="logbook-entry-btn" onclick="saveLogbookEntry(${index})">save</button>
        <button class="logbook-entry-btn" onclick="renderLogbook('${_projCurrentName}','${_lbCurrentLogbook}')">cancel</button>
      `;
    });
}

async function saveLogbookEntry(index) {
  const textarea = document.querySelector(`.logbook-entry[data-index="${index}"] textarea`);
  if (!textarea) return;
  const content = textarea.value.trim();
  if (!content) return;

  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}/${encodeURIComponent(_lbCurrentLogbook)}/${index}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      await renderLogbook(_projCurrentName, _lbCurrentLogbook);
      toast('Entry updated');
    } else {
      toast(d.error || 'Failed', 'error');
    }
  } catch (e) {
    toast('Failed to save entry', 'error');
  }
}

async function promptNewLogbook() {
  const name = prompt('Logbook name (e.g. experiments, bugs, ideas):');
  if (!name || !name.trim() || !_projCurrentName) return;

  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: name.trim() }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      _lbCurrentLogbook = name.trim();
      await loadLogbookPanel(_projCurrentName);
      toast(`Created logbook "${name.trim()}"`);
    } else {
      toast(d.error || 'Failed', 'error');
    }
  } catch (e) {
    toast('Failed to create logbook', 'error');
  }
}

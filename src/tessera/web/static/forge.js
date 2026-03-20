/* ============================================================
   Tessera — Forge Module JS
   ============================================================ */

// ── Shared helpers ─────────────────────────────────────────
function fgEsc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function fgHumanSize(b) {
  b = Number(b) || 0;
  if (b < 1024)       return b + ' B';
  if (b < 1048576)    return (b / 1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b / 1048576).toFixed(1) + ' MB';
  return (b / 1073741824).toFixed(2) + ' GB';
}

function fgBadgeClass(source) {
  var map = { kaggle: 'fg-badge-kaggle', huggingface: 'fg-badge-huggingface', github: 'fg-badge-github' };
  return 'fg-badge ' + (map[source] || 'fg-badge-default');
}

function fgZoneBadge(zone) {
  var map = { processed: 'fg-zone-processed', archive: 'fg-zone-archive', quarantine: 'fg-zone-quarantine', raw: 'fg-zone-raw' };
  return '<span class="fg-zone-badge ' + (map[zone] || 'fg-zone-raw') + '">' + fgEsc(zone || '—') + '</span>';
}

function fgStatusClass(status) {
  var map = { running: 'fg-status-running', success: 'fg-status-success', failed: 'fg-status-failed', pending: 'fg-status-pending' };
  return 'fg-job-status ' + (map[status] || 'fg-status-pending');
}

function fgDotClass(status) {
  var map = { running: 'fg-dot-running', success: 'fg-dot-success', failed: 'fg-dot-failed', pending: 'fg-dot-pending' };
  return 'fg-job-dot ' + (map[status] || 'fg-dot-pending');
}

// ── Dashboard live job feed ────────────────────────────────
var ForgeDash = (function () {
  var feedEl = null;
  var timer = null;

  function formatJob(j) {
    var ref = j.source_ref || j.repo_key || '—';
    var time = (j.started_at || '').slice(0, 16).replace('T', ' ');
    return '<div class="fg-job-row">' +
      '<span class="' + fgDotClass(j.status) + '"></span>' +
      '<span class="fg-job-ref">' + fgEsc(ref) + '</span>' +
      '<span class="' + fgStatusClass(j.status) + '">' + fgEsc(j.status) + '</span>' +
      '<span class="fg-job-time">' + fgEsc(time) + '</span>' +
    '</div>';
  }

  function refresh() {
    if (!feedEl) return;
    fetch('/api/v1/forge/ingest?limit=6')
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (!d.jobs || !d.jobs.length) {
          feedEl.innerHTML = '<div class="fg-loading" style="padding:1rem"><span style="font-size:0.8rem;color:var(--text-3)">Henüz iş yok.</span></div>';
          return;
        }
        feedEl.innerHTML = d.jobs.map(formatJob).join('');
      })
      .catch(function () {});
  }

  return {
    init: function (el) {
      feedEl = el;
      refresh();
      timer = setInterval(refresh, 5000);
    }
  };
})();

// ── Source bars chart ──────────────────────────────────────
function fgRenderBars(el, sourceCounts) {
  var entries = Object.keys(sourceCounts).map(function (k) {
    return { label: k, value: sourceCounts[k] };
  }).sort(function (a, b) { return b.value - a.value; });

  if (!entries.length) { el.innerHTML = '<span style="color:var(--text-3);font-size:0.8rem">Veri yok</span>'; return; }

  var max = entries[0].value;
  el.innerHTML = entries.map(function (e) {
    var pct = max ? Math.round(e.value / max * 100) : 0;
    return '<div class="fg-bar-row">' +
      '<span class="fg-bar-label">' + fgEsc(e.label) + '</span>' +
      '<div class="fg-bar-track"><div class="fg-bar-fill" style="width:' + pct + '%"></div></div>' +
      '<span class="fg-bar-count">' + e.value + '</span>' +
    '</div>';
  }).join('');
}

// ── Datasets SPA ───────────────────────────────────────────
var ForgeDatasets = (function () {
  var state = {
    datasets: [],
    filtered: [],
    activeSource: null,
    activeId: null,
    inFlight: false,
    previewCache: {},
  };

  var els = {};

  function init() {
    els.sidebar = document.getElementById('fg-sidebar');
    els.grid    = document.getElementById('fg-grid');
    els.search  = document.getElementById('fg-search');
    els.count   = document.getElementById('fg-count');
    els.panel   = document.getElementById('fg-detail-panel');
    els.panelBody = document.getElementById('fg-detail-body');
    els.closeBtn  = document.getElementById('fg-detail-close');

    if (!els.grid) return;

    // Gather initial datasets from server-rendered grid cards
    els.grid.querySelectorAll('.fg-grid-card').forEach(function (card) {
      card.addEventListener('click', function () {
        loadDetail(card.dataset.id);
      });
    });

    // Source sidebar items
    document.querySelectorAll('.fg-sidebar-item').forEach(function (item) {
      item.addEventListener('click', function () {
        var source = item.dataset.source || null;
        setActiveSource(source);
      });
    });

    // Search
    if (els.search) {
      els.search.addEventListener('input', function () {
        filterGrid();
      });
    }

    // Close panel
    if (els.closeBtn) {
      els.closeBtn.addEventListener('click', function () {
        closePanel();
      });
    }

    // Handle back/forward
    window.addEventListener('popstate', function (e) {
      if (e.state && e.state.datasetId) {
        loadDetail(e.state.datasetId);
      } else {
        closePanel();
      }
    });

    // Deep link
    var hash = location.hash.slice(1);
    if (hash) { loadDetail(hash); }
  }

  function setActiveSource(source) {
    state.activeSource = source;
    document.querySelectorAll('.fg-sidebar-item').forEach(function (item) {
      item.classList.toggle('active', (item.dataset.source || null) === source);
    });
    filterGrid();
  }

  function filterGrid() {
    var q = els.search ? els.search.value.toLowerCase().trim() : '';
    var source = state.activeSource;
    var visible = 0;

    els.grid.querySelectorAll('.fg-grid-card').forEach(function (card) {
      var matchSrc  = !source || card.dataset.source === source;
      var matchName = !q || card.dataset.name.includes(q);
      var show = matchSrc && matchName;
      card.style.display = show ? '' : 'none';
      if (show) visible++;
    });

    if (els.count) els.count.textContent = visible + ' dataset';
  }

  async function loadDetail(id) {
    if (state.inFlight && state.activeId === id) return;
    state.activeId = id;
    state.inFlight = true;

    // Highlight selected
    els.grid.querySelectorAll('.fg-grid-card').forEach(function (c) {
      c.classList.toggle('selected', c.dataset.id === id);
    });

    // Open panel
    els.panel.classList.remove('hidden');
    els.panelBody.innerHTML = '<div class="fg-loading"><div class="fg-spinner"></div></div>';

    try {
      var res = await fetch('/api/v1/forge/datasets/' + encodeURIComponent(id));
      if (!res.ok) throw new Error('HTTP ' + res.status);
      var d = await res.json();
      els.panelBody.innerHTML = buildDetailHTML(d.dataset, d.versions || []);
      history.pushState({ datasetId: id }, '', '/forge/datasets#' + id);
    } catch (err) {
      els.panelBody.innerHTML = '<p style="color:var(--text-3);padding:1rem;font-size:0.82rem">Yüklenemedi: ' + fgEsc(err.message) + '</p>';
    } finally {
      state.inFlight = false;
    }
  }

  function closePanel() {
    state.activeId = null;
    els.panel.classList.add('hidden');
    els.grid.querySelectorAll('.fg-grid-card').forEach(function (c) { c.classList.remove('selected'); });
    history.replaceState(null, '', '/forge/datasets');
  }

  function buildDetailHTML(ds, versions) {
    var latest = versions[0] || null;
    var size   = latest ? fgHumanSize(latest.file_size_bytes) : '—';
    var fmt    = latest && latest.format ? latest.format.toUpperCase() : '—';
    var zone   = latest ? (latest.zone || '—') : '—';
    var cols   = latest && latest.column_count ? latest.column_count : null;
    var meta   = (latest && latest.metadata_json) ? latest.metadata_json : {};
    var updatedAt = latest ? (latest.created_at || '').slice(0, 10) : null;
    var dateStart = meta.date_start || null;
    var dateEnd   = meta.date_end   || null;
    var dateCol   = meta.date_column || null;

    var tagsHtml = (ds.tags || []).slice(0, 12).map(function (t) {
      return '<span class="fg-tag">' + fgEsc(t) + '</span>';
    }).join('');

    var descHtml = ds.description
      ? '<p class="fg-desc">' + fgEsc(ds.description) + '</p>'
      : '<p class="fg-desc fg-desc--empty">Açıklama mevcut değil.</p>';

    var temporal = (dateStart || dateEnd)
      ? statCard('Kapsam', (dateStart || '?') + ' → ' + (dateEnd || '?'), dateCol ? dateCol : null)
      : statCard('Kapsam', '—', 'Tarih kolonu yok');

    var statCardsHtml =
      '<div class="fg-stat-cards">' +
        statCard('Sürüm', String(versions.length), updatedAt ? updatedAt : null) +
        statCard('Boyut', size, fmt + (cols ? ' · ' + cols + ' kol' : '')) +
        temporal +
      '</div>';

    var metaHtml =
      '<div class="fg-detail-meta">' +
        '<span class="' + fgBadgeClass(ds.source) + '">' + fgEsc(ds.source) + '</span>' +
        (ds.source_ref ? '<span style="font-family:monospace;font-size:0.72rem;color:var(--text-3)">' + fgEsc(ds.source_ref.slice(0, 40)) + '</span>' : '') +
        (ds.url ? '<a class="fg-ext-link" href="' + fgEsc(ds.url) + '" target="_blank" rel="noopener">' +
          '<svg viewBox="0 0 12 12" width="10" height="10" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M7 1h4v4M11 1l-6 6M5 3H2a1 1 0 00-1 1v6a1 1 0 001 1h6a1 1 0 001-1V8"/></svg>Aç</a>' : '') +
      '</div>';

    var actionsHtml =
      '<div class="fg-detail-actions">' +
        '<button class="fg-btn" onclick="ForgeDatasets.reingest(\'' + fgEsc(ds.id) + '\',\'' + fgEsc(ds.source) + '\',\'' + fgEsc(ds.source_ref) + '\')">' +
          '<svg viewBox="0 0 16 16" width="12" height="12" fill="none" stroke="currentColor" stroke-width="1.7"><path d="M13.5 8A5.5 5.5 0 1 1 8 2.5c1.8 0 3.4.87 4.4 2.2"/><polyline points="11 2 13.5 4.7 11 7.3"/></svg>' +
          'Yeniden İngest' +
        '</button>' +
        '<button class="fg-btn fg-btn-danger" onclick="ForgeDatasets.deleteDataset(\'' + fgEsc(ds.id) + '\')">' +
          '<svg viewBox="0 0 16 16" width="12" height="12" fill="none" stroke="currentColor" stroke-width="1.7"><path d="M3 4h10M6 4V3h4v1M5 4l.5 9h5l.5-9"/></svg>' +
          'Sil' +
        '</button>' +
      '</div>';

    // Versions table
    var verRows = versions.length
      ? versions.map(function (v) {
          return '<tr>' +
            '<td><span class="fg-ver-badge">' + fgEsc(v.version) + '</span></td>' +
            '<td style="color:var(--text-3)">' + fgEsc((v.created_at || '').slice(0, 10)) + '</td>' +
            '<td>' + fgEsc(fgHumanSize(v.file_size_bytes)) + '</td>' +
            '<td>' + fgZoneBadge(v.zone) + '</td>' +
          '</tr>';
        }).join('')
      : '<tr><td colspan="4" style="color:var(--text-3);text-align:center;padding:0.75rem">Henüz sürüm yok</td></tr>';

    var verCardHtml =
      '<div class="fg-ver-card">' +
        '<div class="fg-ver-card-hd"><span>Sürümler</span><span class="fg-count-badge">' + versions.length + '</span></div>' +
        '<table class="fg-ver-table"><thead><tr><th>Sürüm</th><th>Tarih</th><th>Boyut</th><th>Bölge</th></tr></thead><tbody>' + verRows + '</tbody></table>' +
      '</div>';

    // Preview card
    var previewId = 'fg-prev-' + fgEsc(ds.id);
    var previewHtml =
      '<div class="fg-preview-card" id="' + previewId + '">' +
        '<div class="fg-preview-hd">' +
          '<span>Veri Önizleme</span>' +
          '<button class="fg-btn" style="font-size:0.72rem;padding:0.2rem 0.5rem" onclick="ForgeDatasets.togglePreview(\'' + fgEsc(ds.id) + '\')">' +
            '<span id="fg-prev-lbl-' + fgEsc(ds.id) + '">Önizle</span>' +
          '</button>' +
        '</div>' +
        '<div id="fg-prev-body-' + fgEsc(ds.id) + '" style="display:none">' +
          '<div class="fg-loading" id="fg-prev-load-' + fgEsc(ds.id) + '"><div class="fg-spinner"></div></div>' +
          '<div id="fg-prev-err-'   + fgEsc(ds.id) + '" style="display:none;padding:0.75rem;font-size:0.78rem;color:var(--text-3)"></div>' +
          '<div class="fg-preview-body" id="fg-prev-tbl-' + fgEsc(ds.id) + '" style="display:none"></div>' +
          '<div class="fg-preview-footer" id="fg-prev-foot-' + fgEsc(ds.id) + '" style="display:none"></div>' +
        '</div>' +
      '</div>';

    return '<div class="fg-detail-title">' + fgEsc(ds.name) + '</div>' +
      metaHtml + actionsHtml + statCardsHtml +
      '<div class="fg-info-card"><div class="fg-info-card-hd">Açıklama</div>' +
        '<div class="fg-info-card-body">' + descHtml +
          (tagsHtml ? '<div class="fg-tags">' + tagsHtml + '</div>' : '') +
        '</div></div>' +
      previewHtml + verCardHtml;
  }

  function statCard(label, val, sub) {
    var isLong = String(val).length > 10;
    return '<div class="fg-stat-card">' +
      '<div class="fg-stat-card-label">' + fgEsc(label) + '</div>' +
      '<div class="fg-stat-card-val' + (isLong ? ' fg-stat-card-val--sm' : '') + '">' + fgEsc(val) + '</div>' +
      (sub ? '<div class="fg-stat-card-sub">' + fgEsc(sub) + '</div>' : '') +
    '</div>';
  }

  return {
    init: init,

    reingest: async function (id, source, sourceRef) {
      if (!confirm('Bu dataset yeniden ingest edilecek. Devam?')) return;
      try {
        var res = await fetch('/api/v1/forge/ingest', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ source: source, source_ref: sourceRef }),
        });
        var d = await res.json();
        if (!res.ok) throw new Error(d.detail || 'Hata');
        alert('İngest başlatıldı. Job ID: ' + d.job_id);
      } catch (e) { alert('Hata: ' + e.message); }
    },

    deleteDataset: async function (id) {
      if (!confirm('Bu dataset kalıcı olarak silinecek. Emin misin?')) return;
      try {
        var res = await fetch('/api/v1/forge/datasets/' + encodeURIComponent(id), { method: 'DELETE' });
        if (!res.ok) { var d = await res.json(); throw new Error(d.detail || 'HTTP ' + res.status); }
        // Remove card from grid and close panel
        var card = els.grid.querySelector('.fg-grid-card[data-id="' + id + '"]');
        if (card) card.remove();
        closePanel();
        filterGrid();
      } catch (e) { alert('Silinemedi: ' + e.message); }
    },

    togglePreview: async function (id) {
      var body   = document.getElementById('fg-prev-body-' + id);
      var lblEl  = document.getElementById('fg-prev-lbl-' + id);
      if (!body) return;
      var open = body.style.display !== 'none';
      if (open) { body.style.display = 'none'; if (lblEl) lblEl.textContent = 'Önizle'; return; }
      body.style.display = '';
      if (lblEl) lblEl.textContent = 'Kapat';
      if (state.previewCache[id]) return;

      var loadEl = document.getElementById('fg-prev-load-' + id);
      var errEl  = document.getElementById('fg-prev-err-'  + id);
      var tblEl  = document.getElementById('fg-prev-tbl-'  + id);
      var footEl = document.getElementById('fg-prev-foot-' + id);

      if (loadEl) loadEl.style.display = 'flex';
      if (errEl)  errEl.style.display  = 'none';
      if (tblEl)  tblEl.style.display  = 'none';
      if (footEl) footEl.style.display = 'none';

      try {
        var res = await fetch('/api/v1/forge/datasets/' + encodeURIComponent(id) + '/preview');
        if (!res.ok) throw new Error('HTTP ' + res.status);
        var d = await res.json();
        if (loadEl) loadEl.style.display = 'none';
        if (d.error) {
          var msg = { no_readable_file: 'Önizlenebilir dosya yok.', read_failed: 'Okuma hatası: ' + (d.error_detail || ''), pandas_missing: 'Pandas eksik.', unsupported_format: 'Desteklenmeyen format.' }[d.error] || ('Hata: ' + d.error);
          if (errEl) { errEl.textContent = msg; errEl.style.display = ''; }
          return;
        }
        if (tblEl) { tblEl.innerHTML = buildPreviewTable(d); tblEl.style.display = ''; }
        if (footEl) {
          var info = d.preview_rows + ' satır';
          if (d.total_rows > d.preview_rows) info += ' / ' + d.total_rows + ' toplam';
          if (d.truncated_columns) info += ' · ' + d.columns.length + ' sütun';
          info += ' · ' + fgEsc(d.source_file);
          footEl.textContent = info; footEl.style.display = '';
        }
        state.previewCache[id] = true;
      } catch (err) {
        if (loadEl) loadEl.style.display = 'none';
        if (errEl) { errEl.textContent = 'Yüklenemedi: ' + err.message; errEl.style.display = ''; }
      }
    },
  };

  function buildPreviewTable(d) {
    var cols = d.columns; var rows = d.rows;
    if (!cols || !cols.length) return '<p style="padding:0.75rem;color:var(--text-3);font-size:0.78rem">Sütun bulunamadı.</p>';
    var thead = '<thead><tr>' + cols.map(function (c) { return '<th title="' + fgEsc(d.dtypes[c] || '') + '">' + fgEsc(c) + '</th>'; }).join('') + '</tr></thead>';
    var tbody = '<tbody>' + rows.map(function (row) {
      return '<tr>' + cols.map(function (c) {
        var v = row[c];
        if (v === null || v === undefined) return '<td><span class="fg-null">—</span></td>';
        return '<td title="' + fgEsc(String(v)) + '">' + fgEsc(String(v)) + '</td>';
      }).join('') + '</tr>';
    }).join('') + '</tbody>';
    return '<table class="fg-preview-table">' + thead + tbody + '</table>';
  }
})();

// ── Ingest page ────────────────────────────────────────────
var ForgeIngest = (function () {
  var timer = null;

  function init() {
    var form      = document.getElementById('fg-ingest-form');
    var sourceEl  = document.getElementById('fg-ingest-source');
    var refEl     = document.getElementById('fg-ingest-ref');
    var tagsEl    = document.getElementById('fg-ingest-tags');
    var forceEl   = document.getElementById('fg-ingest-force');
    var jobsEl    = document.getElementById('fg-ingest-jobs');

    if (!form) return;

    form.addEventListener('submit', async function (e) {
      e.preventDefault();
      var btn = form.querySelector('button[type=submit]');
      btn.disabled = true;
      btn.textContent = 'Başlatılıyor…';

      var payload = {
        source: sourceEl.value,
        source_ref: refEl.value.trim(),
        tags: tagsEl ? tagsEl.value.split(',').map(function (t) { return t.trim(); }).filter(Boolean) : [],
        force: forceEl ? forceEl.checked : false,
      };

      try {
        var res = await fetch('/api/v1/forge/ingest', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        var d = await res.json();
        if (!res.ok) throw new Error(d.detail || 'Hata');
        btn.textContent = 'Başlatıldı ✓';
        setTimeout(function () { btn.disabled = false; btn.textContent = 'İngest Başlat'; }, 2500);
        refreshJobs(jobsEl);
      } catch (err) {
        alert('Hata: ' + err.message);
        btn.disabled = false;
        btn.textContent = 'İngest Başlat';
      }
    });

    if (jobsEl) {
      refreshJobs(jobsEl);
      timer = setInterval(function () { refreshJobs(jobsEl); }, 4000);
    }
  }

  function refreshJobs(el) {
    if (!el) return;
    fetch('/api/v1/forge/ingest?limit=10')
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (!d.jobs || !d.jobs.length) { el.innerHTML = '<div style="color:var(--text-3);font-size:0.82rem;padding:0.5rem 0">Henüz iş yok.</div>'; return; }
        el.innerHTML = d.jobs.map(function (j) {
          var ref = j.source_ref || '—';
          var time = (j.started_at || '').slice(0, 16).replace('T', ' ');
          var err = j.error_message ? '<div style="color:var(--text-3);font-size:0.72rem;padding:0.1rem 0 0.2rem 1.3rem">' + fgEsc(j.error_message.slice(0, 80)) + '</div>' : '';
          return '<div class="fg-job-item">' +
            '<span class="' + fgDotClass(j.status) + '"></span>' +
            '<div style="flex:1;min-width:0">' +
              '<div style="display:flex;align-items:center;gap:0.5rem">' +
                '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:0.8rem;font-family:monospace">' + fgEsc(ref) + '</span>' +
                '<span class="' + fgStatusClass(j.status) + '">' + fgEsc(j.status) + '</span>' +
                '<span class="fg-job-time">' + fgEsc(time) + '</span>' +
              '</div>' + err +
            '</div>' +
          '</div>';
        }).join('');
      })
      .catch(function () {});
  }

  return { init: init };
})();

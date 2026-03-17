/* ==========================================================
   Tessera — Archiver Module JavaScript
   Depends on: app.js (showToast, humanSize, formatDate)
   ========================================================== */

'use strict';

// ── XSS escaping ─────────────────────────────────────────────
function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ── Size formatter (mirrors app.js humanSize) ─────────────────
function human(b) {
  b = Number(b) || 0;
  if (b < 1024)       return b + ' B';
  if (b < 1048576)    return (b / 1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b / 1048576).toFixed(1) + ' MB';
  return (b / 1073741824).toFixed(2) + ' GB';
}

// ── Date formatter ────────────────────────────────────────────
function fmtDate(iso) {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString('tr-TR', {
      year: 'numeric', month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit',
    });
  } catch { return String(iso).slice(0, 16).replace('T', ' '); }
}

function fmtDateShort(iso) {
  if (!iso) return '—';
  return String(iso).slice(0, 10);
}

// ── Risk helpers ──────────────────────────────────────────────
var RISK_CLASS = {
  HIGH: 'ar-risk-high', MEDIUM: 'ar-risk-medium',
  LOW:  'ar-risk-low',  CLEAN:  'ar-risk-clean',
};

var RISK_COLOR = {
  HIGH: '#821f10', MEDIUM: '#6e4c00', LOW: '#1a6e40', CLEAN: '#1a6e40',
};

var RISK_STROKE = {
  HIGH: '#e05252', MEDIUM: '#e09a20', LOW: '#4db6ac', CLEAN: '#43a047',
};

function riskBadge(level) {
  var cls = RISK_CLASS[level] || 'ar-risk-unknown';
  return '<span class="ar-badge ' + cls + '">' + esc(level || '—') + '</span>';
}

function statusBadge(status) {
  var cls = 'ar-badge ar-status-' + (status || 'pending');
  return '<span class="' + cls + '">' + esc(status || '?') + '</span>';
}

// ── Fetch wrapper ─────────────────────────────────────────────
async function apiFetch(path, opts) {
  var res = await fetch('/api/v1/archiver' + path, opts || {});
  if (!res.ok) {
    var body = null;
    try { body = await res.json(); } catch (_) {}
    throw new Error((body && body.detail) || ('HTTP ' + res.status));
  }
  return res.json();
}

// =========================================================
// Modal
// =========================================================
var Modal = (function () {
  var _backdrop = null;
  var _onClose  = null;

  function _ensureBackdrop() {
    if (_backdrop) return;
    _backdrop = document.createElement('div');
    _backdrop.className = 'ar-modal-backdrop';
    _backdrop.setAttribute('role', 'dialog');
    _backdrop.setAttribute('aria-modal', 'true');
    document.body.appendChild(_backdrop);

    _backdrop.addEventListener('click', function (e) {
      if (e.target === _backdrop) close();
    });

    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && _backdrop.classList.contains('open')) close();
    });
  }

  function open(opts) {
    // opts: { title, body, footer, onClose, width }
    _ensureBackdrop();
    _onClose = opts.onClose || null;

    _backdrop.innerHTML = [
      '<div class="ar-modal"' + (opts.width ? ' style="max-width:' + opts.width + '"' : '') + '>',
        '<div class="ar-modal-hd">',
          '<span>' + esc(opts.title || '') + '</span>',
          '<button class="ar-modal-close" aria-label="Kapat">',
            '<svg viewBox="0 0 14 14" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2">',
            '<path d="M2 2l10 10M12 2L2 12"/></svg>',
          '</button>',
        '</div>',
        '<div class="ar-modal-body">' + (opts.body || '') + '</div>',
        opts.footer ? '<div class="ar-modal-footer">' + opts.footer + '</div>' : '',
      '</div>',
    ].join('');

    _backdrop.querySelector('.ar-modal-close')
             .addEventListener('click', close);

    requestAnimationFrame(function () {
      _backdrop.style.display = 'flex';
      requestAnimationFrame(function () {
        _backdrop.classList.add('open');
      });
    });

    return _backdrop.querySelector('.ar-modal-body');
  }

  function close() {
    if (!_backdrop) return;
    _backdrop.classList.remove('open');
    setTimeout(function () {
      _backdrop.style.display = 'none';
      _backdrop.innerHTML = '';
    }, 200);
    if (_onClose) { _onClose(); _onClose = null; }
  }

  return { open: open, close: close };
})();

// =========================================================
// Poller — polls a job until done/failed
// =========================================================
function Poller(jobId, opts) {
  // opts: { onUpdate(job), onDone(job), onError(err), interval }
  this.jobId    = jobId;
  this.opts     = opts || {};
  this.interval = opts.interval || 1800;
  this._timer   = null;
  this._stopped = false;
  this._lastLog = 0;
}

Poller.prototype.start = function () {
  this._poll();
};

Poller.prototype.stop = function () {
  this._stopped = true;
  if (this._timer) clearTimeout(this._timer);
};

Poller.prototype._poll = function () {
  var self = this;
  apiFetch('/jobs/' + self.jobId)
    .then(function (job) {
      if (self._stopped) return;
      if (self.opts.onUpdate) self.opts.onUpdate(job);
      if (job.status === 'done' || job.status === 'failed') {
        if (self.opts.onDone) self.opts.onDone(job);
      } else {
        self._timer = setTimeout(function () { self._poll(); }, self.interval);
      }
    })
    .catch(function (err) {
      if (self._stopped) return;
      if (self.opts.onError) self.opts.onError(err);
    });
};

// =========================================================
// JobLog — renders real-time log output in .ar-log-terminal
// =========================================================
function JobLog(containerEl) {
  this.container = containerEl;
  this.bodyEl    = containerEl.querySelector('.ar-log-body');
  this.titleEl   = containerEl.querySelector('.ar-log-title');
  this._lines    = [];
}

JobLog.prototype.setTitle = function (t) {
  if (this.titleEl) this.titleEl.textContent = t;
};

JobLog.prototype.setLines = function (lines) {
  // lines: array of strings from job.logs
  var self = this;
  if (!lines || !lines.length) return;
  // Only append new lines
  var newLines = lines.slice(self._lines.length);
  self._lines = lines.slice();
  newLines.forEach(function (line) {
    var el = document.createElement('div');
    el.className = 'ar-log-line' + self._classify(line);
    el.textContent = line;
    self.bodyEl.appendChild(el);
  });
  // Scroll to bottom
  self.bodyEl.scrollTop = self.bodyEl.scrollHeight;
};

JobLog.prototype._classify = function (line) {
  var l = line.toLowerCase();
  if (l.includes('error') || l.includes('fail') || l.includes('başarısız')) return ' err';
  if (l.includes('warn') || l.includes('uyarı')) return ' warn';
  if (l.includes('done') || l.includes('tamamland') || l.includes('success')) return ' ok';
  if (l.includes('[info]') || l.includes('başlatıldı')) return ' info';
  return '';
};

JobLog.prototype.showCursor = function () {
  var cur = document.createElement('span');
  cur.className = 'ar-log-cursor';
  this.bodyEl.appendChild(cur);
};

JobLog.prototype.hideCursor = function () {
  var cur = this.bodyEl.querySelector('.ar-log-cursor');
  if (cur) cur.remove();
};

// =========================================================
// DonutChart — SVG risk ring renderer
// =========================================================
function DonutChart(svgEl, opts) {
  opts = opts || {};
  this.svg  = svgEl;
  this.opts = opts;
  this.r    = opts.r || 46;
  this.cx   = opts.cx || 60;
  this.cy   = opts.cy || 60;
  this.stroke = opts.stroke || 12;
}

DonutChart.prototype.render = function (segments) {
  // segments: [{label, value, color}]
  var total = segments.reduce(function (s, seg) { return s + (seg.value || 0); }, 0);
  if (!total) {
    this._renderEmpty();
    return;
  }

  var paths = [];
  var offset = 0;
  var circ = 2 * Math.PI * this.r;

  for (var i = 0; i < segments.length; i++) {
    var seg = segments[i];
    if (!seg.value) continue;
    var pct  = seg.value / total;
    var dash = pct * circ;
    var gap  = circ - dash;
    paths.push(
      '<circle cx="' + this.cx + '" cy="' + this.cy + '" r="' + this.r + '"' +
      ' fill="none"' +
      ' stroke="' + esc(seg.color) + '"' +
      ' stroke-width="' + this.stroke + '"' +
      ' stroke-dasharray="' + dash.toFixed(2) + ' ' + gap.toFixed(2) + '"' +
      ' stroke-dashoffset="' + (-offset * circ).toFixed(2) + '"' +
      ' stroke-linecap="butt">' +
      '<title>' + esc(seg.label) + ': ' + seg.value + '</title>' +
      '</circle>'
    );
    offset += pct;
  }

  this.svg.innerHTML = paths.join('');
  this.svg.setAttribute('viewBox', '0 0 120 120');
};

DonutChart.prototype._renderEmpty = function () {
  this.svg.innerHTML =
    '<circle cx="' + this.cx + '" cy="' + this.cy + '" r="' + this.r + '"' +
    ' fill="none" stroke="var(--border-2)" stroke-width="' + this.stroke + '"/>';
  this.svg.setAttribute('viewBox', '0 0 120 120');
};

// =========================================================
// BarChart — simple horizontal bar renderer
// =========================================================
function renderBars(containerEl, data, maxVal) {
  // data: [{label, value}]
  if (!data || !data.length) {
    containerEl.innerHTML = '<p class="ar-empty">Veri yok.</p>';
    return;
  }
  var max = maxVal || Math.max.apply(null, data.map(function (d) { return d.value; })) || 1;
  containerEl.innerHTML = data.slice(0, 8).map(function (d) {
    var pct = Math.round((d.value / max) * 100);
    return [
      '<div class="ar-bar-row">',
        '<span class="ar-bar-label" title="' + esc(d.label) + '">' + esc(d.label || '—') + '</span>',
        '<div class="ar-bar-track"><div class="ar-bar-fill" style="width:' + pct + '%"></div></div>',
        '<span class="ar-bar-val">' + d.value + '</span>',
      '</div>',
    ].join('');
  }).join('');
}

// =========================================================
// TimelineChart — 7-day activity bar chart
// =========================================================
function renderTimeline(containerEl, days) {
  // days: [{label:'Mon', value:3}, ...]
  if (!days || !days.length) return;
  var max = Math.max.apply(null, days.map(function (d) { return d.value; })) || 1;
  var html = '<div class="ar-tl-chart">';
  days.forEach(function (d) {
    var h = Math.max(Math.round((d.value / max) * 52), d.value ? 4 : 2);
    html += [
      '<div class="ar-tl-col">',
        '<div class="ar-tl-bar" style="height:' + h + 'px" title="' + esc(d.label) + ': ' + d.value + '"></div>',
        '<div class="ar-tl-label">' + esc(d.label) + '</div>',
      '</div>',
    ].join('');
  });
  html += '</div>';
  containerEl.innerHTML = html;
}

// =========================================================
// Archive form submit helper
// =========================================================
function submitArchiveJob(opts, onStart, onUpdate, onDone) {
  // opts: {repo, force, include_heavy}
  apiFetch('/jobs/archive', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(opts),
  })
  .then(function (data) {
    if (onStart) onStart(data.job_id);
    var poller = new Poller(data.job_id, {
      onUpdate: onUpdate,
      onDone: onDone,
      onError: function (err) {
        showToast('Arşiv işi takip hatası: ' + err.message, 'error');
      },
    });
    poller.start();
  })
  .catch(function (err) {
    showToast('Arşiv başlatılamadı: ' + err.message, 'error');
  });
}

// =========================================================
// Scan job submit helper
// =========================================================
function submitScanJob(repo, onStart, onDone) {
  apiFetch('/jobs/scan', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ repo: repo }),
  })
  .then(function (data) {
    if (onStart) onStart(data.job_id);
    new Poller(data.job_id, {
      onDone: onDone,
      onError: function (err) { showToast(err.message, 'error'); },
    }).start();
  })
  .catch(function (err) { showToast(err.message, 'error'); });
}

// =========================================================
// Confirm modal helper (replaces window.confirm)
// =========================================================
function confirmModal(message, onConfirm) {
  Modal.open({
    title: 'Onay',
    body: '<p style="color:var(--text-2);line-height:1.6;">' + esc(message) + '</p>',
    footer: [
      '<button class="ar-btn" id="ar-confirm-cancel">İptal</button>',
      '<button class="ar-btn ar-btn-primary" id="ar-confirm-ok">Devam</button>',
    ].join(''),
  });
  document.getElementById('ar-confirm-cancel')
          .addEventListener('click', Modal.close);
  document.getElementById('ar-confirm-ok')
          .addEventListener('click', function () { Modal.close(); onConfirm(); });
}

// =========================================================
// Tiles split-panel controller
// =========================================================
var TilesSPA = (function () {
  var _listEl   = null;
  var _detailEl = null;
  var _active   = null;
  var _inFlight = false;

  function init(listEl, detailEl) {
    _listEl   = listEl;
    _detailEl = detailEl;
  }

  function loadDetail(repoKey) {
    if (_inFlight || _active === repoKey) return;
    _active   = repoKey;
    _inFlight = true;

    // Encode key for URL: "github:ns/repo" → "github/ns/repo"
    var parts   = repoKey.split(':');
    var provider = parts[0];
    var rest     = (parts[1] || '').split('/');
    var ns       = rest[0];
    var repo     = rest.slice(1).join('/');

    _listEl.style.display   = 'none';
    _detailEl.style.display = '';
    _detailEl.innerHTML     = '<div class="ar-loading"><div class="ar-spinner"></div></div>';

    history.replaceState(null, '', '/archiver/tiles#' + encodeURIComponent(repoKey));

    apiFetch('/tiles/' + encodeURIComponent(provider) +
             '/' + encodeURIComponent(ns) +
             '/' + encodeURIComponent(repo))
      .then(function (data) {
        _detailEl.innerHTML = buildDetail(data);
        _detailEl.classList.add('ar-fade-in');
        _inFlight = false;
      })
      .catch(function (err) {
        _detailEl.innerHTML =
          '<div class="ar-empty"><p>Yüklenemedi: ' + esc(err.message) + '</p></div>';
        _inFlight = false;
      });
  }

  function back() {
    _detailEl.style.display = 'none';
    _detailEl.innerHTML     = '';
    _listEl.style.display   = '';
    _active                 = null;
    history.replaceState(null, '', '/archiver/tiles');
  }

  function buildDetail(data) {
    var repo     = data.repo     || {};
    var versions = data.versions || [];
    var scan     = data.scan     || null;

    // Header
    var hdHtml = [
      '<div class="ar-back-bar">',
        '<button class="ar-back-btn" onclick="TilesSPA.back()">',
          '<svg viewBox="0 0 14 14" width="12" height="12" fill="none" stroke="currentColor" stroke-width="1.8">',
          '<path d="M9 2L4 7l5 5"/></svg>',
          'Tiles',
        '</button>',
      '</div>',
      '<div class="ar-detail-content ar-fade-in">',
    ].join('');

    // Title block
    var titleHtml = [
      '<div class="ar-detail-hd">',
        '<div>',
          '<div class="ar-detail-title">' + esc(repo.namespace || '') + '/' + esc(repo.repo || '') + '</div>',
          '<div class="ar-detail-ns">' + esc(repo.provider || '') + '</div>',
          '<div class="ar-detail-meta">',
            riskBadge(repo.risk_level),
            repo.language ? '<span class="ar-badge ar-risk-unknown">' + esc(repo.language) + '</span>' : '',
            repo.stars != null ? '<span style="font-size:0.8rem;color:var(--text-3);">★ ' + repo.stars + '</span>' : '',
          '</div>',
        '</div>',
        '<div style="display:flex;gap:0.5rem;flex-wrap:wrap;">',
          '<button class="ar-btn ar-btn-primary" onclick="TilesSPA.archive(\'' + esc(repo.key || '') + '\')">',
            '<svg viewBox="0 0 16 16" width="13" height="13" fill="none" stroke="currentColor" stroke-width="1.7">',
            '<path d="M2 4h12M2 4v10a1 1 0 001 1h10a1 1 0 001-1V4M6 4V3h4v1"/></svg>',
            'Arşivle',
          '</button>',
          '<button class="ar-btn" onclick="TilesSPA.scan(\'' + esc(repo.key || '') + '\')">',
            '<svg viewBox="0 0 16 16" width="13" height="13" fill="none" stroke="currentColor" stroke-width="1.7">',
            '<circle cx="8" cy="8" r="5"/><path d="M8 5v3l2 2"/></svg>',
            'Tara',
          '</button>',
        '</div>',
      '</div>',
    ].join('');

    // Stat cards
    var size = versions.length && versions[0].size_bytes ? human(versions[0].size_bytes) : '—';
    var statHtml = [
      '<div class="ar-stat-cards">',
        _statCard('Versiyon', String(versions.length), versions.length ? fmtDateShort(versions[0].archived_at) + ' son' : null),
        _statCard('Boyut', size, repo.domain || null),
        _statCard('Bulgular', scan ? String(scan.total_findings || 0) : '—',
                  scan ? 'Risk: ' + (scan.risk_level || '—') : 'Taranmadı'),
      '</div>',
    ].join('');

    // Description
    var descHtml = [
      '<div class="ar-info-card">',
        '<div class="ar-info-card-hd">Açıklama</div>',
        '<div class="ar-info-card-body">',
          repo.description
            ? '<p class="ar-desc">' + esc(repo.description) + '</p>'
            : '<p class="ar-desc-empty">Açıklama mevcut değil.</p>',
          repo.purpose ? '<p class="ar-desc" style="margin-top:0.5rem;color:var(--text-3);font-size:0.8rem;">Amaç: ' + esc(repo.purpose) + '</p>' : '',
        '</div>',
      '</div>',
    ].join('');

    // Scan card
    var scanHtml = '';
    if (scan) {
      scanHtml = [
        '<div class="ar-scan-card">',
          '<div class="ar-card-hd">',
            '<span>Son Tarama</span>',
            riskBadge(scan.risk_level),
          '</div>',
          '<div style="display:flex;gap:1.5rem;padding:0.9rem 1rem;font-size:0.85rem;border-bottom:1px solid var(--border);">',
            '<div><strong style="color:#e05252;">' + (scan.high_count || 0) + '</strong> <span style="color:var(--text-3);">Yüksek</span></div>',
            '<div><strong style="color:#e09a20;">' + (scan.medium_count || 0) + '</strong> <span style="color:var(--text-3);">Orta</span></div>',
            '<div><strong style="color:#4db6ac;">' + (scan.low_count || 0) + '</strong> <span style="color:var(--text-3);">Düşük</span></div>',
            '<div style="margin-left:auto;color:var(--text-3);font-size:0.78rem;">' + fmtDateShort(scan.scanned_at) + '</div>',
          '</div>',
        '</div>',
      ].join('');
    }

    // Version history
    var verHtml = [
      '<div class="ar-ver-card">',
        '<div class="ar-ver-card-hd">',
          '<span>Versiyon Geçmişi</span>',
          '<span class="ar-count-badge">' + versions.length + '</span>',
        '</div>',
        versions.length ? [
          '<div style="overflow-x:auto;">',
          '<table class="ar-ver-table">',
            '<thead><tr>',
              '<th>Versiyon</th><th>Tarih</th><th>Boyut</th><th>SHA-256</th>',
            '</tr></thead>',
            '<tbody>',
            versions.map(function (v) {
              return '<tr>' +
                '<td class="ar-td-badge"><span class="ar-ver-badge">' + esc(v.version) + '</span></td>' +
                '<td class="ar-td-muted">' + esc(fmtDateShort(v.archived_at)) + '</td>' +
                '<td>' + esc(v.size_bytes ? human(v.size_bytes) : '—') + '</td>' +
                '<td class="ar-td-mono">' + esc((v.checksum_sha256 || '').slice(0, 16)) + '…</td>' +
              '</tr>';
            }).join(''),
            '</tbody>',
          '</table></div>',
        ].join('')
        : '<p class="ar-empty" style="padding:1.5rem;">Versiyon bulunamadı.</p>',
      '</div>',
    ].join('');

    return hdHtml + titleHtml + statHtml + descHtml + scanHtml + verHtml + '</div>';
  }

  function _statCard(label, val, sub) {
    return [
      '<div class="ar-stat-card">',
        '<div class="ar-stat-card-label">' + esc(label) + '</div>',
        '<div class="ar-stat-card-val">' + esc(val) + '</div>',
        sub ? '<div class="ar-stat-card-sub">' + esc(sub) + '</div>' : '',
      '</div>',
    ].join('');
  }

  function archive(repoKey) {
    var modal = Modal.open({
      title: 'Repo Arşivle',
      body: [
        '<div class="ar-form-group">',
          '<label class="ar-label">Repo</label>',
          '<input class="ar-input" id="ar-arch-repo" value="' + esc(repoKey) + '" readonly>',
        '</div>',
        '<div style="margin-top:0.75rem;display:flex;flex-direction:column;gap:0.4rem;">',
          '<label class="ar-checkbox-row">',
            '<input type="checkbox" id="ar-arch-force"> Zorla yeniden arşivle',
          '</label>',
          '<label class="ar-checkbox-row">',
            '<input type="checkbox" id="ar-arch-heavy"> Ağır dosyaları dahil et',
          '</label>',
        '</div>',
        '<div id="ar-arch-status" style="margin-top:0.75rem;display:none;">',
          '<div class="ar-log-terminal">',
            '<div class="ar-log-toolbar">',
              '<span class="ar-log-dot" style="background:#e05252"></span>',
              '<span class="ar-log-dot" style="background:#e09a20"></span>',
              '<span class="ar-log-dot" style="background:#4caf50"></span>',
              '<span class="ar-log-title">arşivleniyor…</span>',
            '</div>',
            '<div class="ar-log-body" id="ar-arch-log"></div>',
          '</div>',
        '</div>',
      ].join(''),
      footer: [
        '<button class="ar-btn" onclick="Modal.close()">Kapat</button>',
        '<button class="ar-btn ar-btn-primary" id="ar-arch-submit">Arşivle</button>',
      ].join(''),
    });

    document.getElementById('ar-arch-submit')
            .addEventListener('click', function () {
      var repo    = document.getElementById('ar-arch-repo').value.trim();
      var force   = document.getElementById('ar-arch-force').checked;
      var heavy   = document.getElementById('ar-arch-heavy').checked;
      var statusEl = document.getElementById('ar-arch-status');
      var logEl   = document.getElementById('ar-arch-log');
      var logLines = [];

      document.getElementById('ar-arch-submit').disabled = true;
      statusEl.style.display = '';

      submitArchiveJob(
        { repo: repo, force: force, include_heavy: heavy },
        function (jobId) {
          document.querySelector('#ar-arch-status .ar-log-title').textContent = 'job ' + jobId.slice(0, 8) + '…';
        },
        function (job) {
          // onUpdate
          var newLines = (job.logs || []).slice(logLines.length);
          logLines = job.logs || [];
          newLines.forEach(function (line) {
            var el = document.createElement('div');
            el.className = 'ar-log-line';
            el.textContent = line;
            logEl.appendChild(el);
          });
          logEl.scrollTop = logEl.scrollHeight;
        },
        function (job) {
          // onDone
          if (job.status === 'done') {
            showToast('Arşiv tamamlandı!', 'success');
          } else {
            showToast('Arşiv başarısız: ' + (job.error || '?'), 'error');
          }
        }
      );
    });
  }

  function scan(repoKey) {
    confirmModal(repoKey + ' taranacak. Devam edilsin mi?', function () {
      submitScanJob(repoKey,
        function (jobId) { showToast('Tarama başlatıldı: ' + jobId.slice(0, 8), 'info'); },
        function (job) {
          if (job.status === 'done') {
            showToast('Tarama tamamlandı — ' + ((job.result && job.result.risk_level) || ''), 'success');
            loadDetail(repoKey); // refresh panel
          } else {
            showToast('Tarama başarısız: ' + (job.error || '?'), 'error');
          }
        }
      );
    });
  }

  return { init: init, loadDetail: loadDetail, back: back, archive: archive, scan: scan };
})();  // TilesSPA

// =========================================================
// Dashboard live feed (job polling)
// =========================================================
var DashboardFeed = (function () {
  var _el   = null;
  var _timer = null;

  function init(feedEl) {
    _el = feedEl;
    _refresh();
  }

  function _refresh() {
    apiFetch('/jobs?limit=8')
      .then(function (data) {
        _render(data.jobs || []);
        _timer = setTimeout(_refresh, 5000);
      })
      .catch(function () {
        _timer = setTimeout(_refresh, 10000);
      });
  }

  function _render(jobs) {
    if (!jobs.length) {
      _el.innerHTML = '<p class="ar-empty">Henüz iş yok.</p>';
      return;
    }
    _el.innerHTML = jobs.map(function (job) {
      var running = job.status === 'running';
      return [
        '<div class="ar-job-item">',
          running ? '<span class="ar-pulse"></span>' : statusBadge(job.status),
          '<span class="ar-job-repo">' + esc(job.repo_key || job.job_type) + '</span>',
          '<span class="ar-job-time">' + esc(fmtDateShort(job.started_at)) + '</span>',
        '</div>',
      ].join('');
    }).join('');
  }

  return { init: init };
})();

// =========================================================
// Findings accordion toggle
// =========================================================
function toggleFindingGroup(hd) {
  var body = hd.nextElementSibling;
  if (!body) return;
  var open = body.classList.toggle('open');
  var arrow = hd.querySelector('.ar-finding-arrow');
  if (arrow) arrow.style.transform = open ? 'rotate(90deg)' : '';
}

// =========================================================
// Auto-wire on DOM ready
// =========================================================
document.addEventListener('DOMContentLoaded', function () {
  // Finding group toggles
  document.querySelectorAll('.ar-finding-group-hd').forEach(function (hd) {
    hd.addEventListener('click', function () { toggleFindingGroup(hd); });
    // Open HIGH by default
    if (hd.dataset.severity === 'HIGH') toggleFindingGroup(hd);
  });
});

// Export globals needed by templates
window.Modal       = Modal;
window.TilesSPA    = TilesSPA;
window.DashboardFeed = DashboardFeed;
window.Poller    = Poller;
window.JobLog    = JobLog;
window.DonutChart = DonutChart;
window.renderBars = renderBars;
window.renderTimeline = renderTimeline;
window.submitArchiveJob = submitArchiveJob;
window.submitScanJob = submitScanJob;
window.confirmModal = confirmModal;
window.riskBadge   = riskBadge;
window.statusBadge = statusBadge;
window.esc  = esc;
window.human = human;
window.fmtDate = fmtDate;
window.fmtDateShort = fmtDateShort;
window.toggleFindingGroup = toggleFindingGroup;

// =========================================================
// Upload Modal
// =========================================================
var UploadModal = (function () {
  var _logLines = [];

  function open() {
    var overlay = document.getElementById('ar-upload-modal-overlay');
    if (!overlay) return;
    overlay.style.display = 'flex';
    document.body.style.overflow = 'hidden';
    setTimeout(function () {
      var inp = document.getElementById('um-repo-input');
      if (inp) inp.focus();
    }, 60);
  }

  function close() {
    var overlay = document.getElementById('ar-upload-modal-overlay');
    if (!overlay) return;
    overlay.style.display = 'none';
    document.body.style.overflow = '';
  }

  function copyLog() {
    var body = document.getElementById('um-log-body');
    if (!body) return;
    var text = _logLines.join('\n') || body.innerText;
    navigator.clipboard.writeText(text).then(function () {
      showToast('Kopyalandı', 'success');
    }).catch(function () {});
  }

  function _setStatus(state, text) {
    var badge = document.getElementById('um-status-badge');
    if (!badge) return;
    badge.className = 'ar-modal-status-badge ' + state;
    badge.textContent = text;
  }

  function _appendLog(line) {
    _logLines.push(line);
    var body = document.getElementById('um-log-body');
    var nums = document.getElementById('um-line-nums');
    if (!body) return;
    // Clear placeholder on first line
    if (_logLines.length === 1) body.innerHTML = '';
    var el = document.createElement('span');
    el.textContent = line + '\n';
    body.appendChild(el);
    if (nums) nums.textContent = Array.from({length: _logLines.length}, function(_, i){ return i+1; }).join('\n');
    body.parentElement.scrollTop = body.parentElement.scrollHeight;
  }

  function _initForm() {
    var btn = document.getElementById('um-submit-btn');
    if (!btn || btn._bound) return;
    btn._bound = true;

    btn.addEventListener('click', function () {
      var repo  = document.getElementById('um-repo-input').value.trim();
      var force = document.getElementById('um-force-check').checked;
      var heavy = document.getElementById('um-heavy-check').checked;
      if (!repo) { showToast('Repo adı giriniz.', 'warning'); return; }

      var logTitle = document.getElementById('um-log-title');
      var logBody  = document.getElementById('um-log-body');
      var lineNums = document.getElementById('um-line-nums');

      btn.disabled = true;
      _logLines = [];
      if (logBody)  { logBody.innerHTML = ''; }
      if (lineNums) { lineNums.textContent = '1'; }
      _setStatus('running', 'running');
      if (logTitle) logTitle.textContent = 'Output';

      submitArchiveJob(
        { repo: repo, force: force, include_heavy: heavy },
        function (jobId) {
          if (logTitle) logTitle.textContent = 'job/' + jobId.slice(0, 8);
          _appendLog('Starting job ' + jobId.slice(0, 8) + '…');
        },
        function (job) {
          var newLines = (job.logs || []).slice(_logLines.length);
          newLines.forEach(function (line) { _appendLog(line); });
        },
        function (job) {
          btn.disabled = false;
          if (job.status === 'done') {
            _setStatus('done', 'done');
            showToast('Upload tamamlandı!', 'success');
          } else {
            _setStatus('failed', 'failed');
            if (job.error) _appendLog('ERROR: ' + job.error);
            showToast('Hata: ' + (job.error || '?'), 'error');
          }
        }
      );
    });

    // Enter key on input submits
    var inp = document.getElementById('um-repo-input');
    if (inp) {
      inp.addEventListener('keydown', function (e) {
        if (e.key === 'Enter') btn.click();
      });
    }
  }

  // Escape key closes
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') close();
  });

  _initForm();

  return { open: open, close: close, copyLog: copyLog };
})();
window.UploadModal = UploadModal;
// Legacy alias so existing onclick="UploadPanel.toggle()" still works
window.UploadPanel = { toggle: UploadModal.open, open: UploadModal.open, close: UploadModal.close };

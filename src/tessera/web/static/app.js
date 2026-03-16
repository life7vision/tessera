function humanSize(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + " MB";
  return (bytes / 1073741824).toFixed(1) + " GB";
}

async function copyText(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    await navigator.clipboard.writeText(text);
  }
}

function wireCopyButtons() {
  document.querySelectorAll("[data-copy]").forEach((node) => {
    node.addEventListener("click", async (e) => {
      e.preventDefault();
      await copyText(node.dataset.copy);
      node.classList.add("copied");
      setTimeout(() => node.classList.remove("copied"), 1200);
    });
  });
}

function humanizeSizes() {
  document.querySelectorAll("[data-human-size]").forEach((node) => {
    const v = Number(node.dataset.humanSize);
    if (!Number.isNaN(v)) node.textContent = humanSize(v);
  });
}

function wireLiveSearch() {
  const form = document.querySelector("[data-live-search]");
  const box  = document.querySelector("[data-live-results]");
  if (!form || !box) return;

  const input = form.querySelector("input[name='q']");
  let timer = null;

  input.addEventListener("input", () => {
    clearTimeout(timer);
    const q = input.value.trim();
    if (!q) { box.hidden = true; box.innerHTML = ""; return; }

    timer = setTimeout(async () => {
      const res  = await fetch(`/api/v1/datasets?q=${encodeURIComponent(q)}&limit=6`);
      const data = await res.json();
      box.hidden = data.datasets.length === 0;
      box.innerHTML = data.datasets.map((d) => `
        <a class="live-result-item" href="/dataset/${d.id}">
          <span>${d.name}</span>
          <span style="font-size:0.78rem;color:var(--text-3)">${d.source}</span>
        </a>
      `).join("");
    }, 280);
  });

  document.addEventListener("click", (e) => {
    if (!form.contains(e.target)) { box.hidden = true; }
  });
}

// ── Toast notifications ────────────────────────────────────────────
let _toastContainer = null;

function getToastContainer() {
  if (!_toastContainer) {
    _toastContainer = document.createElement("div");
    _toastContainer.id = "toast-container";
    _toastContainer.style.cssText = [
      "position:fixed", "bottom:1.25rem", "right:1.25rem",
      "display:flex", "flex-direction:column", "gap:0.5rem",
      "z-index:9999", "pointer-events:none",
    ].join(";");
    document.body.appendChild(_toastContainer);
  }
  return _toastContainer;
}

function showToast(message, type = "info", duration = 4000) {
  const colors = {
    info:    { bg: "var(--surface)", border: "var(--border)", color: "var(--text)" },
    success: { bg: "var(--green-bg, #1a2e20)", border: "var(--green, #4caf50)", color: "var(--green, #4caf50)" },
    error:   { bg: "var(--red-bg, #2e1a1a)",   border: "var(--red, #e05252)",    color: "var(--red, #e05252)" },
    warning: { bg: "var(--amber-bg, #2e2510)",  border: "#d4900a",               color: "#d4900a" },
  };
  const c = colors[type] || colors.info;

  const toast = document.createElement("div");
  toast.style.cssText = [
    `background:${c.bg}`, `border:1px solid ${c.border}`, `color:${c.color}`,
    "border-radius:8px", "padding:0.7rem 1rem", "font-size:0.85rem",
    "max-width:320px", "box-shadow:0 4px 12px rgba(0,0,0,0.3)",
    "pointer-events:auto", "opacity:0", "transition:opacity 0.2s",
  ].join(";");
  toast.textContent = message;

  getToastContainer().appendChild(toast);
  requestAnimationFrame(() => { toast.style.opacity = "1"; });

  setTimeout(() => {
    toast.style.opacity = "0";
    setTimeout(() => toast.remove(), 220);
  }, duration);
}

// ── Date formatting ────────────────────────────────────────────────
function formatDate(iso) {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("tr-TR", {
      year: "numeric", month: "short", day: "numeric",
      hour: "2-digit", minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

function humanizeDates() {
  document.querySelectorAll("[data-iso-date]").forEach((node) => {
    const formatted = formatDate(node.dataset.isoDate);
    node.textContent = formatted;
    node.title = node.dataset.isoDate;
  });
}

document.addEventListener("DOMContentLoaded", () => {
  wireCopyButtons();
  humanizeSizes();
  humanizeDates();
  wireLiveSearch();
});

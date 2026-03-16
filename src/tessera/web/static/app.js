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

document.addEventListener("DOMContentLoaded", () => {
  wireCopyButtons();
  humanizeSizes();
  wireLiveSearch();
});

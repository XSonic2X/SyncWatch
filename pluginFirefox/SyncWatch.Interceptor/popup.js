let SERVER = 'http://localhost:5000';

async function loadServer() {
  try {
    const st = await browser.storage.local.get(['server', 'capture']);
    if (st && st.server) SERVER = st.server.replace(/\/+$/, '');
    const c = st && st.capture;
    if (c) {
      const res = document.getElementById('capRes');
      const key = (c.maxW || 1280) + 'x' + (c.maxH || 720);
      if ([...res.options].some(o => o.value === key)) res.value = key;
      if (c.bitrate) document.getElementById('capBitrate').value = Math.round(c.bitrate / 100000) / 10;
      if (c.fps) document.getElementById('capFps').value = c.fps;
      document.getElementById('capAutoFps').checked = !!c.autoFps;
    }
  } catch (e) {}
  document.getElementById('serverUrl').value = SERVER;
}

function readCaptureForm() {
  const parts = document.getElementById('capRes').value.split('x');
  const mbps = Number(document.getElementById('capBitrate').value);
  const fps = Number(document.getElementById('capFps').value);
  return {
    maxW: Number(parts[0]) || 1280,
    maxH: Number(parts[1]) || 720,
    bitrate: Math.round((mbps >= 0.3 ? mbps : 2) * 1000000),
    fps: Math.min(60, Math.max(5, Math.round(fps || 30))),
    autoFps: document.getElementById('capAutoFps').checked
  };
}

async function saveCapture() {
  await browser.storage.local.set({ capture: readCaptureForm() });
}

['capRes', 'capBitrate', 'capFps', 'capAutoFps'].forEach(id => {
  document.getElementById(id).addEventListener('change', () => { saveCapture(); });
});

document.getElementById('saveServer').addEventListener('click', async () => {
  const v = document.getElementById('serverUrl').value.trim().replace(/\/+$/, '');
  if (v) { SERVER = v; await browser.storage.local.set({ server: v }); loadSuggestions(); }
});

document.getElementById('startBtn').addEventListener('click', async () => {
  const [tab] = await browser.tabs.query({ active: true, currentWindow: true });
  if (tab) {
    try { await browser.storage.local.remove('directBroadcast'); } catch (e) {}
    await saveCapture();
    await browser.tabs.executeScript(tab.id, { file: "signalr.min.js", allFrames: true });
    await browser.tabs.executeScript(tab.id, { file: "content.js", allFrames: true });
  }
});

async function injectPlayer(tabId) {
  await browser.tabs.executeScript(tabId, { file: "signalr.min.js", allFrames: true });
  await browser.tabs.executeScript(tabId, { file: "content.js", allFrames: true });
}

document.getElementById('relayBtn').addEventListener('click', async () => {
  const typed = document.getElementById('directUrl').value.trim();
  const [tab] = await browser.tabs.query({ active: true, currentWindow: true });
  if (!tab) return;
  let url = typed;
  if (!url) {
    try {
      const resp = await browser.runtime.sendMessage({ action: "getTabStream", tabId: tab.id });
      if (resp && resp.streamUrl) url = resp.streamUrl;
    } catch (e) {}
  }
  if (!/^https?:\/\//i.test(url)) {
    document.getElementById('foundUrl').textContent = 'Нужна прямая ссылка http(s) на m3u8 или mp4';
    return;
  }
  await browser.storage.local.set({ directBroadcast: { url: url, at: Date.now() } });
  await injectPlayer(tab.id);
});

async function loadFound() {
  const el = document.getElementById('foundUrl');
  try {
    const [tab] = await browser.tabs.query({ active: true, currentWindow: true });
    if (!tab) return;
    const resp = await browser.runtime.sendMessage({ action: "getTabStream", tabId: tab.id });
    if (resp && resp.streamUrl) {
      el.textContent = resp.streamUrl;
      el.title = resp.streamUrl;
      const input = document.getElementById('directUrl');
      if (!input.value) input.value = resp.streamUrl;
    } else {
      el.textContent = 'На вкладке ссылка не поймана — вставьте её вручную';
    }
  } catch (e) {
    el.textContent = 'На вкладке ссылка не поймана — вставьте её вручную';
  }
}

async function loadSuggestions() {
  const listEl = document.getElementById('suggestionsList');
  try {
    const res = await fetch(SERVER + "/api/suggestions");
    const items = await res.json();
    if (!items.length) { listEl.innerHTML = '<div class="empty">Зрители пока ничего не предложили</div>'; return; }
    listEl.innerHTML = "";
    items.forEach(item => {
      const div = document.createElement('div');
      div.className = 'item';
      div.innerHTML = `
        <div class="item-title" title="${item.url}">${item.title || item.url}</div>
        <div class="item-actions">
          <button class="btn-small open-btn">Открыть</button>
          <button class="btn-small btn-delete del-btn">✕</button>
        </div>`;
      div.querySelector('.open-btn').onclick = () => browser.tabs.create({ url: item.url });
      div.querySelector('.del-btn').onclick = async () => {
        await fetch(SERVER + "/api/suggestions/" + item.id, { method: "DELETE" });
        loadSuggestions();
      };
      listEl.appendChild(div);
    });
  } catch (e) {
    listEl.innerHTML = '<div class="empty">Сервер не запущен</div>';
  }
}

loadServer().then(() => { loadSuggestions(); loadFound(); });
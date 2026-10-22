// Детектор прямых видеоссылок — отдельно по вкладкам
const tabStreams = new Map();
const playerSeen = new Map();

function remember(tabId, url, asPlaylist, forceFile) {
    if (tabId < 0 || !url) return;
    if (url.includes('/proxy?url=') || url.includes('/api/live')) return;
    if (/analytics|doubleclick|googlesyndication/i.test(url)) return;
    const nextIsList = asPlaylist || /\.m3u8|\.mpd/i.test(url);
    const file = !!forceFile || /\.mp4|\.webm|\.m4v|\.mkv|\.mov/i.test(url);
    if (!nextIsList && !file) return;
    const prev = tabStreams.get(tabId);
    const prevIsList = prev && /\.m3u8|\.mpd/i.test(prev.url);
    if (prevIsList && !nextIsList) return;
    tabStreams.set(tabId, { url: url, at: Date.now() });
}

browser.webRequest.onBeforeRequest.addListener(
    function(details) {
        remember(details.tabId, details.url, false);
    },
    { urls: ["<all_urls>"] }
);

browser.webRequest.onHeadersReceived.addListener(
    function(details) {
        const headers = details.responseHeaders || [];
        const ct = headers.find(h => (h.name || '').toLowerCase() === 'content-type');
        const type = ct && ct.value ? ct.value.toLowerCase() : '';
        if (type.includes('mpegurl') || type.includes('dash+xml'))
            remember(details.tabId, details.url, true, false);
        else if (type.includes('video/mp4') || type.includes('video/webm') || type.includes('video/mp2t'))
            remember(details.tabId, details.url, false, true);
    },
    { urls: ["<all_urls>"] },
    ["responseHeaders"]
);

function streamOf(tabId) {
    const rec = tabStreams.get(tabId);
    return rec ? { streamUrl: rec.url, at: rec.at } : { streamUrl: null, at: 0 };
}

browser.runtime.onMessage.addListener((message, sender, sendResponse) => {
    if (message.action === "getLastStream") {
        sendResponse(sender.tab ? streamOf(sender.tab.id) : { streamUrl: null, at: 0 });
        return;
    }
    if (message.action === "getTabStream") {
        sendResponse(streamOf(message.tabId));
        return;
    }
    if (message.action === "playerFound") {
        const tabId = sender.tab ? sender.tab.id : -1;
        if (tabId >= 0) playerSeen.set(tabId, Date.now());
        sendResponse({ ok: true });
        return;
    }
    if (message.action === "hadPlayer") {
        const tabId = sender.tab ? sender.tab.id : message.tabId;
        const at = playerSeen.get(tabId) || 0;
        sendResponse({ found: Date.now() - at < 8000 });
        return;
    }
    if (message.action === "getCookies") {
        if (!message.url) {
            sendResponse({ cookie: "" });
            return;
        }
        browser.cookies.getAll({ url: message.url }).then(cookies => {
            sendResponse({ cookie: cookies.map(c => c.name + "=" + c.value).join("; ") });
        }).catch(() => sendResponse({ cookie: "" }));
        return true;
    }
});

browser.tabs.onRemoved.addListener(tabId => {
    tabStreams.delete(tabId);
    playerSeen.delete(tabId);
});

browser.commands.onCommand.addListener(async (command) => {
    if (command === "capture-player") {
        const [tab] = await browser.tabs.query({ active: true, currentWindow: true });
        if (tab) {
            try { await browser.storage.local.remove("directBroadcast"); } catch (e) {}
            await browser.tabs.executeScript(tab.id, { file: "signalr.min.js", allFrames: true });
            await browser.tabs.executeScript(tab.id, { file: "content.js", allFrames: true });
        }
    }
});

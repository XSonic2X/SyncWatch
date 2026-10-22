(async function () {
    let SERVER = "http://localhost:5000";
    const capture = { maxW: 1280, maxH: 720, bitrate: 2000000, fps: 30, autoFps: true, fpsUse: 30 };
    const AUDIO_BITRATE = 96000;
    const RECORDER_SLICE_MS = 255;
    const FLAG_NEW_FILE = 0;
    const FLAG_CONTINUE = 1;
    const RECORDER_STOP_MS = 400;
    const AUDIO_TRACK_WAIT_MS = 700;
    const SUPPRESS_LOCAL_MS = 1200;
    const DIRECT_URL_POLL_MS = 2000;
    const CAPTURE_MANUAL = 0;
    const MIN_EMIT_GAP_MS = 8;
    const WARMUP_MS = 800;
    const REBIND_READY_MS = 1200;

    try {
        const st = await browser.storage.local.get(["server", "capture"]);
        if (st && st.server) SERVER = st.server.replace(/\/+$/, "");
        const c = st && st.capture;
        if (c) {
            const w = Number(c.maxW), h = Number(c.maxH), br = Number(c.bitrate), fps = Number(c.fps);
            if (w >= 160 && w <= 3840) capture.maxW = w;
            if (h >= 90 && h <= 2160) capture.maxH = h;
            if (br >= 200000 && br <= 20000000) capture.bitrate = br;
            if (fps >= 5 && fps <= 60) capture.fps = Math.round(fps);
            capture.autoFps = !!c.autoFps;
            capture.fpsUse = capture.fps;
        }
    } catch (e) { }

    function parentHandlesThisFrame() {
        try {
            return window !== window.top && !!window.parent.document && window.parent.document.nodeType === 1;
        } catch (e) {
            return false;
        }
    }

    if (parentHandlesThisFrame()) return;

    function getAllVideoElements(root = document, depth = 0) {
        if (!root || depth > 5) return [];
        let videos = [];
        try { videos = Array.from(root.querySelectorAll('video')); } catch (e) { }
        try {
            for (const el of root.querySelectorAll('*')) {
                if (el.shadowRoot) videos = videos.concat(getAllVideoElements(el.shadowRoot, depth + 1));
            }
        } catch (e) { }
        try {
            for (const frame of root.querySelectorAll('iframe, frame')) {
                try {
                    if (frame.contentDocument)
                        videos = videos.concat(getAllVideoElements(frame.contentDocument, depth + 1));
                } catch (e) { }
            }
        } catch (e) { }
        return videos;
    }

    function videoIsPlayer(v) {
        if (!v) return false;
        const r = v.getBoundingClientRect();
        return r.width >= 200 && r.height >= 110;
    }

    function findBestVideo() {
        const videos = getAllVideoElements();
        if (videos.length === 0) return null;
        const working = videos.filter(v => !v.error && v.networkState !== 3);
        const list = working.length > 0 ? working : videos;
        const playing = list.find(v => !v.paused && v.currentTime > 0);
        if (playing) return playing;
        return list.sort((a, b) => {
            const rA = a.getBoundingClientRect(), rB = b.getBoundingClientRect();
            return (rB.width * rB.height) - (rA.width * rA.height);
        })[0];
    }

    let video = findBestVideo();
    if (!video || (window !== window.top && !videoIsPlayer(video))) {
        if (window === window.top && !video) {
            setTimeout(async () => {
                let found = false;
                try {
                    const resp = await browser.runtime.sendMessage({ action: "hadPlayer" });
                    found = !!(resp && resp.found);
                } catch (e) { }
                if (!found) alert("SyncWatch: Видеоплеер не найден на этой странице!");
            }, 1500);
        }
        return;
    }
    try { browser.runtime.sendMessage({ action: "playerFound" }); } catch (e) { }

    const bootId = Date.now() + ":" + Math.random();
    window.__syncWatchBoot = bootId;
    if (window.__syncWatchCleanup) window.__syncWatchCleanup();

    const connection = new signalR.HubConnectionBuilder()
        .withUrl(SERVER + "/roomHub")
        .build();

    window.__syncWatchConn = connection;

    let mediaWs = null, recorder = null;
    let stopped = false, recordStream = null;
    let cv = null, ctx = null, cvStream = null;
    let frameTrack = null;
    let captureScaled = false;
    let lastCapturedMedia = -1;
    let lastEmitAt = 0;
    let emitTimer = 0;
    let warmupTimer = 0;
    let captureWarmed = false;
    let liveAnnounced = false;
    let recorderGen = 0;
    let recorderArm = 0;
    let recorderReadyAt = 0;
    let keyRetry = 0;
    let timedCapture = false;
    let recorderHasAudio = false;
    let srcRenewGuard = 0;
    let rVFC_id = null;
    let loopPending = false;
    let keepAliveTimer = null;
    let suppressLocal = false;
    let captureGen = 0;
    let rebindGen = 0;
    let rebindBusy = false;
    let needRenew = false;
    let directUrl = "";
    let directSince = 0;
    let rawAudioStream = null;
    const handedAudio = [];
    let boundSrc = "";
    let relayRestarting = false;
    let relayAgain = false;
    let mediaWatchArmed = false;
    let allowLateAudio = true;
    let endNotified = false;
    let endFlushTimer = 0;
    let pauseDecideTimer = 0;
    let sawEndEdge = false;
    let seenTime = 0;
    let sliceTimer = 0;
    let hostAudible = true;
    let fpsMeasured = false;

    function cleanup() {
        stopped = true;
        if (window.__syncWatchInterval) clearInterval(window.__syncWatchInterval);
        if (window.__syncWatchBgInterval) clearInterval(window.__syncWatchBgInterval);
        if (window.__syncWatchWatcher) clearInterval(window.__syncWatchWatcher);
        if (window.__syncWatchDirectWatch) clearInterval(window.__syncWatchDirectWatch);
        if (emitTimer) { clearTimeout(emitTimer); emitTimer = 0; }
        if (warmupTimer) { clearTimeout(warmupTimer); warmupTimer = 0; }
        if (endFlushTimer) { clearTimeout(endFlushTimer); endFlushTimer = 0; }
        if (pauseDecideTimer) { clearTimeout(pauseDecideTimer); pauseDecideTimer = 0; }
        if (sliceTimer) { clearTimeout(sliceTimer); sliceTimer = 0; }
        if (rVFC_id && video && 'cancelVideoFrameCallback' in video) {
            try { video.cancelVideoFrameCallback(rVFC_id); } catch (e) { }
        }
        detachVideoListeners(video);
        const rec = recorder;
        recorder = null;
        const finish = () => {
            if (finish.done) return;
            finish.done = true;
            if (mediaWs) {
                try { mediaWs.onclose = null; mediaWs.close(); } catch (e) { }
                mediaWs = null;
            }
            if (recordStream) {
                recordStream.getTracks().forEach(t => { try { t.stop(); } catch (e) { } });
            }
            recordStream = null;
            if (cvStream) {
                cvStream.getTracks().forEach(t => { try { t.stop(); } catch (e) { } });
                cvStream = null;
            }
            frameTrack = null;
            restorePageSound(false);
            try { connection.stop(); } catch (e) { }
        };
        if (rec && rec.state !== "inactive") {
            rec.ondataavailable = null;
            rec.onstop = finish;
            try { rec.stop(); } catch (e) { finish(); }
            setTimeout(finish, RECORDER_STOP_MS);
        } else finish();
    }
    window.__syncWatchCleanup = cleanup;
    connection.on("ForceDisconnect", () => cleanup());

    function emitCanvasFrame() {
        emitTimer = 0;
        if (holdFrames) return;
        if (!frameTrack || frameTrack.readyState !== "live" || typeof frameTrack.requestFrame !== "function") return;
        lastEmitAt = performance.now();
        try { frameTrack.requestFrame(); } catch (e) { }
    }

    function drawFrame() {
        try {
            if (holdFrames) return;
            if (!(ctx && video && video.readyState >= 2)) return;
            ctx.drawImage(video, 0, 0, cv.width, cv.height);
            if (!frameTrack || frameTrack.readyState !== "live") return;
            const wait = MIN_EMIT_GAP_MS - (performance.now() - lastEmitAt);
            if (wait <= 0) {
                if (emitTimer) { clearTimeout(emitTimer); emitTimer = 0; }
                emitCanvasFrame();
            } else if (!emitTimer) {
                emitTimer = setTimeout(emitCanvasFrame, wait);
            }
        } catch (e) { }
    }

    function renderFrame(now, metadata) {
        loopPending = false;
        if (stopped || !video) return;
        if (recordStream && !relayRestarting && !directUrl && needsScale() !== captureScaled) {
            restartRelay();
            return;
        }
        noteGuardTime();
        if (!video.paused) {
            const mediaTime = metadata && typeof metadata.mediaTime === "number" ? metadata.mediaTime : (video.currentTime || 0);
            if (mediaTime !== lastCapturedMedia) {
                lastCapturedMedia = mediaTime;
                drawFrame();
            }
        }
        ensureLoop();
    }

    function ensureLoop() {
        if (loopPending || stopped || !video) return;
        loopPending = true;
        if ('requestVideoFrameCallback' in video) {
            rVFC_id = video.requestVideoFrameCallback(renderFrame);
        } else {
            rVFC_id = requestAnimationFrame(renderFrame);
        }
    }

    function armBgInterval() {
        if (window.__syncWatchBgInterval) clearInterval(window.__syncWatchBgInterval);
        const fps = Math.max(1, capture.fpsUse || capture.fps || 30);
        window.__syncWatchBgInterval = setInterval(() => {
            if (!stopped && video && !video.paused && document.hidden) drawFrame();
        }, 1000 / fps);
    }
    armBgInterval();

    document.addEventListener('visibilitychange', () => {
        if (!document.hidden && !stopped && video && !video.paused) {
            loopPending = false;
            ensureLoop();
        }
    });

    document.addEventListener("click", () => { resumeLocalAudio(); }, true);

    function controlsLive() {
        return !suppressLocal && video && video.isConnected && !video.error;
    }

    function syncRecorder() {
        if (!recorder) return;
        try {
            if (video && video.paused && recorder.state === 'recording') recorder.pause();
            else if (video && !video.paused && recorder.state === 'paused') recorder.resume();
        } catch (e) { }
    }

    function finishCapture(done) {
        const finish = () => { if (typeof done === "function") done(); };
        if (sliceTimer) { clearTimeout(sliceTimer); sliceTimer = 0; }
        if (endFlushTimer || !recorder || recorder.state !== "recording") {
            finish();
            return;
        }
        drawFrame();
        try { recorder.requestData(); } catch (e) { }
        endFlushTimer = setTimeout(() => {
            endFlushTimer = 0;
            const rec = recorder;
            if (!rec || rec.state !== "recording") {
                finish();
                return;
            }
            drawFrame();
            let finished = false;
            const doneOnce = () => {
                if (finished) return;
                finished = true;
                finish();
            };
            const prevStop = rec.onstop;
            rec.onstop = () => {
                if (typeof prevStop === "function") prevStop();
                setTimeout(doneOnce, 200);
            };
            try { rec.stop(); } catch (e) { doneOnce(); }
            setTimeout(doneOnce, 800);
        }, RECORDER_STOP_MS);
    }

    function playbackEnded() {
        if (!video) return false;
        if (video.ended) return true;
        const dur = video.duration;
        return Number.isFinite(dur) && dur > 1 && (video.currentTime || 0) >= dur - 0.4;
    }

    const END_GUARD_SEC = 8;
    function closeToEnd() {
        if (!video) return false;
        if (video.ended) return true;
        const dur = video.duration;
        return Number.isFinite(dur) && dur > 1 && (video.currentTime || 0) >= dur - END_GUARD_SEC;
    }

    function notifyEnded() {
        if (directUrl || !video || endNotified) return;
        endNotified = true;
        drawFrame();
        needRenew = true;
        if (!video.isConnected) return;
        const at = Number.isFinite(video.duration) ? video.duration : (video.currentTime || 0);
        if (connection.state === signalR.HubConnectionState.Connected)
            connection.invoke("SendControl", "main-room", "ended", at);
    }

    function discardTail() {
        if (mediaWs && mediaWs.readyState === WebSocket.OPEN) {
            try { mediaWs.send("discard"); } catch (e) { }
        }
    }

    function scheduleCleanFile() {
        if (keyRetry >= 2 || stopped || directUrl) return;
        keyRetry++;
        timedCapture = true;
        holdFrames = true;
        if (emitTimer) { clearTimeout(emitTimer); emitTimer = 0; }
        setTimeout(() => { if (!stopped && !directUrl) restartRecorder(); }, 0);
    }

    function restartRecorder() {
        recorderGen++;
        const old = recorder;
        recorder = null;
        let started = false;
        const startNew = () => {
            if (started || stopped) return;
            started = true;
            startRecorder(true);
        };
        if (old && old.state !== 'inactive') {
            old.ondataavailable = null;
            old.onstop = startNew;
            try { old.stop(); } catch (e) { startNew(); }
            setTimeout(startNew, RECORDER_STOP_MS);
        } else {
            startNew();
        }
    }

    let renewBusy = false;
    let renewQuietUntil = 0;
    let fileGuardUntil = 0;
    let guardTime = 0;
    let guardSeen = 0;
    let guardAt = 0;
    let renewOverride = false;
    let holdFrames = false;

    function noteGuardTime() {
        if (!fileGuardUntil || !video || performance.now() >= fileGuardUntil) return;
        const cur = video.currentTime || 0;
        if (cur > guardSeen) guardSeen = cur;
    }
    function seekJumped() {
        if (!video || !guardAt) return false;
        const cur = video.currentTime || 0;
        const elapsed = (performance.now() - guardAt) / 1000;
        if (cur + 0.6 < guardSeen) return true;
        return cur > guardTime + elapsed + 2;
    }
    function youngFile() {
        return fileGuardUntil && performance.now() < fileGuardUntil && !seekJumped();
    }
    function renewStream(force) {
        const override = renewOverride;
        renewOverride = false;
        if (!needRenew && !force) return;
        if (!override && youngFile()) {
            needRenew = false;
            return;
        }
        if (renewBusy) {
            needRenew = false;
            return;
        }
        if (!force && performance.now() < renewQuietUntil) {
            needRenew = false;
            return;
        }
        needRenew = false;
        renewBusy = true;
        renewQuietUntil = performance.now() + 5000;
        stopKeyframeBurst();
        setTimeout(() => { renewBusy = false; }, 700);
        liveAnnounced = false;
        timedCapture = true;
        keyRetry = 0;
        if (mediaWs && mediaWs.readyState === WebSocket.OPEN) {
            try { mediaWs.send("renew"); } catch (e) { }
        }
        if (connection.state === signalR.HubConnectionState.Connected)
            connection.invoke("SendControl", "main-room", "renew", video ? (video.currentTime || 0) : 0).catch(() => { });
        restartRecorder();
    }

    const onPlay = () => {
        if (pauseDecideTimer) { clearTimeout(pauseDecideTimer); pauseDecideTimer = 0; }
        if (!(video && Number.isFinite(video.duration) && video.duration > 1
            && (video.currentTime || 0) >= video.duration - 1))
            sawEndEdge = false;
        endNotified = false;
        resumeLocalAudio();
        if (!directUrl) ensureLoop();
        if (suppressLocal) return;
        if (srcRenewGuard && performance.now() < srcRenewGuard) {
            if (!directUrl) syncRecorder();
        } else if (!directUrl) {
            if (needRenew && !captureVideoLive()) {
                renewOverride = true;
                restartRelay();
            } else if (needRenew && mediaCaptureEnded()) {
                const stream = captureRaw(video);
                const clone = hasLiveAudio(stream) ? wireLocal(stream) : null;
                if (clone && recordStream) {
                    recordStream.getAudioTracks().forEach(t => { try { recordStream.removeTrack(t); } catch (e) { } });
                    recordStream.addTrack(clone);
                }
                renewOverride = true;
                renewStream();
            } else if (needRenew) {
                renewOverride = true;
                renewStream();
            }
            else syncRecorder();
        }
        if (!controlsLive()) return;
        if (connection.state === signalR.HubConnectionState.Connected)
            connection.invoke("SendControl", "main-room", "play", video.currentTime);
    };

    const onPause = () => {
        if (directUrl) {
            if (!controlsLive()) return;
            if (connection.state === signalR.HubConnectionState.Connected)
                connection.invoke("SendControl", "main-room", "pause", video.currentTime);
            return;
        }
        if (closeToEnd()) return;
        if (pauseDecideTimer) clearTimeout(pauseDecideTimer);
        pauseDecideTimer = setTimeout(() => {
            pauseDecideTimer = 0;
            if (!video || stopped) return;
            if (closeToEnd()) return;
            drawFrame();
            syncRecorder();
            needRenew = true;
            if (!controlsLive()) return;
            discardTail();
            if (connection.state === signalR.HubConnectionState.Connected)
                connection.invoke("SendControl", "main-room", "pause", video.currentTime);
        }, 200);
    };

    const onEnded = () => {
        if (pauseDecideTimer) { clearTimeout(pauseDecideTimer); pauseDecideTimer = 0; }
        finishCapture(notifyEnded);
    };

    const onSeeked = () => {
        if (pauseDecideTimer) { clearTimeout(pauseDecideTimer); pauseDecideTimer = 0; }
        if (suppressLocal) {
            drawFrame();
            return;
        }
        const srcNow = video ? (video.currentSrc || video.src || "") : "";
        if (boundSrc && srcNow && srcNow !== boundSrc) {
            drawFrame();
            return;
        }
        if (!directUrl && srcRenewGuard && performance.now() < srcRenewGuard) {
            seenTime = video ? (video.currentTime || 0) : 0;
            drawFrame();
            return;
        }
        const cur = video ? (video.currentTime || 0) : 0;
        const jumpedBack = cur + 0.35 < seenTime;
        seenTime = cur;
        if (jumpedBack || !playbackEnded()) {
            sawEndEdge = false;
            endNotified = false;
            needRenew = true;
        }
        drawFrame();
        if (!directUrl && needRenew && video && !video.paused) renewStream();
        if (!controlsLive()) return;
        if (connection.state === signalR.HubConnectionState.Connected)
            connection.invoke("SendControl", "main-room", "seek", video.currentTime);
    };

    function attachVideoListeners(v) {
        if (!v) return;
        v.addEventListener('play', onPlay);
        v.addEventListener('pause', onPause);
        v.addEventListener('ended', onEnded);
        v.addEventListener('seeked', onSeeked);
    }

    function detachVideoListeners(v) {
        if (!v) return;
        try {
            v.removeEventListener('play', onPlay);
            v.removeEventListener('pause', onPause);
            v.removeEventListener('ended', onEnded);
            v.removeEventListener('seeked', onSeeked);
        } catch (e) { }
    }

    connection.on("ReceiveControl", (action, time) => {
        if (action === "ended" || action === "live" || action === "renew") return;
        suppressLocal = true;
        setTimeout(() => { suppressLocal = false; }, SUPPRESS_LOCAL_MS);

        if (!video) return;

        if (action === "pause") {
            if (!directUrl && (endNotified || playbackEnded())) return;
            if (!directUrl) {
                needRenew = true;
                discardTail();
            }
            video.pause();
            if (time >= 0 && Math.abs(video.currentTime - time) > 0.4) {
                video.currentTime = Math.max(0, time);
            }
            setTimeout(() => { drawFrame(); }, 150);
        } else if (action === "play") {
            video.play().catch(() => { });
            if (!directUrl) {
                renewOverride = true;
                renewStream(true);
            }
        } else if (action === "seek") {
            const target = Math.max(0, time);
            const here = video.currentTime || 0;
            if (!directUrl && Math.abs(here - target) > 0.35) {
                const wasEnded = (video && video.ended) || endNotified || mediaCaptureEnded();
                endNotified = false;
                sawEndEdge = false;
                needRenew = true;
                seenTime = target;
                try { video.currentTime = target; } catch (e) { }
                if (video.paused) video.play().catch(() => { });

                // Если видео уже досмотрели до конца — старый захват мёртв,
                // делаем чистый перезапуск захвата вместо попытки оживить старый поток
                if (wasEnded) {
                    renewOverride = true;
                    restartRelay();
                } else {
                    renewStream(true);
                }
            } else {
                try { video.currentTime = target; } catch (e) { }
            }
            setTimeout(() => { drawFrame(); }, 200);
        }
    });

    function detectFps(v) {
        return new Promise((resolve) => {
            let settled = false;
            const done = (n) => {
                if (settled) return;
                settled = true;
                const fps = Math.round(Number(n) || 0);
                resolve(fps >= 8 && fps <= 60 ? fps : 0);
            };
            if (!v || v.paused) { done(0); return; }
            if (typeof v.requestVideoFrameCallback === "function") {
                const stamps = [];
                let handle = 0;
                const begun = performance.now();
                const finish = () => {
                    if (handle && typeof v.cancelVideoFrameCallback === "function") {
                        try { v.cancelVideoFrameCallback(handle); } catch (e) { }
                    }
                    handle = 0;
                    if (stamps.length < 4) { done(0); return; }
                    const deltas = [];
                    for (let i = 1; i < stamps.length; i++) {
                        const d = stamps[i] - stamps[i - 1];
                        if (d > 5 && d < 200) deltas.push(d);
                    }
                    if (deltas.length < 3) { done(0); return; }
                    deltas.sort((a, b) => a - b);
                    done(1000 / deltas[Math.floor(deltas.length / 2)]);
                };
                const onFrame = (now) => {
                    if (settled) return;
                    stamps.push(now);
                    if (stamps.length >= 16 || performance.now() - begun > 800) finish();
                    else handle = v.requestVideoFrameCallback(onFrame);
                };
                handle = v.requestVideoFrameCallback(onFrame);
                setTimeout(finish, 1000);
                return;
            }
            if (typeof v.mozDecodedFrames === "number") {
                const start = v.mozDecodedFrames;
                const t0 = performance.now();
                setTimeout(() => {
                    const dt = (performance.now() - t0) / 1000;
                    done(dt > 0.2 ? (v.mozDecodedFrames - start) / dt : 0);
                }, 600);
                return;
            }
            done(0);
        });
    }

    async function chooseFps() {
        if (fpsMeasured) {
            armBgInterval();
            return;
        }
        capture.fpsUse = capture.fps;
        if (!capture.autoFps) {
            fpsMeasured = true;
            armBgInterval();
            return;
        }
        try {
            const detected = await detectFps(video);
            capture.fpsUse = detected || capture.fps;
        } catch (e) {
            capture.fpsUse = capture.fps;
        }
        fpsMeasured = true;
        armBgInterval();
    }

    function captureVideoLive() {
        return !!(recordStream && recordStream.getVideoTracks().some(t => t.readyState === "live"));
    }

    function mediaCaptureEnded() {
        if (!rawAudioStream) return false;
        const tracks = rawAudioStream.getAudioTracks();
        return tracks.length > 0 && tracks.every(t => t.readyState === "ended");
    }

    function localGraph() {
        const existing = window.__syncWatchLocalAudio;
        if (existing && existing.actx && existing.actx.state !== "closed") return existing;
        const actx = new (window.AudioContext || window.webkitAudioContext)();
        const gainNode = actx.createGain();
        const dest = actx.createMediaStreamDestination();
        gainNode.connect(actx.destination);
        const local = { actx, gainNode, dest, srcNode: null };
        window.__syncWatchLocalAudio = local;
        try { actx.resume(); } catch (e) { }
        return local;
    }

    function hadCapture(v) {
        const local = window.__syncWatchLocalAudio;
        return !!(rawAudioStream || handedAudio.length
            || (local && local.srcNode)
            || (v && v.__syncWatchCaptures && v.__syncWatchCaptures.length));
    }

    function restorePageSound(keepContext) {
        const v = video;
        const local = window.__syncWatchLocalAudio;
        if (!hadCapture(v)) {
            if (!keepContext && local && local.actx && local.actx.state !== "closed") {
                try { local.actx.close(); } catch (e) { }
                window.__syncWatchLocalAudio = null;
            }
            return;
        }
        if (local && local.srcNode && local.dest) {
            try { local.srcNode.disconnect(local.dest); } catch (e) { }
        }
        handedAudio.forEach(t => { try { t.stop(); } catch (e) { } });
        handedAudio.length = 0;
        if (local && local.gainNode && local.actx) {
            try { local.srcNode && local.srcNode.connect(local.gainNode); } catch (e) { }
            try { local.gainNode.connect(local.actx.destination); } catch (e) { }
            try {
                const audible = v && !v.muted && v.volume > 0;
                local.gainNode.gain.value = audible ? (v.volume || 1) : 0;
            } catch (e) { }
            try { if (local.actx.state !== "closed") local.actx.resume(); } catch (e) { }
        }
    }

    function hasLiveAudio(stream) {
        return !!(stream && stream.getAudioTracks().some(t => t.readyState === "live"));
    }

    function resumeLocalAudio() {
        const local = window.__syncWatchLocalAudio;
        if (!local || !local.actx || local.actx.state === "closed") return;
        if (local.actx.state !== "running") {
            try { local.actx.resume(); } catch (e) { }
        }
    }

    function syncHostSound(v) {
        const local = window.__syncWatchLocalAudio;
        if (!v) return;
        const audible = !v.muted && v.volume > 0;
        const turnedOn = audible && !hostAudible;
        hostAudible = audible;
        if (local && local.gainNode)
            local.gainNode.gain.value = audible ? (v.volume || 1) : 0;
        if (!audible || !local) return;
        resumeLocalAudio();
        const sourceDead = mediaCaptureEnded() || !local.srcNode;
        if (!turnedOn && !sourceDead) return;
        const stream = captureRaw(v);
        if (hasLiveAudio(stream)) wireLocal(stream);
    }

    function wireLocal(stream) {
        const local = localGraph();
        if (!local || !hasLiveAudio(stream)) return null;
        resumeLocalAudio();
        try { if (local.srcNode) local.srcNode.disconnect(); } catch (e) { }
        let srcNode = null;
        try { srcNode = local.actx.createMediaStreamSource(stream); }
        catch (e) { return null; }
        srcNode.connect(local.gainNode);
        srcNode.connect(local.dest);
        local.srcNode = srcNode;
        local.gainNode.gain.value = video && (video.muted || video.volume === 0) ? 0 : (video ? (video.volume || 1) : 1);
        hostAudible = !(video && (video.muted || video.volume === 0));
        const tracks = local.dest.stream.getAudioTracks();
        if (!tracks.length) return null;
        const clone = tracks[0].clone();
        handedAudio.push(clone);
        return clone;
    }

    function ensureRecorderAudio() {
        if (!recordStream || hasLiveAudio(recordStream)) return;
        let raw = rawAudioStream;
        if (!hasLiveAudio(raw)) raw = captureRaw(video);
        const clone = hasLiveAudio(raw) ? wireLocal(raw) : null;
        if (!clone || !recordStream) return;
        recordStream.getAudioTracks().forEach(t => {
            try { recordStream.removeTrack(t); } catch (e) { }
            try { t.stop(); } catch (e) { }
        });
        try { recordStream.addTrack(clone); } catch (e) { }
    }

    function adoptRaw(stream) {
        const prev = rawAudioStream;
        if (!stream || stream === prev) return stream || prev;
        if (!hasLiveAudio(stream)) return hasLiveAudio(prev) ? prev : stream;
        rawAudioStream = stream;
        if (prev && prev !== stream)
            prev.getTracks().forEach(t => { try { t.stop(); } catch (e) { } });
        return stream;
    }

    function rememberCapture(v, stream) {
        if (!v || !stream) return;
        if (!v.__syncWatchCaptures) v.__syncWatchCaptures = [];
        if (v.__syncWatchCaptures.indexOf(stream) < 0) v.__syncWatchCaptures.push(stream);
    }

    function captureRaw(v) {
        const prev = rawAudioStream;
        let stream = null;
        try {
            stream = typeof v.mozCaptureStream === "function" ? v.mozCaptureStream()
                : (typeof v.captureStream === "function" ? v.captureStream() : null);
        } catch (e) { stream = null; }
        if (stream) rememberCapture(v, stream);
        if (!stream) return prev;
        if (stream === prev) return prev;
        if (!hasLiveAudio(stream)) {
            if (!stream.__syncWatchWait) {
                stream.__syncWatchWait = true;
                const onAdd = () => {
                    if (!hasLiveAudio(stream)) return;
                    stream.removeEventListener("addtrack", onAdd);
                    adoptRaw(stream);
                    if (v === video) wireLocal(stream);
                };
                stream.addEventListener("addtrack", onAdd);
            }
            return hasLiveAudio(prev) ? prev : stream;
        }
        return adoptRaw(stream);
    }

    function attachAudio(v) {
        if (!v) return null;
        if (!v.__syncWatchVol) {
            v.__syncWatchVol = true;
            v.addEventListener("volumechange", () => {
                if (v !== video) return;
                syncHostSound(v);
            });
        }
        if (!window.__syncWatchAudioPoke) {
            window.__syncWatchAudioPoke = true;
            const poke = () => resumeLocalAudio();
            document.addEventListener("pointerdown", poke, true);
            document.addEventListener("keydown", poke, true);
        }
        const stream = captureRaw(v);
        if (!stream) return null;
        if (hasLiveAudio(stream)) return wireLocal(stream);
        const onAdd = () => {
            if (!hasLiveAudio(stream) || stopped || v !== video) return;
            stream.removeEventListener("addtrack", onAdd);
            const clone = wireLocal(stream);
            resumeLocalAudio();
            if (!clone || !allowLateAudio) return;
            if (recorderHasAudio || (recordStream && hasLiveAudio(recordStream))) return;
            if (recordStream && recordStream.getAudioTracks().length === 0)
                recordStream.addTrack(clone);
            if (!captureWarmed || relayRestarting) return;
            if (recorder && recorder.state !== "inactive") {
                allowLateAudio = false;
                needRenew = true;
                renewStream();
            }
        };
        stream.addEventListener("addtrack", onAdd);
        return null;
    }

    function tearDownCapture() {
        if (warmupTimer) { clearTimeout(warmupTimer); warmupTimer = 0; }
        const old = recorder;
        recorder = null;
        const stoppedRec = new Promise(resolve => {
            if (!old || old.state === "inactive") {
                resolve();
                return;
            }
            let settled = false;
            const finish = () => {
                if (settled) return;
                settled = true;
                resolve();
            };
            old.onstop = finish;
            try { old.stop(); } catch (e) { finish(); }
            setTimeout(finish, RECORDER_STOP_MS);
        });
        return stoppedRec.then(() => {
            captureGen++;
            captureWarmed = false;
            if (old) old.ondataavailable = null;
            if (recordStream) {
                recordStream.getTracks().forEach(t => { try { t.stop(); } catch (e) { } });
            }
            recordStream = null;
            if (cvStream) {
                cvStream.getTracks().forEach(t => { try { t.stop(); } catch (e) { } });
                cvStream = null;
            }
            ctx = null;
            cv = null;
            frameTrack = null;
            lastCapturedMedia = -1;
            lastEmitAt = 0;
            if (emitTimer) { clearTimeout(emitTimer); emitTimer = 0; }
        });
    }

    function waitForAudio(ms) {
        return new Promise(resolve => {
            if (!rawAudioStream || rawAudioStream.getAudioTracks().length) {
                resolve();
                return;
            }
            let done = false;
            const finish = () => {
                if (done) return;
                done = true;
                rawAudioStream && rawAudioStream.removeEventListener("addtrack", onAdd);
                resolve();
            };
            const onAdd = () => {
                if (rawAudioStream && rawAudioStream.getAudioTracks().length) finish();
            };
            rawAudioStream.addEventListener("addtrack", onAdd);
            setTimeout(finish, ms);
        });
    }

    function bindMediaWatch(v) {
        if (!v || v.__syncWatchMediaWatch) return;
        v.__syncWatchMediaWatch = true;
        let pending = false;
        const mark = () => { pending = true; };
        v.addEventListener("emptied", mark);
        v.addEventListener("loadstart", mark);
        v.addEventListener("loadeddata", () => {
            if (!pending || !mediaWatchArmed || stopped || v !== video || v.ended) return;
            pending = false;
            if (rebindBusy || v.readyState < 2 || v.videoWidth <= 0) return;
            followMediaChange();
        });
    }

    async function restartRelay() {
        const override = renewOverride;
        renewOverride = false;
        if (stopped || directUrl || !video) return;
        if (!override && youngFile()) return;
        needRenew = false;
        if (relayRestarting) {
            relayAgain = true;
            return;
        }
        relayRestarting = true;
        suppressLocal = true;
        try {
            do {
                relayAgain = false;
                allowLateAudio = true;
                boundSrc = video.currentSrc || video.src || boundSrc;
                await chooseFps();
                if (stopped || directUrl || !video) return;
                if (relayAgain) continue;

                await tearDownCapture();
                if (stopped || directUrl || !video || relayAgain) continue;
                if (!setupCapture()) return;
                if (rawAudioStream && !rawAudioStream.getAudioTracks().length) {
                    await waitForAudio(AUDIO_TRACK_WAIT_MS);
                    if (stopped || directUrl || !video) return;
                    if (relayAgain) continue;
                    if (rawAudioStream && rawAudioStream.getAudioTracks().length
                        && recordStream && recordStream.getAudioTracks().length === 0) {
                        const clone = wireLocal(rawAudioStream);
                        if (clone) recordStream.addTrack(clone);
                    }
                }
                if (relayAgain) continue;

                boundSrc = video.currentSrc || video.src || boundSrc;
                liveAnnounced = false;
                if (mediaWs && mediaWs.readyState === WebSocket.OPEN) {
                    try { mediaWs.send("renew"); } catch (e) { }
                }
                if (connection.state === signalR.HubConnectionState.Connected)
                    connection.invoke("SendControl", "main-room", "renew", video.currentTime || 0).catch(() => { });
                captureWarmed = false;
                if (mediaWs && mediaWs.readyState === WebSocket.OPEN) {
                    startRecorder();
                } else if (!mediaWs || mediaWs.readyState === WebSocket.CLOSED) {
                    connectMediaWs();
                }
                if (connection.state === signalR.HubConnectionState.Connected) {
                    connection.invoke("BroadcastVideo", "main-room", "", window.location.href,
                        video.currentTime || 0, !!video.paused, "relay").catch(() => { });
                }
            } while (relayAgain && !stopped && !directUrl);
        } finally {
            relayRestarting = false;
            if (video && !video.paused) needRenew = false;
            setTimeout(() => { suppressLocal = false; }, SUPPRESS_LOCAL_MS);
            if (relayAgain && !stopped && !directUrl) {
                relayAgain = false;
                restartRelay();
            }
        }
    }

    function setupCapture() {
        if (stopped || recordStream) return true;
        if (!video) return false;

        try {
            const audioTrack = attachAudio(video);
            const scale = needsScale();
            frameTrack = null;
            lastCapturedMedia = -1;
            lastEmitAt = 0;
            captureScaled = scale;

            const vw = video.videoWidth || capture.maxW, vh = video.videoHeight || capture.maxH;
            let w = vw, h = vh;
            if (w > capture.maxW || h > capture.maxH) {
                const k = Math.min(capture.maxW / w, capture.maxH / h);
                w = Math.max(2, Math.round(w * k / 2) * 2);
                h = Math.max(2, Math.round(h * k / 2) * 2);
            }
            cv = document.createElement("canvas");
            cv.width = w;
            cv.height = h;
            ctx = cv.getContext("2d", { alpha: false });
            if (!ctx) return false;
            ctx.imageSmoothingEnabled = true;
            try { ctx.imageSmoothingQuality = "high"; } catch (e) { }

            cvStream = openCanvasStream();
            const videoTrack = cvStream && cvStream.getVideoTracks()[0];
            if (!videoTrack) return false;
            drawFrame();

            if (!keepAliveTimer) {
                keepAliveTimer = setInterval(() => {
                    if (stopped || directUrl || !video) return;
                    if (video.ended && endNotified && (!recorder || recorder.state !== "recording")) return;
                    const stale = performance.now() - lastEmitAt > 90;
                    if (video.paused || stale || closeToEnd()) drawFrame();
                    if (!video.ended && closeToEnd() && recorder && recorder.state === "recording") {
                        try { recorder.requestData(); } catch (e) { }
                    }
                }, 100);
            }
            ensureLoop();

            const tracks = [videoTrack];
            if (audioTrack) tracks.push(audioTrack);
            recordStream = new MediaStream(tracks);
            return true;
        } catch (e) {
            console.error("[SyncWatch] Ошибка захвата:", e);
            return false;
        }
    }

    function needsScale() {
        const w = video && video.videoWidth || 0;
        const h = video && video.videoHeight || 0;
        if (w < 2 || h < 2) return captureScaled;
        return w > capture.maxW || h > capture.maxH;
    }

    function retieCanvas() {
        if (!recordStream || !cv) return;
        if (emitTimer) { clearTimeout(emitTimer); emitTimer = 0; }
        try { if (ctx && video && video.readyState >= 2) ctx.drawImage(video, 0, 0, cv.width, cv.height); } catch (e) { }
        const stream = openCanvasStream();
        const track = stream && stream.getVideoTracks()[0];
        if (!track) {
            if (stream) stream.getTracks().forEach(t => { try { t.stop(); } catch (e) { } });
            return;
        }
        if (cvStream && cvStream !== stream)
            cvStream.getTracks().forEach(t => { try { t.stop(); } catch (e) { } });
        cvStream = stream;
        frameTrack = track;
        recordStream.getVideoTracks().forEach(t => {
            try { recordStream.removeTrack(t); } catch (e) { }
            try { t.stop(); } catch (e) { }
        });
        recordStream.addTrack(track);
        lastEmitAt = 0;
        lastCapturedMedia = -1;
    }

    function openCanvasStream() {
        if (timedCapture) {
            try {
                const stream = cv.captureStream(Math.max(8, capture.fpsUse || capture.fps || 30));
                const track = stream && stream.getVideoTracks()[0];
                frameTrack = track && typeof track.requestFrame === "function" ? track : null;
                if (stream) return stream;
            } catch (e) { }
        }
        try {
            const stream = cv.captureStream(CAPTURE_MANUAL);
            const track = stream.getVideoTracks()[0];
            if (track && typeof track.requestFrame === "function") {
                frameTrack = track;
                return stream;
            }
            stream.getTracks().forEach(t => { try { t.stop(); } catch (e) { } });
        } catch (e) { }
        frameTrack = null;
        return cv.captureStream(Math.max(1, capture.fpsUse || capture.fps || 30));
    }

    function signalLiveFile() {
        if (liveAnnounced || stopped || directUrl) return;
        liveAnnounced = true;
        fileGuardUntil = performance.now() + 8000;
        guardTime = video ? (video.currentTime || 0) : 0;
        guardSeen = guardTime;
        guardAt = performance.now();
        if (connection.state !== signalR.HubConnectionState.Connected) return;
        const at = video ? (video.currentTime || 0) : 0;
        connection.invoke("SendControl", "main-room", "live", at).catch(() => { });
    }

    let headerPump = 0;
    function armHeaderPump() {
        if (headerPump) return;
        let n = 0;
        headerPump = setInterval(() => {
            n++;
            if (stopped || n > 40) {
                clearInterval(headerPump);
                headerPump = 0;
                return;
            }
            drawFrame();
        }, 100);
    }

    let keyBurst = 0;
    let stamp = 0;
    function stampFrame() {
        try {
            if (holdFrames || !(ctx && cv)) return;
            if (video && video.readyState >= 2) {
                ctx.drawImage(video, 0, 0, cv.width, cv.height);
            }
            stamp = (stamp + 1) & 255;
            ctx.fillStyle = "rgb(" + stamp + "," + (255 - stamp) + ",0)";
            ctx.fillRect(0, 0, 2, 2);
            if (emitTimer) { clearTimeout(emitTimer); emitTimer = 0; }
            emitCanvasFrame();
        } catch (e) { }
    }
    function stopKeyframeBurst() {
        if (!keyBurst) return;
        clearInterval(keyBurst);
        keyBurst = 0;
    }
    function armKeyframeBurst() {
        if (keyBurst) return;
        let n = 0;
        keyBurst = setInterval(() => {
            n++;
            if (stopped || liveAnnounced || n > 24) {
                clearInterval(keyBurst);
                keyBurst = 0;
                return;
            }
            stampFrame();
        }, 40);
    }

    function vintWidth(b) {
        if (!b) return 0;
        let n = 1;
        let bit = 0x80;
        while ((b & bit) === 0) {
            n++;
            bit >>= 1;
            if (n > 8) return 0;
        }
        return n;
    }

    function realClusterAt(bytes) {
        const last = Math.min(bytes.length - 6, 12000);
        for (let i = 0; i <= last; i++) {
            if (bytes[i] !== 0x1F || bytes[i + 1] !== 0x43 || bytes[i + 2] !== 0xB6 || bytes[i + 3] !== 0x75) continue;
            const width = vintWidth(bytes[i + 4]);
            if (!width || i + 4 + width >= bytes.length) continue;
            if (bytes[i + 4 + width] === 0xE7) return i;
        }
        return -1;
    }

    function openRecorder(publish) {
        if (stopped || !recordStream || !mediaWs || mediaWs.readyState !== WebSocket.OPEN) {
            holdFrames = false;
            return;
        }
        if (recorder && recorder.state !== 'inactive') {
            holdFrames = false;
            return;
        }
        const mine = ++recorderGen;
        let first = true;
        let heldHeader = null;
        let sendChain = Promise.resolve();
        const gen = captureGen;
        const selectedMime = 'video/webm;codecs=vp8,opus';

        try {
            recorder = new MediaRecorder(recordStream, {
                mimeType: selectedMime,
                videoBitsPerSecond: capture.bitrate,
                audioBitsPerSecond: AUDIO_BITRATE
            });
        } catch (e) {
            recorder = new MediaRecorder(recordStream);
        }

        recorder.ondataavailable = (e) => {
            if (!publish || gen !== captureGen || stopped) return;
            if (!e.data || e.data.size === 0) return;
            const blob = e.data;
            sendChain = sendChain.then(async () => {
                if (mine !== recorderGen || gen !== captureGen || stopped) return;
                if (!mediaWs || mediaWs.readyState !== WebSocket.OPEN) return;
                const bytes = new Uint8Array(await blob.arrayBuffer());
                if (!bytes.length || mine !== recorderGen) return;
                if (first) {
                    const clusterAt = realClusterAt(bytes);
                    if (clusterAt < 0) {
                        if (bytes[0] === 0x1A && bytes.length <= 4000) heldHeader = bytes;
                        else if (bytes.length > 4000) scheduleCleanFile();
                        return;
                    }
                    const head = heldHeader && bytes[0] !== 0x1A ? heldHeader.length : 0;
                    if (head + clusterAt > 4000) {
                        scheduleCleanFile();
                        return;
                    }
                    keyRetry = 0;
                    first = false;
                    let payload = bytes;
                    if (heldHeader && bytes[0] !== 0x1A) {
                        payload = new Uint8Array(heldHeader.length + bytes.length);
                        payload.set(heldHeader, 0);
                        payload.set(bytes, heldHeader.length);
                    }
                    heldHeader = null;
                    mediaWs.send(new Blob([new Uint8Array([FLAG_NEW_FILE]), payload]));
                    signalLiveFile();
                    return;
                }
                mediaWs.send(new Blob([new Uint8Array([FLAG_CONTINUE]), bytes]));
            }).catch(() => { });
        };

        recorderHasAudio = hasLiveAudio(recordStream);
        recorderReadyAt = performance.now();
        try { recorder.start(RECORDER_SLICE_MS); } catch (e) { }
        holdFrames = false;
        if (publish) {
            const watchMine = mine;
            setTimeout(() => {
                if (stopped || directUrl || liveAnnounced || watchMine !== recorderGen) return;
                scheduleCleanFile();
            }, 3500);
        }
        if (!publish) syncRecorder();
        if (publish) {
            armHeaderPump();
            armKeyframeBurst();
        }
        if (!captureWarmed && !warmupTimer) {
            warmupTimer = setTimeout(() => {
                warmupTimer = 0;
                if (stopped || directUrl) return;
                if (!mediaWs || mediaWs.readyState !== WebSocket.OPEN) return;
                captureWarmed = true;
                liveAnnounced = false;
                timedCapture = false;
                try { mediaWs.send("renew"); } catch (e) { }
                restartRecorder();
            }, WARMUP_MS);
        }
    }

    function startRecorder(forceRun) {
        if (stopped || !setupCapture() || !mediaWs || mediaWs.readyState !== WebSocket.OPEN) return;
        if (recorder && recorder.state !== 'inactive') return;
        if (!forceRun) {
            openRecorder(!!captureWarmed);
            return;
        }
        const arm = ++recorderArm;
        const begun = performance.now();
        const tick = () => {
            if (arm !== recorderArm || stopped) return;
            if (video && (video.seeking || video.readyState < 2) && performance.now() - begun < 1500) {
                setTimeout(tick, 100);
                return;
            }
            holdFrames = true;
            stopKeyframeBurst();
            if (emitTimer) { clearTimeout(emitTimer); emitTimer = 0; }
            retieCanvas();
            ensureRecorderAudio();
            if (arm !== recorderArm || stopped) {
                holdFrames = false;
                return;
            }
            openRecorder(true);
            stampFrame();
        };
        tick();
    }

    function stopRelay() {
        captureGen++;
        if (warmupTimer) { clearTimeout(warmupTimer); warmupTimer = 0; }
        if (recorder) {
            recorder.ondataavailable = null;
            try { recorder.stop(); } catch (e) { }
        }
        recorder = null;
        if (recordStream) {
            recordStream.getTracks().forEach(t => { try { t.stop(); } catch (e) { } });
        }
        recordStream = null;
        restorePageSound(true);
        if (mediaWs) {
            try { mediaWs.onclose = null; mediaWs.close(); } catch (e) { }
            mediaWs = null;
        }
    }

    function stopDirectWatch() {
        if (window.__syncWatchDirectWatch) {
            clearInterval(window.__syncWatchDirectWatch);
            window.__syncWatchDirectWatch = null;
        }
    }

    const MEDIA_WS_URL = SERVER.replace(/^http/, "ws") + "/ws/media?role=host";
    let wsTries = 0;
    function extensionAlive() {
        try { return !!(browser && browser.runtime && browser.runtime.id); }
        catch (e) { return false; }
    }
    function connectMediaWs() {
        if (stopped || directUrl || window.__syncWatchBoot !== bootId) return;
        if (!extensionAlive()) { stopped = true; return; }
        if (wsTries >= 6) return;
        wsTries++;
        const sock = new WebSocket(MEDIA_WS_URL);
        mediaWs = sock;
        sock.binaryType = "arraybuffer";
        sock.onopen = () => {
            if (sock !== mediaWs || stopped) { try { sock.close(); } catch (e) { } return; }
            wsTries = 0;
            startRecorder();
        };
        sock.onclose = () => {
            if (sock !== mediaWs || stopped || window.__syncWatchBoot !== bootId) return;
            if (!extensionAlive()) { stopped = true; return; }
            captureGen++;
            if (recorder && recorder.state !== "inactive") {
                try { recorder.stop(); } catch (e) { }
            }
            recorder = null;
            setTimeout(connectMediaWs, Math.min(5000, 400 * wsTries));
        };
    }

    function rebindVideo(newVideo) {
        if (!newVideo || (newVideo === video && newVideo.isConnected)) return;
        const gen = ++rebindGen;
        rebindBusy = true;
        const target = newVideo;

        suppressLocal = true;
        const previous = video;
        detachVideoListeners(previous);
        if (rVFC_id && previous && typeof previous.cancelVideoFrameCallback === 'function') {
            try { previous.cancelVideoFrameCallback(rVFC_id); } catch (e) { }
        }
        rVFC_id = null;
        loopPending = false;
        video = target;
        attachVideoListeners(video);
        bindMediaWatch(video);

        let started = false;
        const begin = async () => {
            if (started || gen !== rebindGen || stopped) return;
            started = true;
            try {
                if (!manualDirect) {
                    try {
                        const found = await maybeDirectUrl(target);
                        if (gen !== rebindGen || stopped) return;
                        if (found) {
                            directUrl = found.url;
                            directSince = found.at || Date.now();
                        } else {
                            directUrl = "";
                        }
                    } catch (e) {
                        directUrl = "";
                    }
                    if (gen !== rebindGen || stopped) return;
                }

                if (directUrl) {
                    stopRelay();
                    publishDirect();
                    if (!window.__syncWatchDirectWatch)
                        window.__syncWatchDirectWatch = setInterval(watchDirectUrl, DIRECT_URL_POLL_MS);
                    setTimeout(() => { if (gen === rebindGen) suppressLocal = false; }, SUPPRESS_LOCAL_MS);
                    return;
                }
                if (!canCaptureCanvas(target)) {
                    const later = await waitForDirect(target);
                    if (gen !== rebindGen || stopped) return;
                    if (later && later.url) {
                        directUrl = later.url;
                        directSince = later.at || Date.now();
                        stopRelay();
                        publishDirect();
                        if (!window.__syncWatchDirectWatch)
                            window.__syncWatchDirectWatch = setInterval(watchDirectUrl, DIRECT_URL_POLL_MS);
                    }
                    setTimeout(() => { if (gen === rebindGen) suppressLocal = false; }, SUPPRESS_LOCAL_MS);
                    return;
                }

                stopDirectWatch();
                if (gen !== rebindGen || stopped) return;
                await restartRelay();
            } finally {
                if (gen === rebindGen) rebindBusy = false;
            }
        };

        const ready = () => target.readyState >= 2 && target.videoWidth > 0;
        if (ready()) {
            begin();
            return;
        }

        const arm = () => {
            if (!ready()) return;
            target.removeEventListener('loadeddata', arm);
            begin();
        };
        target.addEventListener('loadeddata', arm);
        setTimeout(() => {
            target.removeEventListener('loadeddata', arm);
            begin();
        }, REBIND_READY_MS);
    }

    window.__syncWatchWatcher = setInterval(() => {
        if (stopped) return;

        // Если текущее видео уже захвачено, воспроизводится и живо — не нагружаем CPU обходом DOM
        if (video && video.isConnected && !video.error && !video.paused && video.currentTime > 0) {
            if (mediaWatchArmed && !rebindBusy && !video.ended) {
                const src = video.currentSrc || video.src || "";
                if (src && boundSrc && src !== boundSrc && video.readyState >= 2 && video.videoWidth > 0)
                    followMediaChange();
            }
            return;
        }

        const currentBest = findBestVideo();
        if (!currentBest) return;

        const isOldDead = !video || !video.isConnected || video.error;
        const isNewActive = currentBest !== video && (!currentBest.paused && currentBest.currentTime > 0);

        if (isOldDead || isNewActive) {
            rebindVideo(currentBest);
            return;
        }
        if (mediaWatchArmed && !rebindBusy && video === currentBest && !video.ended) {
            const src = video.currentSrc || video.src || "";
            if (src && boundSrc && src !== boundSrc && video.readyState >= 2 && video.videoWidth > 0)
                followMediaChange();
        }
    }, 1000);

    let manualDirect = false;
    try {
        const stDirect = await browser.storage.local.get("directBroadcast");
        const pending = stDirect && stDirect.directBroadcast;
        if (pending && pending.url && Date.now() - pending.at < 20000) {
            directUrl = pending.url;
            directSince = Date.now();
            manualDirect = true;
        }
    } catch (e) { }

    function acceptMediaUrl(raw) {
        if (!raw || typeof raw !== "string") return "";
        let u = raw.trim().replace(/\\\//g, "/").replace(/\\u0026/g, "&").replace(/&amp;/g, "&");
        try { u = new URL(u, window.location.href).href; } catch (e) { return ""; }
        if (!/^https?:\/\//i.test(u)) return "";
        if (u.includes("/proxy?url=") || u.includes("/api/live")) return "";
        if (/analytics|doubleclick|googlesyndication|\/ads?[\/.?]/i.test(u)) return "";
        return u;
    }

    function isPlaylist(u) {
        return /\.m3u8|\.mpd|mpegurl/i.test(u);
    }

    function isMediaFile(u) {
        return /\.(mp4|webm|m4v|mkv|mov)(\?|#|$)/i.test(u);
    }

    function pickBest(list) {
        const urls = list.map(acceptMediaUrl).filter(Boolean);
        const lists = urls.filter(isPlaylist);
        if (lists.length) return lists[lists.length - 1];
        const files = urls.filter(isMediaFile);
        if (files.length) return files[files.length - 1];
        return "";
    }

    function ownMediaUrl(v) {
        if (!v) return "";
        const raw = [v.currentSrc, v.src];
        try { v.querySelectorAll("source").forEach(s => raw.push(s.src, s.getAttribute("src"))); } catch (e) { }
        const best = pickBest(raw);
        if (best) return best;
        const src = acceptMediaUrl(v.currentSrc || v.src || "");
        if (/^https?:\/\//i.test(src)) return src;
        return "";
    }

    function urlsFromPage() {
        const raw = [];
        try {
            performance.getEntriesByType("resource").forEach(e => raw.push(e.name));
        } catch (e) { }
        return raw;
    }

    function canCaptureCanvas(v) {
        try {
            if (!v || !(v.videoWidth > 0) || v.readyState < 2) return false;
            const c = document.createElement("canvas");
            c.width = 2;
            c.height = 2;
            const g = c.getContext("2d", { willReadFrequently: true });
            if (!g) return false;
            g.drawImage(v, 0, 0, 2, 2);
            g.getImageData(0, 0, 1, 1);
            if (typeof c.captureStream !== "function") return false;
            const stream = c.captureStream(0);
            const ok = stream.getVideoTracks().length > 0;
            stream.getTracks().forEach(t => { try { t.stop(); } catch (e) { } });
            return ok;
        } catch (e) {
            return false;
        }
    }

    async function waitUntilDecodable(v, ms) {
        const start = Date.now();
        while (Date.now() - start < ms) {
            if (v && v.readyState >= 2 && v.videoWidth > 0) return true;
            await new Promise(r => setTimeout(r, 200));
        }
        return !!(v && v.readyState >= 2 && v.videoWidth > 0);
    }

    async function discoverUrl(v) {
        let sniffed = "";
        let sniffedAt = 0;
        try {
            const resp = await browser.runtime.sendMessage({ action: "getLastStream" });
            sniffed = acceptMediaUrl(resp && resp.streamUrl);
            sniffedAt = resp && resp.at ? resp.at : 0;
        } catch (e) { }
        const own = ownMediaUrl(v);
        if (sniffed) return { url: sniffed, at: sniffedAt || Date.now() };
        if (own) return { url: own, at: Date.now() };
        const page = videoIsPlayer(v) ? urlsFromPage() : [];
        const pageList = pickBest(page.filter(u => isPlaylist(acceptMediaUrl(u) || "")));
        if (pageList) return { url: pageList, at: Date.now() };
        return null;
    }

    async function maybeDirectUrl(v) {
        if (!v || manualDirect) return null;
        await waitUntilDecodable(v, REBIND_READY_MS);
        if (canCaptureCanvas(v)) return null;
        return discoverUrl(v);
    }

    async function waitForDirect(v) {
        const deadline = Date.now() + 15000;
        while (!stopped && Date.now() < deadline) {
            const found = await discoverUrl(v);
            if (found && found.url) return found;
            await new Promise(r => setTimeout(r, 700));
        }
        return null;
    }

    async function followMediaChange() {
        if (stopped || !video || manualDirect) return;
        const src = video.currentSrc || video.src || "";
        if (!src || src === boundSrc) return;
        if (video.readyState < 2 || video.videoWidth <= 0) return;
        boundSrc = src;
        const fileAt = recorderReadyAt;
        srcRenewGuard = performance.now() + 8000;
        const found = await maybeDirectUrl(video);
        if (stopped || !video) return;
        if (found && found.url) {
            if (found.url === directUrl) return;
            directUrl = found.url;
            directSince = found.at || Date.now();
            stopRelay();
            await publishDirect();
            if (!window.__syncWatchDirectWatch)
                window.__syncWatchDirectWatch = setInterval(watchDirectUrl, DIRECT_URL_POLL_MS);
            return;
        }
        if (!canCaptureCanvas(video)) {
            const later = await waitForDirect(video);
            if (stopped || !video) return;
            if (later && later.url) {
                directUrl = later.url;
                directSince = later.at || Date.now();
                stopRelay();
                await publishDirect();
                if (!window.__syncWatchDirectWatch)
                    window.__syncWatchDirectWatch = setInterval(watchDirectUrl, DIRECT_URL_POLL_MS);
                return;
            }
            return;
        }
        directUrl = "";
        stopDirectWatch();
        endNotified = false;
        sawEndEdge = false;
        if (recorderReadyAt > fileAt && recorderHasAudio) {
            srcRenewGuard = 0;
            return;
        }
        allowLateAudio = false;
        try {
            if (recordStream && !hasLiveAudio(recordStream)) {
                const stream = captureRaw(video);
                const clone = hasLiveAudio(stream) ? wireLocal(stream) : null;
                if (clone) recordStream.addTrack(clone);
                if (!hasLiveAudio(recordStream)) await waitForAudio(AUDIO_TRACK_WAIT_MS);
                const late = hasLiveAudio(rawAudioStream) ? wireLocal(rawAudioStream) : null;
                if (late && recordStream && !hasLiveAudio(recordStream)) recordStream.addTrack(late);
            }
        } finally {
            allowLateAudio = true;
        }
        if (stopped || !video) return;
        if (recorderReadyAt > fileAt && recorderHasAudio) {
            srcRenewGuard = 0;
            return;
        }
        srcRenewGuard = 0;
        if (captureVideoLive()) {
            needRenew = true;
            renewStream();
            return;
        }
        await restartRelay();
    }

    function mediaKind(url) {
        return /\.m3u8|mpegurl|\.mpd/i.test(url) ? "hls" : "mp4";
    }

    async function publishDirect() {
        if (!directUrl || stopped) return;
        if (connection.state !== signalR.HubConnectionState.Connected) return;
        let cookie = "";
        try {
            const resp = await browser.runtime.sendMessage({ action: "getCookies", url: directUrl });
            if (resp && resp.cookie) cookie = resp.cookie;
        } catch (e) { }
        await connection.invoke("BroadcastVideo", "main-room", directUrl, window.location.href,
            video.currentTime || 0, !!video.paused, mediaKind(directUrl), cookie);
    }

    async function watchDirectUrl() {
        if (stopped || !directUrl) return;
        try {
            const resp = await browser.runtime.sendMessage({ action: "getLastStream" });
            if (!resp || !resp.streamUrl || resp.streamUrl === directUrl) return;
            if (!resp.at || resp.at <= directSince) return;
            const nextHls = /\.m3u8|\.mpd/i.test(resp.streamUrl);
            const curHls = /\.m3u8|\.mpd/i.test(directUrl);
            if (curHls && !nextHls) return;
            directUrl = resp.streamUrl;
            directSince = resp.at;
            await publishDirect();
        } catch (e) { }
    }

    if (!manualDirect) {
        try {
            const found = await maybeDirectUrl(video);
            if (found) {
                directUrl = found.url;
                directSince = found.at || Date.now();
            }
        } catch (e) { }
    }

    try {
        await connection.start();
        await connection.invoke("JoinRoom", "main-room", "host");
    } catch (err) {
        alert("SyncWatch: Ошибка подключения к серверу!\n" + SERVER);
        return;
    }

    attachVideoListeners(video);
    try {
        if (!canCaptureCanvas(video)) {
            const ok = await waitUntilDecodable(video, REBIND_READY_MS);
            if (stopped) return;
            if (ok && canCaptureCanvas(video)) directUrl = "";
        }
        if (directUrl) {
            stopRelay();
            await publishDirect();
            window.__syncWatchDirectWatch = setInterval(watchDirectUrl, DIRECT_URL_POLL_MS);
        } else if (!canCaptureCanvas(video)) {
            const later = await waitForDirect(video);
            if (stopped) return;
            if (later && later.url) {
                directUrl = later.url;
                directSince = later.at || Date.now();
                stopRelay();
                await publishDirect();
                window.__syncWatchDirectWatch = setInterval(watchDirectUrl, DIRECT_URL_POLL_MS);
            } else {
                alert("Прямую ссылку найти не удалось, а картинка плеера закрыта. Захват не включаю, звук не трогаю.");
                return;
            }
        } else {
            stopDirectWatch();
            boundSrc = video.currentSrc || video.src || "";
            bindMediaWatch(video);
            if (!mediaWs || mediaWs.readyState === WebSocket.CLOSED) connectMediaWs();
            mediaWatchArmed = true;
            chooseFps();
            await connection.invoke("BroadcastVideo", "main-room", "", window.location.href,
                video.currentTime || 0, !!video.paused, "relay");
        }
    } catch (e) { }

    window.__syncWatchInterval = setInterval(() => {
        if (!video || stopped) return;
        if (connection.state !== signalR.HubConnectionState.Connected) return;
        const dur = Number.isFinite(video.duration) ? video.duration : 0;
        seenTime = video.currentTime || 0;
        if (dur > 1 && seenTime >= dur - 0.4) sawEndEdge = true;
        connection.invoke("SyncMetadata", "main-room", seenTime, dur, !!video.paused);
    }, 500);

    let hostName = "";
    try { hostName = directUrl ? new URL(directUrl).host : ""; } catch (e) { }
    if (directUrl)
        alert(manualDirect
            ? "Ссылка транслируется через сервер. Зрители смотрят её через SyncWatch."
            : "Картинка плеера закрыта. Транслирую ссылку через сервер" + (hostName ? " (" + hostName + ")." : "."));
    else
        alert("Плеер успешно захвачен!");
})();
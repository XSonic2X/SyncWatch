using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace SyncWatch;

public sealed class LiveBroadcastState
{
    private const int ReplayWindowMs = 2000;
    private const int ReplayHardBytes = 1024 * 1024;
    private const int ReceiveBufferBytes = 256 * 1024;
    private const int MaxHeaderBytes = 2 * 1024 * 1024;
    private const byte FlagNewFile = 0;
    private const int CoarseSliceMs = 1000;
    private const int CoarseBudgetMs = 7000;

    private byte[]? _initFramed;
    private bool _haveInit;
    private int _recentBytes;
    private int _recentClusterCount; // Оптимизация: O(1) учёт кластеров
    private int _lastClusterTime = -1;
    private int _clusterBaseMs = -1;
    private int _maxMediaMs = -1;
    private int _clusterOrigin = -1;
    private bool _waitHeader;
    private DateTime _fileMarkUtc = DateTime.MinValue;
    private DateTime _freshUtc = DateTime.MinValue;
    private bool _originLogged;
    private bool _shiftRefused;
    private bool _fineSlices;
    private int _holdFrom = -1;
    private DateTime _holdSinceUtc = DateTime.MinValue;
    private bool _holdFlushArmed;
    private int _holdTicket;
    private readonly MemoryStream _hold = new MemoryStream();
    private readonly MemoryStream _head = new MemoryStream();
    private readonly Queue<Piece> _recent = new Queue<Piece>();
    private readonly object _lock = new object();
    private readonly Dictionary<Guid, Subscriber> _subscribers = new Dictionary<Guid, Subscriber>();
    private int _session;

    private sealed class Piece
    {
        public readonly byte[] Data;
        public readonly int Time;
        public readonly bool Cluster;

        public Piece(byte[] data, int time, bool cluster)
        {
            Data = data;
            Time = time;
            Cluster = cluster;
        }
    }

    private sealed class Subscriber
    {
        public readonly ChannelWriter<byte[]> Writer;
        public bool GotInit;

        public Subscriber(ChannelWriter<byte[]> writer)
        {
            Writer = writer;
        }
    }

    public void ResetHost()
    {
        int session;
        lock (_lock) session = _session;
        ResetForSession(session, keepWaiting: false);
    }

    public void ReleaseLive()
    {
        int session;
        lock (_lock) session = _session;
        ResetForSession(session, keepWaiting: true);
    }

    public (double SinceMs, bool HadInit) NoteViewer()
    {
        lock (_lock)
        {
            double since = _fileMarkUtc == DateTime.MinValue ? -1 : (DateTime.UtcNow - _fileMarkUtc).TotalMilliseconds;
            return (since, _haveInit && _initFramed != null);
        }
    }

    public Guid Subscribe(ChannelWriter<byte[]> writer)
    {
        Guid id = Guid.NewGuid();
        Subscriber sub = new Subscriber(writer);
        lock (_lock)
        {
            if (_initFramed != null)
            {
                writer.TryWrite(_initFramed);
                sub.GotInit = true;
            }
            bool aligned = false;
            foreach (Piece piece in _recent)
            {
                if (aligned || piece.Cluster)
                {
                    aligned = true;
                    writer.TryWrite(piece.Data);
                }
            }
            _subscribers[id] = sub;
        }
        return id;
    }

    public void Unsubscribe(Guid id)
    {
        lock (_lock) _subscribers.Remove(id);
    }

    public async Task RunHostAsync(WebSocket ws, CancellationToken ct)
    {
        int session;
        lock (_lock)
        {
            session = ++_session;
            _fileMarkUtc = DateTime.UtcNow;
            _originLogged = false;
            _shiftRefused = false;
        }
        RelayLog.Add("хост подключился");
        ResetForSession(session, keepWaiting: true);
        byte[] buffer = new byte[ReceiveBufferBytes];
        using MemoryStream ms = new MemoryStream();
        try
        {
            while (!ct.IsCancellationRequested && ws.State == WebSocketState.Open)
            {
                lock (_lock)
                {
                    if (session != _session) break;
                }
                WebSocketReceiveResult res;
                try { res = await ws.ReceiveAsync(buffer, ct); }
                catch { break; }
                if (res.MessageType == WebSocketMessageType.Close) break;
                ms.Write(buffer, 0, res.Count);
                if (!res.EndOfMessage) continue;

                lock (_lock)
                {
                    if (session != _session) break;
                }

                if (res.MessageType == WebSocketMessageType.Text)
                {
                    byte[] whole = ms.ToArray();
                    ms.SetLength(0);
                    string command = Encoding.UTF8.GetString(whole);
                    if (command == "reset")
                        ResetForSession(session, keepWaiting: false);
                    else if (command == "discard")
                        DiscardReplay();
                    else if (command == "renew")
                        BeginFreshStream();
                    continue;
                }

                if (res.MessageType == WebSocketMessageType.Binary && ms.Length > 1)
                {
                    // Быстрое извлечение без промежуточного выделения `whole[1..]`
                    int len = (int)ms.Length - 1;
                    byte[] payload = new byte[len];
                    byte[] msBuf = ms.GetBuffer();
                    byte flag = msBuf[0];
                    Buffer.BlockCopy(msBuf, 1, payload, 0, len);
                    ms.SetLength(0);

                    Publish(session, payload, flag == FlagNewFile);
                }
                else
                {
                    ms.SetLength(0);
                }
            }
        }
        finally
        {
            ResetForSession(session, keepWaiting: true);
        }
    }

    public void DiscardReplay()
    {
        lock (_lock)
        {
            _recent.Clear();
            _recentBytes = 0;
            _recentClusterCount = 0;
            _lastClusterTime = -1;
        }
    }

    public void BeginFreshStream()
    {
        List<Subscriber> kick = new List<Subscriber>();
        bool duplicate = false;
        lock (_lock)
        {
            DateTime now = DateTime.UtcNow;
            if (_freshUtc != DateTime.MinValue && (now - _freshUtc).TotalMilliseconds < 700)
            {
                duplicate = true;
            }
            else
            {
                _freshUtc = now;
                List<Guid> drop = new List<Guid>();
                foreach (KeyValuePair<Guid, Subscriber> pair in _subscribers)
                {
                    if (!pair.Value.GotInit) continue;
                    kick.Add(pair.Value);
                    drop.Add(pair.Key);
                }
                foreach (Guid id in drop)
                    _subscribers.Remove(id);
                ClearMedia();
                _waitHeader = true;
                _fileMarkUtc = DateTime.UtcNow;
                _originLogged = false;
                _shiftRefused = false;
            }
        }
        if (duplicate) return;
        RelayLog.Add("новый файл, ждём заголовок");
        Complete(kick);
    }

    private void ResetForSession(int session, bool keepWaiting)
    {
        List<Subscriber> kick = new List<Subscriber>();
        lock (_lock)
        {
            if (session != _session) return;
            List<Guid> drop = new List<Guid>();
            foreach (KeyValuePair<Guid, Subscriber> pair in _subscribers)
            {
                if (!keepWaiting || pair.Value.GotInit)
                {
                    kick.Add(pair.Value);
                    drop.Add(pair.Key);
                }
            }
            foreach (Guid id in drop)
                _subscribers.Remove(id);
            ClearMedia();
        }
        Complete(kick);
    }

    private void ClearMedia()
    {
        _initFramed = null;
        _haveInit = false;
        _recentBytes = 0;
        _recentClusterCount = 0;
        _lastClusterTime = -1;
        _clusterBaseMs = -1;
        _maxMediaMs = -1;
        _clusterOrigin = -1;
        _waitHeader = false;
        _head.SetLength(0);
        _recent.Clear();
        _fineSlices = false;
        _holdFrom = -1;
        _holdSinceUtc = DateTime.MinValue;
        _hold.SetLength(0);
        _holdFlushArmed = false;
        _holdTicket++;
    }

    private void Publish(int session, byte[] data, bool isHeader)
    {
        List<Subscriber>? kick = null;
        lock (_lock)
        {
            if (session != _session || (_waitHeader && !isHeader)) return;
            if (isHeader) _waitHeader = false;

            // Если пришёл флаг нового файла (isHeader) — старый незавершённый заголовок 
            // в _head обязан быть сброшен в ноль, иначе он раздувается до 800 Кб!
            if (isHeader)
            {
                if (_haveInit)
                {
                    kick = new List<Subscriber>();
                    List<Guid> drop = new List<Guid>();
                    foreach (KeyValuePair<Guid, Subscriber> pair in _subscribers)
                    {
                        if (!pair.Value.GotInit) continue;
                        kick.Add(pair.Value);
                        drop.Add(pair.Key);
                    }
                    foreach (Guid id in drop)
                        _subscribers.Remove(id);
                }
                ClearMedia();
            }

            if (!_haveInit)
                Absorb(data);
            else
                Forward(data);
        }
        if (kick != null) Complete(kick);
    }

    private void Absorb(byte[] data)
    {
        _head.Write(data, 0, data.Length);

        // Если накопилось больше 64 Кб без кластера — это застрявшие дельта-кадры, сбрасываем
        if (_head.Length > 64 * 1024 && !WebmSplit.TrySplitInit(_head.GetBuffer().AsSpan(0, (int)_head.Length), out _))
        {
            _head.SetLength(0);
            return;
        }

        byte[] all = _head.ToArray();
        if (!WebmSplit.TrySplitInit(all, out int at)) return;

        WebmSplit.InitSlice(all, at, out int start, out int end);

        // Защита: заголовок WebM (EBML + Tracks) физически не может быть больше 4 Кб.
        // Если получилось больше — значит перед кластером попал мусор. 
        // Ищем настоящий заголовок 1A 45 DF A3 непосредственно перед кластером:
        if (end - start > 4096 || start >= end)
        {
            int lastEbml = -1;
            int searchFrom = Math.Max(0, at - 4096);
            for (int i = searchFrom; i <= at - 4; i++)
            {
                if (all[i] == 0x1A && all[i + 1] == 0x45 && all[i + 2] == 0xDF && all[i + 3] == 0xA3)
                    lastEbml = i;
            }
            if (lastEbml >= 0)
            {
                start = lastEbml;
                end = at;
            }
            else
            {
                start = 0;
                end = Math.Min(at, 1024);
            }
        }

        _initFramed = all[start..end];
        _haveInit = true;
        double headerMs = _fileMarkUtc == DateTime.MinValue ? 0 : (DateTime.UtcNow - _fileMarkUtc).TotalMilliseconds;
        RelayLog.Add($"заголовок готов через {headerMs:0} мс, {_initFramed.Length} байт");
        Send(_initFramed, true);

        if (at < all.Length)
            Forward(all[at..]);

        _head.SetLength(0);
    }

    private void Forward(byte[] data)
    {
        data = NormalizeOrigin(data);
        if (_fineSlices)
        {
            Emit(data);
            return;
        }
        if (_hold.Length == 0)
            _holdSinceUtc = DateTime.UtcNow;
        _hold.Write(data, 0, data.Length);
        int latest = _lastClusterTime;
        if (WebmSplit.StartsWithCluster(data) && WebmSplit.TryReadClusterTime(data, out int time))
            latest = time;

        int peekBase = _clusterBaseMs;
        int mediaEnd = WebmSplit.NoteTimes(data, ref peekBase);
        if (peekBase >= 0) _clusterBaseMs = peekBase;
        if (mediaEnd > latest) latest = mediaEnd;
        if (_holdFrom < 0 && latest >= 0)
            _holdFrom = latest;
        bool span = _holdFrom >= 0 && latest >= 0 && latest - _holdFrom >= CoarseSliceMs;
        bool aged = (DateTime.UtcNow - _holdSinceUtc).TotalMilliseconds >= CoarseSliceMs;
        bool budget = latest >= CoarseBudgetMs;
        if (!span && !aged && !budget)
        {
            if (!_holdFlushArmed)
            {
                _holdFlushArmed = true;
                int ticket = ++_holdTicket;
                _ = Task.Delay(CoarseSliceMs).ContinueWith(_ => FlushHold(ticket));
            }
            return;
        }
        _holdFlushArmed = false;
        _holdTicket++;
        byte[] blob = _hold.ToArray();
        _hold.SetLength(0);
        _holdFrom = latest >= 0 ? latest : _holdFrom;
        _holdSinceUtc = DateTime.MinValue;
        if (budget)
            _fineSlices = true;
        Emit(blob);
    }

    private void FlushHold(int ticket)
    {
        lock (_lock)
        {
            if (ticket != _holdTicket || _hold.Length == 0) return;
            _holdFlushArmed = false;
            byte[] blob = _hold.ToArray();
            _hold.SetLength(0);
            _holdFrom = -1;
            _holdSinceUtc = DateTime.MinValue;
            Emit(blob);
        }
    }

    public void FlushTail()
    {
        lock (_lock)
        {
            string last = _lastClusterTime < 0 ? "нет" : _lastClusterTime + " мс";
            string media = _maxMediaMs < 0 ? "нет" : _maxMediaMs + " мс";
            if (_hold.Length == 0)
            {
                RelayLog.Add($"конец файла, хвост пуст, кластер {last}, медиа до {media}");
                return;
            }
            RelayLog.Add($"конец файла, отдаём {_hold.Length} байт, кластер {last}, медиа до {media}");
            _holdFlushArmed = false;
            int ticket = ++_holdTicket;
            FlushHoldLocked(ticket);
        }
    }

    public void CloseFileSoon()
    {
        DateTime mark;
        lock (_lock) mark = _fileMarkUtc;
        _ = Task.Delay(1500).ContinueWith(_ => CloseIfSameFile(mark));
    }

    private void CloseIfSameFile(DateTime mark)
    {
        List<Subscriber> kick = new List<Subscriber>();
        lock (_lock)
        {
            if (mark != _fileMarkUtc || mark == DateTime.MinValue) return;
            foreach (Subscriber sub in _subscribers.Values)
                kick.Add(sub);
            _subscribers.Clear();
        }
        if (kick.Count == 0) return;
        RelayLog.Add("файл закрыт, зритель доигрывает хвост");
        Complete(kick);
    }

    private void FlushHoldLocked(int ticket)
    {
        if (ticket != _holdTicket || _hold.Length == 0) return;
        _holdFlushArmed = false;
        byte[] blob = _hold.ToArray();
        _hold.SetLength(0);
        _holdFrom = -1;
        _holdSinceUtc = DateTime.MinValue;
        Emit(blob);
    }

    private void Emit(byte[] data)
    {
        Send(data, false);
        Remember(data);
    }

    private byte[] NormalizeOrigin(byte[] data)
    {
        if (!WebmSplit.StartsWithCluster(data) || !WebmSplit.TryReadClusterTime(data, out int time))
            return data;
        if (_clusterOrigin < 0)
        {
            _clusterOrigin = time;
            if (!_originLogged)
            {
                _originLogged = true;
                RelayLog.Add(_clusterOrigin > 0
                    ? $"первый кластер {_clusterOrigin} мс, сдвигаем к 0"
                    : "первый кластер 0 мс");
            }
        }
        if (_clusterOrigin <= 0 || time < _clusterOrigin)
            return data;
        byte[]? shifted = WebmSplit.TryShiftNonNegative(data, _clusterOrigin);
        if (shifted == null && !_shiftRefused)
        {
            _shiftRefused = true;
            RelayLog.Add($"сдвиг отменён, кластер {time} мс");
        }
        return shifted ?? data;
    }

    private void Remember(byte[] data)
    {
        int time = _lastClusterTime;
        bool cluster = false;
        int mediaEnd = WebmSplit.NoteTimes(data, ref _clusterBaseMs);
        if (mediaEnd > _maxMediaMs) _maxMediaMs = mediaEnd;
        if (WebmSplit.StartsWithCluster(data) && WebmSplit.TryReadClusterTime(data, out int read))
        {
            cluster = true;
            time = read;
            _lastClusterTime = read;
            _recentClusterCount++;
        }
        _recent.Enqueue(new Piece(data, time, cluster));
        _recentBytes += data.Length;
        TrimRecent();
    }

    private void TrimRecent()
    {
        while (_recent.Count > 1)
        {
            Piece first = _recent.Peek();
            bool stale = _lastClusterTime >= 0 && first.Time >= 0 && _lastClusterTime - first.Time > ReplayWindowMs;
            bool huge = _recentBytes > ReplayHardBytes;
            if (!stale && !huge) break;

            // O(1) проверка наличия последующего кластера
            bool laterCluster = first.Cluster ? (_recentClusterCount > 1) : (_recentClusterCount > 0);
            if (!laterCluster && !huge) break;

            Piece dropped = _recent.Dequeue();
            if (dropped.Cluster) _recentClusterCount--;
            _recentBytes -= dropped.Data.Length;

            while (_recent.Count > 1 && !_recent.Peek().Cluster)
            {
                dropped = _recent.Dequeue();
                _recentBytes -= dropped.Data.Length;
            }
        }
    }

    private void Send(byte[] framed, bool init)
    {
        foreach (Subscriber sub in _subscribers.Values)
        {
            sub.Writer.TryWrite(framed);
            if (init) sub.GotInit = true;
        }
    }

    private static void Complete(List<Subscriber> kick)
    {
        foreach (Subscriber sub in kick)
        {
            try { sub.Writer.TryComplete(); } catch { }
        }
    }
}
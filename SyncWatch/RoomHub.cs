using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;

namespace SyncWatch;

public sealed class RoomHub : Hub
{
    private static readonly ConcurrentDictionary<string, string> HostByRoom = new ConcurrentDictionary<string, string>();
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<string, (double Margin, double EffectiveTime, DateTime Time)>> ViewerStats = new ConcurrentDictionary<string, ConcurrentDictionary<string, (double, double, DateTime)>>();
    private static readonly ConcurrentDictionary<string, double> CurrentPaceByRoom = new ConcurrentDictionary<string, double>();
    private static readonly ConcurrentDictionary<string, DateTime> FreshUntil = new ConcurrentDictionary<string, DateTime>();
    private static readonly ConcurrentDictionary<string, int> RoomEpoch = new ConcurrentDictionary<string, int>();
    private readonly ProxyState _proxy;
    private readonly LiveBroadcastState _live;

    private const double PaceSlow = 0.5;
    private const double PaceHold = 0.9;
    private const double PaceNormal = 1.0;
    private const double PaceFast = 1.1;
    private const double BufferSlowSec = 4.0;
    private const double BufferHoldSec = 6.0;
    private const double BufferFastSec = 10.0;
    private const double ReportFreshSeconds = 3.5;
    private const double ReportStartedAtSec = 0.5;
    private const double PauseReportSeconds = 4.0;
    private const double PauseMinTimeSec = 1.0;
    private const int PaceIgnoreMs = 1200;
    private const double PaceHysteresisSec = 0.8;

    public RoomHub(ProxyState proxy, LiveBroadcastState live)
    {
        _proxy = proxy;
        _live = live;
    }

    public async Task JoinRoom(string room, string role)
    {
        room = string.IsNullOrWhiteSpace(room) ? "main-room" : room;
        await Groups.AddToGroupAsync(Context.ConnectionId, room);

        if (string.Equals(role, "host", StringComparison.OrdinalIgnoreCase))
        {
            if (HostByRoom.TryGetValue(room, out string? old) && old != Context.ConnectionId)
                _ = Clients.Client(old).SendAsync("ForceDisconnect");
            HostByRoom[room] = Context.ConnectionId;
            int epoch = RoomEpoch.AddOrUpdate(room, 1, static (_, n) => n + 1);
            RoomCache.LastVideo.TryRemove(room, out _);
            _live.ReleaseLive();
            await Clients.OthersInGroup(room).SendAsync("ReceiveVideo", "", "", 0d, false, "relay", epoch);
        }
        else
        {
            if (RoomCache.LastVideo.TryGetValue(room, out (string Url, string PageUrl, double Time, bool Paused, string Kind) v))
            {
                int epoch = RoomEpoch.TryGetValue(room, out int n) ? n : 0;
                await Clients.Caller.SendAsync("ReceiveVideo", v.Url, v.PageUrl, v.Time, v.Paused, v.Kind, epoch);
            }
        }
    }

    public async Task ReportBufferMargin(string room, double bufferMargin, double effectiveTime)
    {
        room = string.IsNullOrWhiteSpace(room) ? "main-room" : room;
        if (FreshUntil.TryGetValue(room, out DateTime fresh) && DateTime.UtcNow < fresh)
            return;

        ConcurrentDictionary<string, (double Margin, double EffectiveTime, DateTime Time)> roomMap = ViewerStats.GetOrAdd(room, static _ => new ConcurrentDictionary<string, (double, double, DateTime)>());
        roomMap[Context.ConnectionId] = (bufferMargin, effectiveTime, DateTime.UtcNow);

        // Однопроходный расчёт minBuffer без аллокаций памяти (без .Where().ToList().Min())
        double minBuffer = double.MaxValue;
        DateTime now = DateTime.UtcNow;
        foreach (var v in roomMap.Values)
        {
            if ((now - v.Time).TotalSeconds < ReportFreshSeconds && v.Margin >= 0 && v.EffectiveTime > ReportStartedAtSec)
            {
                if (v.Margin < minBuffer)
                    minBuffer = v.Margin;
            }
        }

        if (minBuffer == double.MaxValue) return;

        double newPace = HoldPace(room, PaceForBuffer(minBuffer), minBuffer);
        bool changed = !CurrentPaceByRoom.TryGetValue(room, out double prev) || Math.Abs(prev - newPace) > 0.01;
        CurrentPaceByRoom[room] = newPace;
        if (changed)
            await Clients.Group(room).SendAsync("SyncRoomPace", newPace, Math.Round(minBuffer, 1));
    }

    private static double PaceForBuffer(double minBuffer)
    {
        if (minBuffer < BufferSlowSec) return PaceSlow;
        if (minBuffer < BufferHoldSec) return PaceHold;
        if (minBuffer > BufferFastSec) return PaceFast;
        return PaceNormal;
    }

    private static double PaceEdge(double slower, double faster)
    {
        if (slower == PaceSlow && faster == PaceHold) return BufferSlowSec;
        if (slower == PaceHold && faster == PaceNormal) return BufferHoldSec;
        if (slower == PaceNormal && faster == PaceFast) return BufferFastSec;
        return double.NaN;
    }

    private static double HoldPace(string room, double proposed, double minBuffer)
    {
        if (!CurrentPaceByRoom.TryGetValue(room, out double current)) return proposed;
        if (Math.Abs(current - proposed) < 0.01) return current;
        double lo = Math.Min(current, proposed);
        double hi = Math.Max(current, proposed);
        double edge = PaceEdge(lo, hi);
        if (double.IsNaN(edge)) return proposed;
        if (proposed > current && minBuffer < edge + Hysteresis(lo, hi)) return current;
        if (proposed < current && minBuffer > edge - Hysteresis(lo, hi)) return current;
        return proposed;
    }

    private static double Hysteresis(double slower, double faster)
        => slower == PaceNormal && faster == PaceFast ? PaceHysteresisSec : 0;

    public async Task SendControl(string room, string action, double time)
    {
        room = string.IsNullOrWhiteSpace(room) ? "main-room" : room;
        bool resetPace = action is "pause" or "play";

        if (action == "live")
            RelayLog.Add($"хост начал с {time:0.0} с");
        if (action == "ended")
        {
            RelayLog.Add($"хост кончил на {time:0.0} с");
            _live.FlushTail();
            _live.CloseFileSoon();
        }

        if (action == "renew")
            _live.BeginFreshStream();

        if (action == "pause")
        {
            if (ViewerStats.TryGetValue(room, out ConcurrentDictionary<string, (double Margin, double EffectiveTime, DateTime Time)>? roomMap))
            {
                double minTime = double.MaxValue;
                DateTime now = DateTime.UtcNow;
                foreach (var v in roomMap.Values)
                {
                    if ((now - v.Time).TotalSeconds < PauseReportSeconds && v.EffectiveTime > PauseMinTimeSec)
                    {
                        if (v.EffectiveTime < minTime)
                            minTime = v.EffectiveTime;
                    }
                }

                if (minTime != double.MaxValue && minTime > 0 && minTime < time)
                    time = minTime;
            }
        }

        if (action == "pause")
            _live.DiscardReplay();

        if (resetPace)
        {
            if (ViewerStats.TryGetValue(room, out ConcurrentDictionary<string, (double Margin, double EffectiveTime, DateTime Time)>? stats))
                stats.Clear();
            FreshUntil[room] = DateTime.UtcNow.AddMilliseconds(PaceIgnoreMs);
            CurrentPaceByRoom[room] = PaceHold;
        }

        await Clients.Group(room).SendAsync("ReceiveControl", action, Math.Max(0, time));
        if (resetPace)
            await Clients.Group(room).SendAsync("SyncRoomPace", PaceHold, 0d);
    }

    public Task SyncMetadata(string room, double currentTime, double duration, bool paused)
    {
        room = string.IsNullOrWhiteSpace(room) ? "main-room" : room;
        if (!IsCurrentHost(room)) return Task.CompletedTask;
        return Clients.OthersInGroup(room).SendAsync("ReceiveMetadata", currentTime, duration, paused);
    }

    public Task BroadcastVideo(string room, string url, string pageUrl, double time, bool paused, string kind, string? cookie = null)
    {
        room = string.IsNullOrWhiteSpace(room) ? "main-room" : room;
        if (!IsCurrentHost(room)) return Task.CompletedTask;
        bool relay = string.IsNullOrEmpty(url);
        if (!relay)
        {
            _proxy.Allow(url, pageUrl, cookie);
            _live.ReleaseLive();
        }
        if (!IsCurrentHost(room)) return Task.CompletedTask;
        int epoch = RoomEpoch.GetOrAdd(room, 1);
        RoomCache.LastVideo[room] = (url ?? "", pageUrl, time, paused, relay ? "relay" : (kind ?? "mp4"));
        RelayLog.Add($"видео t={time:0.0} с, пауза={(paused ? "да" : "нет")}, {(relay ? "relay" : (kind ?? "mp4"))}");
        return Clients.OthersInGroup(room).SendAsync("ReceiveVideo", url ?? "", pageUrl, time, paused, relay ? "relay" : (kind ?? "mp4"), epoch);
    }

    private bool IsCurrentHost(string room)
     => HostByRoom.TryGetValue(room, out string? host) && host == Context.ConnectionId;

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        foreach (string room in HostByRoom.Where(kv => kv.Value == Context.ConnectionId).Select(static kv => kv.Key).ToList())
        {
            HostByRoom.TryRemove(room, out _);
            RoomCache.LastVideo.TryRemove(room, out _);
        }
        foreach (ConcurrentDictionary<string, (double Margin, double EffectiveTime, DateTime Time)> dict in ViewerStats.Values)
            dict.TryRemove(Context.ConnectionId, out _);
        return base.OnDisconnectedAsync(exception);
    }
}
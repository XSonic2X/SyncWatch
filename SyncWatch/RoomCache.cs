using System.Collections.Concurrent;

namespace SyncWatch;

public static class RoomCache
{
    public static readonly ConcurrentDictionary<string, (string Url, string PageUrl, double Time, bool Paused, string Kind)> LastVideo = new ConcurrentDictionary<string, (string, string, double, bool, string)>();
}

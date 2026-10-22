using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace SyncWatch;

public sealed class ProxyState
{
    private static readonly Regex HlsUriRegex = new Regex("URI=\"([^\"]+)\"", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    public static readonly HttpClient Http = new HttpClient(new SocketsHttpHandler
    {
        AllowAutoRedirect = true,
        UseCookies = false,
        AutomaticDecompression = DecompressionMethods.All,
        MaxConnectionsPerServer = 16,
        PooledConnectionLifetime = TimeSpan.FromMinutes(5)
    })
    { Timeout = Timeout.InfiniteTimeSpan };

    private sealed record Entry(string Prefix, string Referer, string Cookie);
    private readonly ConcurrentDictionary<string, List<Entry>> _byHost = new ConcurrentDictionary<string, List<Entry>>();

    public void Allow(string url, string referer, string? cookie = null)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out Uri? u)) return;
        if (u.Scheme != "http" && u.Scheme != "https") return;
        string path = u.GetLeftPart(UriPartial.Path);
        string c = cookie ?? "";
        List<Entry> list = _byHost.GetOrAdd(u.Host, static _ => new List<Entry>());
        lock (list)
        {
            Put(list, path, referer ?? "", c);
            if (path.EndsWith(".m3u8", StringComparison.OrdinalIgnoreCase) ||
                path.EndsWith(".mpd", StringComparison.OrdinalIgnoreCase))
                Put(list, DirPrefix(path), referer ?? "", c);
        }
    }

    public void AllowReferenced(string playlistUrl, string body, string referer)
    {
        if (!Uri.TryCreate(playlistUrl, UriKind.Absolute, out Uri? baseUri)) return;
        string cookie = CookieFor(baseUri) ?? "";

        foreach (ReadOnlySpan<char> rawLine in body.AsSpan().EnumerateLines())
        {
            ReadOnlySpan<char> line = rawLine.Trim();
            if (line.Length == 0) continue;
            if (line[0] == '#')
            {
                string lineStr = line.ToString();
                foreach (Match m in HlsUriRegex.Matches(lineStr))
                    AllowResolved(baseUri, m.Groups[1].Value, referer, cookie);
                continue;
            }
            AllowResolved(baseUri, line.ToString(), referer, cookie);
        }
    }

    private void AllowResolved(Uri baseUri, string raw, string referer, string cookie)
    {
        if (!Uri.TryCreate(raw, UriKind.Absolute, out Uri? abs) &&
            !Uri.TryCreate(baseUri, raw, out abs)) return;
        string childCookie = abs.Host.Equals(baseUri.Host, StringComparison.OrdinalIgnoreCase) ? cookie : "";
        Allow(abs.ToString(), referer, childCookie);
    }

    private static void Put(List<Entry> list, string prefix, string referer, string cookie)
    {
        int idx = list.FindIndex(e => e.Prefix == prefix);
        if (idx >= 0)
        {
            Entry prev = list[idx];
            if (cookie.Length > prev.Cookie.Length || (referer.Length > 0 && prev.Referer.Length == 0))
                list[idx] = prev with
                {
                    Cookie = cookie.Length > prev.Cookie.Length ? cookie : prev.Cookie,
                    Referer = referer.Length > 0 ? referer : prev.Referer
                };
            return;
        }
        list.Add(new Entry(prefix, referer, cookie));
    }

    public bool IsAllowed(Uri u)
     => _byHost.TryGetValue(u.Host, out List<Entry>? l) && Match(l, u) != null;

    public string? RefererFor(Uri u)
     => _byHost.TryGetValue(u.Host, out List<Entry>? l) ? Match(l, u)?.Referer : null;

    public string? CookieFor(Uri u)
     => _byHost.TryGetValue(u.Host, out List<Entry>? l) ? Match(l, u)?.Cookie : null;

    private static Entry? Match(List<Entry> list, Uri u)
    {
        string path = u.GetLeftPart(UriPartial.Path);
        lock (list)
            return list.Where(e => path.StartsWith(e.Prefix, StringComparison.OrdinalIgnoreCase))
                       .OrderByDescending(static e => e.Prefix.Length)
                       .FirstOrDefault();
    }

    private static string DirPrefix(string p)
    {
        int i = p.LastIndexOf('/');
        return i > 0 ? p[..(i + 1)] : "/";
    }

    public static string RewriteHls(string body, string baseUrl, string referer)
    {
        StringBuilder sb = new StringBuilder(body.Length + 4096);
        foreach (ReadOnlySpan<char> rawLine in body.AsSpan().EnumerateLines())
        {
            ReadOnlySpan<char> line = rawLine.TrimEnd('\r');
            if (line.Length == 0)
            {
                sb.AppendLine();
                continue;
            }
            if (line[0] == '#')
            {
                string lineStr = line.ToString();
                if (lineStr.Contains("URI=\"", StringComparison.OrdinalIgnoreCase))
                {
                    lineStr = HlsUriRegex.Replace(lineStr,
                        m => "URI=\"" + Wrap(m.Groups[1].Value, baseUrl, referer) + "\"");
                }
                sb.AppendLine(lineStr);
            }
            else
            {
                sb.AppendLine(Wrap(line.ToString(), baseUrl, referer));
            }
        }
        return sb.ToString();
    }

    private static string Wrap(string u, string baseUrl, string referer)
    {
        if (!Uri.TryCreate(u, UriKind.Absolute, out Uri? abs))
        {
            if (!Uri.TryCreate(new Uri(baseUrl), u, out abs)) return u;
        }
        string s = "/proxy?url=" + Uri.EscapeDataString(abs.ToString());
        if (!string.IsNullOrEmpty(referer)) s += "&ref=" + Uri.EscapeDataString(referer);
        return s;
    }
}
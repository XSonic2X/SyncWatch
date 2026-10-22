using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;

namespace SyncWatch;

public class Program
{
    public static void Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "--check-webm")
        {
            WebmSplit.SelfCheck();
            Console.WriteLine("webm ok");
            return;
        }

        WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

        builder.WebHost.ConfigureKestrel(static o =>
        {
            // Живой ответ может молчать, пока хост не прислал заголовок.
            // Иначе Kestrel закрывает соединение, зритель открывает новое и теряет начало.
            o.Limits.MinResponseDataRate = null;
            o.Limits.MinRequestBodyDataRate = null;
        });

        builder.Services.AddSignalR();
        builder.Services.AddCors(static o => o.AddDefaultPolicy(static p => p.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));
        builder.Services.AddSingleton<ProxyState>();
        builder.Services.AddSingleton<LiveBroadcastState>();
        builder.WebHost.UseUrls("http://0.0.0.0:5000");

        WebApplication app = builder.Build();

        app.UseDefaultFiles();
        app.UseStaticFiles();
        app.UseCors();
        app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(15) });

        app.MapHub<RoomHub>("/roomHub");

        LiveBroadcastState liveState = app.Services.GetRequiredService<LiveBroadcastState>();
        ProxyState proxyState = app.Services.GetRequiredService<ProxyState>();

        app.Map("/ws/media", async ctx =>
        {
            if (!ctx.WebSockets.IsWebSocketRequest)
            {
                ctx.Response.StatusCode = 400;
                return;
            }

            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.RequestAborted);
            WebSocket ws = await ctx.WebSockets.AcceptWebSocketAsync();

            try
            {
                await liveState.RunHostAsync(ws, cts.Token);
            }
            catch { }
        });

        app.MapGet("/favicon.ico", () => Results.NoContent());

        app.MapGet("/logs", () => Results.Content(RelayLog.Page(), "text/html; charset=utf-8"));
        app.MapPost("/api/log", async (HttpContext ctx) =>
        {
            using StreamReader reader = new StreamReader(ctx.Request.Body);
            string text = (await reader.ReadToEndAsync()).Trim().Replace('\n', ' ');
            if (text.Length == 0 || text.Length > 180 || !text.StartsWith("клиент ", StringComparison.Ordinal))
                return Results.BadRequest();
            RelayLog.Add(text);
            return Results.Ok();
        });
        app.MapGet("/api/logs", () =>
        {
            string[] lines = RelayLog.Snapshot();
            string body = lines.Length == 0
                ? "Пока пусто. Здесь будет старт файла и задержка live?t=."
                : string.Join('\n', lines);
            return Results.Text(body, "text/plain", Encoding.UTF8);
        });

        app.MapGet("/api/live/ready", () =>
        {
            (_, bool hadInit) = liveState.NoteViewer();
            return Results.Json(new { ready = hadInit });
        });

        app.MapGet("/api/live", async ctx =>
        {
            string token = ctx.Request.Query["t"].ToString();
            if (string.IsNullOrWhiteSpace(token)) token = "-";
            (double sinceMs, bool hadInit) = liveState.NoteViewer();
            string sinceText = sinceMs < 0 ? "старта файла ещё не было" : $"{sinceMs:0} мс после старта файла";
            RelayLog.Add($"live?t={token} открыт, {sinceText}, {(hadInit ? "заголовок уже есть" : "заголовок ждёт")}");
            DateTime openedUtc = DateTime.UtcNow;
            bool loggedByte = false;
            ctx.Response.StatusCode = 200;
            ctx.Response.Headers["Content-Type"] = "video/webm";
            ctx.Response.Headers["Cache-Control"] = "no-cache, no-store, must-revalidate";
            ctx.Response.Headers["Pragma"] = "no-cache";
            ctx.Response.Headers["Accept-Ranges"] = "none";
            ctx.Response.Headers["Access-Control-Allow-Origin"] = "*";
            ctx.Response.Headers["X-Content-Type-Options"] = "nosniff";
            ctx.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();

            // Нельзя выбрасывать куски из середины WebM: браузер теряет заголовок и начинает дёргаться.
            Channel<byte[]> channel = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

            Guid subId = liveState.Subscribe(channel.Writer);

            try
            {
                await foreach (byte[] chunk in channel.Reader.ReadAllAsync(ctx.RequestAborted))
                {
                    if (!loggedByte)
                    {
                        loggedByte = true;
                        RelayLog.Add($"live?t={token} первый байт через {(DateTime.UtcNow - openedUtc).TotalMilliseconds:0} мс");
                    }
                    await ctx.Response.Body.WriteAsync(chunk, ctx.RequestAborted);
                    await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception) { }
            finally
            {
                liveState.Unsubscribe(subId);
            }
        });

        app.Map("/proxy", async ctx =>
        {
            string url = ctx.Request.Query["url"].ToString();
            string refParam = ctx.Request.Query["ref"].ToString();

            if (!Uri.TryCreate(url, UriKind.Absolute, out Uri? uri) ||
                (uri.Scheme != "http" && uri.Scheme != "https") ||
                !proxyState.IsAllowed(uri))
            {
                ctx.Response.StatusCode = 403;
                await ctx.Response.WriteAsync("source not announced by host", ctx.RequestAborted);
                return;
            }

            string referer = string.IsNullOrWhiteSpace(refParam) ? proxyState.RefererFor(uri) ?? "" : refParam;
            bool looksPlaylist = url.Contains(".m3u8", StringComparison.OrdinalIgnoreCase);

            using HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Get, uri);
            if (ctx.Request.Headers.TryGetValue("Range", out Microsoft.Extensions.Primitives.StringValues range))
                req.Headers.TryAddWithoutValidation("Range", range.ToString());
            req.Headers.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0");
            req.Headers.TryAddWithoutValidation("Accept", "*/*");
            string? cookie = proxyState.CookieFor(uri);
            if (!string.IsNullOrEmpty(cookie))
                req.Headers.TryAddWithoutValidation("Cookie", cookie);
            if (!string.IsNullOrEmpty(referer))
            {
                req.Headers.TryAddWithoutValidation("Referer", referer);
                if (Uri.TryCreate(referer, UriKind.Absolute, out Uri? r0))
                    req.Headers.TryAddWithoutValidation("Origin", r0.GetLeftPart(UriPartial.Authority));
            }

            HttpResponseMessage resp;
            try { resp = await ProxyState.Http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ctx.RequestAborted); }
            catch { ctx.Response.StatusCode = 502; return; }

            using (resp)
            {
                ctx.Response.StatusCode = (int)resp.StatusCode;
                string ct = resp.Content.Headers.ContentType?.ToString() ?? "";
                if (looksPlaylist || ct.Contains("mpegurl", StringComparison.OrdinalIgnoreCase))
                {
                    string body = await resp.Content.ReadAsStringAsync(ctx.RequestAborted);
                    proxyState.AllowReferenced(url, body, referer);
                    byte[] bytes = Encoding.UTF8.GetBytes(ProxyState.RewriteHls(body, url, referer));
                    ctx.Response.ContentType = "application/vnd.apple.mpegurl";
                    ctx.Response.Headers["Cache-Control"] = "no-store";
                    ctx.Response.ContentLength = bytes.LongLength;
                    await ctx.Response.Body.WriteAsync(bytes, ctx.RequestAborted);
                    return;
                }

                foreach (KeyValuePair<string, IEnumerable<string>> header in resp.Content.Headers)
                {
                    if (header.Key.Equals("Content-Encoding", StringComparison.OrdinalIgnoreCase)) continue;
                    if (header.Key.Equals("Content-Length", StringComparison.OrdinalIgnoreCase) &&
                        resp.Content.Headers.ContentEncoding.Count > 0) continue;
                    ctx.Response.Headers[header.Key] = string.Join(", ", header.Value);
                }
                if (resp.Headers.TryGetValues("Accept-Ranges", out IEnumerable<string>? ranges))
                    ctx.Response.Headers["Accept-Ranges"] = string.Join(", ", ranges);
                ctx.Response.Headers["Access-Control-Allow-Origin"] = "*";
                await resp.Content.CopyToAsync(ctx.Response.Body, ctx.RequestAborted);
            }
        });

        ConcurrentDictionary<int, Suggestion> suggestions = new ConcurrentDictionary<int, Suggestion>();
        object idLock = new object();
        int nextId = 1;

        app.MapGet("/api/suggestions", () => Results.Json(suggestions.Values.OrderBy(static s => s.Id).ToList()));
        app.MapPost("/api/suggestions", (SuggestionDto dto) =>
        {
            if (dto?.Url is not { Length: > 3 }) return Results.BadRequest();
            int id; lock (idLock) id = nextId++;
            string title = string.IsNullOrWhiteSpace(dto.Title)
                ? (Uri.TryCreate(dto.Url, UriKind.Absolute, out Uri? u) ? u.Host : dto.Url)
                : dto.Title!;
            Suggestion s = new Suggestion(id, dto.Url!, title, DateTime.UtcNow);
            suggestions[id] = s;
            return Results.Json(s);
        });
        app.MapDelete("/api/suggestions/{id:int}", (int id) =>
            suggestions.TryRemove(id, out _) ? Results.Ok() : Results.NotFound());

        app.Run();
    }
}

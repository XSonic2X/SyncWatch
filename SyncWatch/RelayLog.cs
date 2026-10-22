using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace SyncWatch;

// Журнал старта файла и задержки /api/live. Кольцо в памяти, без файла.
public static class RelayLog
{
    private const int Limit = 200;
    private static readonly object Gate = new object();
    private static readonly Queue<string> Lines = new Queue<string>();

    public static void Add(string text)
    {
        string line = DateTime.Now.ToString("HH:mm:ss.fff") + "  " + text;
        lock (Gate)
        {
            Lines.Enqueue(line);
            while (Lines.Count > Limit) Lines.Dequeue();
        }
    }

    public static string[] Snapshot()
    {
        lock (Gate) return Lines.ToArray();
    }

    public static string Page()
    {
        StringBuilder sb = new StringBuilder();
        sb.Append("""
<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8"><title>SyncWatch — логи</title>
<style>
body{margin:0;background:#0e0f13;color:#d7dbe7;font:13px/1.45 ui-monospace,Consolas,monospace}
header{display:flex;gap:14px;align-items:center;padding:12px 16px;background:#16181f;border-bottom:1px solid #262a35}
a{color:#7ab7ff}
pre{margin:0;padding:14px 16px;white-space:pre-wrap}
</style></head><body>
<header><b>SyncWatch</b><span>журнал</span><a href="/">к просмотру</a></header><pre id="log">
""");
        string[] lines = Snapshot();
        if (lines.Length == 0)
            sb.Append("Пока пусто. Здесь будет старт файла и задержка live?t=.");
        else
        {
            foreach (string line in lines)
            {
                sb.Append(WebUtility.HtmlEncode(line));
                sb.Append('\n');
            }
        }
        sb.Append("""
</pre><script>
async function pull(){
  const r=await fetch('/api/logs',{cache:'no-store'});
  const t=await r.text();
  const el=document.getElementById('log');
  if(el.textContent!==t){el.textContent=t;scrollTo(0,document.body.scrollHeight);}
}
setInterval(pull,1000);
</script></body></html>
""");
        return sb.ToString();
    }
}

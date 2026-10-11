from fastapi.responses import HTMLResponse


def dashboard_html() -> HTMLResponse:
    return HTMLResponse(
        """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Distributed Task Queue</title>
    <style>
      :root { color-scheme: light dark; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }
      body { margin: 0; background: #0f172a; color: #e2e8f0; }
      main { max-width: 1120px; margin: 0 auto; padding: 32px 20px; }
      h1 { margin: 0 0 8px; font-size: 32px; letter-spacing: 0; }
      .subtle { color: #94a3b8; margin: 0 0 28px; }
      .grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 14px;
      }
      .metric, .events {
        background: #111827;
        border: 1px solid #334155;
        border-radius: 8px;
        padding: 16px;
      }
      .metric span { display: block; color: #94a3b8; font-size: 13px; }
      .metric strong { display: block; margin-top: 8px; font-size: 30px; }
      .events { margin-top: 20px; }
      .event { border-bottom: 1px solid #243044; padding: 12px 0; font-size: 14px; }
      .event:last-child { border-bottom: 0; }
      code { color: #bfdbfe; overflow-wrap: anywhere; }
      .status { display: inline-block; min-width: 80px; color: #86efac; }
    </style>
  </head>
  <body>
    <main>
      <h1>Distributed Task Queue</h1>
      <p class="subtle">Live operational view of Redis-backed background jobs.</p>
      <section class="grid" id="metrics"></section>
      <section class="events">
        <h2>Recent job events</h2>
        <div id="events"></div>
      </section>
    </main>
    <script>
      const metrics = document.getElementById("metrics");
      const events = document.getElementById("events");
      const labels = {
        total_jobs: "Total jobs",
        queued_jobs: "Queued",
        running_jobs: "Running",
        succeeded_jobs: "Succeeded",
        failed_jobs: "Failed",
        dead_letter_jobs: "Dead-letter"
      };

      async function refreshStats() {
        const response = await fetch("/queues/stats");
        const data = await response.json();
        metrics.innerHTML = Object.entries(labels).map(([key, label]) =>
          `<article class="metric"><span>${label}</span><strong>${data[key]}</strong></article>`
        ).join("");
      }

      function addEvent(event) {
        const row = document.createElement("div");
        row.className = "event";
        row.innerHTML = `<span class="status">${event.status}</span> `
          + `<code>${event.job_id}</code> ${event.message}`;
        events.prepend(row);
        while (events.children.length > 30) events.lastChild.remove();
        refreshStats();
      }

      refreshStats();
      const scheme = location.protocol === "https:" ? "wss" : "ws";
      const socket = new WebSocket(`${scheme}://${location.host}/ws/jobs`);
      socket.onmessage = (message) => addEvent(JSON.parse(message.data));
      setInterval(refreshStats, 5000);
    </script>
  </body>
</html>
        """
    )

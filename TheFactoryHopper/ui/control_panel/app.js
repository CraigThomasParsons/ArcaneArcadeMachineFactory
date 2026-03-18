const stageRoot = document.getElementById("stages");
const eventsRoot = document.getElementById("events");
const refreshBtn = document.getElementById("refreshBtn");
const runAllBtn = document.getElementById("runAllBtn");
const stageCardTemplate = document.getElementById("stageCardTemplate");

const statusClass = (status) => {
  if (status === "ok") return "ok";
  if (status === "failed") return "failed";
  return "unknown";
};

const formatEvent = (event) => {
  const ts = event.ts || "";
  const stage = event.stage || "unknown";
  const summary = event.summary || event.event || "(no summary)";
  const status = event.status || "unknown";
  return { ts, stage, summary, status };
};

const runAction = async (payload) => {
  const response = await fetch("/api/action", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!response.ok) {
    const msg = data?.error || data?.result?.output || "Action failed";
    throw new Error(msg);
  }

  return data;
};

const renderStages = (stages) => {
  stageRoot.innerHTML = "";

  for (const stage of stages) {
    const node = stageCardTemplate.content.cloneNode(true);

    const name = node.querySelector(".stage-name");
    const badge = node.querySelector(".badge.status");
    const queueDepth = node.querySelector(".queue-depth");
    const outboxCount = node.querySelector(".outbox-count");
    const archiveCount = node.querySelector(".archive-count");
    const failedCount = node.querySelector(".failed-count");
    const detail = node.querySelector(".detail");
    const runBtn = node.querySelector(".lane-run");

    name.textContent = stage.name;
    badge.textContent = stage.status || "unknown";
    badge.classList.add(statusClass(stage.status));

    queueDepth.textContent = String(stage.queue_depth ?? 0);
    outboxCount.textContent = String(stage.outbox_count ?? 0);
    archiveCount.textContent = String(stage.archive_count ?? 0);
    failedCount.textContent = String(stage.failed_count ?? 0);

    const event = stage.last_event || {};
    detail.textContent = event.summary || "No events yet.";

    runBtn.addEventListener("click", async () => {
      runBtn.disabled = true;
      runBtn.textContent = "Running...";
      try {
        await runAction({ action: "run_once", stage: stage.name });
        await refresh();
      } catch (error) {
        alert(`Run failed for ${stage.name}: ${error.message}`);
      } finally {
        runBtn.disabled = false;
        runBtn.textContent = "Run Once";
      }
    });

    stageRoot.appendChild(node);
  }
};

const renderEvents = (events) => {
  eventsRoot.innerHTML = "";

  const sorted = [...events].reverse();
  for (const item of sorted) {
    const e = formatEvent(item);
    const div = document.createElement("div");
    div.className = "event";
    div.innerHTML = `
      <div><strong>${e.stage}</strong> - ${e.summary}</div>
      <div class="meta">${e.ts} | ${e.status}</div>
    `;
    eventsRoot.appendChild(div);
  }
};

const refresh = async () => {
  const response = await fetch("/api/overview", { cache: "no-store" });
  const data = await response.json();

  renderStages(data.stages || []);
  renderEvents(data.events || []);
};

runAllBtn.addEventListener("click", async () => {
  runAllBtn.disabled = true;
  runAllBtn.textContent = "Running...";
  try {
    await runAction({ action: "worker_all_once" });
    await refresh();
  } catch (error) {
    alert(`Run all failed: ${error.message}`);
  } finally {
    runAllBtn.disabled = false;
    runAllBtn.textContent = "Run All Once";
  }
});

refreshBtn.addEventListener("click", refresh);

refresh();
setInterval(refresh, 2500);

#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

function readJson(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw);
}

function loadOpenBlockers(registryPath) {
  if (!fs.existsSync(registryPath)) {
    return [];
  }
  try {
    const doc = readJson(registryPath);
    const blockers = doc.blockers && typeof doc.blockers === "object" ? doc.blockers : {};
    return Object.values(blockers)
      .filter((entry) => entry && typeof entry === "object")
      .filter((entry) => String(entry.status || "") === "open")
      .sort((a, b) => String(a.opened_at || "").localeCompare(String(b.opened_at || "")));
  } catch {
    return [];
  }
}

function loadSignals(signalsDir) {
  if (!fs.existsSync(signalsDir)) {
    return [];
  }
  const entries = fs.readdirSync(signalsDir).filter((name) => name.endsWith(".json"));
  const docs = [];
  for (const name of entries) {
    const filePath = path.join(signalsDir, name);
    try {
      const doc = readJson(filePath);
      docs.push({ filePath, ...doc });
    } catch {
      docs.push({
        filePath,
        schema_version: "v1",
        signal_id: path.basename(name, ".json"),
        ts: "",
        stage: "",
        job_id: "",
        reason: "invalid_json",
        requires_user_action: true,
        details: { parse_error: true },
      });
    }
  }
  docs.sort((a, b) => String(a.ts).localeCompare(String(b.ts)));
  return docs;
}

function summarizeSignals(signals) {
  const requiresUserAction = signals.filter((s) => !!s.requires_user_action);
  const blocked = signals.filter((s) => String(s.reason).includes("blocked") || String(s.reason).includes("no_ready") || !!s.requires_user_action);
  const byReason = {};

  for (const signal of signals) {
    const key = String(signal.reason || "unknown");
    byReason[key] = (byReason[key] || 0) + 1;
  }

  const topReasons = Object.entries(byReason)
    .sort((a, b) => b[1] - a[1])
    .map(([reason, count]) => ({ reason, count }));

  const actionable = requiresUserAction.map((s) => ({
    signal_id: s.signal_id || "",
    ts: s.ts || "",
    stage: s.stage || "",
    job_id: s.job_id || "",
    reason: s.reason || "",
    file: s.filePath,
  }));

  return {
    schema_version: "v1",
    ts: new Date().toISOString(),
    summary: {
      total_signals: signals.length,
      blocked_count: blocked.length,
      requires_user_action_count: requiresUserAction.length,
      top_reasons: topReasons,
    },
    actionable,
  };
}

function summarizeOpenBlockers(openBlockers) {
  const actionable = openBlockers
    .filter((b) => !!b.requires_user_action)
    .map((b) => ({
      blocker_id: b.blocker_id || "",
      opened_at: b.opened_at || "",
      stage: b.stage || "",
      job_id: b.job_id || "",
      reason: b.reason || "",
      severity: b.severity || "",
      signal_path: b.signal_path || "",
    }));

  return {
    open_blockers_count: openBlockers.length,
    requires_user_action_open_count: actionable.length,
    actionable,
  };
}

function main() {
  const args = process.argv.slice(2);
  const factoryRoot = args[0];
  if (!factoryRoot) {
    console.error(JSON.stringify({ result: "failed", error: "factoryRoot argument is required" }));
    process.exit(2);
  }

  const signalsDir = path.join(factoryRoot, "TheFactoryHopper", "docs", "scrum_master_signals");
  const openBlockersPath = path.join(signalsDir, "open_blockers.json");
  const outPath = path.join(factoryRoot, "TheFactoryHopper", "docs", "scrum_master_review.json");

  const signals = loadSignals(signalsDir);
  const signalReport = summarizeSignals(signals);
  const openBlockers = loadOpenBlockers(openBlockersPath);
  const blockerReport = summarizeOpenBlockers(openBlockers);
  const report = {
    ...signalReport,
    open_blockers: blockerReport,
  };
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n", "utf8");

  const output = {
    result: "ok",
    runtime: process.argv0.includes("bun") ? "bun" : "node",
    signals_dir: signalsDir,
    review_path: outPath,
    ...report,
  };
  console.log(JSON.stringify(output, null, 2));
}

main();

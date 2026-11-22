// Pure functions: turn HN API items into yahnr rows and keep a 24-hour window.
// No Cloudflare APIs here, so `node --test` can test them directly.

export const FRONT_PAGE_SIZE = 30;
export const WINDOW_SECONDS = 24 * 60 * 60;

// One HN API item -> the row shape that public/js/app.js expects.
export function toRow(item, order) {
  const isJob = item.type === "job";
  const url = item.url || `item?id=${item.id}`; // app.js prefixes HN for non-http urls
  let domain = "";
  if (item.url) {
    try {
      domain = new URL(item.url).hostname.replace(/^www\./, "");
    } catch {
      domain = "";
    }
  }
  return {
    order: String(order),
    title: item.title || "",
    url,
    domain,
    points: item.score || 0,
    user: item.by || "",
    num_comments: item.descendants || 0,
    // app.js skips rows without a thread_id; jobs have no comment thread.
    thread_id: isJob ? "" : String(item.id),
    type: isJob ? "Jobs" : "Other",
    posted_time: item.time,
  };
}

// Merge a new front-page snapshot into the stored window.
// Keeps one row per story: the highest points, the earliest posted_time, and
// the last time the story was on the front page. Drops stories not seen in 24h.
export function merge(previous, rows, nowSeconds) {
  const byId = new Map();
  for (const row of previous) {
    if (row.thread_id && row.last_seen > nowSeconds - WINDOW_SECONDS) {
      byId.set(row.thread_id, row);
    }
  }
  for (const row of rows) {
    if (!row.thread_id) continue;
    const old = byId.get(row.thread_id);
    if (!old) {
      byId.set(row.thread_id, { ...row, last_seen: nowSeconds });
      continue;
    }
    const best = row.points >= old.points ? row : old;
    byId.set(row.thread_id, {
      ...best,
      posted_time: Math.min(old.posted_time, row.posted_time),
      last_seen: nowSeconds,
    });
  }
  return [...byId.values()].sort((a, b) => b.points - a.points);
}

// "2026-11-21" in UTC, for the daily copy.
export function utcDay(nowSeconds) {
  return new Date(nowSeconds * 1000).toISOString().slice(0, 10);
}

// Map a request path to a KV key, or null if the path is not data.
//   /data/now.json                -> "now"
//   /data/hn-data-2026-11-21.json -> "day:2026-11-21"
export function dataKey(pathname) {
  if (pathname === "/data/now.json") return "now";
  const m = pathname.match(/^\/data\/hn-data-(\d{4}-\d{2}-\d{2})\.json$/);
  return m ? `day:${m[1]}` : null;
}

// Seconds a /data response may be cached. "now" and today's copy change on
// every cron run; a past day never changes.
export function maxAge(key, nowSeconds) {
  return key === "now" || key === `day:${utcDay(nowSeconds)}` ? 300 : 86400;
}

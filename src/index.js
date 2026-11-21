// yahnr on Cloudflare Workers.
// - scheduled (every 15 minutes): read the HN front page, merge it into KV.
// - fetch: serve /data/*.json from KV; everything else comes from ./public.
import { FRONT_PAGE_SIZE, dataKey, merge, toRow, utcDay } from "./hn.js";

const HN_API = "https://hacker-news.firebaseio.com/v0";

async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`);
  return res.json();
}

async function frontPage() {
  const ids = (await getJson(`${HN_API}/topstories.json`)).slice(0, FRONT_PAGE_SIZE);
  const items = await Promise.all(ids.map((id) => getJson(`${HN_API}/item/${id}.json`)));
  // Deleted or dead items come back as null or without a title.
  return items.map((item, i) => (item && item.title ? toRow(item, i + 1) : null)).filter(Boolean);
}

export async function refresh(env, nowSeconds) {
  const rows = await frontPage();
  const previous = (await env.YAHNR.get("now", "json")) || [];
  const merged = merge(previous, rows, nowSeconds);
  const body = JSON.stringify(merged);
  await env.YAHNR.put("now", body);
  // Overwrite today's copy on every run, so the last run of the day wins.
  await env.YAHNR.put(`day:${utcDay(nowSeconds)}`, body);
  return { fetched: rows.length, stored: merged.length };
}

export default {
  async scheduled(controller, env, ctx) {
    const result = await refresh(env, Math.floor(controller.scheduledTime / 1000));
    console.log(JSON.stringify({ event: "refresh", ...result }));
  },

  async fetch(request, env) {
    const key = dataKey(new URL(request.url).pathname);
    if (key === null) return env.ASSETS.fetch(request);

    const body = await env.YAHNR.get(key);
    if (body === null) return new Response("Not found", { status: 404 });
    // now.json changes every 15 minutes; a past day never changes.
    const maxAge = key === "now" ? 300 : 86400;
    return new Response(body, {
      headers: {
        "content-type": "application/json; charset=utf-8",
        "cache-control": `public, max-age=${maxAge}`,
      },
    });
  },
};

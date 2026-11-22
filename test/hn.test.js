import assert from "node:assert/strict";
import { test } from "node:test";
import { dataKey, maxAge, merge, toRow, utcDay, WINDOW_SECONDS } from "../src/hn.js";

const story = {
  by: "dhouston",
  descendants: 71,
  id: 8863,
  score: 104,
  time: 1175714200,
  title: "My YC app: Dropbox - Throw away your USB drive",
  type: "story",
  url: "http://www.getdropbox.com/u/2/screencast.html",
};

test("toRow maps a story to the app.js row shape", () => {
  assert.deepEqual(toRow(story, 1), {
    order: "1",
    title: story.title,
    url: story.url,
    domain: "getdropbox.com",
    points: 104,
    user: "dhouston",
    num_comments: 71,
    thread_id: "8863",
    type: "Other",
    posted_time: 1175714200,
  });
});

test("toRow: Ask HN has no url, so it links to the HN item", () => {
  const row = toRow({ ...story, url: undefined }, 2);
  assert.equal(row.url, "item?id=8863");
  assert.equal(row.domain, "");
});

test("toRow: a job has no thread, so app.js skips it", () => {
  const row = toRow({ ...story, type: "job", descendants: undefined }, 3);
  assert.equal(row.thread_id, "");
  assert.equal(row.type, "Jobs");
  assert.equal(row.num_comments, 0);
});

test("merge keeps the highest points and the earliest posted_time", () => {
  const now = 2_000_000_000;
  const first = merge([], [toRow({ ...story, score: 50 }, 5)], now - 900);
  const second = merge(first, [toRow({ ...story, score: 104, time: story.time + 60 }, 1)], now);
  assert.equal(second.length, 1);
  assert.equal(second[0].points, 104);
  assert.equal(second[0].posted_time, story.time);
  assert.equal(second[0].last_seen, now);
});

test("merge keeps the old row when the new one has fewer points", () => {
  const now = 2_000_000_000;
  const first = merge([], [toRow({ ...story, score: 200 }, 1)], now - 900);
  const second = merge(first, [toRow({ ...story, score: 150 }, 9)], now);
  assert.equal(second[0].points, 200);
  assert.equal(second[0].last_seen, now);
});

test("merge drops stories not seen in 24 hours and sorts by points", () => {
  const now = 2_000_000_000;
  const old = { ...toRow({ ...story, id: 1, score: 999 }, 1), last_seen: now - WINDOW_SECONDS };
  const recent = { ...toRow({ ...story, id: 2, score: 10 }, 2), last_seen: now - 60 };
  const merged = merge([old, recent], [toRow({ ...story, id: 3, score: 20 }, 1)], now);
  assert.deepEqual(merged.map((r) => r.thread_id), ["3", "2"]);
});

test("utcDay and dataKey", () => {
  assert.equal(utcDay(1763683200), "2025-11-21");
  assert.equal(dataKey("/data/now.json"), "now");
  assert.equal(dataKey("/data/hn-data-2026-11-21.json"), "day:2026-11-21");
  assert.equal(dataKey("/data/../secret"), null);
  assert.equal(dataKey("/index.html"), null);
});

test("maxAge: now and today change often, a past day never changes", () => {
  const now = 1763683200; // 2025-11-21
  assert.equal(maxAge("now", now), 300);
  assert.equal(maxAge("day:2025-11-21", now), 300);
  assert.equal(maxAge("day:2025-11-20", now), 86400);
});

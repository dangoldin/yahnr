yahnr
=====

Yet another Hacker News reader. Show top stories from past 24 hours.

Live at https://yahnr.dangoldin.com.

## How it works

One Cloudflare Worker does everything:

- **Every 15 minutes** (Cron Trigger), it reads the top 30 stories from the
  [HN API](https://github.com/HackerNews/API) and merges them into Workers KV.
  KV keeps one row per story seen in the last 24 hours, with its highest
  points.
- **Requests** for `/data/now.json` and `/data/hn-data-YYYY-MM-DD.json` come
  from KV. Everything else is a static file in `public/`.
- `public/js/app.js` loads the JSON and sorts the stories by points.
  `?date=hn-data-YYYY-MM-DD` shows the copy for one day (UTC).

KV keys:

| Key | Holds |
|---|---|
| `now` | Stories seen in the last 24 hours |
| `day:YYYY-MM-DD` | The last `now` of that day (UTC) |

## Develop

```
npm install
npm test         # unit tests for src/hn.js
npm run dev      # local Worker on :8787 with local KV
curl localhost:8787/__scheduled   # run the cron once
npm run check    # tests + a deploy dry run
```

## Deploy

```
npm run login    # once per Mac
npm run deploy
```

The scripts remove `CLOUDFLARE_API_TOKEN` from the environment, so wrangler
uses your own login and not the DNS-only token in `~/.keys`.

The first deploy creates the KV namespace and the `yahnr.dangoldin.com`
custom domain. Cloudflare owns that DNS record; it is not in the dev-ops
Terraform.

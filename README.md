# 🔍 眼明手快找不同

跨團契破冰遊戲。主持用大螢幕開房，團友掃 QR Code 帶房號入房，喺兩幅圖上鬥快搵不同。10 款畫風，兩個玩法：全場即時搶點、各自鬥快搵齊。

## 檔案

- `public/index.html` — 主持版面、手機入房、QR Code、主持後台。正式版冇「下載部署包」按鈕。
- `worker.js` — 每個房號一個 SQLite Durable Object `SpotDiffRoom`。WebSocket 路徑係 `/ws?room=`。
- `wrangler.jsonc` — Cloudflare Workers 設定。靜態檔同連線後端同一條網址。

## 部署

```bash
CI=1 npx --yes wrangler@4 deploy
```

線上網址：https://spot-the-diff.ymtwill.workers.dev/

`/health` 回傳 `{"ok":true,"service":"spot-the-diff"}`。

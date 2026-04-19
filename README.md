# Distill-Style Monitor

A Chrome-compatible extension that watches full pages or specific elements for content changes and shows browser notifications.

## What it does

- Monitor the whole current page or click-pick a specific element.
- Store monitors locally in the browser.
- Re-check targets on a schedule using extension alarms.
- Compare the latest snapshot with the previous one.
- Notify you when a change is detected.
- Manage monitors from a simple dashboard.

## How to run it

1. Open Chrome and go to `chrome://extensions`.
2. Turn on **Developer mode**.
3. Create a `.env` file in the project root with:
   `SHOPIFY_SHOP`
   `SHOPIFY_CLIENT_ID`
   `SHOPIFY_CLIENT_SECRET`
   `SHOPIFY_API_VERSION`
4. Click **Load unpacked**.
5. Select this folder.

## How to use it

1. Visit any page you want to monitor.
2. Open the extension popup.
3. Choose either:
   - `Pick element on this tab`
   - `Monitor full page`
4. Set the name and interval.
5. Open the dashboard to review snapshots, refresh checks, or delete monitors.

## Notes

- The current implementation tracks text content changes.
- Scheduled checks use the Chrome alarms API, so the practical minimum interval is one minute.
- Some sites render data dynamically after initial HTML load. Those pages may need a richer capture approach than this first version.
- Shopify configuration is loaded locally from `.env` and is intentionally ignored by Git.

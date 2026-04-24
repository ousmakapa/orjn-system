import {
  DEFAULT_INTERVAL_MINUTES,
  buildHtmlDiff,
  getMonitors,
  saveMonitors,
  uid,
  addLog,
  canonicalizeBrand
} from "./shared.js";
import { updateShopifyForMonitor, syncMonitorBrandsToShopify, normalizeAllShopifyVendors, syncMonitorShopifyStatus } from "./shopify.js";

const ALARM_PREFIX = "monitor:";
const MAX_HISTORY_ENTRIES = 12;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Serialise all storage writes so concurrent runMonitor calls don't overwrite each other.
let storageLock = Promise.resolve();
function withStorageLock(fn) {
  const next = storageLock.then(() => fn());
  storageLock = next.catch(() => {});
  return next;
}

const captureTabIds = new Set();
const MAX_CONCURRENT_CAPTURES = 10;     // normal scheduled checks
const MAX_CONCURRENT_BATCH = 10;        // batch import (monitor several shoes)
let activeCaptureCount = 0;
let activeBatchCount = 0;
const captureQueue = [];
const batchQueue = [];

function acquireSlot(isBatch = false) {
  if (isBatch) {
    if (activeBatchCount < MAX_CONCURRENT_BATCH) { activeBatchCount++; return Promise.resolve(); }
    return new Promise((resolve) => batchQueue.push(resolve));
  }
  if (activeCaptureCount < MAX_CONCURRENT_CAPTURES) { activeCaptureCount++; return Promise.resolve(); }
  return new Promise((resolve) => captureQueue.push(resolve));
}

function releaseSlot(isBatch = false) {
  if (isBatch) {
    if (batchQueue.length > 0) { batchQueue.shift()(); } else { activeBatchCount--; }
    return;
  }
  if (captureQueue.length > 0) { captureQueue.shift()(); } else { activeCaptureCount--; }
}

// ── Stop checks ───────────────────────────────────────────────────────────────
let stopChecksFlag = false;

function stopAllChecks() {
  stopChecksFlag = true;
  for (const tabId of [...captureTabIds]) {
    captureTabIds.delete(tabId);
    chrome.tabs.remove(tabId).catch(() => {});
  }
  while (captureQueue.length) captureQueue.shift()();
  while (batchQueue.length) batchQueue.shift()();
  activeCaptureCount = 0;
  activeBatchCount = 0;
  setTimeout(() => { stopChecksFlag = false; }, 500);
}

// ── Undo stack ────────────────────────────────────────────────────────────────
const undoStack = [];
const UNDO_WINDOW_MS = 60 * 60 * 1000;

function pruneUndoStack() {
  const cutoff = Date.now() - UNDO_WINDOW_MS;
  while (undoStack.length && undoStack[0].timestamp < cutoff) undoStack.shift();
}

function pushUndo(description, restoreFn) {
  pruneUndoStack();
  undoStack.push({ timestamp: Date.now(), description, restore: restoreFn });
}

async function undoLast() {
  pruneUndoStack();
  if (!undoStack.length) return { ok: false, error: "Nothing to undo" };
  const entry = undoStack.pop();
  try { await entry.restore(); return { ok: true, description: entry.description }; }
  catch (e) { return { ok: false, error: e.message }; }
}

async function ensureAlarm(monitor) {
  if (!monitor.autoCheck) {
    await chrome.alarms.clear(`${ALARM_PREFIX}${monitor.id}`);
    return;
  }
  await chrome.alarms.create(`${ALARM_PREFIX}${monitor.id}`, {
    periodInMinutes: Math.max(1, Number(monitor.intervalMinutes) || DEFAULT_INTERVAL_MINUTES)
  });
}

async function clearAlarm(monitorId) {
  await chrome.alarms.clear(`${ALARM_PREFIX}${monitorId}`);
}

function waitForTabLoad(tabId, timeoutMs = 60000) {
  return new Promise((resolve, reject) => {
    let done = false;

    const finish = (fn) => {
      if (done) return;
      done = true;
      clearTimeout(timeout);
      chrome.tabs.onUpdated.removeListener(updatedListener);
      chrome.tabs.onRemoved.removeListener(removedListener);
      fn();
    };

    const timeout = setTimeout(() => finish(() => reject(new Error("Timed out loading page"))), timeoutMs);

    function updatedListener(updatedTabId, changeInfo) {
      if (updatedTabId !== tabId || changeInfo.status !== "complete") return;
      finish(resolve);
    }

    function removedListener(removedTabId) {
      if (removedTabId !== tabId) return;
      finish(() => reject(new Error("Tab closed by user")));
    }

    try {
      chrome.tabs.get(tabId, (tab) => {
        if (done) return;
        if (chrome.runtime.lastError) return;
        if (tab?.status === "complete") finish(resolve);
      });
    } catch (_) {}

    chrome.tabs.onUpdated.addListener(updatedListener);
    chrome.tabs.onRemoved.addListener(removedListener);
  });
}

// ── Shared extraction logic injected into both capture paths ─────────────────
// Returns { buildFullText, extractProduct } as a string block that can be
// eval'd inside any executeScript func (same-origin, isolated world is fine).
// We define it once here and splice it into both funcs below to avoid drift.

function pageExtractionCode() {
  // NOTE: this function's BODY is extracted as a string and injected inline.
  // It must be self-contained — no references to outer scope.

  const nt = (v) => (v ?? "").replace(/\s+/g, " ").replace(/\u00a0/g, " ").trim();

  function buildFullText() {
    const parts = [];
    const visible = nt(document.body?.innerText || "");
    if (visible) parts.push("=== PAGE TEXT ===\n" + visible);

    const jlds = [];
    document.querySelectorAll('script[type="application/ld+json"]').forEach((s) => {
      try { jlds.push(JSON.stringify(JSON.parse(s.textContent), null, 2)); } catch (_) {}
    });
    if (jlds.length) parts.push("=== STRUCTURED DATA (JSON-LD) ===\n" + jlds.join("\n---\n"));

    const metas = [];
    document.querySelectorAll("meta[name], meta[property]").forEach((m) => {
      const k = m.getAttribute("name") || m.getAttribute("property") || "";
      const v = m.getAttribute("content") || "";
      if (k && v && v.length < 600) metas.push(k + ": " + v);
    });
    if (metas.length) parts.push("=== META TAGS ===\n" + metas.join("\n"));
    return parts.join("\n\n");
  }

  function extractProduct() {
    const r = { name: null, brand: null, price: null, currency: null, sku: null, description: null, sizes: [], outOfStock: [], source: [] };

    // 1. JSON-LD (Schema.org Product)
    document.querySelectorAll('script[type="application/ld+json"]').forEach((script) => {
      try {
        const data = JSON.parse(script.textContent);
        const items = Array.isArray(data) ? data : [data];
        for (const item of items) {
          const t = item["@type"];
          if (t !== "Product" && !(Array.isArray(t) && t.includes("Product"))) continue;
          if (item.name && !r.name) { r.name = String(item.name); r.source.push("json-ld"); }
          if (item.description && !r.description) r.description = String(item.description).slice(0, 3000);
          if (item.sku && !r.sku) r.sku = String(item.sku);
          if (!r.sku && item.mpn) r.sku = String(item.mpn);
          if (item.brand && !r.brand) r.brand = typeof item.brand === "string" ? item.brand : (item.brand?.name ? String(item.brand.name) : null);
          const offers = item.offers ? (Array.isArray(item.offers) ? item.offers : [item.offers]) : [];
          if (!r.price && offers[0]?.price != null) { r.price = String(offers[0].price); r.currency = offers[0].priceCurrency || ""; }
          offers.slice(0, 80).forEach((o) => {
            const n = nt(o.name || o.description || "");
            if (!n || r.sizes.includes(n)) return;
            r.sizes.push(n);
            const av = String(o.availability || "").toLowerCase();
            if (av && !av.includes("instock") && !av.includes("onlineonly") && !av.includes("limitedavailability") && !av.includes("preorder")) {
              r.outOfStock.push(n);
            }
          });
        }
      } catch (_) {}
    });

    // 2. Open Graph / meta
    const mg = (a, v) => document.querySelector(`meta[${a}="${v}"]`)?.getAttribute("content") || null;
    if (!r.name) { const v = mg("property","og:title") || mg("name","twitter:title"); if (v) { r.name = v; r.source.push("og:title"); } }
    if (!r.brand) r.brand = mg("property","og:brand") || null;
    if (!r.price) {
      const v = mg("property","og:price:amount") || mg("property","product:price:amount");
      if (v) { r.price = v; r.currency = mg("property","og:price:currency") || mg("property","product:price:currency") || ""; r.source.push("og:price"); }
    }
    if (!r.sku) r.sku = mg("name","product:sku") || mg("property","product:retailer_item_id") || null;

    // 3. Schema.org microdata (itemprop)
    const ip = (name) => { const e = document.querySelector(`[itemprop="${name}"]`); return e ? (e.getAttribute("content") || e.textContent?.trim() || null) : null; };
    if (!r.name) { const v = ip("name"); if (v) { r.name = v; r.source.push("microdata"); } }
    if (!r.brand) r.brand = ip("brand") || null;
    if (!r.sku) r.sku = ip("sku") || ip("productID") || ip("gtin13") || null;
    if (!r.price) { const v = ip("price"), cur = ip("priceCurrency"); if (v) { r.price = v; r.currency = cur || ""; } }
    if (!r.description) r.description = ip("description") || null;

    // 4. DOM fallbacks
    if (!r.name) { const h1 = document.querySelector("h1"); if (h1) { r.name = h1.textContent?.trim(); r.source.push("h1"); } }
    if (!r.price) {
      for (const sel of [".price","[class*='price']","[data-price]","[class*='Price']",".product-price",".sale-price",".current-price"]) {
        const e = document.querySelector(sel);
        if (e) { const t = e.textContent?.trim(); if (t && /[\d.,]+/.test(t)) { r.price = t; r.source.push("dom"); break; } }
      }
    }
    if (!r.brand) {
      for (const sel of [".brand","[class*='brand']","[data-brand]","[class*='Brand']"]) {
        const e = document.querySelector(sel);
        if (e) { const t = e.textContent?.trim(); if (t && t.length < 60) { r.brand = t; break; } }
      }
    }
    if (!r.sku) {
      for (const sel of ["[class*='sku']","[data-sku]","[data-product-id]","[class*='reference']","[class*='product-code']","[id*='sku']"]) {
        const e = document.querySelector(sel);
        if (e) { const v = e.getAttribute("data-sku") || e.getAttribute("data-product-id") || e.textContent?.trim(); if (v && v.length < 40) { r.sku = v; break; } }
      }
      if (!r.sku) {
        const m = (document.body?.textContent || "").match(/(?:SKU|Ref\.?|Reference|Code|Art\.?\s*No\.?)\s*:?\s*([A-Z0-9\-_.]{3,25})/i);
        if (m) r.sku = m[1];
      }
    }

    // 5. Sizes from <select> or swatches
    if (!r.sizes.length) {
      const sel = document.querySelector('select[name*="size" i],select[id*="size" i],select[class*="size" i],select[data-option-name*="size" i]');
      if (sel) {
        Array.from(sel.options).forEach((o) => {
          const t = o.text.trim();
          if (!t || /^(select|choose|--)/i.test(t)) return;
          r.sizes.push(t);
          if (o.disabled || /out.?of.?stock|sold.?out|unavailable/i.test(o.text)) r.outOfStock.push(t);
        });
      }
    }
    if (!r.sizes.length) {
      for (const csel of ['[class*="size"] button,[class*="size"] label','[class*="swatch"] button,[class*="swatch"] label','[data-option-name*="size" i] button,[data-option-name*="size" i] label']) {
        const els = document.querySelectorAll(csel);
        if (els.length > 0 && els.length < 60) {
          els.forEach((el) => {
            const t = el.textContent?.trim();
            if (!t || t.length > 15 || r.sizes.includes(t)) return;
            r.sizes.push(t);
            const cls = ((el.className || "") + " " + (el.getAttribute("aria-label") || "")).toLowerCase();
            if (el.disabled || /disabled|out.?of.?stock|sold.?out|unavailable/i.test(cls)) r.outOfStock.push(t);
          });
          if (r.sizes.length) break;
        }
      }
    }

    if (r.price && r.currency && !r.price.includes(r.currency)) r.price = r.price.trim() + " " + r.currency;
    return r;
  }
}

// ── Capture from the tab the user already has open — no new tab needed ───────
async function captureSnapshotFromCurrentTab(tabId, monitor) {
  const selectors = Array.isArray(monitor.selectors)
    ? monitor.selectors
    : (monitor.selector ? [monitor.selector] : []);
  const priceAdjustment = Number(monitor.priceAdjustment) || 80;

  // New monitors often show the PDP data briefly and then re-render it away.
  // Start observing the live tab immediately so we can latch onto that transient
  // state before falling back to a plain one-shot extraction.
  captureTabIds.add(tabId);
  try {
    await injectCaptureObserver(tabId, selectors, true, monitor.name, priceAdjustment);
    const observed = await pollForResult(tabId, 8000);
    if (observed && observed.ok) {
      return observed;
    }
  } catch (_) {
    // Fall through to the direct one-shot capture below.
  } finally {
    captureTabIds.delete(tabId);
    chrome.scripting.executeScript({
      target: { tabId },
      func: () => {
        if (window.__monitorCaptureObserver) {
          try { window.__monitorCaptureObserver.disconnect(); } catch (_) {}
          window.__monitorCaptureObserver = null;
        }
        if (window.__monitorHeartbeat) {
          clearInterval(window.__monitorHeartbeat);
          window.__monitorHeartbeat = null;
        }
      }
    }).catch(() => {});
  }

  const [{ result }] = await chrome.scripting.executeScript({
    target: { tabId },
    func: (selectors, monitorName, priceAdjustment) => {
      const nt = (v) => (v ?? "").replace(/\s+/g, " ").replace(/\u00a0/g, " ").trim();

      function buildFullText() {
        const parts = [];
        const visible = nt(document.body?.innerText || "");
        if (visible) parts.push("=== PAGE TEXT ===\n" + visible);
        const jlds = [];
        document.querySelectorAll('script[type="application/ld+json"]').forEach((s) => {
          try { jlds.push(JSON.stringify(JSON.parse(s.textContent), null, 2)); } catch (_) {}
        });
        if (jlds.length) parts.push("=== STRUCTURED DATA (JSON-LD) ===\n" + jlds.join("\n---\n"));
        const metas = [];
        document.querySelectorAll("meta[name], meta[property]").forEach((m) => {
          const k = m.getAttribute("name") || m.getAttribute("property") || "";
          const v = m.getAttribute("content") || "";
          if (k && v && v.length < 600) metas.push(k + ": " + v);
        });
        if (metas.length) parts.push("=== META TAGS ===\n" + metas.join("\n"));
        return parts.join("\n\n");
      }

      function extractProduct() {
        const r = { name: null, brand: null, type: null, color: null, colorRaw: null, colorFinal: null, gender: null, price: null, currency: null, sku: null, description: null, images: [], sizes: [], outOfStock: [], source: [] };
        const BRANDS = ["Nike","Nike Sportswear","Nike SB","Air Jordan","Jordan","Jordan Brand","Adidas","Adidas Originals","Puma","Reebok","Reebok Classic","New Balance","NewBalance","NB","Converse","Converse All Star","Converse CONS","Vans","Van's","Under Armour","Under Armor","UA","Asics","Asics Tiger","Saucony","Brooks","Brooks Running","Hoka","Hoka One One","ON Cloud","On Running","On Cloud Running","Salomon","Salomon Sportstyle","Timberland","Timberland Pro","UGG","UGG Australia","Dr. Martens","Dr Martens","Doc Martens","Birkenstock","Clarks","Clarks Originals","The North Face","North Face","TNF","Columbia","Columbia Sportswear","Patagonia","Supreme","Off-White","Off White","Offwhite","Balenciaga","Gucci","Louis Vuitton","LV","Yeezy","Adidas Yeezy","Fila","Tommy Hilfiger","Tommy","Ralph Lauren","Polo Ralph Lauren","Polo","Lacoste","Champion","Kappa","Umbro","Ellesse","Diadora","Le Coq Sportif","Lecoqsportif","Mizuno","Karhu","Crocs","Skechers","Skecher","Steve Madden","Ecco","Geox","Camper","Stussy","Stüssy","Palace","Palace Skateboards","Kith","Carhartt","Carhartt WIP","Dickies","Stone Island","Moncler","Arc'teryx","Arcteryx","Merrell","Keen","Teva","Calvin Klein","CK","Hugo Boss","Boss","Boss by Hugo Boss"];
        const TYPE_KEYWORDS = {"basketball":"Basketball","casual":"Lifestyle","lifestyle":"Lifestyle","running":"Running","football":"Football","soccer":"Soccer","training":"Training","hiking":"Hiking","trail":"Trail","tennis":"Tennis","golf":"Golf","skate":"Skate","skateboarding":"Skate","crossfit":"Training","cross-training":"Training","walking":"Walking","sneaker":"Lifestyle","slip-on":"Lifestyle","sandal":"Sandal","boot":"Boot","loafer":"Lifestyle"};
        const COLORS = {"Black":["black","onyx","jet","ebony","obsidian","raven","coal","ink","shadow","noir","licorice","pitch","tripleblack","triple-black","coreblack","core-black","phantom","anthracite","soot","carbon"],"White":["white","ivory","snow","pearl","sail","cream","bone","eggshell","linen","frost","alabaster","porcelain","chalk","milk","cotton","ghost","offwhite","off-white","whisper","paper","shell","antique"],"Red":["red","crimson","scarlet","ruby","burgundy","maroon","wine","cherry","carmine","cardinal","tomato","garnet","vermillion","vermilion","brick","blood","firebrick","cranberry","raspberry","strawberry","rose","claret","mahogany","terra","cotta","sienna","auburn","rubyred","oxblood","merlot","poppy","coralred","sunsetred","chili","rubywine"],"Blue":["blue","navy","cobalt","royal","indigo","denim","sky","powder","midnight","steel","slate","sapphire","azure","thunder","ice","cornflower","periwinkle","iris","ultramarine","prussian","admiral","marine","federal","storm","glacier","arctic","aegean","obsidian-blue","turbo","polar","mistblue","oceanblue","deepblue","lightblue","darkblue","hyperblue","universityblue","carolinablue"],"Green":["green","olive","sage","forest","army","jade","emerald","mint","fern","moss","pine","volt","lime","hunter","bottle","kelly","shamrock","chartreuse","avocado","pistachio","pear","leaf","basil","seaweed","jungle","cucumber","matcha","celadon","viridian","malachite","voltgreen","neongreen","electricgreen","loden","spruce","evergreen","clover","pea","grassy","seaglass","seafoamgreen"],"Yellow":["yellow","gold","golden","mustard","lemon","canary","butter","banana","honey","sunflower","flaxen","straw","blonde","champagne","vanilla","daffodil","citrine","topaz","citrus","maize","corn","ambergold","sandgold","sulphur","mustardseed","dijon","neonyellow","electricyellow"],"Orange":["orange","amber","tangerine","apricot","rust","copper","pumpkin","saffron","coral","burnt","cinnamon","papaya","mango","melon","clay","ginger","tiger","marigold","bronze","peach","persimmon","nectarine","cantaloupe","sunset","burntorange","terracotta","carrot","kumquat"],"Violet":["purple","violet","lavender","lilac","plum","grape","mauve","amethyst","orchid","wisteria","heather","thistle","periwinkle","mulberry","eggplant","byzantium","aubergine","boysenberry","violetdust","deeppurple","royalpurple"],"Pink":["pink","blush","fuchsia","magenta","salmon","rose","bubblegum","flamingo","watermelon","peony","carnation","petal","flush","rouge","blossom","pastel","candy","lollipop","neon","cerise","hot","dusty","millennial","rosepink","powderpink","softpink","brightpink","shockpink"],"Brown":["brown","tan","beige","camel","mocha","chocolate","coffee","sand","taupe","nude","natural","khaki","wheat","stone","walnut","hazel","toffee","espresso","sepia","umber","fawn","oatmeal","biscuit","latte","ecru","buff","driftwood","chestnut","cacao","bark","leather","suede","caramel","pecan","almond","acorn","cocoa","mink","tobacco","saddle","oak","hickory","truffle","earth","mud","dune","bran"],"Gray":["gray","grey","silver","charcoal","ash","smoke","graphite","pewter","cement","concrete","cloud","wolf","pebble","flint","iron","lead","fossil","heather","marengo","dove","cool","smokey","stonegrey","stone-gray","coolgrey","cool-grey","neutralgray","neutralgrey","platinum","gunmetal"],"Turquoise":["turquoise","teal","aqua","cyan","seafoam","aquamarine","caribbean","lagoon","cerulean","peacock","ocean","pool","mintblue","tiffany","robinsegg","bluegreen","turq"],"Multicolor":["multi","multicolor","multi-color","assorted"]};
        const canonicalizeBrand = (value) => {
          const normalized = nt(value);
          if (!normalized) return "";
          const lower = normalized.toLowerCase();
          if (lower === "air jordan" || lower === "jordan") return "Jordan";
          if (lower === "on" || lower === "on running" || lower === "on cloud") return "ON Cloud";
          for (const b of BRANDS) {
            if (lower === b.toLowerCase()) return b;
          }
          return normalized;
        };
        const pickBrand = (t) => {
          if (!t) return null;
          const l = t.toLowerCase();
          if (l.includes("air jordan")) return "Jordan";
          if (l.includes("on running") || l.includes("on cloud")) return "ON Cloud";
          for (const b of BRANDS) {
            if (l.includes(b.toLowerCase())) return canonicalizeBrand(b);
          }
          return null;
        };
        const pickType = (t) => { if (!t) return null; const l = t.toLowerCase(); for (const [kw, mapped] of Object.entries(TYPE_KEYWORDS)) { if (l.includes(kw)) return mapped; } return null; };
        const pickColor = (t) => {
          if (!t) return "Multicolor";
          const normalized = t.toLowerCase().replace(/[^a-z0-9\s-]/g, " ");
          const words = normalized.split(/\s+/).filter(Boolean);
          const compact = normalized.replace(/[^a-z0-9]/g, "");
          for (const [base, synonyms] of Object.entries(COLORS)) {
            if (synonyms.some((c) => {
              const key = String(c).toLowerCase();
              if (words.includes(key)) return true;
              const keyCompact = key.replace(/[^a-z0-9]/g, "");
              return keyCompact.length >= 6 && compact.includes(keyCompact);
            })) return base;
          }
          return "Multicolor";
        };
        const looksLikeSku = (v) => v && /^[A-Z0-9]{3,12}(-[A-Z0-9]{2,6}){0,2}$/i.test(v.trim()) && v.length >= 4 && v.length <= 20;
        document.querySelectorAll('script[type="application/ld+json"]').forEach((script) => {
          try {
            const data = JSON.parse(script.textContent);
            const items = Array.isArray(data) ? data : [data];
            for (const item of items) {
              const t = item["@type"];
              if (t !== "Product" && !(Array.isArray(t) && t.includes("Product"))) continue;
              if (item.name && !r.name) { r.name = String(item.name); r.source.push("json-ld"); }
              if (item.description && !r.description) r.description = String(item.description).slice(0, 3000);
              if (item.sku && !r.sku) r.sku = String(item.sku);
              if (!r.sku && item.mpn) r.sku = String(item.mpn);
              if (item.brand && !r.brand) r.brand = typeof item.brand === "string" ? item.brand : (item.brand?.name ? String(item.brand.name) : null);
              if (!r.color && item.color) r.color = String(item.color);
              const offers = item.offers ? (Array.isArray(item.offers) ? item.offers : [item.offers]) : [];
              if (!r.price && offers[0]?.price != null) { r.price = String(offers[0].price); r.currency = offers[0].priceCurrency || ""; }
              offers.slice(0, 80).forEach((o) => {
                const n = nt(o.name || o.description || "");
                if (!n || r.sizes.includes(n)) return;
                r.sizes.push(n);
                const av = String(o.availability || "").toLowerCase();
                if (av && !av.includes("instock") && !av.includes("onlineonly") && !av.includes("limitedavailability") && !av.includes("preorder")) r.outOfStock.push(n);
              });
            }
          } catch (_) {}
        });
        const mg = (a, v) => document.querySelector(`meta[${a}="${v}"]`)?.getAttribute("content") || null;
        if (!r.name) { const v = mg("property","og:title") || mg("name","twitter:title"); if (v) { r.name = v; r.source.push("og:title"); } }
        if (!r.brand) r.brand = mg("property","og:brand") || null;
        if (!r.price) { const v = mg("property","og:price:amount") || mg("property","product:price:amount"); if (v) { r.price = v; r.currency = mg("property","og:price:currency") || ""; r.source.push("og:price"); } }
        if (!r.sku) r.sku = mg("name","product:sku") || mg("property","product:retailer_item_id") || null;
        if (!r.color) r.color = mg("property","og:color") || mg("name","product:color") || null;
        const ip = (name) => { const e = document.querySelector(`[itemprop="${name}"]`); return e ? (e.getAttribute("content") || e.textContent?.trim() || null) : null; };
        if (!r.name) { const v = ip("name"); if (v) { r.name = v; r.source.push("microdata"); } }
        if (!r.brand) r.brand = ip("brand") || null;
        if (!r.sku) r.sku = ip("sku") || ip("productID") || ip("gtin13") || null;
        if (!r.price) { const v = ip("price"), cur = ip("priceCurrency"); if (v) { r.price = v; r.currency = cur || ""; } }
        if (!r.description) r.description = ip("description") || null;
        if (!r.color) r.color = ip("color") || null;
        if (!r.name) { const h1 = document.querySelector("h1"); if (h1) { r.name = h1.textContent?.trim(); r.source.push("h1"); } }
        if (!r.price) { for (const sel of [".price","[class*='price']","[data-price]","[class*='Price']",".product-price",".sale-price",".current-price"]) { const e = document.querySelector(sel); if (!e) continue; const t = e.textContent?.trim() || ''; if (/\b(installment|interest.?free|klarna|afterpay|sezzle|quadpay|pay in \d|4x |zip\s*pay|split\s*pay|paidy|laybuy)\b/i.test(t)) continue; const pm = t.match(/[\$€£¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/); if (pm) { r.price = pm[1].replace(',', '.'); r.source.push("dom"); break; } } }
        if (!r.brand) { for (const sel of [".product__vendor",".product-vendor","[class*='vendor']",".brand","[class*='brand']","[data-brand]"]) { const e = document.querySelector(sel); if (e) { const t = e.textContent?.trim(); if (t && t.length < 60) { r.brand = t; break; } } } }
        if (!r.sku) {
          for (const sel of ["[class*='sku']","[data-sku]","[data-product-id]","[data-variant-sku]","[class*='reference']","[class*='product-code']","[id*='sku']"]) { const e = document.querySelector(sel); if (e) { const v = e.getAttribute("data-sku") || e.getAttribute("data-variant-sku") || e.getAttribute("data-product-id") || e.textContent?.trim(); if (looksLikeSku(v)) { r.sku = v.trim(); break; } } }
        }
        try { const sp = window.meta?.product || window.ShopifyAnalytics?.meta?.product; if (sp) { if (!r.brand && sp.vendor) { r.brand = String(sp.vendor); r.source.push("shopify"); } if (!r.sku && sp.sku) r.sku = String(sp.sku); } } catch (_) {}
        const metaEl = document.querySelector('.product__meta,.product-meta,[class*="product__information"],[class*="product-details"],[class*="product-info"]');
        const metaText = (metaEl || document.body || {}).textContent?.slice(0, 12000) || "";
        if (!r.sku) { const m = metaText.match(/\bSupplier.?sku\s*[#:\s]+([A-Z0-9]{3,15}(?:-[A-Z0-9]{2,8}){0,2})/i); if (m && looksLikeSku(m[1])) r.sku = m[1].trim(); }
        if (!r.sku) { const m = metaText.match(/\b(?:SKU|Style\s*No\.?|Style|Reference\s*No\.?|Reference|Ref\.?|Art\.?\s*No\.?|Item\s*No\.?|Product\s*Code|Style\s*Code|Item\s*Code|Model\s*(?:No\.?|#))[:\s#]+([A-Z0-9]{3,15}(?:-[A-Z0-9]{2,8}){0,2})/i); if (m && looksLikeSku(m[1])) r.sku = m[1].trim(); }
        if (!r.sku) { const m = metaText.match(/\bProduct\s*#\s*[:\s]+([A-Z0-9]{3,15}(?:-[A-Z0-9]{2,8}){0,2})/i); if (m && looksLikeSku(m[1])) r.sku = m[1].trim(); }
        { const bt = document.body?.textContent || ''; const sm = bt.match(/Supplier[\s-]*sku\s*#\s*:?\s*([A-Z0-9]{3,20}(?:-[A-Z0-9]{1,8}){0,3})/i); if (sm) r.sku = sm[1].trim(); }
        { const flPanel = document.querySelector('#ProductDetails-tabs-details-panel'); if (flPanel) { for (const span of flPanel.querySelectorAll(':scope > span')) { const t = (span.textContent || '').trim(); const m = t.match(/Supplier[\s-]*sku\s*#\s*:?\s*([A-Z0-9]{3,20}(?:-[A-Z0-9]{1,8}){0,3})/i); if (m) { r.sku = m[1].trim(); break; } } } }
        if (!r.brand) { const m = metaText.match(/\bBrand[:\s]+([^\n\r,/<]{2,40})/i); if (m) { r.brand = m[1].trim(); r.source.push("labeled"); } }
        if (!r.type) { const m = metaText.match(/\b(?:Type|Category|Sport|Usage|Department)[:\s]+([^\n\r,/<]{2,30})/i); if (m) { r.type = pickType(m[1]) || m[1].trim(); r.source.push("labeled"); } }
        if (!r.color) { const m = metaText.match(/\bColou?r[:\s]+([^\n\r,/<]{2,30})/i); if (m) r.color = m[1].split('/')[0].trim(); }
        let flSelectedColor = null;
        let flSelectedColorNormalized = null;
        {
          const styleCandidates = [
            document.querySelector('.ProductDetails-form__selectedStyle,[class*="selectedStyle"]')?.textContent?.trim() || '',
            document.querySelector('.ColorwayStyles-field.button-field--selected,[class*="button-field--selected"]')?.getAttribute('aria-label')?.trim() || '',
            document.querySelector('.ColorwayStyles-field.button-field--selected img[alt],[class*="button-field--selected"] img[alt]')?.getAttribute('alt')?.trim() || ''
          ];
          for (const candidate of styleCandidates) {
            if (!candidate) continue;
            const cleaned = candidate
              .replace(/^Color\s+/i, '')
              .replace(/\s+is selected.*$/i, '')
              .trim();
            const first = cleaned.split('/')[0].trim();
            if (!first) continue;
            flSelectedColor = first;
            flSelectedColorNormalized = pickColor(first);
            r.colorRaw = first;
            r.color = flSelectedColorNormalized;
            r.source.push("fl-selected-style-color");
            break;
          }
        }
        if (!flSelectedColorNormalized && !r.color) { const prodArea = document.querySelector('main,[role="main"]') || document.body; for (const el of prodArea.querySelectorAll('p,span,div')) { if (el.children.length > 0) continue; const t = (el.textContent || '').trim(); if (!t || t.length > 60) continue; const cm = t.match(/^([A-Z][a-zA-Z]+(?:\/[A-Z][a-zA-Z]+)*)$/); if (cm) { const normalized = pickColor(cm[1].split('/')[0]); if (normalized !== "Multicolor") { r.color = normalized; break; } } } }
        if (!r.gender) {
          const flHeader = document.querySelector('.ProductDetails-header-V2');
          if (flHeader) {
            const genderCandidates = [
              flHeader.querySelector('span.font-caption.my-2'),
              ...flHeader.querySelectorAll('span.font-caption,span[class*="font-caption"]')
            ];
            for (const candidate of genderCandidates) {
              const t = candidate?.textContent?.trim();
              if (t && /\b(men|women|kids|youth|boys|girls|unisex|infant|toddler)/i.test(t)) {
                r.gender = t;
                r.source.push("fl-header-gender");
                break;
              }
            }
          }
        }
        if (!r.gender) { const m = metaText.match(/\b(Men'?s?|Women'?s?|Kids'?|Youth|Boys'?|Girls'?|Unisex|Infant|Toddler)\b/i); if (m) r.gender = m[1]; }
        if (!r.gender) { for (const sel of ['.product-type','[class*="product-type"]','[class*="eyebrow"]','.product__gender','[class*="gender"]']) { try { const e = document.querySelector(sel); if (e) { const t = e.textContent?.trim(); if (t && /\b(men|women|kids|youth|boys|girls|unisex|infant|toddler)/i.test(t)) { r.gender = t; break; } } } catch(_){} } }
        if (r.gender) r.gender = r.gender.replace(/['\u2019]s?\s*$/i, '').trim();
        if (!r.type && r.name) { const t = pickType(r.name); if (t) { r.type = t; r.source.push("name"); } }
        { const flBcLinks = Array.from(document.querySelectorAll('nav[aria-label="Breadcrumb"] li a, li a[href*="pageType=browse"]')); for (const a of [...flBcLinks].reverse()) { const txt = (a.textContent || '').trim(); const t = pickType(txt); if (t) { r.type = t; r.source.push("fl-breadcrumb"); break; } const href = a.getAttribute('href') || ''; const sm = href.match(/style:([^:&]+)/i); if (sm) { const decoded = decodeURIComponent(sm[1].replace(/\+/g,' ')); const t2 = pickType(decoded); if (t2) { r.type = t2; r.source.push("fl-breadcrumb-href"); break; } } } }
        if (!r.type) { const bcEls = document.querySelectorAll('nav[aria-label*="breadcrumb" i] a,nav[aria-label*="breadcrumb" i] span,[class*="breadcrumb"] a,[class*="breadcrumb"] span,[class*="breadcrumb"] li,.breadcrumbs a,.breadcrumbs span,ol[class*="breadcrumb"] li,[data-testid*="breadcrumb"] span,[data-test*="breadcrumb"] span'); const crumbs = Array.from(bcEls).map(el => el.textContent?.trim()).filter(t => t && t.length > 1 && t.length < 60 && !/^[\s\/]+$/.test(t)); for (const c of [...crumbs].reverse().slice(1)) { if (pickType(c)) { r.type = pickType(c); r.source.push("breadcrumb"); break; } } }
        if (!r.type) { const bt = document.body?.innerText || ''; const bm = bt.match(/(?:Men'?s?|Women'?s?|Kids'?|Boys'?|Girls'?|Unisex)\s*\/\s*(?:Shoes?|Sneakers?|Footwear|Clothing|Apparel)\s*\/\s*([^\n\/]{3,60})/i); if (bm) { const mapped = pickType(bm[1].trim()); if (mapped) { r.type = mapped; r.source.push("breadcrumb-text"); } } }
        if (!r.description) {
          const flPanel = document.querySelector('#ProductDetails-tabs-details-panel,[aria-labelledby*="ProductDetails-tabs-details-tab"]');
          if (flPanel) {
            const contentDiv = flPanel.querySelector(':scope > div');
            const src = contentDiv || flPanel;
            const text = (src.textContent || '').replace(/\s+/g, ' ').trim();
            if (text.length > 20) r.description = text.slice(0, 3000);
          }
        }
        if (!r.description) {
          const pt = document.body?.innerText || '';
          const dm = pt.match(/\bDETAILS\b\s*\n+([\s\S]+?)\n+\s*(?:REVIEWS?|RATINGS?)\b/i);
          if (dm) {
            const metaLineRe = /^(?:Product\s*(?:#|No\.?|Code)|Supplier[\s\-]*sku|SKU|Style\s*(?:No\.?|#|Code)|Reference(?:\s*No\.?)?|Item\s*(?:#|No\.?|Code)|Colou?r|Width|Fit|Material)\b[^:\n]*:[^\n]*/i;
            const rawLines = dm[1].trim().split('\n');
            let start = 0;
            while (start < rawLines.length && metaLineRe.test(rawLines[start].trim())) start++;
            const desc = rawLines.slice(start).join('\n').trim();
            if (desc.length > 20) r.description = desc.slice(0, 3000);
          }
        }
        if (!r.description) {
          // Accordion heading "Details" → next sibling content
          const allHds = Array.from(document.querySelectorAll('h2,h3,h4,button,summary'));
          for (const hd of allHds) {
            if (!/^\s*details?\s*$/i.test(hd.textContent?.trim())) continue;
            const content = hd.nextElementSibling || hd.parentElement?.nextElementSibling;
            if (content) { const d = (content.innerText || content.textContent || '').trim(); if (d.length > 20 && d.length < 6000) { r.description = d.slice(0, 3000); break; } }
          }
        }
        if (!r.description) {
          const descSels = ['.product-description','.product__description','.product-single__description','[data-product-description]','.rte','.product-info__description','[class*="product-description"]','[class*="product-body"]','[class*="product-text"]','[class*="description"]'];
          for (const sel of descSels) { try { const e = document.querySelector(sel); if (e) { const d = (e.innerText || e.textContent || "").trim(); if (d.length > 80) { r.description = d.slice(0, 3000); break; } } } catch (_) {} }
        }
        if (!r.description) {
          const els = document.querySelectorAll('div,section,article');
          for (const el of els) {
            if (el.querySelectorAll('div,section').length > 20) continue;
            const text = (el.innerText || el.textContent || "").trim();
            if (text.length > 80 && text.length < 8000 && /\b(Highlights?|Composition|Features?|Caratteristiche|Descripci)/i.test(text.slice(0, 120))) { r.description = text.slice(0, 3000); break; }
          }
        }
        if (r.description) { const lines = r.description.split(/\n/); const clean = []; for (const ln of lines) { const cm = !r.color && ln.match(/^Colou?r[:\s]+(.+)/i); const ss = ln.match(/\bSupplier[\s-]*sku\s*#?[:\s]+([A-Z0-9]{3,20}(?:-[A-Z0-9]{1,8}){0,3})/i); const sm = ln.match(/^(?:Product\s*(?:Code|#|No\.?)|SKU|Style\s*(?:No\.?|Code|#)|Reference(?:\s*No\.?)?|Ref\.?|Art\.?\s*No\.?|Item\s*(?:No\.?|Code|#))\s*[:#\s]+([A-Z0-9]{3,20}(?:-[A-Z0-9]{1,8}){0,3})/i); if (cm) { r.color = cm[1].split('/')[0].trim(); } else if (ss && looksLikeSku(ss[1])) { r.sku = ss[1].trim(); } else if (sm && looksLikeSku(sm[1])) { if (!r.sku) r.sku = sm[1].trim(); } else { clean.push(ln); } } r.description = clean.join("\n").trim(); }
        if (r.name) {
          const sfx = r.name.match(/\s*[-\u2013]\s*(Men'?s?|Women'?s?|Kids'?|Youth|Boys'?|Girls'?|Unisex|Infant|Toddler)\b.*/i);
          if (sfx) { if (!r.gender) r.gender = sfx[1].replace(/['\u2019]s?\s*$/i, '').trim(); r.name = r.name.slice(0, r.name.length - sfx[0].length).trim(); }
        }
        if (r.name && /no longer available|product.*not found|page.*not found|item.*not found/i.test(r.name)) r.name = null;
        if (monitorName) {
          const gm = monitorName.match(/^(.+?)\s*[-\u2013]\s*(Men'?s?|Women'?s?|Kids'?|Youth|Boys'?|Girls'?|Unisex|Infant|Toddler)\b/i);
          if (gm) { if (!r.name) r.name = gm[1].trim(); if (!r.gender) r.gender = gm[2].replace(/['\u2019]s?\s*$/i, '').trim(); }
          else if (!r.name) { const pm = monitorName.match(/^(.+?)\s*\|/); if (pm) r.name = pm[1].replace(/\s*[-\u2013]\s*$/, '').trim(); }
        }
        const nameBrand = r.name ? pickBrand(r.name) : null;
        const searchText = [r.name, r.description, document.title].filter(Boolean).join(" ");
        if (!r.brand) r.brand = nameBrand || pickBrand(searchText) || pickBrand(metaText);
        if (r.brand) r.brand = canonicalizeBrand(r.brand);
        if (!r.type) r.type = pickType(searchText) || pickType(metaText);
        r.color = flSelectedColorNormalized || pickColor(r.color || searchText);
        r.colorFinal = r.color;
        if (!r.sizes.length) { const sel = document.querySelector('select[name*="size" i],select[id*="size" i],select[class*="size" i],select[data-option-name*="size" i]'); if (sel) { Array.from(sel.options).forEach((o) => { const t = o.text.trim(); if (!t || /^(select|choose|--)/i.test(t)) return; r.sizes.push(t); if (o.disabled || /out.?of.?stock|sold.?out|unavailable/i.test(o.text)) r.outOfStock.push(t); }); } }
        if (!r.sizes.length) { document.querySelectorAll('input[type="radio"][name*="size" i],input[type="radio"][data-option-value-id]').forEach((input) => { const t = (input.value || "").trim(); if (!t || /^(select|choose|--)/i.test(t) || t.length > 20) return; if (!r.sizes.includes(t)) r.sizes.push(t); if (input.classList.contains("disabled") || input.disabled) r.outOfStock.push(t); }); }
        if (!r.sizes.length) { for (const csel of ['[class*="size"] button,[class*="size"] label','[class*="swatch"] button,[class*="swatch"] label','[data-option-name*="size" i] button,[data-option-name*="size" i] label','[class*="size-selector"] button','[class*="sizebtn"]','[class*="size-btn"]']) { const els = document.querySelectorAll(csel); if (els.length > 0 && els.length < 80) { els.forEach((el) => { const t = el.textContent?.trim(); if (!t || t.length > 20 || r.sizes.includes(t)) return; r.sizes.push(t); const cls = ((el.className || "") + " " + (el.getAttribute("aria-label") || "")).toLowerCase(); const oos = el.classList.contains("disabled") || el.disabled || el.getAttribute("aria-disabled") === "true" || /disabled|out.?of.?stock|sold.?out|unavailable|not-available/i.test(cls) || window.getComputedStyle(el).textDecoration.includes("line-through"); if (oos) r.outOfStock.push(t); }); if (r.sizes.length) break; } } }
        if (!r.sizes.length) { const pa = document.querySelector('main,[role="main"]') || document.body; pa.querySelectorAll('button').forEach((el) => { const t = (el.textContent || "").trim(); if (!t || t.length > 10 || r.sizes.includes(t)) return; if (!/^\d+(?:[.,]\d+)?(?:\s*(?:US|EU|UK|CM|EUR|½))?$/i.test(t)) return; r.sizes.push(t); const oos = el.classList.contains("disabled") || el.disabled || el.getAttribute("aria-disabled") === "true" || window.getComputedStyle(el).textDecoration.includes("line-through"); if (oos) r.outOfStock.push(t); }); }
        const swatchEls = new Set();
        document.querySelectorAll('[class*="ColorChip"],[class*="color-chip"],[class*="colorway"],[class*="ColorSwatch"],[class*="color-swatch"],[class*="StyleChip"],[class*="style-chip"],[class*="VariantImage"],[class*="variant-color"],[class*="SwatchImage"],[class*="color-option"],[class*="product-variants"],[class*="ProductVariants"]').forEach(el => swatchEls.add(el));
        const isInSwatch = (img) => { let el = img.parentElement; while (el && el !== document.body) { if (swatchEls.has(el)) return true; const cls = el.className || ''; if (/color.?chip|color.?swatch|colorway|style.?chip|swatch.?img|variant.?img|color.?option/i.test(cls)) return true; el = el.parentElement; } return false; };
        const imgRoot = document.querySelector('.ProductGallery,[class*="ProductGallery"],[class*="product-gallery"],[class*="pdp-gallery"],[class*="Gallery--pdp"],[class*="GallerySlider"],[class*="gallery-slider"],.product-media,.pdp-product-images') || document.querySelector('main,[role="main"]') || document.body;
        const seenImgs = new Set();
        imgRoot.querySelectorAll('img').forEach((img) => {
          if (isInSwatch(img)) return;
          let src = '';
          if (img.srcset) { const best = img.srcset.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 }); if (best.u) src = best.u; }
          if (!src) src = img.currentSrc || img.src || '';
          if (!src || !/^https?:\/\//.test(src)) { const lz = img.getAttribute('data-src') || img.getAttribute('data-lazy-src') || img.getAttribute('data-original') || img.getAttribute('data-lazy') || ''; if (/^https?:\/\//.test(lz)) src = lz; else if (img.dataset && img.dataset.srcset) { const ds = img.dataset.srcset; const dsBest = ds.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 }); if (dsBest.u) src = dsBest.u; } }
          if (!src || !/^https?:\/\//.test(src)) return;
          const w = img.naturalWidth || parseInt(img.getAttribute('width') || '0');
          const h = img.naturalHeight || parseInt(img.getAttribute('height') || '0');
          if ((w > 0 && w < 200) || (h > 0 && h < 200)) return;
          if (/logo|icon|badge|star|rating|avatar|social|sprite|pixel|tracking|placeholder|blank/i.test(src)) return;
          if (seenImgs.has(src)) return;
          seenImgs.add(src);
          r.images.push(src);
        });
        r.images = r.images.slice(0, 8);
        if (r.price != null) { const raw = parseFloat(String(r.price).replace(',', '.')); if (!isNaN(raw)) r.price = String(Math.round(raw + priceAdjustment)); }
        return r;
      }

      const fullPage = {
        text: buildFullText(),
        productData: extractProduct()
      };

      function extractLiveData(sels) {
        const roots = [];
        if (sels && sels.length) { for (const s of sels) { try { const e = document.querySelector(s); if (e) roots.push(e); } catch (_) {} } }
        if (!roots.length) roots.push(document.body);
        const rawNums = new Set();
        for (const root of roots) {
          const bnpl = /\b(installment|interest.?free|klarna|afterpay|sezzle|quadpay|pay in \d|4x |zip\s*pay|split\s*pay|paidy|laybuy)\b/i;
          const addPrices = (txt) => { if (bnpl.test(txt)) return; for (const m of txt.matchAll(/[\$€£¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/g)) { const n = parseFloat(m[1].replace(",", ".")); if (n >= 1 && n < 100000) rawNums.add(n); } };
          root.querySelectorAll("[class*='price'],[data-price],[class*='Price'],[data-test*='price'],[class*='final-price'],[class*='sale-price'],[class*='current-price'],[class*='product-price'],ins,del,.was-price,.compare-at,.original-price").forEach((el) => addPrices(el.textContent || ""));
          if (!rawNums.size) { root.querySelectorAll("span,b,strong,div,p").forEach((el) => { if (el.children.length > 3) return; const txt = (el.textContent || "").trim(); if (txt.length > 50) return; addPrices(txt); }); }
        }
        const sorted = [...rawNums].sort((a, b) => a - b);
        const r5 = Math.round;
        const mk = priceAdjustment;
        const price = sorted.length ? sorted[0] : null;
        const cmp = sorted.length >= 2 ? sorted[sorted.length - 1] : null;
        const inStock = [], outOfStock = [];
        for (const root of roots) {
          root.querySelectorAll('select[name*="size" i],select[id*="size" i],select[class*="size" i],select[data-option-name*="size" i]').forEach((sel) => {
            Array.from(sel.options).forEach((o) => {
              const t = o.text.trim();
              if (!t || /^(select|choose|--)/i.test(t)) return;
              const list = (o.disabled || /out.?of.?stock|sold.?out|unavailable/i.test(o.text)) ? outOfStock : inStock;
              if (!list.includes(t)) list.push(t);
            });
          });
          root.querySelectorAll('input[type="radio"][name*="size" i],input[type="radio"][data-option-value-id]').forEach((input) => {
            const t = (input.value || "").trim();
            if (!t || /^(select|choose|--)/i.test(t) || t.length > 20) return;
            const list = (input.classList.contains("disabled") || input.disabled) ? outOfStock : inStock;
            if (!list.includes(t)) list.push(t);
          });
          if (!inStock.length && !outOfStock.length) {
            for (const csel of ['[class*="size"] button,[class*="size"] label','[class*="swatch"] button,[class*="swatch"] label','[data-option-name*="size" i] button,[data-option-name*="size" i] label']) {
              const els = root.querySelectorAll(csel);
              if (!els.length || els.length > 60) continue;
              els.forEach((el) => {
                const t = el.textContent?.trim();
                if (!t || t.length > 20) return;
                const cls = ((el.className || "") + " " + (el.getAttribute("aria-label") || "")).toLowerCase();
                const oos = el.classList.contains("disabled") || el.disabled || el.getAttribute("aria-disabled") === "true" || /disabled|out.?of.?stock|sold.?out|unavailable|not-available/i.test(cls) || window.getComputedStyle(el).textDecoration.includes("line-through");
                const list = oos ? outOfStock : inStock;
                if (!list.includes(t)) list.push(t);
              });
              if (inStock.length || outOfStock.length) break;
            }
          }
        }
        { const flBtns = document.querySelectorAll('#tabPanel button[class*="SizeSelector"]'); if (flBtns.length) { flBtns.forEach(btn => { const t = btn.querySelector('span')?.textContent?.trim() || ''; if (!t) return; const oos = btn.classList.contains('SizeSelectorNewDesign-button--disabled') || /sold.?out/i.test(btn.getAttribute('aria-label') || ''); const list = oos ? outOfStock : inStock; if (!list.includes(t)) list.push(t); }); } }
        return { price: price != null ? r5(price + mk) : null, compareAt: cmp != null ? r5(cmp + mk) : null, inStock, outOfStock };
      }
      const liveData = extractLiveData(selectors);

      if (!selectors || selectors.length === 0) {
        return { ok: true, summary: fullPage.text, html: fullPage.text, matched: "full-page", fullPage, liveData };
      }

      const htmlParts = [], summaryParts = [], errors = [];
      for (const selector of selectors) {
        try {
          const element = document.querySelector(selector);
          if (!element) { errors.push("Not found: " + selector); continue; }
          htmlParts.push(element.outerHTML || "");
          summaryParts.push(nt(element.innerText || element.textContent || ""));
        } catch (err) {
          errors.push("Invalid: " + selector + " — " + err.message);
        }
      }
      if (!htmlParts.length) return { ok: false, summary: "", html: "", error: errors.join("; "), fullPage, liveData };
      return { ok: true, summary: summaryParts.join("\n\n"), html: htmlParts.join("\n"), matched: selectors.join(", "), fullPage, liveData };
    },
    args: [selectors, monitor.name, priceAdjustment]
  });

  return result;
}

// Inject a MutationObserver into an already-loaded tab that fires tryCapture()
// every time the DOM changes and stores the result in window.__monitorResult.
// The service worker then polls for that value at 100 ms intervals.
async function injectCaptureObserver(tabId, selectors, captureFullPage, monitorName, priceAdjustment = 80) {
  await chrome.scripting.executeScript({
    target: { tabId },
    func: (selectors, captureFullPage, monitorName, priceAdjustment) => {
      window.__monitorObserverActive = true;
      window.__monitorResult = null;

      const nt = (v) => (v ?? "").replace(/\s+/g, " ").replace(/\u00a0/g, " ").trim();

      function buildFullText() {
        const parts = [];
        const visible = nt(document.body?.innerText || "");
        if (visible) parts.push("=== PAGE TEXT ===\n" + visible);
        const jlds = [];
        document.querySelectorAll('script[type="application/ld+json"]').forEach((s) => {
          try { jlds.push(JSON.stringify(JSON.parse(s.textContent), null, 2)); } catch (_) {}
        });
        if (jlds.length) parts.push("=== STRUCTURED DATA (JSON-LD) ===\n" + jlds.join("\n---\n"));
        const metas = [];
        document.querySelectorAll("meta[name], meta[property]").forEach((m) => {
          const k = m.getAttribute("name") || m.getAttribute("property") || "";
          const v = m.getAttribute("content") || "";
          if (k && v && v.length < 600) metas.push(k + ": " + v);
        });
        if (metas.length) parts.push("=== META TAGS ===\n" + metas.join("\n"));
        return parts.join("\n\n");
      }

      function extractProduct() {
        const r = { name: null, brand: null, type: null, color: null, colorRaw: null, colorFinal: null, gender: null, price: null, currency: null, sku: null, description: null, images: [], sizes: [], outOfStock: [], source: [] };
        const BRANDS = ["Nike","Nike Sportswear","Nike SB","Air Jordan","Jordan","Jordan Brand","Adidas","Adidas Originals","Puma","Reebok","Reebok Classic","New Balance","NewBalance","NB","Converse","Converse All Star","Converse CONS","Vans","Van's","Under Armour","Under Armor","UA","Asics","Asics Tiger","Saucony","Brooks","Brooks Running","Hoka","Hoka One One","ON Cloud","On Running","On Cloud Running","Salomon","Salomon Sportstyle","Timberland","Timberland Pro","UGG","UGG Australia","Dr. Martens","Dr Martens","Doc Martens","Birkenstock","Clarks","Clarks Originals","The North Face","North Face","TNF","Columbia","Columbia Sportswear","Patagonia","Supreme","Off-White","Off White","Offwhite","Balenciaga","Gucci","Louis Vuitton","LV","Yeezy","Adidas Yeezy","Fila","Tommy Hilfiger","Tommy","Ralph Lauren","Polo Ralph Lauren","Polo","Lacoste","Champion","Kappa","Umbro","Ellesse","Diadora","Le Coq Sportif","Lecoqsportif","Mizuno","Karhu","Crocs","Skechers","Skecher","Steve Madden","Ecco","Geox","Camper","Stussy","Stüssy","Palace","Palace Skateboards","Kith","Carhartt","Carhartt WIP","Dickies","Stone Island","Moncler","Arc'teryx","Arcteryx","Merrell","Keen","Teva","Calvin Klein","CK","Hugo Boss","Boss","Boss by Hugo Boss"];
        const TYPE_KEYWORDS = {"basketball":"Basketball","casual":"Lifestyle","lifestyle":"Lifestyle","running":"Running","football":"Football","soccer":"Soccer","training":"Training","hiking":"Hiking","trail":"Trail","tennis":"Tennis","golf":"Golf","skate":"Skate","skateboarding":"Skate","crossfit":"Training","cross-training":"Training","walking":"Walking","sneaker":"Lifestyle","slip-on":"Lifestyle","sandal":"Sandal","boot":"Boot","loafer":"Lifestyle"};
        const COLORS = {"Black":["black","onyx","jet","ebony","obsidian","raven","coal","ink","shadow","noir","licorice","pitch","tripleblack","triple-black","coreblack","core-black","phantom","anthracite","soot","carbon"],"White":["white","ivory","snow","pearl","sail","cream","bone","eggshell","linen","frost","alabaster","porcelain","chalk","milk","cotton","ghost","offwhite","off-white","whisper","paper","shell","antique"],"Red":["red","crimson","scarlet","ruby","burgundy","maroon","wine","cherry","carmine","cardinal","tomato","garnet","vermillion","vermilion","brick","blood","firebrick","cranberry","raspberry","strawberry","rose","claret","mahogany","terra","cotta","sienna","auburn","rubyred","oxblood","merlot","poppy","coralred","sunsetred","chili","rubywine"],"Blue":["blue","navy","cobalt","royal","indigo","denim","sky","powder","midnight","steel","slate","sapphire","azure","thunder","ice","cornflower","periwinkle","iris","ultramarine","prussian","admiral","marine","federal","storm","glacier","arctic","aegean","obsidian-blue","turbo","polar","mistblue","oceanblue","deepblue","lightblue","darkblue","hyperblue","universityblue","carolinablue"],"Green":["green","olive","sage","forest","army","jade","emerald","mint","fern","moss","pine","volt","lime","hunter","bottle","kelly","shamrock","chartreuse","avocado","pistachio","pear","leaf","basil","seaweed","jungle","cucumber","matcha","celadon","viridian","malachite","voltgreen","neongreen","electricgreen","loden","spruce","evergreen","clover","pea","grassy","seaglass","seafoamgreen"],"Yellow":["yellow","gold","golden","mustard","lemon","canary","butter","banana","honey","sunflower","flaxen","straw","blonde","champagne","vanilla","daffodil","citrine","topaz","citrus","maize","corn","ambergold","sandgold","sulphur","mustardseed","dijon","neonyellow","electricyellow"],"Orange":["orange","amber","tangerine","apricot","rust","copper","pumpkin","saffron","coral","burnt","cinnamon","papaya","mango","melon","clay","ginger","tiger","marigold","bronze","peach","persimmon","nectarine","cantaloupe","sunset","burntorange","terracotta","carrot","kumquat"],"Violet":["purple","violet","lavender","lilac","plum","grape","mauve","amethyst","orchid","wisteria","heather","thistle","periwinkle","mulberry","eggplant","byzantium","aubergine","boysenberry","violetdust","deeppurple","royalpurple"],"Pink":["pink","blush","fuchsia","magenta","salmon","rose","bubblegum","flamingo","watermelon","peony","carnation","petal","flush","rouge","blossom","pastel","candy","lollipop","neon","cerise","hot","dusty","millennial","rosepink","powderpink","softpink","brightpink","shockpink"],"Brown":["brown","tan","beige","camel","mocha","chocolate","coffee","sand","taupe","nude","natural","khaki","wheat","stone","walnut","hazel","toffee","espresso","sepia","umber","fawn","oatmeal","biscuit","latte","ecru","buff","driftwood","chestnut","cacao","bark","leather","suede","caramel","pecan","almond","acorn","cocoa","mink","tobacco","saddle","oak","hickory","truffle","earth","mud","dune","bran"],"Gray":["gray","grey","silver","charcoal","ash","smoke","graphite","pewter","cement","concrete","cloud","wolf","pebble","flint","iron","lead","fossil","heather","marengo","dove","cool","smokey","stonegrey","stone-gray","coolgrey","cool-grey","neutralgray","neutralgrey","platinum","gunmetal"],"Turquoise":["turquoise","teal","aqua","cyan","seafoam","aquamarine","caribbean","lagoon","cerulean","peacock","ocean","pool","mintblue","tiffany","robinsegg","bluegreen","turq"],"Multicolor":["multi","multicolor","multi-color","assorted"]};
        const canonicalizeBrand = (value) => {
          const normalized = nt(value);
          if (!normalized) return "";
          const lower = normalized.toLowerCase();
          if (lower === "air jordan" || lower === "jordan") return "Jordan";
          if (lower === "on" || lower === "on running" || lower === "on cloud") return "ON Cloud";
          for (const b of BRANDS) {
            if (lower === b.toLowerCase()) return b;
          }
          return normalized;
        };
        const pickBrand = (t) => {
          if (!t) return null;
          const l = t.toLowerCase();
          if (l.includes("air jordan")) return "Jordan";
          if (l.includes("on running") || l.includes("on cloud")) return "ON Cloud";
          for (const b of BRANDS) {
            if (l.includes(b.toLowerCase())) return canonicalizeBrand(b);
          }
          return null;
        };
        const pickType = (t) => { if (!t) return null; const l = t.toLowerCase(); for (const [kw, mapped] of Object.entries(TYPE_KEYWORDS)) { if (l.includes(kw)) return mapped; } return null; };
        const pickColor = (t) => {
          if (!t) return "Multicolor";
          const normalized = t.toLowerCase().replace(/[^a-z0-9\s-]/g, " ");
          const words = normalized.split(/\s+/).filter(Boolean);
          const compact = normalized.replace(/[^a-z0-9]/g, "");
          for (const [base, synonyms] of Object.entries(COLORS)) {
            if (synonyms.some((c) => {
              const key = String(c).toLowerCase();
              if (words.includes(key)) return true;
              const keyCompact = key.replace(/[^a-z0-9]/g, "");
              return keyCompact.length >= 6 && compact.includes(keyCompact);
            })) return base;
          }
          return "Multicolor";
        };
        const looksLikeSku = (v) => v && /^[A-Z0-9]{3,12}(-[A-Z0-9]{2,6}){0,2}$/i.test(v.trim()) && v.length >= 4 && v.length <= 20;
        document.querySelectorAll('script[type="application/ld+json"]').forEach((script) => {
          try {
            const data = JSON.parse(script.textContent);
            const items = Array.isArray(data) ? data : [data];
            for (const item of items) {
              const t = item["@type"];
              if (t !== "Product" && !(Array.isArray(t) && t.includes("Product"))) continue;
              if (item.name && !r.name) { r.name = String(item.name); r.source.push("json-ld"); }
              if (item.description && !r.description) r.description = String(item.description).slice(0, 3000);
              if (item.sku && !r.sku) r.sku = String(item.sku);
              if (!r.sku && item.mpn) r.sku = String(item.mpn);
              if (item.brand && !r.brand) r.brand = typeof item.brand === "string" ? item.brand : (item.brand?.name ? String(item.brand.name) : null);
              if (!r.color && item.color) r.color = String(item.color);
              const offers = item.offers ? (Array.isArray(item.offers) ? item.offers : [item.offers]) : [];
              if (!r.price && offers[0]?.price != null) { r.price = String(offers[0].price); r.currency = offers[0].priceCurrency || ""; }
              offers.slice(0, 80).forEach((o) => {
                const n = nt(o.name || o.description || "");
                if (!n || r.sizes.includes(n)) return;
                r.sizes.push(n);
                const av = String(o.availability || "").toLowerCase();
                if (av && !av.includes("instock") && !av.includes("onlineonly") && !av.includes("limitedavailability") && !av.includes("preorder")) r.outOfStock.push(n);
              });
            }
          } catch (_) {}
        });
        const mg = (a, v) => document.querySelector(`meta[${a}="${v}"]`)?.getAttribute("content") || null;
        if (!r.name) { const v = mg("property","og:title") || mg("name","twitter:title"); if (v) { r.name = v; r.source.push("og:title"); } }
        if (!r.brand) r.brand = mg("property","og:brand") || null;
        if (!r.price) { const v = mg("property","og:price:amount") || mg("property","product:price:amount"); if (v) { r.price = v; r.currency = mg("property","og:price:currency") || ""; r.source.push("og:price"); } }
        if (!r.sku) r.sku = mg("name","product:sku") || mg("property","product:retailer_item_id") || null;
        if (!r.color) r.color = mg("property","og:color") || mg("name","product:color") || null;
        const ip = (name) => { const e = document.querySelector(`[itemprop="${name}"]`); return e ? (e.getAttribute("content") || e.textContent?.trim() || null) : null; };
        if (!r.name) { const v = ip("name"); if (v) { r.name = v; r.source.push("microdata"); } }
        if (!r.brand) r.brand = ip("brand") || null;
        if (!r.sku) r.sku = ip("sku") || ip("productID") || ip("gtin13") || null;
        if (!r.price) { const v = ip("price"), cur = ip("priceCurrency"); if (v) { r.price = v; r.currency = cur || ""; } }
        if (!r.description) r.description = ip("description") || null;
        if (!r.color) r.color = ip("color") || null;
        if (!r.name) { const h1 = document.querySelector("h1"); if (h1) { r.name = h1.textContent?.trim(); r.source.push("h1"); } }
        if (!r.price) { for (const sel of [".price","[class*='price']","[data-price]","[class*='Price']",".product-price",".sale-price",".current-price"]) { const e = document.querySelector(sel); if (!e) continue; const t = e.textContent?.trim() || ''; if (/\b(installment|interest.?free|klarna|afterpay|sezzle|quadpay|pay in \d|4x |zip\s*pay|split\s*pay|paidy|laybuy)\b/i.test(t)) continue; const pm = t.match(/[\$€£¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/); if (pm) { r.price = pm[1].replace(',', '.'); r.source.push("dom"); break; } } }
        if (!r.brand) { for (const sel of [".product__vendor",".product-vendor","[class*='vendor']",".brand","[class*='brand']","[data-brand]"]) { const e = document.querySelector(sel); if (e) { const t = e.textContent?.trim(); if (t && t.length < 60) { r.brand = t; break; } } } }
        if (!r.sku) { for (const sel of ["[class*='sku']","[data-sku]","[data-product-id]","[data-variant-sku]","[class*='reference']","[class*='product-code']","[id*='sku']"]) { const e = document.querySelector(sel); if (e) { const v = e.getAttribute("data-sku") || e.getAttribute("data-variant-sku") || e.getAttribute("data-product-id") || e.textContent?.trim(); if (looksLikeSku(v)) { r.sku = v.trim(); break; } } } }
        try { const sp = window.meta?.product || window.ShopifyAnalytics?.meta?.product; if (sp) { if (!r.brand && sp.vendor) { r.brand = String(sp.vendor); r.source.push("shopify"); } if (!r.sku && sp.sku) r.sku = String(sp.sku); } } catch (_) {}
        const metaEl = document.querySelector('.product__meta,.product-meta,[class*="product__information"],[class*="product-details"],[class*="product-info"]');
        const metaText = (metaEl || document.body || {}).textContent?.slice(0, 12000) || "";
        if (!r.sku) { const m = metaText.match(/\bSupplier.?sku\s*[#:\s]+([A-Z0-9]{3,15}(?:-[A-Z0-9]{2,8}){0,2})/i); if (m && looksLikeSku(m[1])) r.sku = m[1].trim(); }
        if (!r.sku) { const m = metaText.match(/\b(?:SKU|Style\s*No\.?|Style|Reference\s*No\.?|Reference|Ref\.?|Art\.?\s*No\.?|Item\s*No\.?|Product\s*Code|Style\s*Code|Item\s*Code|Model\s*(?:No\.?|#))[:\s#]+([A-Z0-9]{3,15}(?:-[A-Z0-9]{2,8}){0,2})/i); if (m && looksLikeSku(m[1])) r.sku = m[1].trim(); }
        if (!r.sku) { const m = metaText.match(/\bProduct\s*#\s*[:\s]+([A-Z0-9]{3,15}(?:-[A-Z0-9]{2,8}){0,2})/i); if (m && looksLikeSku(m[1])) r.sku = m[1].trim(); }
        { const bt = document.body?.textContent || ''; const sm = bt.match(/Supplier[\s-]*sku\s*#\s*:?\s*([A-Z0-9]{3,20}(?:-[A-Z0-9]{1,8}){0,3})/i); if (sm) r.sku = sm[1].trim(); }
        { const flPanel = document.querySelector('#ProductDetails-tabs-details-panel'); if (flPanel) { for (const span of flPanel.querySelectorAll(':scope > span')) { const t = (span.textContent || '').trim(); const m = t.match(/Supplier[\s-]*sku\s*#\s*:?\s*([A-Z0-9]{3,20}(?:-[A-Z0-9]{1,8}){0,3})/i); if (m) { r.sku = m[1].trim(); break; } } } }
        if (!r.brand) { const m = metaText.match(/\bBrand[:\s]+([^\n\r,/<]{2,40})/i); if (m) { r.brand = m[1].trim(); r.source.push("labeled"); } }
        if (!r.type) { const m = metaText.match(/\b(?:Type|Category|Sport|Usage|Department)[:\s]+([^\n\r,/<]{2,30})/i); if (m) { r.type = pickType(m[1]) || m[1].trim(); r.source.push("labeled"); } }
        if (!r.color) { const m = metaText.match(/\bColou?r[:\s]+([^\n\r,/<]{2,30})/i); if (m) r.color = m[1].split('/')[0].trim(); }
        let flSelectedColor = null;
        let flSelectedColorNormalized = null;
        {
          const styleCandidates = [
            document.querySelector('.ProductDetails-form__selectedStyle,[class*="selectedStyle"]')?.textContent?.trim() || '',
            document.querySelector('.ColorwayStyles-field.button-field--selected,[class*="button-field--selected"]')?.getAttribute('aria-label')?.trim() || '',
            document.querySelector('.ColorwayStyles-field.button-field--selected img[alt],[class*="button-field--selected"] img[alt]')?.getAttribute('alt')?.trim() || ''
          ];
          for (const candidate of styleCandidates) {
            if (!candidate) continue;
            const cleaned = candidate
              .replace(/^Color\s+/i, '')
              .replace(/\s+is selected.*$/i, '')
              .trim();
            const first = cleaned.split('/')[0].trim();
            if (!first) continue;
            flSelectedColor = first;
            flSelectedColorNormalized = pickColor(first);
            r.colorRaw = first;
            r.color = flSelectedColorNormalized;
            r.source.push("fl-selected-style-color");
            break;
          }
        }
        if (!flSelectedColorNormalized && !r.color) { const prodArea = document.querySelector('main,[role="main"]') || document.body; for (const el of prodArea.querySelectorAll('p,span,div')) { if (el.children.length > 0) continue; const t = (el.textContent || '').trim(); if (!t || t.length > 60) continue; const cm = t.match(/^([A-Z][a-zA-Z]+(?:\/[A-Z][a-zA-Z]+)*)$/); if (cm) { const normalized = pickColor(cm[1].split('/')[0]); if (normalized !== "Multicolor") { r.color = normalized; break; } } } }
        if (!r.gender) {
          const flHeader = document.querySelector('.ProductDetails-header-V2');
          if (flHeader) {
            const genderCandidates = [
              flHeader.querySelector('span.font-caption.my-2'),
              ...flHeader.querySelectorAll('span.font-caption,span[class*="font-caption"]')
            ];
            for (const candidate of genderCandidates) {
              const t = candidate?.textContent?.trim();
              if (t && /\b(men|women|kids|youth|boys|girls|unisex|infant|toddler)/i.test(t)) {
                r.gender = t;
                r.source.push("fl-header-gender");
                break;
              }
            }
          }
        }
        if (!r.gender) { const m = metaText.match(/\b(Men'?s?|Women'?s?|Kids'?|Youth|Boys'?|Girls'?|Unisex|Infant|Toddler)\b/i); if (m) r.gender = m[1]; }
        if (!r.gender) { for (const sel of ['.product-type','[class*="product-type"]','[class*="eyebrow"]','.product__gender','[class*="gender"]']) { try { const e = document.querySelector(sel); if (e) { const t = e.textContent?.trim(); if (t && /\b(men|women|kids|youth|boys|girls|unisex|infant|toddler)/i.test(t)) { r.gender = t; break; } } } catch(_){} } }
        if (r.gender) r.gender = r.gender.replace(/['\u2019]s?\s*$/i, '').trim();
        if (!r.type && r.name) { const t = pickType(r.name); if (t) { r.type = t; r.source.push("name"); } }
        { const flBcLinks = Array.from(document.querySelectorAll('nav[aria-label="Breadcrumb"] li a, li a[href*="pageType=browse"]')); for (const a of [...flBcLinks].reverse()) { const txt = (a.textContent || '').trim(); const t = pickType(txt); if (t) { r.type = t; r.source.push("fl-breadcrumb"); break; } const href = a.getAttribute('href') || ''; const sm = href.match(/style:([^:&]+)/i); if (sm) { const decoded = decodeURIComponent(sm[1].replace(/\+/g,' ')); const t2 = pickType(decoded); if (t2) { r.type = t2; r.source.push("fl-breadcrumb-href"); break; } } } }
        if (!r.type) { const bcEls = document.querySelectorAll('nav[aria-label*="breadcrumb" i] a,nav[aria-label*="breadcrumb" i] span,[class*="breadcrumb"] a,[class*="breadcrumb"] span,[class*="breadcrumb"] li,.breadcrumbs a,.breadcrumbs span,ol[class*="breadcrumb"] li,[data-testid*="breadcrumb"] span,[data-test*="breadcrumb"] span'); const crumbs = Array.from(bcEls).map(el => el.textContent?.trim()).filter(t => t && t.length > 1 && t.length < 60 && !/^[\s\/]+$/.test(t)); for (const c of [...crumbs].reverse().slice(1)) { if (pickType(c)) { r.type = pickType(c); r.source.push("breadcrumb"); break; } } }
        if (!r.type) { const bt = document.body?.innerText || ''; const bm = bt.match(/(?:Men'?s?|Women'?s?|Kids'?|Boys'?|Girls'?|Unisex)\s*\/\s*(?:Shoes?|Sneakers?|Footwear|Clothing|Apparel)\s*\/\s*([^\n\/]{3,60})/i); if (bm) { const mapped = pickType(bm[1].trim()); if (mapped) { r.type = mapped; r.source.push("breadcrumb-text"); } } }
        if (!r.description) {
          const flPanel = document.querySelector('#ProductDetails-tabs-details-panel,[aria-labelledby*="ProductDetails-tabs-details-tab"]');
          if (flPanel) {
            const contentDiv = flPanel.querySelector(':scope > div');
            const src = contentDiv || flPanel;
            const text = (src.textContent || '').replace(/\s+/g, ' ').trim();
            if (text.length > 20) r.description = text.slice(0, 3000);
          }
        }
        if (!r.description) {
          const pt = document.body?.innerText || '';
          const dm = pt.match(/\bDETAILS\b\s*\n+([\s\S]+?)\n+\s*(?:REVIEWS?|RATINGS?)\b/i);
          if (dm) {
            const metaLineRe = /^(?:Product\s*(?:#|No\.?|Code)|Supplier[\s\-]*sku|SKU|Style\s*(?:No\.?|#|Code)|Reference(?:\s*No\.?)?|Item\s*(?:#|No\.?|Code)|Colou?r|Width|Fit|Material)\b[^:\n]*:[^\n]*/i;
            const rawLines = dm[1].trim().split('\n');
            let start = 0;
            while (start < rawLines.length && metaLineRe.test(rawLines[start].trim())) start++;
            const desc = rawLines.slice(start).join('\n').trim();
            if (desc.length > 20) r.description = desc.slice(0, 3000);
          }
        }
        if (!r.description) {
          // Accordion heading "Details" → next sibling content
          const allHds = Array.from(document.querySelectorAll('h2,h3,h4,button,summary'));
          for (const hd of allHds) {
            if (!/^\s*details?\s*$/i.test(hd.textContent?.trim())) continue;
            const content = hd.nextElementSibling || hd.parentElement?.nextElementSibling;
            if (content) { const d = (content.innerText || content.textContent || '').trim(); if (d.length > 20 && d.length < 6000) { r.description = d.slice(0, 3000); break; } }
          }
        }
        if (!r.description) {
          const descSels = ['.product-description','.product__description','.product-single__description','[data-product-description]','.rte','.product-info__description','[class*="product-description"]','[class*="product-body"]','[class*="product-text"]','[class*="description"]'];
          for (const sel of descSels) { try { const e = document.querySelector(sel); if (e) { const d = (e.innerText || e.textContent || "").trim(); if (d.length > 80) { r.description = d.slice(0, 3000); break; } } } catch (_) {} }
        }
        if (!r.description) {
          const els = document.querySelectorAll('div,section,article');
          for (const el of els) {
            if (el.querySelectorAll('div,section').length > 20) continue;
            const text = (el.innerText || el.textContent || "").trim();
            if (text.length > 80 && text.length < 8000 && /\b(Highlights?|Composition|Features?|Caratteristiche|Descripci)/i.test(text.slice(0, 120))) { r.description = text.slice(0, 3000); break; }
          }
        }
        if (r.description) { const lines = r.description.split(/\n/); const clean = []; for (const ln of lines) { const cm = !r.color && ln.match(/^Colou?r[:\s]+(.+)/i); const ss = ln.match(/\bSupplier[\s-]*sku\s*#?[:\s]+([A-Z0-9]{3,20}(?:-[A-Z0-9]{1,8}){0,3})/i); const sm = ln.match(/^(?:Product\s*(?:Code|#|No\.?)|SKU|Style\s*(?:No\.?|Code|#)|Reference(?:\s*No\.?)?|Ref\.?|Art\.?\s*No\.?|Item\s*(?:No\.?|Code|#))\s*[:#\s]+([A-Z0-9]{3,20}(?:-[A-Z0-9]{1,8}){0,3})/i); if (cm) { r.color = cm[1].split('/')[0].trim(); } else if (ss && looksLikeSku(ss[1])) { r.sku = ss[1].trim(); } else if (sm && looksLikeSku(sm[1])) { if (!r.sku) r.sku = sm[1].trim(); } else { clean.push(ln); } } r.description = clean.join("\n").trim(); }
        if (r.name) {
          const sfx = r.name.match(/\s*[-\u2013]\s*(Men'?s?|Women'?s?|Kids'?|Youth|Boys'?|Girls'?|Unisex|Infant|Toddler)\b.*/i);
          if (sfx) { if (!r.gender) r.gender = sfx[1].replace(/['\u2019]s?\s*$/i, '').trim(); r.name = r.name.slice(0, r.name.length - sfx[0].length).trim(); }
        }
        if (r.name && /no longer available|product.*not found|page.*not found|item.*not found/i.test(r.name)) r.name = null;
        if (monitorName) {
          const gm = monitorName.match(/^(.+?)\s*[-\u2013]\s*(Men'?s?|Women'?s?|Kids'?|Youth|Boys'?|Girls'?|Unisex|Infant|Toddler)\b/i);
          if (gm) { if (!r.name) r.name = gm[1].trim(); if (!r.gender) r.gender = gm[2].replace(/['\u2019]s?\s*$/i, '').trim(); }
          else if (!r.name) { const pm = monitorName.match(/^(.+?)\s*\|/); if (pm) r.name = pm[1].replace(/\s*[-\u2013]\s*$/, '').trim(); }
        }
        const nameBrand = r.name ? pickBrand(r.name) : null;
        const searchText = [r.name, r.description, document.title].filter(Boolean).join(" ");
        if (!r.brand) r.brand = nameBrand || pickBrand(searchText) || pickBrand(metaText);
        if (r.brand) r.brand = canonicalizeBrand(r.brand);
        if (!r.type) r.type = pickType(searchText) || pickType(metaText);
        r.color = flSelectedColorNormalized || pickColor(r.color || searchText);
        r.colorFinal = r.color;
        if (!r.sizes.length) { const sel = document.querySelector('select[name*="size" i],select[id*="size" i],select[class*="size" i],select[data-option-name*="size" i]'); if (sel) { Array.from(sel.options).forEach((o) => { const t = o.text.trim(); if (!t || /^(select|choose|--)/i.test(t)) return; r.sizes.push(t); if (o.disabled || /out.?of.?stock|sold.?out|unavailable/i.test(o.text)) r.outOfStock.push(t); }); } }
        if (!r.sizes.length) { document.querySelectorAll('input[type="radio"][name*="size" i],input[type="radio"][data-option-value-id]').forEach((input) => { const t = (input.value || "").trim(); if (!t || /^(select|choose|--)/i.test(t) || t.length > 20) return; if (!r.sizes.includes(t)) r.sizes.push(t); if (input.classList.contains("disabled") || input.disabled) r.outOfStock.push(t); }); }
        if (!r.sizes.length) { for (const csel of ['[class*="size"] button,[class*="size"] label','[class*="swatch"] button,[class*="swatch"] label','[data-option-name*="size" i] button,[data-option-name*="size" i] label','[class*="size-selector"] button','[class*="sizebtn"]','[class*="size-btn"]']) { const els = document.querySelectorAll(csel); if (els.length > 0 && els.length < 80) { els.forEach((el) => { const t = el.textContent?.trim(); if (!t || t.length > 20 || r.sizes.includes(t)) return; r.sizes.push(t); const cls = ((el.className || "") + " " + (el.getAttribute("aria-label") || "")).toLowerCase(); const oos = el.classList.contains("disabled") || el.disabled || el.getAttribute("aria-disabled") === "true" || /disabled|out.?of.?stock|sold.?out|unavailable|not-available/i.test(cls) || window.getComputedStyle(el).textDecoration.includes("line-through"); if (oos) r.outOfStock.push(t); }); if (r.sizes.length) break; } } }
        if (!r.sizes.length) { const pa = document.querySelector('main,[role="main"]') || document.body; pa.querySelectorAll('button').forEach((el) => { const t = (el.textContent || "").trim(); if (!t || t.length > 10 || r.sizes.includes(t)) return; if (!/^\d+(?:[.,]\d+)?(?:\s*(?:US|EU|UK|CM|EUR|½))?$/i.test(t)) return; r.sizes.push(t); const oos = el.classList.contains("disabled") || el.disabled || el.getAttribute("aria-disabled") === "true" || window.getComputedStyle(el).textDecoration.includes("line-through"); if (oos) r.outOfStock.push(t); }); }
        const swatchEls = new Set();
        document.querySelectorAll('[class*="ColorChip"],[class*="color-chip"],[class*="colorway"],[class*="ColorSwatch"],[class*="color-swatch"],[class*="StyleChip"],[class*="style-chip"],[class*="VariantImage"],[class*="variant-color"],[class*="SwatchImage"],[class*="color-option"],[class*="product-variants"],[class*="ProductVariants"]').forEach(el => swatchEls.add(el));
        const isInSwatch = (img) => { let el = img.parentElement; while (el && el !== document.body) { if (swatchEls.has(el)) return true; const cls = el.className || ''; if (/color.?chip|color.?swatch|colorway|style.?chip|swatch.?img|variant.?img|color.?option/i.test(cls)) return true; el = el.parentElement; } return false; };
        const imgRoot = document.querySelector('.ProductGallery,[class*="ProductGallery"],[class*="product-gallery"],[class*="pdp-gallery"],[class*="Gallery--pdp"],[class*="GallerySlider"],[class*="gallery-slider"],.product-media,.pdp-product-images') || document.querySelector('main,[role="main"]') || document.body;
        const seenImgs = new Set();
        imgRoot.querySelectorAll('img').forEach((img) => {
          if (isInSwatch(img)) return;
          let src = '';
          if (img.srcset) { const best = img.srcset.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 }); if (best.u) src = best.u; }
          if (!src) src = img.currentSrc || img.src || '';
          if (!src || !/^https?:\/\//.test(src)) { const lz = img.getAttribute('data-src') || img.getAttribute('data-lazy-src') || img.getAttribute('data-original') || img.getAttribute('data-lazy') || ''; if (/^https?:\/\//.test(lz)) src = lz; else if (img.dataset && img.dataset.srcset) { const ds = img.dataset.srcset; const dsBest = ds.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 }); if (dsBest.u) src = dsBest.u; } }
          if (!src || !/^https?:\/\//.test(src)) return;
          const w = img.naturalWidth || parseInt(img.getAttribute('width') || '0');
          const h = img.naturalHeight || parseInt(img.getAttribute('height') || '0');
          if ((w > 0 && w < 200) || (h > 0 && h < 200)) return;
          if (/logo|icon|badge|star|rating|avatar|social|sprite|pixel|tracking|placeholder|blank/i.test(src)) return;
          if (seenImgs.has(src)) return;
          seenImgs.add(src);
          r.images.push(src);
        });
        r.images = r.images.slice(0, 8);
        if (r.price != null) { const raw = parseFloat(String(r.price).replace(',', '.')); if (!isNaN(raw)) r.price = String(Math.round(raw + priceAdjustment)); }
        return r;
      }

      function extractLiveData(sels) {
        const roots = [];
        if (sels && sels.length) { for (const s of sels) { try { const e = document.querySelector(s); if (e) roots.push(e); } catch (_) {} } }
        if (!roots.length) roots.push(document.body);
        const rawNums = new Set();
        for (const root of roots) {
          const bnpl = /\b(installment|interest.?free|klarna|afterpay|sezzle|quadpay|pay in \d|4x |zip\s*pay|split\s*pay|paidy|laybuy)\b/i;
          const addPrices = (txt) => { if (bnpl.test(txt)) return; for (const m of txt.matchAll(/[\$€£¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/g)) { const n = parseFloat(m[1].replace(",", ".")); if (n >= 1 && n < 100000) rawNums.add(n); } };
          root.querySelectorAll("[class*='price'],[data-price],[class*='Price'],[data-test*='price'],[class*='final-price'],[class*='sale-price'],[class*='current-price'],[class*='product-price'],ins,del,.was-price,.compare-at,.original-price").forEach((el) => addPrices(el.textContent || ""));
          if (!rawNums.size) { root.querySelectorAll("span,b,strong,div,p").forEach((el) => { if (el.children.length > 3) return; const txt = (el.textContent || "").trim(); if (txt.length > 50) return; addPrices(txt); }); }
        }
        const sorted = [...rawNums].sort((a, b) => a - b);
        const r5 = Math.round;
        const mk = priceAdjustment;
        const price = sorted.length ? sorted[0] : null;
        const cmp = sorted.length >= 2 ? sorted[sorted.length - 1] : null;
        const inStock = [], outOfStock = [];
        for (const root of roots) {
          root.querySelectorAll('select[name*="size" i],select[id*="size" i],select[class*="size" i],select[data-option-name*="size" i]').forEach((sel) => {
            Array.from(sel.options).forEach((o) => {
              const t = o.text.trim();
              if (!t || /^(select|choose|--)/i.test(t)) return;
              const list = (o.disabled || /out.?of.?stock|sold.?out|unavailable/i.test(o.text)) ? outOfStock : inStock;
              if (!list.includes(t)) list.push(t);
            });
          });
          root.querySelectorAll('input[type="radio"][name*="size" i],input[type="radio"][data-option-value-id]').forEach((input) => {
            const t = (input.value || "").trim();
            if (!t || /^(select|choose|--)/i.test(t) || t.length > 20) return;
            const list = (input.classList.contains("disabled") || input.disabled) ? outOfStock : inStock;
            if (!list.includes(t)) list.push(t);
          });
          if (!inStock.length && !outOfStock.length) {
            for (const csel of ['[class*="size"] button,[class*="size"] label','[class*="swatch"] button,[class*="swatch"] label','[data-option-name*="size" i] button,[data-option-name*="size" i] label']) {
              const els = root.querySelectorAll(csel);
              if (!els.length || els.length > 60) continue;
              els.forEach((el) => {
                const t = el.textContent?.trim();
                if (!t || t.length > 20) return;
                const cls = ((el.className || "") + " " + (el.getAttribute("aria-label") || "")).toLowerCase();
                const oos = el.classList.contains("disabled") || el.disabled || el.getAttribute("aria-disabled") === "true" || /disabled|out.?of.?stock|sold.?out|unavailable|not-available/i.test(cls) || window.getComputedStyle(el).textDecoration.includes("line-through");
                const list = oos ? outOfStock : inStock;
                if (!list.includes(t)) list.push(t);
              });
              if (inStock.length || outOfStock.length) break;
            }
          }
        }
        { const flBtns = document.querySelectorAll('#tabPanel button[class*="SizeSelector"]'); if (flBtns.length) { flBtns.forEach(btn => { const t = btn.querySelector('span')?.textContent?.trim() || ''; if (!t) return; const oos = btn.classList.contains('SizeSelectorNewDesign-button--disabled') || /sold.?out/i.test(btn.getAttribute('aria-label') || ''); const list = oos ? outOfStock : inStock; if (!list.includes(t)) list.push(t); }); } }
        return { price: price != null ? r5(price + mk) : null, compareAt: cmp != null ? r5(cmp + mk) : null, inStock, outOfStock };
      }

      let captureObserver = null;
      let heartbeat = null;

      function finish(result) {
        window.__monitorResult = result;
        if (captureObserver) { captureObserver.disconnect(); captureObserver = null; }
        if (heartbeat) { clearInterval(heartbeat); heartbeat = null; }
        window.__monitorCaptureObserver = null;
        window.__monitorHeartbeat = null;
      }

      function attemptCapture() {
        if (window.__monitorResult) return true;

        // 1. All required selectors must be present
        if (selectors && selectors.length) {
          const anyMissing = selectors.some((s) => { try { return !document.querySelector(s); } catch (_) { return true; } });
          if (anyMissing) return false;
        }

        // 2. For FL product pages: wait for gallery images — only needed on first run
        //    (normal checks only need price + sizes, no image wait required).
        const flImgs = captureFullPage ? Array.from(document.querySelectorAll('img[data-id="ProductImage"]')) : [];
        if (captureFullPage) {
          const isFL = !!document.querySelector('[class*="ProductGallery"],[class*="ProductDetails-form"],[class*="ProductDetails-tab"]');
          if (flImgs.length > 0) {
            const ready = flImgs.some(img => /^https?:\/\//.test(img.getAttribute('src') || ''));
            if (!ready) return false; // images in DOM but src not set yet — keep waiting
          } else {
            // No FL images yet — if page has FL product indicators, keep waiting
            if (isFL) return false;
            // Non-FL page: fall through and capture whatever is there
          }
          if (isFL) {
            const flSelectedStyleReady =
              !!document.querySelector('.ProductDetails-form__selectedStyle,[class*="selectedStyle"]') ||
              !!document.querySelector('.ColorwayStyles-field.button-field--selected,[class*="button-field--selected"]') ||
              !!document.querySelector('.ColorwayStyles-field.button-field--selected img[alt],[class*="button-field--selected"] img[alt]');
            if (!flSelectedStyleReady) return false;
          }
        }

        // 3. Extract data
        const fullPage = captureFullPage ? { text: buildFullText(), productData: extractProduct() } : null;
        const liveData = extractLiveData(selectors);

        // 4. If FL images were in the DOM but extractProduct got none, keep waiting
        if (captureFullPage && flImgs.length > 0 && !fullPage?.productData?.images?.length) return false;

        if (!selectors || !selectors.length) {
          finish({ ok: true, summary: fullPage ? fullPage.text : "", html: fullPage ? fullPage.text : "", matched: "full-page", fullPage, liveData });
          return true;
        }

        const htmlParts = [], summaryParts = [];
        for (const selector of selectors) {
          try {
            const element = document.querySelector(selector);
            if (!element) return false;
            htmlParts.push(element.outerHTML || "");
            summaryParts.push(nt(element.innerText || element.textContent || ""));
          } catch (_) { return false; }
        }
        finish({ ok: true, summary: summaryParts.join("\n\n"), html: htmlParts.join("\n"), matched: selectors.join(", "), fullPage, liveData });
        return true;
      }

      attemptCapture();

      captureObserver = new MutationObserver(() => { if (!window.__monitorResult) attemptCapture(); });
      captureObserver.observe(document.documentElement, { childList: true, subtree: true });
      window.__monitorCaptureObserver = captureObserver;

      // Heartbeat every 100 ms — needed for the case where FL images are already
      // in the DOM when we inject (no more DOM mutations will fire).
      heartbeat = setInterval(() => {
        if (window.__monitorResult) { clearInterval(heartbeat); heartbeat = null; return; }
        attemptCapture();
      }, 100);
      window.__monitorHeartbeat = heartbeat;
    },
    args: [selectors, captureFullPage, monitorName, priceAdjustment]
  });
}

async function tryInjectCaptureObserver(tabId, selectors, captureFullPage, monitorName, priceAdjustment = 80, attempts = 20, delayMs = 75) {
  for (let i = 0; i < attempts; i += 1) {
    if (!captureTabIds.has(tabId)) return false;
    try {
      await injectCaptureObserver(tabId, selectors, captureFullPage, monitorName, priceAdjustment);
      return true;
    } catch (_) {
      if (i < attempts - 1) await sleep(delayMs);
    }
  }
  return false;
}

// Returns: result object on success, null if tab closed by user, false if timed out
async function pollForResult(tabId, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!captureTabIds.has(tabId)) return null;
    await sleep(25);
    if (!captureTabIds.has(tabId)) return null;
    try {
      const [{ result }] = await chrome.scripting.executeScript({
        target: { tabId },
        func: () => window.__monitorResult || null
      });
      if (result) return result;
    } catch (_) {
      if (!captureTabIds.has(tabId)) return null;
    }
  }
  return captureTabIds.has(tabId) ? false : null;
}

async function captureSnapshotInHiddenTab(monitor, captureFullPage = false, _retryCount = 0, isBatch = false) {
  await acquireSlot(isBatch);
  if (stopChecksFlag) { releaseSlot(isBatch); return { ok: false, stopped: true }; }

  const selectors = Array.isArray(monitor.selectors)
    ? monitor.selectors
    : (monitor.selector ? [monitor.selector] : []);

  const tab = await chrome.tabs.create({ url: monitor.url, active: false });
  captureTabIds.add(tab.id);

  try {
    const fastInjected = await tryInjectCaptureObserver(
      tab.id,
      selectors,
      captureFullPage,
      monitor.name,
      Number(monitor.priceAdjustment) || 80
    );
    if (fastInjected) {
      const earlyResult = await pollForResult(tab.id, 5000);
      if (earlyResult && earlyResult.ok) {
        captureTabIds.delete(tab.id);
        await chrome.tabs.remove(tab.id).catch(() => {});
        return earlyResult;
      }
    }

    // Step 1: wait for initial load
    await waitForTabLoad(tab.id);
    if (!captureTabIds.has(tab.id)) {
      if (_retryCount < 2) return captureSnapshotInHiddenTab(monitor, captureFullPage, _retryCount + 1, isBatch);
      return { ok: false, error: "Tab closed unexpectedly" };
    }

    // Step 2: do not burn time reloading on the initial first-run capture.
    // If we need a retry, a reload can still help recover.
    if (captureFullPage && _retryCount > 0) {
      await chrome.tabs.reload(tab.id);
      await waitForTabLoad(tab.id);
      if (!captureTabIds.has(tab.id)) {
        if (_retryCount < 2) return captureSnapshotInHiddenTab(monitor, captureFullPage, _retryCount + 1, isBatch);
        return { ok: false, error: "Tab closed unexpectedly after reload" };
      }
    }

    // Step 3: inject observer once and poll up to 60 s
    await injectCaptureObserver(tab.id, selectors, captureFullPage, monitor.name, Number(monitor.priceAdjustment) || 80);
    const result = await pollForResult(tab.id, 60000);

    if (result === null) {
      if (_retryCount < 2) return captureSnapshotInHiddenTab(monitor, captureFullPage, _retryCount + 1, isBatch);
      return { ok: false, error: "Tab closed unexpectedly" };
    }
    if (result === false) return { ok: false, error: "Content not detected within 60 seconds" };

    captureTabIds.delete(tab.id);
    await chrome.tabs.remove(tab.id).catch(() => {});
    return result;

  } catch (error) {
    if (captureTabIds.has(tab.id)) {
      captureTabIds.delete(tab.id);
      await chrome.tabs.remove(tab.id).catch(() => {});
    }
    if (_retryCount < 2) return captureSnapshotInHiddenTab(monitor, captureFullPage, _retryCount + 1, isBatch);
    return { ok: false, error: error.message || "Capture failed" };
  } finally {
    releaseSlot(isBatch);
  }
}

async function runMonitor(monitorId, reason = "scheduled", currentTabId = null, isBatch = false) {
  const monitors = await getMonitors();
  const index = monitors.findIndex((item) => item.id === monitorId);
  if (index === -1) {
    return;
  }

  const monitor = monitors[index];
  const next = { ...monitor };
  const wasError = monitor.status === "error";
  let desiredShopifyStatus = null;

  try {
    let monitorHostname = "";
    try {
      monitorHostname = new URL(monitor.url || "").hostname || "";
    } catch (_) {}
    const isFootlockerMonitor = /(^|\.)footlocker\.com\b/i.test(monitorHostname);
    const needsFootlockerColorRefresh =
      isFootlockerMonitor && (
        !monitor.productData ||
        !monitor.productData.color ||
        !Array.isArray(monitor.productData.source) ||
        !monitor.productData.source.includes("fl-selected-style-color")
      );

    const needsFullCapture =
      !monitor.lastHtmlSnapshot ||
      !monitor.productData ||
      !monitor.productData.name ||
      !monitor.productData.sku ||
      !Array.isArray(monitor.productData.images) ||
      !monitor.productData.images.length ||
      needsFootlockerColorRefresh;

    // On first run, capture from the tab the user already has open (faster,
    // already logged-in, no extra network request). Fall back to hidden tab if
    // the tab is gone or not scriptable.
    const preferHiddenTabCapture = needsFootlockerColorRefresh;
    const snapshot = needsFullCapture && currentTabId && !preferHiddenTabCapture
      ? await captureSnapshotFromCurrentTab(currentTabId, monitor).catch(() =>
          captureSnapshotInHiddenTab(monitor, true, 0, isBatch)
        )
      : await captureSnapshotInHiddenTab(monitor, needsFullCapture, 0, isBatch);

    if (snapshot.stopped) return;

    next.lastCheckedAt = new Date().toISOString();
    if (snapshot.liveData) {
      next.lastExtractedData = snapshot.liveData;
      next.lastExtractedAt = next.lastCheckedAt;
    }

    if (!snapshot.ok) {
      next.status = "error";
      next.lastError = snapshot.error;
      if (!wasError) desiredShopifyStatus = "draft";
    } else {
      next.status = "ok";
      next.lastError = "";

      if (needsFullCapture) {
        next.lastSnapshot = snapshot.summary;
        next.lastHtmlSnapshot = snapshot.html;
        next.lastChangedAt = "";
        next.previousSnapshot = monitor.previousSnapshot || "";
        next.previousHtmlSnapshot = monitor.previousHtmlSnapshot || "";
        next.lastHtmlDiff = monitor.lastHtmlDiff || "";
        next.changeHistory = monitor.changeHistory || [];

        if (snapshot.fullPage) {
          next.initialFullPageText = snapshot.fullPage.text;
          next.initialCapturedAt = new Date().toISOString();
          next.productData = snapshot.fullPage.productData || null;
          if (next.productData) {
            next.productData.brand = canonicalizeBrand(next.productData.brand);
            const extractedGender = next.productData.gender || null;
            next.productData.gender = extractedGender || "Not defined";
            next.productData.extractedGender = extractedGender;
            next.productData.genderDisplay = next.productData.genderDisplay || next.productData.gender;
          }
          if (monitor.productDataOverrides && next.productData) {
            next.productData = { ...next.productData, ...monitor.productDataOverrides };
          } else if (monitor.productDataOverrides) {
            next.productData = { ...monitor.productDataOverrides };
          }
          if (next.productData) {
            next.productData.brand = canonicalizeBrand(next.productData.brand);
            next.productData.gender = next.productData.extractedGender || "Not defined";
            next.productData.genderDisplay = next.productData.genderDisplay || next.productData.gender;
          }
        }

      } else {
        const htmlChanged = snapshot.html !== monitor.lastHtmlSnapshot;
        const prevLive = monitor.lastExtractedData;
        const newLive = snapshot.liveData;
        const liveChanges = [];
        if (prevLive && newLive) {
          if (prevLive.price !== newLive.price) liveChanges.push(`Price: $${prevLive.price ?? "?"} → $${newLive.price ?? "?"}`);
          const prevOos = [...(prevLive.outOfStock || [])].sort().join(",");
          const newOos = [...(newLive.outOfStock || [])].sort().join(",");
          const prevIn = [...(prevLive.inStock || [])].sort().join(",");
          const newIn = [...(newLive.inStock || [])].sort().join(",");
          if (prevOos !== newOos || prevIn !== newIn) liveChanges.push("Sizes changed");
        }
        if (htmlChanged || liveChanges.length) {
          const changedAt = new Date().toISOString();
          const previousHtml = monitor.lastHtmlSnapshot || "";
          const currentHtml = snapshot.html;
          const previousText = monitor.lastSnapshot || "";
          const currentText = snapshot.summary;
          const htmlDiff = buildHtmlDiff(previousHtml, currentHtml);

          if (liveChanges.length && newLive) {
            const pd = monitor.productData || {};
            const brand = pd.brand || "";
            const sku = pd.sku || "";
            addLog({
              type: "change",
              title: [brand, sku].filter(Boolean).join(" ") || monitor.name,
              productName: pd.name || monitor.name || "",
              brand, sku,
              details: liveChanges,
              monitorId: monitor.id,
              url: monitor.url
            }).catch(() => {});
          }

          if (liveChanges.length && prevLive) next.previousExtractedData = prevLive;
          next.previousSnapshot = monitor.lastSnapshot;
          next.previousHtmlSnapshot = previousHtml;
          next.lastSnapshot = currentText;
          next.lastHtmlSnapshot = currentHtml;
          next.lastHtmlDiff = htmlDiff;
          next.lastChangedAt = changedAt;
          next.changeCount = (monitor.changeCount || 0) + 1;
          next.changeHistory = [
            {
              id: uid("change"),
              changedAt,
              reason,
              previousText,
              currentText,
              previousHtml,
              currentHtml,
              htmlDiff,
              liveChanges
            },
            ...(monitor.changeHistory || [])
          ].slice(0, MAX_HISTORY_ENTRIES);

          const notifMsg = liveChanges.length
            ? liveChanges.join(" · ")
            : (monitor.selector ? "The watched element changed." : "The monitored page content changed.");

          await chrome.notifications.create(`change:${monitor.id}:${Date.now()}`, {
            type: "basic",
            iconUrl: chrome.runtime.getURL("icon.svg"),
            title: `Change detected: ${monitor.name}`,
            message: notifMsg
          });
        }

      }

      if (snapshot.liveData) {
        chrome.storage.local.get("shopifyTestMode").then(({ shopifyTestMode }) => {
          if (!shopifyTestMode) updateShopifyForMonitor(next, snapshot.liveData).catch(() => {});
        });
      } else if (wasError) {
        desiredShopifyStatus = "active";
      }
    }
  } catch (error) {
    next.lastCheckedAt = new Date().toISOString();
    next.status = "error";
    next.lastError = error.message;
    if (!wasError) desiredShopifyStatus = "draft";
  }

  next.lastRunReason = reason;

  // Re-read inside a lock so concurrent runMonitor calls don't overwrite each other.
  await withStorageLock(async () => {
    const fresh = await getMonitors();
    const freshIdx = fresh.findIndex((m) => m.id === monitorId);
    if (freshIdx !== -1) {
      fresh[freshIdx] = next;
      await saveMonitors(fresh);
    }
  });

  if (desiredShopifyStatus) {
    chrome.storage.local.get("shopifyTestMode").then(({ shopifyTestMode }) => {
      if (!shopifyTestMode) syncMonitorShopifyStatus(next, desiredShopifyStatus).catch(() => {});
    });
  }
}

async function createMonitor(payload, currentTabId = null) {
  const monitor = {
    id: uid(),
    name: payload.name || "Untitled monitor",
    url: payload.url,
    selectors: Array.isArray(payload.selectors)
      ? payload.selectors
      : (payload.selector ? [payload.selector] : []),
    autoCheck: payload.autoCheck ?? false,
    intervalMinutes: Math.max(1, Number(payload.intervalMinutes) || DEFAULT_INTERVAL_MINUTES),
    createdAt: new Date().toISOString(),
    lastCheckedAt: "",
    lastChangedAt: "",
    lastSnapshot: "",
    previousSnapshot: "",
    lastHtmlSnapshot: "",
    previousHtmlSnapshot: "",
    lastHtmlDiff: "",
    changeHistory: [],
    lastError: "",
    status: "idle",
    changeCount: 0,
    initialFullPageText: "",
    initialCapturedAt: "",
    productData: null,
    lastExtractedData: null,
    previousExtractedData: null,
    lastExtractedAt: "",
    productDataOverrides: payload.productDataOverrides || null,
    priceAdjustment: Number(payload.priceAdjustment) || 80
  };

  const monitors = await getMonitors();
  monitors.unshift(monitor);
  await saveMonitors(monitors);
  await ensureAlarm(monitor);
  pushUndo(`Create: ${monitor.name}`, async () => {
    await deleteMonitor(monitor.id);
  });
  await runMonitor(monitor.id, "created", currentTabId);
  return monitor;
}

async function createMonitorsBatch(items, selectors, senderTabId, sharedOverrides = null, priceAdjustment = 80) {
  const created = [];
  await withStorageLock(async () => {
    const monitors = await getMonitors();
    for (const item of items) {
      const monitor = {
        id: uid(), name: item.name || "Untitled", url: item.url,
        selectors: selectors || [], autoCheck: false,
        intervalMinutes: DEFAULT_INTERVAL_MINUTES,
        createdAt: new Date().toISOString(), lastCheckedAt: "", lastChangedAt: "",
        lastSnapshot: "", previousSnapshot: "", lastHtmlSnapshot: "",
        previousHtmlSnapshot: "", lastHtmlDiff: "", changeHistory: [],
        lastError: "", status: "idle", changeCount: 0,
        initialFullPageText: "", initialCapturedAt: "",
        productData: null, lastExtractedData: null, previousExtractedData: null,
        lastExtractedAt: "", productDataOverrides: sharedOverrides || null,
        priceAdjustment: Number(priceAdjustment) || 80
      };
      monitors.unshift(monitor);
      created.push(monitor);
      pushUndo(`Create: ${monitor.name}`, async () => { await deleteMonitor(monitor.id); });
    }
    await saveMonitors(monitors);
  });
  for (const monitor of created) ensureAlarm(monitor);
  const total = created.length;
  let done = 0, errors = 0;
  const sendProgress = () => {
    if (!senderTabId) return;
    chrome.tabs.sendMessage(senderTabId, { type: "batch-progress", done, total, errors }).catch(() => {});
  };
  created.forEach(async (monitor) => {
    await runMonitor(monitor.id, "created", null, true);
    done++;
    const fresh = await getMonitors();
    if (fresh.find(m => m.id === monitor.id)?.status === "error") errors++;
    sendProgress();
  });
  return created;
}

async function updateMonitor(payload) {
  const monitors = await getMonitors();
  const index = monitors.findIndex((item) => item.id === payload.id);
  if (index === -1) {
    throw new Error("Monitor not found");
  }

  const previous = { ...monitors[index] };
  const updated = {
    ...previous,
    ...payload,
    intervalMinutes: Math.max(1, Number(payload.intervalMinutes) || previous.intervalMinutes || DEFAULT_INTERVAL_MINUTES)
  };

  monitors[index] = updated;
  await saveMonitors(monitors);
  await ensureAlarm(updated);
  pushUndo(`Update: ${updated.name}`, async () => {
    const mons = await getMonitors();
    const idx = mons.findIndex((m) => m.id === previous.id);
    if (idx !== -1) { mons[idx] = previous; await saveMonitors(mons); await ensureAlarm(previous); }
  });
  return updated;
}

async function deleteMonitor(monitorId) {
  await deleteMonitors([monitorId]);
}

async function deleteMonitors(monitorIds) {
  const idSet = new Set(monitorIds);
  const deletedItems = [];
  await withStorageLock(async () => {
    const monitors = await getMonitors();
    const filtered = monitors.filter((m) => {
      if (idSet.has(m.id)) { deletedItems.push(m); return false; }
      return true;
    });
    await saveMonitors(filtered);
  });
  for (const m of deletedItems) {
    clearAlarm(m.id);
    const snapshot = { ...m };
    pushUndo(`Delete: ${m.name}`, async () => {
      const mons = await getMonitors();
      mons.unshift(snapshot);
      await saveMonitors(mons);
      await ensureAlarm(snapshot);
    });
  }
}

async function normalizeMonitorBrandsAndShopify() {
  const monitors = await getMonitors();
  await syncMonitorBrandsToShopify(monitors).catch(() => {});
  await normalizeAllShopifyVendors().catch(() => {});
}

chrome.tabs.onRemoved.addListener((tabId) => {
  captureTabIds.delete(tabId);
});

chrome.runtime.onInstalled.addListener(async () => {
  const monitors = await getMonitors();
  await Promise.all(monitors.map((monitor) => ensureAlarm(monitor)));
  normalizeMonitorBrandsAndShopify().catch(() => {});
});

chrome.runtime.onStartup.addListener(async () => {
  const monitors = await getMonitors();
  await Promise.all(monitors.map((monitor) => ensureAlarm(monitor)));
  normalizeMonitorBrandsAndShopify().catch(() => {});
});

chrome.alarms.onAlarm.addListener(async (alarm) => {
  if (!alarm.name.startsWith(ALARM_PREFIX)) {
    return;
  }

  const { autoIntervalEnabled } = await chrome.storage.local.get("autoIntervalEnabled");
  if (autoIntervalEnabled === false) return; // auto checks disabled

  const monitorId = alarm.name.slice(ALARM_PREFIX.length);
  await runMonitor(monitorId);
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    try {
      if (message.type === "create-monitor") {
        // Use the tab the message came from (content-script path) or the tab
        // ID explicitly passed by the popup, so we capture from the already-
        // loaded page rather than opening a new hidden tab.
        const currentTabId = message.payload.useNewTab ? null : (sender?.tab?.id ?? message.payload.tabId ?? null);
        const monitor = await createMonitor(message.payload, currentTabId);
        sendResponse({ ok: true, monitor });
        return;
      }

      if (message.type === "refresh-monitor") {
        await runMonitor(message.monitorId, "manual");
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "delete-monitor") {
        await deleteMonitor(message.monitorId);
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "delete-monitors-batch") {
        await deleteMonitors(message.monitorIds);
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "update-monitor") {
        const monitor = await updateMonitor(message.payload);
        sendResponse({ ok: true, monitor });
        return;
      }

      if (message.type === "get-monitors") {
        const monitors = await getMonitors();
        sendResponse({ ok: true, monitors });
        return;
      }

      if (message.type === "normalize-monitor-brands") {
        await normalizeMonitorBrandsAndShopify();
        const monitors = await getMonitors();
        sendResponse({ ok: true, monitors });
        return;
      }

      if (message.type === "create-monitors-batch") {
        const senderTabId = sender?.tab?.id ?? null;
        const created = await createMonitorsBatch(
          message.payload.items,
          message.payload.selectors,
          senderTabId,
          message.payload.productDataOverrides || null,
          message.payload.priceAdjustment
        );
        sendResponse({ ok: true, count: created.length });
        return;
      }

      if (message.type === "test-mode-start") {
        const monitors = await getMonitors();
        await chrome.storage.local.set({
          shopifyTestMode: true,
          testModeSnapshot: JSON.stringify(monitors)
        });
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "test-mode-end") {
        const { testModeSnapshot } = await chrome.storage.local.get("testModeSnapshot");
        if (testModeSnapshot) {
          const snapshot = JSON.parse(testModeSnapshot);
          await withStorageLock(() => saveMonitors(snapshot));
        }
        await chrome.storage.local.remove(["shopifyTestMode", "testModeSnapshot"]);
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "restore-monitor") {
        await withStorageLock(async () => {
          const monitors = await getMonitors();
          const idx = monitors.findIndex(m => m.id === message.monitor.id);
          const restored = {
            ...message.monitor,
            shopifyProductId: "",
            shopifyImportedAt: "",
            shopifyLastSyncAt: "",
            shopifySyncStatus: "idle"
          };
          if (idx === -1) {
            monitors.unshift(restored);
          } else {
            monitors[idx] = { ...monitors[idx], ...restored };
          }
          await saveMonitors(monitors);
        });
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "stop-checks") {
        stopAllChecks();
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "undo-last") {
        const result = await undoLast();
        sendResponse(result);
        return;
      }

      if (message.type === "get-undo-count") {
        pruneUndoStack();
        sendResponse({ ok: true, count: undoStack.length });
        return;
      }

      if (message.type === "reset-change-count") {
        await withStorageLock(async () => {
          const fresh = await getMonitors();
          const idx = fresh.findIndex((m) => m.id === message.monitorId);
          if (idx !== -1) { fresh[idx] = { ...fresh[idx], changeCount: 0 }; await saveMonitors(fresh); }
        });
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "reset-all-change-counts") {
        await withStorageLock(async () => {
          const fresh = await getMonitors();
          let dirty = false;
          fresh.forEach((m, i) => { if (m.changeCount > 0) { fresh[i] = { ...m, changeCount: 0 }; dirty = true; } });
          if (dirty) await saveMonitors(fresh);
        });
        sendResponse({ ok: true });
        return;
      }

      sendResponse({ ok: false, error: "Unknown message type" });
    } catch (error) {
      sendResponse({ ok: false, error: error.message });
    }
  })();

  return true;
});

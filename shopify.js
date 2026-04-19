import { addLog } from "./shared.js";

const IMPORT_HISTORY_KEY = "shopifyImportHistory";
const SHOPIFY_ENV_KEYS = {
  shop: "SHOPIFY_SHOP",
  clientId: "SHOPIFY_CLIENT_ID",
  clientSecret: "SHOPIFY_CLIENT_SECRET",
  apiVersion: "SHOPIFY_API_VERSION"
};
const SHOPIFY_DEFAULTS = {
  shop: "orjn.myshopify.com",
  clientId: "bf0dbb55084b776cfeab72d1a69a8436",
  apiVersion: "2025-01"
};
const SHOPIFY_CACHE_TTL_MS = 5 * 60 * 1000;
let envConfigPromise = null;
let locationsCache = null;
let metadataCache = null;

function getMonitorLogMeta(monitor = {}) {
  const pd = monitor.productData || {};
  const brand = pd.brand || "";
  const sku = pd.sku || "";
  return {
    title: [brand, sku].filter(Boolean).join(" ") || monitor.name || "Shopify",
    productName: pd.name || monitor.name || "",
    brand,
    sku,
    monitorId: monitor.id,
    url: monitor.url
  };
}

function formatShopifyError(error) {
  let reason = error?.message || String(error) || "Unknown error";
  try {
    const match = reason.match(/\d{3}: (.+)$/s);
    if (match) {
      const parsed = JSON.parse(match[1]);
      const msgs = parsed.errors
        ? Object.entries(parsed.errors).map(([k, v]) => `${k}: ${Array.isArray(v) ? v.join(", ") : v}`).join(" | ")
        : JSON.stringify(parsed);
      reason = `Shopify rejected: ${msgs}`;
    }
  } catch (_) {}
  return reason;
}

async function logShopifyError(monitor, details) {
  await addLog({
    type: "error",
    ...getMonitorLogMeta(monitor),
    details
  }).catch(() => {});
}

function isFreshCacheEntry(entry) {
  return !!entry && (Date.now() - entry.timestamp) < SHOPIFY_CACHE_TTL_MS;
}

// ── Color normalization ────────────────────────────────────────────────────
const COLOR_MAP = {
  // Red
  red:"Red",crimson:"Red",scarlet:"Red",ruby:"Red",burgundy:"Red",maroon:"Red",wine:"Red",cherry:"Red",tomato:"Red",garnet:"Red",carmine:"Red",vermillion:"Red",brick:"Red",blood:"Red",firebrick:"Red",cranberry:"Red",raspberry:"Red",strawberry:"Red",rose:"Red",cardinal:"Red",claret:"Red",mahogany:"Red",terra:"Red",cotta:"Red",sienna:"Red",auburn:"Red",
  // Orange
  orange:"Orange",amber:"Orange",tangerine:"Orange",apricot:"Orange",rust:"Orange",burnt:"Orange",copper:"Orange",cinnamon:"Orange",pumpkin:"Orange",papaya:"Orange",mango:"Orange",melon:"Orange",clay:"Orange",ginger:"Orange",tiger:"Orange",marigold:"Orange",saffron:"Orange",bronze:"Orange",
  // Brown
  brown:"Brown",tan:"Brown",beige:"Brown",camel:"Brown",mocha:"Brown",chocolate:"Brown",coffee:"Brown",sand:"Brown",taupe:"Brown",nude:"Brown",cream:"Brown",natural:"Brown",walnut:"Brown",hazel:"Brown",toffee:"Brown",espresso:"Brown",sepia:"Brown",umber:"Brown",fawn:"Brown",wheat:"Brown",oatmeal:"Brown",biscuit:"Brown",latte:"Brown",ecru:"Brown",buff:"Brown",driftwood:"Brown",chestnut:"Brown",cacao:"Brown",bark:"Brown",leather:"Brown",suede:"Brown",
  // Yellow
  yellow:"Yellow",gold:"Yellow",golden:"Yellow",mustard:"Yellow",lemon:"Yellow",lime:"Yellow",canary:"Yellow",butter:"Yellow",banana:"Yellow",flaxen:"Yellow",straw:"Yellow",blonde:"Yellow",champagne:"Yellow",vanilla:"Yellow",honey:"Yellow",sunflower:"Yellow",daffodil:"Yellow",citrine:"Yellow",topaz:"Yellow",citrus:"Yellow",
  // Green
  green:"Green",olive:"Green",sage:"Green",forest:"Green",army:"Green",hunter:"Green",jade:"Green",emerald:"Green",mint:"Green",fern:"Green",moss:"Green",pine:"Green",bottle:"Green",kelly:"Green",shamrock:"Green",chartreuse:"Green",avocado:"Green",pistachio:"Green",pear:"Green",leaf:"Green",basil:"Green",seaweed:"Green",jungle:"Green",cucumber:"Green",matcha:"Green",celadon:"Green",viridian:"Green",malachite:"Green",
  // Turquoise
  turquoise:"Turquoise",teal:"Turquoise",aqua:"Turquoise",cyan:"Turquoise",seafoam:"Turquoise",aquamarine:"Turquoise",caribbean:"Turquoise",lagoon:"Turquoise",cerulean:"Turquoise",peacock:"Turquoise",ocean:"Turquoise",pool:"Turquoise",
  // Blue
  blue:"Blue",navy:"Blue",cobalt:"Blue",royal:"Blue",indigo:"Blue",denim:"Blue",sky:"Blue",powder:"Blue",midnight:"Blue",steel:"Blue",slate:"Blue",sapphire:"Blue",azure:"Blue",cornflower:"Blue",periwinkle:"Blue",iris:"Blue",ultramarine:"Blue",prussian:"Blue",admiral:"Blue",marine:"Blue",federal:"Blue",storm:"Blue",glacier:"Blue",arctic:"Blue",ice:"Blue",aegean:"Blue",
  // Violet
  violet:"Violet",purple:"Violet",lavender:"Violet",lilac:"Violet",plum:"Violet",grape:"Violet",mauve:"Violet",amethyst:"Violet",orchid:"Violet",wisteria:"Violet",heather:"Violet",thistle:"Violet",periwinkle:"Violet",mulberry:"Violet",eggplant:"Violet",byzantium:"Violet",aubergine:"Violet",boysenberry:"Violet",
  // Pink
  pink:"Pink",blush:"Pink",fuchsia:"Pink",magenta:"Pink",flamingo:"Pink",salmon:"Pink",bubblegum:"Pink",watermelon:"Pink",peony:"Pink",carnation:"Pink",petal:"Pink",flush:"Pink",rouge:"Pink",blossom:"Pink",pastel:"Pink",candy:"Pink",lollipop:"Pink",neon:"Pink",cerise:"Pink",hot:"Pink",dusty:"Pink",millennial:"Pink",
  // White
  white:"White",ivory:"White",snow:"White",pearl:"White",alabaster:"White",porcelain:"White",linen:"White",chalk:"White",bone:"White",eggshell:"White",antique:"White",milk:"White",cotton:"White",ghost:"White",frost:"White",
  // Gray
  gray:"Gray",grey:"Gray",silver:"Gray",charcoal:"Gray",ash:"Gray",smoke:"Gray",graphite:"Gray",pewter:"Gray",stone:"Gray",pebble:"Gray",cement:"Gray",concrete:"Gray",flint:"Gray",iron:"Gray",lead:"Gray",fossil:"Gray",heather:"Gray",marengo:"Gray",dove:"Gray",cloud:"Gray",
  // Black
  black:"Black",onyx:"Black",jet:"Black",ebony:"Black",obsidian:"Black",raven:"Black",midnight:"Black",coal:"Black",charcoal:"Black",licorice:"Black",noir:"Black",shadow:"Black",ink:"Black",pitch:"Black",
};

function normalizeColor(raw) {
  if (!raw) return null;
  const words = raw.toLowerCase().replace(/[^a-z\s]/g, " ").split(/\s+/);
  for (const word of words) {
    if (COLOR_MAP[word]) return COLOR_MAP[word];
  }
  // Try partial match
  for (const word of words) {
    for (const [key, val] of Object.entries(COLOR_MAP)) {
      if (word.includes(key) || key.includes(word)) return val;
    }
  }
  return null;
}

// ── EU size charts ─────────────────────────────────────────────────────────
const SIZE_CHARTS = {
  Nike:   { Men: {"3.5":"35.5","4":"36","4.5":"36.5","5":"37.5","5.5":"38","6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"45.5","12":"46","12.5":"47","13":"47.5","13.5":"48","14":"48.5","14.5":"49","15":"49.5","15.5":"50","16":"50.5","16.5":"51","17":"51.5","17.5":"52","18":"52.5"}, Women: {"5":"35.5","5.5":"36","6":"36.5","6.5":"37.5","7":"38","7.5":"38.5","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"42","10.5":"42.5","11":"43","11.5":"44","12":"44.5","12.5":"45","13":"45.5","13.5":"46","14":"47","14.5":"47.5","15":"48"} },
  Jordan: { Men: {"3":"35.5","3.5":"36","4":"36.5","4.5":"37.5","5":"37.5","5.5":"38","6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"45.5","12":"46","12.5":"47","13":"47.5","13.5":"48","14":"48.5","14.5":"49","15":"49.5","15.5":"50","16":"50.5","16.5":"51","17":"51.5","17.5":"52","18":"52.5"}, Women: {"5":"35.5","5.5":"36","6":"36.5","6.5":"37.5","7":"38","7.5":"38.5","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"42","10.5":"42.5","11":"43","11.5":"44","12":"44.5","12.5":"45","13":"45.5","13.5":"46","14":"47","14.5":"47.5","15":"48"} },
  Adidas: { Men: {"4":"36","4.5":"36 2/3","5":"37 1/3","5.5":"38","6":"38 2/3","6.5":"39 1/3","7":"40","7.5":"40 2/3","8":"41 1/3","8.5":"42","9":"42 2/3","9.5":"43 1/3","10":"44","10.5":"44 2/3","11":"45 1/3","11.5":"46","12":"46 2/3","12.5":"47 1/3","13":"48","13.5":"48 2/3","14":"49 1/3","14.5":"50","15":"50 2/3","16":"51 1/3","17":"52 2/3","18":"53 1/3","19":"54 2/3","20":"55 2/3"}, Women: {"5":"36","5.5":"36 2/3","6":"37 1/3","6.5":"38","7":"38 2/3","7.5":"39 1/3","8":"40","8.5":"40 2/3","9":"41 1/3","9.5":"42","10":"42 2/3","10.5":"43 1/3","11":"44","11.5":"44 2/3","12":"45 1/3","12.5":"46","13":"46 2/3"} },
  Reebok: { Men: {"4":"34.5","4.5":"35","5":"36","5.5":"36.5","6":"37.5","6.5":"38.5","7":"39","7.5":"40","8":"40.5","8.5":"41","9":"42","9.5":"42.5","10":"43","10.5":"44","11":"44.5","11.5":"45","12":"46","13":"47","14":"48.5"}, Women: {"6":"36","7":"37","8":"38","9":"39","9.5":"40","10.5":"41","11":"42","11.5":"42.5","12":"43","12.5":"44","13":"44.5","13.5":"45","14":"46","15":"47"} },
  Puma:   { Men: {"6":"38","6.5":"38.5","7":"39","7.5":"40","8":"40.5","8.5":"41","9":"42","9.5":"42.5","10":"43","10.5":"44","11":"44.5","11.5":"45","12":"46","12.5":"46.5","13":"47","14":"48.5","15":"49.5","16":"51"}, Women: {"5.5":"35.5","6":"36","6.5":"37","7":"37.5","7.5":"38","8":"38.5","8.5":"39","9":"40","9.5":"40.5","10":"41","10.5":"42","11":"42.5"} }
};

function getEuSize(usSize, brand, gender) {
  if (!usSize || !brand) return null;
  const b = String(brand).toLowerCase();
  let chartBrand = null;
  if (/nike/i.test(b)) chartBrand = "Nike";
  else if (/jordan/i.test(b)) chartBrand = "Jordan";
  else if (/adidas/i.test(b)) chartBrand = "Adidas";
  else if (/reebok/i.test(b)) chartBrand = "Reebok";
  else if (/puma/i.test(b)) chartBrand = "Puma";
  if (!chartBrand) return null;
  const chartGender = /women|girl|female/i.test(String(gender || "")) ? "Women" : "Men";
  const chart = SIZE_CHARTS[chartBrand]?.[chartGender];
  if (!chart) return null;
  const n = parseFloat(String(usSize).replace(/[^\d.]/g, ""));
  if (isNaN(n) || n <= 0 || n > 30) return null;
  return chart[String(n)] || null;
}

function getRedirectUri() {
  return `https://${chrome.runtime.id}.chromiumapp.org/`;
}

function parseEnvValue(rawValue) {
  const value = rawValue.trim();
  if (!value) return "";
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }
  return value;
}

async function getEnvConfig() {
  if (envConfigPromise) return envConfigPromise;
  envConfigPromise = (async () => {
    const config = {};
    try {
      const res = await fetch(chrome.runtime.getURL(".env"), { cache: "no-store" });
      if (!res.ok) return config;
      const envText = await res.text();
      for (const rawLine of envText.split(/\r?\n/)) {
        const line = rawLine.trim();
        if (!line || line.startsWith("#")) continue;
        const separatorIndex = line.indexOf("=");
        if (separatorIndex === -1) continue;
        const key = line.slice(0, separatorIndex).trim();
        config[key] = parseEnvValue(line.slice(separatorIndex + 1));
      }
    } catch (_) {
      return config;
    }
    return config;
  })();
  return envConfigPromise;
}

async function getShopifyConfig() {
  const env = await getEnvConfig();
  return {
    shop: env[SHOPIFY_ENV_KEYS.shop] || SHOPIFY_DEFAULTS.shop,
    clientId: env[SHOPIFY_ENV_KEYS.clientId] || SHOPIFY_DEFAULTS.clientId,
    clientSecret: env[SHOPIFY_ENV_KEYS.clientSecret] || "",
    apiVersion: env[SHOPIFY_ENV_KEYS.apiVersion] || SHOPIFY_DEFAULTS.apiVersion
  };
}

export async function getAccessToken() {
  const { shopifyToken } = await chrome.storage.local.get("shopifyToken");
  return shopifyToken || null;
}

export async function isConnected() {
  return !!(await getAccessToken());
}

export async function verifyConnection() {
  try {
    const data = await shopifyFetch("/shop.json");
    return { ok: true, shopName: data.shop?.name || "orjn" };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function connectShopify(clientSecret) {
  const { shop, clientId, clientSecret: envClientSecret } = await getShopifyConfig();
  const resolvedClientSecret = clientSecret || envClientSecret;
  if (!resolvedClientSecret) {
    throw new Error("Shopify client secret is not configured");
  }
  const state = crypto.randomUUID();
  const redirectUri = getRedirectUri();
  const authUrl =
    `https://${shop}/admin/oauth/authorize` +
    `?client_id=${clientId}` +
    `&scope=write_products,write_inventory,read_products,read_inventory` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}` +
    `&state=${state}`;

  const responseUrl = await chrome.identity.launchWebAuthFlow({ url: authUrl, interactive: true });
  const params = new URL(responseUrl).searchParams;
  if (params.get("state") !== state) throw new Error("State mismatch — possible CSRF");
  const code = params.get("code");
  if (!code) throw new Error("No auth code returned");

  const res = await fetch(`https://${shop}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ client_id: clientId, client_secret: resolvedClientSecret, code })
  });
  if (!res.ok) throw new Error(`Token exchange failed: ${res.status}`);
  const { access_token } = await res.json();
  if (!access_token) throw new Error("No access token in response");
  await chrome.storage.local.set({ shopifyToken: access_token });
  return access_token;
}

export async function disconnectShopify() {
  locationsCache = null;
  metadataCache = null;
  await chrome.storage.local.remove(["shopifyToken"]);
}

async function shopifyFetch(path, options = {}) {
  const token = await getAccessToken();
  if (!token) throw new Error("Not connected to Shopify");
  const { shop, apiVersion } = await getShopifyConfig();
  const res = await fetch(`https://${shop}/admin/api/${apiVersion}${path}`, {
    ...options,
    headers: {
      "X-Shopify-Access-Token": token,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Shopify API ${res.status}: ${body}`);
  }
  return res.json();
}

export async function getLocations() {
  if (isFreshCacheEntry(locationsCache)) return locationsCache.value;
  const data = await shopifyFetch("/locations.json");
  const locations = data.locations || [];
  locationsCache = { value: locations, timestamp: Date.now() };
  return locations;
}

export async function createProduct(product) {
  const data = await shopifyFetch("/products.json", {
    method: "POST",
    body: JSON.stringify({ product })
  });
  metadataCache = null;
  return data.product;
}

export async function deleteProduct(productId) {
  const token = await getAccessToken();
  if (!token) throw new Error("Not connected to Shopify");
  const { shop, apiVersion } = await getShopifyConfig();
  const res = await fetch(`https://${shop}/admin/api/${apiVersion}/products/${productId}.json`, {
    method: "DELETE",
    headers: { "X-Shopify-Access-Token": token }
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Shopify API ${res.status}: ${body}`);
  }
  metadataCache = null;
}

async function getProductById(productId) {
  if (!productId) return null;
  const data = await shopifyFetch(`/products/${productId}.json?fields=id,variants`);
  return data.product || null;
}

// Fetches all existing vendors, product_types, and tags from the store
export async function getShopifyMetadata() {
  if (isFreshCacheEntry(metadataCache)) return metadataCache.value;
  const vendors = new Set();
  const types = new Set();
  const tags = new Set();

  let pageInfo = null;
  let firstPage = true;
  while (firstPage || pageInfo) {
    firstPage = false;
    const url = pageInfo
      ? `/products.json?limit=250&fields=vendor,product_type,tags&page_info=${pageInfo}`
      : `/products.json?limit=250&fields=vendor,product_type,tags`;
    const { shop, apiVersion } = await getShopifyConfig();
    const res = await fetch(`https://${shop}/admin/api/${apiVersion}${url}`, {
      headers: { "X-Shopify-Access-Token": await getAccessToken(), "Content-Type": "application/json" }
    });
    const link = res.headers.get("Link") || "";
    const data = await res.json();
    (data.products || []).forEach(p => {
      if (p.vendor) vendors.add(p.vendor.trim());
      if (p.product_type) types.add(p.product_type.trim());
      (p.tags || "").split(",").forEach(t => { const s = t.trim(); if (s) tags.add(s); });
    });
    const nextMatch = link.match(/<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"/);
    pageInfo = nextMatch ? nextMatch[1] : null;
  }

  const result = {
    vendors: [...vendors].sort(),
    types: [...types].sort(),
    tags: [...tags].sort()
  };
  metadataCache = { value: result, timestamp: Date.now() };
  return result;
}

// Match a value case-insensitively against an existing Shopify list, return exact casing if found
function matchExisting(value, list) {
  if (!value || !list.length) return value;
  const lower = value.toLowerCase();
  return list.find(v => v.toLowerCase() === lower) || value;
}

// ── SKU → Shopify product ID mapping ──────────────────────────────────────
async function getSkuMapping() {
  const { shopifySkuMapping } = await chrome.storage.local.get("shopifySkuMapping");
  return shopifySkuMapping || {};
}

async function saveSkuMapping(mapping) {
  await chrome.storage.local.set({ shopifySkuMapping: mapping });
}

async function findProductBySkuPrefix(baseSku) {
  const token = await getAccessToken();
  if (!token) return null;
  let pageInfo = null;
  let firstPage = true;
  while (firstPage || pageInfo) {
    firstPage = false;
    const path = pageInfo
      ? `/products.json?limit=250&fields=id,variants&page_info=${pageInfo}`
      : `/products.json?limit=250&fields=id,variants`;
    const { shop, apiVersion } = await getShopifyConfig();
    const res = await fetch(`https://${shop}/admin/api/${apiVersion}${path}`, {
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" }
    });
    const link = res.headers.get("Link") || "";
    const data = await res.json();
    for (const p of (data.products || [])) {
      if ((p.variants || []).some(v => v.sku === baseSku || v.sku?.startsWith(`${baseSku}-`))) return p;
    }
    const next = link.match(/<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"/);
    pageInfo = next ? next[1] : null;
  }
  return null;
}

export async function updateShopifyForMonitor(monitor, liveData) {
  try {
    if (!await isConnected()) return;
    const baseSku = monitor.productData?.sku;
    if (!baseSku) return;

    const mapping = await getSkuMapping();
    let productId = monitor.shopifyProductId || mapping[baseSku];
    let product = null;

    if (productId) {
      try {
        product = await getProductById(productId);
        if (product && mapping[baseSku] !== product.id) {
          mapping[baseSku] = product.id;
          await saveSkuMapping(mapping);
        }
      } catch (_) { productId = null; }
    }

    if (!product) {
      product = await findProductBySkuPrefix(baseSku);
      if (product) { mapping[baseSku] = product.id; await saveSkuMapping(mapping); }
    }

    if (!product?.variants?.length) {
      throw new Error(`No Shopify product found for SKU ${baseSku}`);
    }

    const price = liveData.price != null ? String(liveData.price) : null;
    const hasCompareAt = Object.prototype.hasOwnProperty.call(liveData || {}, "compareAt");
    const compareAt = liveData.compareAt != null ? String(liveData.compareAt) : null;
    const inStock = new Set(liveData.inStock || []);
    const outOfStock = new Set(liveData.outOfStock || []);
    const locations = await getLocations();
    const locationId = locations[0]?.id;
    const prefix = `${baseSku}-`;
    const errors = [];

    await Promise.all(product.variants.map(async (variant) => {
      const tasks = [];
      if (price !== null) {
        const update = { id: variant.id, price };
        if (hasCompareAt) update.compare_at_price = compareAt;
        tasks.push(
          shopifyFetch(`/variants/${variant.id}.json`, {
            method: "PUT",
            body: JSON.stringify({ variant: update })
          }).catch((error) => {
            errors.push(`Price update failed for ${variant.sku || variant.id}: ${formatShopifyError(error)}`);
          })
        );
      }
      if (locationId && variant.sku?.startsWith(prefix)) {
        const usSize = variant.sku.slice(prefix.length);
        if (inStock.has(usSize)) {
          tasks.push(
            setInventoryLevel(variant.inventory_item_id, locationId, 10).catch((error) => {
              errors.push(`In-stock sync failed for ${usSize}: ${formatShopifyError(error)}`);
            })
          );
        } else if (outOfStock.has(usSize)) {
          tasks.push(
            setInventoryLevel(variant.inventory_item_id, locationId, 0).catch((error) => {
              errors.push(`Out-of-stock sync failed for ${usSize}: ${formatShopifyError(error)}`);
            })
          );
        }
      }
      await Promise.all(tasks);
    }));

    if (errors.length) {
      throw new Error(errors.join(" | "));
    }
  } catch (error) {
    await logShopifyError(monitor, [formatShopifyError(error)]);
    throw error;
  }
}

export async function setInventoryLevel(inventoryItemId, locationId, available) {
  return shopifyFetch("/inventory_levels/set.json", {
    method: "POST",
    body: JSON.stringify({ inventory_item_id: inventoryItemId, location_id: locationId, available })
  });
}

// ── Import history ─────────────────────────────────────────────────────────
async function getImportHistory() {
  const { [IMPORT_HISTORY_KEY]: history } = await chrome.storage.local.get(IMPORT_HISTORY_KEY);
  return Array.isArray(history) ? history : [];
}

async function pushImportHistory(entry) {
  const history = await getImportHistory();
  history.unshift(entry); // newest first
  await chrome.storage.local.set({ [IMPORT_HISTORY_KEY]: history });
}

export async function getRecentImports() {
  const history = await getImportHistory();
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  return history.filter(e => new Date(e.importedAt).getTime() > cutoff);
}

export async function undoLastImport() {
  const history = await getImportHistory();
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  const idx = history.findIndex(e => new Date(e.importedAt).getTime() > cutoff);
  if (idx === -1) throw new Error("No imports in the last 24 hours to undo");

  const entry = history[idx];

  // Delete from Shopify
  await deleteProduct(entry.shopifyProductId);

  // Restore monitor via background message
  await chrome.runtime.sendMessage({ type: "restore-monitor", monitor: entry.monitorData });

  // Remove from history
  history.splice(idx, 1);
  await chrome.storage.local.set({ [IMPORT_HISTORY_KEY]: history });

  return entry;
}

// ── Main import ────────────────────────────────────────────────────────────
export async function importMonitorProduct(monitor) {
  const pd = monitor.productData || {};
  const live = monitor.lastExtractedData || {};

  // Fetch existing Shopify metadata to match exact casing
  const meta = await getShopifyMetadata().catch(() => ({ vendors: [], types: [], tags: [] }));

  const price = live.price != null ? String(live.price) : (pd.price != null ? String(pd.price) : "0");
  const compareAt = live.compareAt != null ? String(live.compareAt) : null;

  const inStock = new Set(live.inStock || []);
  const outOfStock = new Set(live.outOfStock || []);
  const allSizesUS = [...inStock, ...outOfStock];

  const baseSku = pd.sku || "";
  if (monitor.shopifyProductId) {
    throw new Error(`Already imported to Shopify for SKU ${baseSku || pd.name || monitor.name}`);
  }
  if (baseSku) {
    const mapping = await getSkuMapping();
    let existing = null;
    if (mapping[baseSku]) {
      existing = await getProductById(mapping[baseSku]).catch(() => null);
    }
    if (!existing) {
      existing = await findProductBySkuPrefix(baseSku);
    }
    if (existing?.id) {
      throw new Error(`Already imported to Shopify for SKU ${baseSku}`);
    }
  }
  const brand = matchExisting(pd.brand || "", meta.vendors);
  const gender = pd.gender || "";
  const productType = matchExisting(pd.type || "", meta.types);
  const normalizedColor = normalizeColor(pd.color) || pd.color || null;

  // Tags: gender + color (normalized if recognized, raw otherwise)
  const rawTags = [gender, normalizedColor].filter(Boolean);
  const tags = rawTags.map(t => matchExisting(t, meta.tags));

  const variants = allSizesUS.length
    ? allSizesUS.map(usSize => {
        const euSize = getEuSize(usSize, brand, gender);
        const option1 = euSize ? String(euSize) : usSize;
        const variantSku = baseSku ? `${baseSku}-${usSize}` : usSize;
        const variant = {
          option1,
          price,
          sku: variantSku,
          inventory_management: "shopify",
          inventory_quantity: inStock.has(usSize) ? 10 : 0,
          fulfillment_service: "manual",
          requires_shipping: true,
          taxable: true
        };
        if (compareAt) variant.compare_at_price = compareAt;
        return variant;
      })
    : [{
        price,
        sku: baseSku,
        inventory_management: "shopify",
        inventory_quantity: 10,
        fulfillment_service: "manual",
        requires_shipping: true,
        taxable: true
      }];

  const product = {
    title: pd.name || monitor.name || "Imported Product",
    body_html: pd.description ? `<p>${pd.description}</p>` : "",
    vendor: brand,
    product_type: productType,
    tags: tags.join(", "),
    status: "active",
    published_scope: "global",
    options: allSizesUS.length ? [{ name: "Size" }] : [],
    variants,
    images: (pd.images || []).map(src => ({ src })),
    metafields: [
      {
        namespace: "custom",
        key: "notes",
        value: monitor.url || "",
        type: "multi_line_text_field"
      }
    ]
  };

  const created = await createProduct(product);

  // Cache SKU → product ID for future auto-updates
  if (baseSku) {
    const mapping = await getSkuMapping();
    mapping[baseSku] = created.id;
    await saveSkuMapping(mapping);
  }

  // Set inventory at primary location
  const locations = await getLocations();
  if (locations.length && created.variants) {
    await Promise.all(created.variants.map(async (variant) => {
      const usSize = allSizesUS.find(us => {
        const eu = getEuSize(us, brand, gender);
        return eu ? String(eu) === variant.option1 : us === variant.option1;
      });
      const qty = usSize
        ? (inStock.has(usSize) ? 10 : 0)
        : (variant.inventory_quantity ?? 10);
      await Promise.all(locations.map((location) =>
        setInventoryLevel(variant.inventory_item_id, location.id, qty).catch(() => {})
      ));
    }));
  }

  // Save to import history for undo
  await pushImportHistory({
    shopifyProductId: created.id,
    monitorData: monitor,
    importedAt: new Date().toISOString(),
    monitorName: monitor.name
  });

  return created;
}

export async function updateVariantPrice(variantId, price, compareAtPrice = null) {
  const body = { variant: { id: variantId, price: String(price) } };
  if (compareAtPrice != null) body.variant.compare_at_price = String(compareAtPrice);
  return shopifyFetch(`/variants/${variantId}.json`, {
    method: "PUT",
    body: JSON.stringify(body)
  });
}

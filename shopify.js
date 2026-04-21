import { addLog, canonicalizeBrand } from "./shared.js";

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
const SHOPIFY_API_MIN_INTERVAL_MS = 520;
const SHOPIFY_API_MAX_RETRIES = 4;
let envConfigPromise = null;
let locationsCache = null;
let metadataCache = null;
let productsSnapshotCache = null;
let shopifyApiQueue = Promise.resolve();
let nextShopifyApiRequestAt = 0;

function getMonitorLogMeta(monitor = {}) {
  const pd = monitor.productData || {};
  const brand = canonicalizeBrand(pd.brand || "");
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chunkArray(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function getRetryDelayMs(res, attempt) {
  const retryAfter = Number(res.headers.get("Retry-After"));
  if (Number.isFinite(retryAfter) && retryAfter > 0) {
    return Math.max(retryAfter * 1000, SHOPIFY_API_MIN_INTERVAL_MS);
  }
  return Math.min(2000 * (attempt + 1), 8000);
}

async function enqueueShopifyApiCall(task) {
  const run = async () => {
    const waitMs = Math.max(0, nextShopifyApiRequestAt - Date.now());
    if (waitMs > 0) {
      await sleep(waitMs);
    }
    nextShopifyApiRequestAt = Date.now() + SHOPIFY_API_MIN_INTERVAL_MS;
    return task();
  };

  const queued = shopifyApiQueue.then(run, run);
  shopifyApiQueue = queued.catch(() => {});
  return queued;
}

function invalidateShopifyCaches({ locations = false, metadata = false, products = false } = {}) {
  if (locations) locationsCache = null;
  if (metadata) metadataCache = null;
  if (products) productsSnapshotCache = null;
}

// ── Color normalization ────────────────────────────────────────────────────
const COLOR_MAP = {
  // Red
  red:"Red",crimson:"Red",scarlet:"Red",ruby:"Red",burgundy:"Red",maroon:"Red",wine:"Red",cherry:"Red",tomato:"Red",garnet:"Red",carmine:"Red",vermillion:"Red",vermilion:"Red",brick:"Red",blood:"Red",firebrick:"Red",cranberry:"Red",raspberry:"Red",strawberry:"Red",rose:"Red",cardinal:"Red",claret:"Red",mahogany:"Red",terra:"Red",cotta:"Red",sienna:"Red",auburn:"Red",rubyred:"Red",oxblood:"Red",merlot:"Red",poppy:"Red",coralred:"Red",sunsetred:"Red",chili:"Red",rubywine:"Red",
  // Orange
  orange:"Orange",amber:"Orange",tangerine:"Orange",apricot:"Orange",rust:"Orange",burnt:"Orange",copper:"Orange",cinnamon:"Orange",pumpkin:"Orange",papaya:"Orange",mango:"Orange",melon:"Orange",clay:"Orange",ginger:"Orange",tiger:"Orange",marigold:"Orange",saffron:"Orange",bronze:"Orange",peach:"Orange",persimmon:"Orange",nectarine:"Orange",cantaloupe:"Orange",sunset:"Orange",burntorange:"Orange",terracotta:"Orange",carrot:"Orange",kumquat:"Orange",
  // Brown
  brown:"Brown",tan:"Brown",beige:"Brown",camel:"Brown",mocha:"Brown",chocolate:"Brown",coffee:"Brown",sand:"Brown",taupe:"Brown",nude:"Brown",cream:"Brown",natural:"Brown",walnut:"Brown",hazel:"Brown",toffee:"Brown",espresso:"Brown",sepia:"Brown",umber:"Brown",fawn:"Brown",wheat:"Brown",oatmeal:"Brown",biscuit:"Brown",latte:"Brown",ecru:"Brown",buff:"Brown",driftwood:"Brown",chestnut:"Brown",cacao:"Brown",bark:"Brown",leather:"Brown",suede:"Brown",stone:"Brown",khaki:"Brown",caramel:"Brown",pecan:"Brown",almond:"Brown",acorn:"Brown",cocoa:"Brown",mink:"Brown",tobacco:"Brown",saddle:"Brown",oak:"Brown",hickory:"Brown",truffle:"Brown",earth:"Brown",mud:"Brown",dune:"Brown",bran:"Brown",
  // Yellow
  yellow:"Yellow",gold:"Yellow",golden:"Yellow",mustard:"Yellow",lemon:"Yellow",lime:"Yellow",canary:"Yellow",butter:"Yellow",banana:"Yellow",flaxen:"Yellow",straw:"Yellow",blonde:"Yellow",champagne:"Yellow",vanilla:"Yellow",honey:"Yellow",sunflower:"Yellow",daffodil:"Yellow",citrine:"Yellow",topaz:"Yellow",citrus:"Yellow",volt:"Yellow",maize:"Yellow",corn:"Yellow",ambergold:"Yellow",sandgold:"Yellow",sulphur:"Yellow",mustardseed:"Yellow",dijon:"Yellow","neonyellow":"Yellow","electricyellow":"Yellow",
  // Green
  green:"Green",olive:"Green",sage:"Green",forest:"Green",army:"Green",hunter:"Green",jade:"Green",emerald:"Green",mint:"Green",fern:"Green",moss:"Green",pine:"Green",bottle:"Green",kelly:"Green",shamrock:"Green",chartreuse:"Green",avocado:"Green",pistachio:"Green",pear:"Green",leaf:"Green",basil:"Green",seaweed:"Green",jungle:"Green",cucumber:"Green",matcha:"Green",celadon:"Green",viridian:"Green",malachite:"Green",voltgreen:"Green",neongreen:"Green",electricgreen:"Green",loden:"Green",spruce:"Green",evergreen:"Green",clover:"Green",pea:"Green",grassy:"Green",seaglass:"Green",seafoamgreen:"Green",
  // Turquoise
  turquoise:"Turquoise",teal:"Turquoise",aqua:"Turquoise",cyan:"Turquoise",seafoam:"Turquoise",aquamarine:"Turquoise",caribbean:"Turquoise",lagoon:"Turquoise",cerulean:"Turquoise",peacock:"Turquoise",ocean:"Turquoise",pool:"Turquoise",mintblue:"Turquoise",tiffany:"Turquoise",robinsegg:"Turquoise",bluegreen:"Turquoise",turq:"Turquoise",
  // Blue
  blue:"Blue",navy:"Blue",cobalt:"Blue",royal:"Blue",indigo:"Blue",denim:"Blue",sky:"Blue",powder:"Blue",midnight:"Blue",steel:"Blue",slate:"Blue",sapphire:"Blue",azure:"Blue",cornflower:"Blue",periwinkle:"Blue",iris:"Blue",ultramarine:"Blue",prussian:"Blue",admiral:"Blue",marine:"Blue",federal:"Blue",storm:"Blue",glacier:"Blue",arctic:"Blue",ice:"Blue",aegean:"Blue",thunder:"Blue","obsidian-blue":"Blue",turbo:"Blue",polar:"Blue",mistblue:"Blue",oceanblue:"Blue",deepblue:"Blue",lightblue:"Blue",darkblue:"Blue",hyperblue:"Blue",universityblue:"Blue",carolinablue:"Blue",
  // Violet
  violet:"Violet",purple:"Violet",lavender:"Violet",lilac:"Violet",plum:"Violet",grape:"Violet",mauve:"Violet",amethyst:"Violet",orchid:"Violet",wisteria:"Violet",heather:"Violet",thistle:"Violet",periwinkle:"Violet",mulberry:"Violet",eggplant:"Violet",byzantium:"Violet",aubergine:"Violet",boysenberry:"Violet",violetdust:"Violet",deeppurple:"Violet",royalpurple:"Violet",
  // Pink
  pink:"Pink",blush:"Pink",fuchsia:"Pink",magenta:"Pink",flamingo:"Pink",salmon:"Pink",bubblegum:"Pink",watermelon:"Pink",peony:"Pink",carnation:"Pink",petal:"Pink",flush:"Pink",rouge:"Pink",blossom:"Pink",pastel:"Pink",candy:"Pink",lollipop:"Pink",neon:"Pink",cerise:"Pink",hot:"Pink",dusty:"Pink",millennial:"Pink",rosepink:"Pink",powderpink:"Pink",softpink:"Pink",brightpink:"Pink",shockpink:"Pink",
  // White
  white:"White",ivory:"White",snow:"White",pearl:"White",alabaster:"White",porcelain:"White",linen:"White",chalk:"White",bone:"White",eggshell:"White",antique:"White",milk:"White",cotton:"White",ghost:"White",frost:"White",sail:"White",offwhite:"White","off-white":"White",whisper:"White",paper:"White",shell:"White",
  // Gray
  gray:"Gray",grey:"Gray",silver:"Gray",charcoal:"Gray",ash:"Gray",smoke:"Gray",graphite:"Gray",pewter:"Gray",pebble:"Gray",cement:"Gray",concrete:"Gray",flint:"Gray",iron:"Gray",lead:"Gray",fossil:"Gray",heather:"Gray",marengo:"Gray",dove:"Gray",cloud:"Gray",wolf:"Gray",cool:"Gray",smokey:"Gray",stonegrey:"Gray","stone-gray":"Gray",coolgrey:"Gray","cool-grey":"Gray",neutralgray:"Gray",neutralgrey:"Gray",platinum:"Gray",gunmetal:"Gray",
  // Black
  black:"Black",onyx:"Black",jet:"Black",ebony:"Black",obsidian:"Black",raven:"Black",midnight:"Black",coal:"Black",charcoal:"Black",licorice:"Black",noir:"Black",shadow:"Black",ink:"Black",pitch:"Black",tripleblack:"Black","triple-black":"Black",coreblack:"Black","core-black":"Black",phantom:"Black",anthracite:"Black",soot:"Black",carbon:"Black",
};

function normalizeColor(raw) {
  if (!raw) return "Multicolor";
  const words = raw.toLowerCase().replace(/[^a-z\s-]/g, " ").split(/\s+/).filter(Boolean);
  for (const word of words) {
    if (COLOR_MAP[word]) return COLOR_MAP[word];
  }
  // Try partial match
  for (const word of words) {
    for (const [key, val] of Object.entries(COLOR_MAP)) {
      if (word.includes(key) || key.includes(word)) return val;
    }
  }
  return "Multicolor";
}

// ── EU size charts ─────────────────────────────────────────────────────────
const SIZE_CHARTS = {
  Nike:   { Men: {"3.5":"35.5","4":"36","4.5":"36.5","5":"37.5","5.5":"38","6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"45.5","12":"46","12.5":"47","13":"47.5","13.5":"48","14":"48.5","14.5":"49","15":"49.5","15.5":"50","16":"50.5","16.5":"51","17":"51.5","17.5":"52","18":"52.5"}, Women: {"5":"35.5","5.5":"36","6":"36.5","6.5":"37.5","7":"38","7.5":"38.5","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"42","10.5":"42.5","11":"43","11.5":"44","12":"44.5","12.5":"45","13":"45.5","13.5":"46","14":"47","14.5":"47.5","15":"48"} },
  Jordan: { Men: {"3":"35.5","3.5":"36","4":"36.5","4.5":"37.5","5":"37.5","5.5":"38","6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"45.5","12":"46","12.5":"47","13":"47.5","13.5":"48","14":"48.5","14.5":"49","15":"49.5","15.5":"50","16":"50.5","16.5":"51","17":"51.5","17.5":"52","18":"52.5"}, Women: {"5":"35.5","5.5":"36","6":"36.5","6.5":"37.5","7":"38","7.5":"38.5","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"42","10.5":"42.5","11":"43","11.5":"44","12":"44.5","12.5":"45","13":"45.5","13.5":"46","14":"47","14.5":"47.5","15":"48"} },
  Adidas: { Men: {"4":"36","4.5":"36 2/3","5":"37 1/3","5.5":"38","6":"38 2/3","6.5":"39 1/3","7":"40","7.5":"40 2/3","8":"41 1/3","8.5":"42","9":"42 2/3","9.5":"43 1/3","10":"44","10.5":"44 2/3","11":"45 1/3","11.5":"46","12":"46 2/3","12.5":"47 1/3","13":"48","13.5":"48 2/3","14":"49 1/3","14.5":"50","15":"50 2/3","16":"51 1/3","17":"52 2/3","18":"53 1/3","19":"54 2/3","20":"55 2/3"}, Women: {"5":"36","5.5":"36 2/3","6":"37 1/3","6.5":"38","7":"38 2/3","7.5":"39 1/3","8":"40","8.5":"40 2/3","9":"41 1/3","9.5":"42","10":"42 2/3","10.5":"43 1/3","11":"44","11.5":"44 2/3","12":"45 1/3","12.5":"46","13":"46 2/3"} },
  Reebok: { Men: {"4":"34.5","4.5":"35","5":"36","5.5":"36.5","6":"37.5","6.5":"38.5","7":"39","7.5":"40","8":"40.5","8.5":"41","9":"42","9.5":"42.5","10":"43","10.5":"44","11":"44.5","11.5":"45","12":"45.5","12.5":"46","13":"47","13.5":"48","14":"48.5","15":"50","16":"52","17":"53.5","18":"55"}, Women: {"5.5":"34.5","6":"35","6.5":"36","7":"36.5","7.5":"37.5","8":"38.5","8.5":"39","9":"40","9.5":"40.5","10":"41","10.5":"42","11":"42.5","11.5":"43","12":"44"} },
  Puma:     { Men: {"6":"38","6.5":"38.5","7":"39","7.5":"40","8":"40.5","8.5":"41","9":"42","9.5":"42.5","10":"43","10.5":"44","11":"44.5","11.5":"45","12":"46","12.5":"46.5","13":"47","14":"48.5","15":"49.5","16":"51"}, Women: {"5.5":"35.5","6":"36","6.5":"37","7":"37.5","7.5":"38","8":"38.5","8.5":"39","9":"40","9.5":"40.5","10":"41","10.5":"42","11":"42.5"} },
  Converse: { Men: {"6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"46","12":"46.5","13":"47.5","14":"49","15":"50","16":"51.5"} }
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
  else if (/converse/i.test(b)) chartBrand = "Converse";
  if (!chartBrand) return null;
  const genderText = String(gender || "").trim();
  let chartGender = null;
  if (/women|girl|female/i.test(genderText)) chartGender = "Women";
  else if (/men|boy|male/i.test(genderText)) chartGender = "Men";
  // Unisex / Both / Not defined / empty → fall back to Men's if available
  if (!chartGender) return null;
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
  invalidateShopifyCaches({ locations: true, metadata: true, products: true });
  await chrome.storage.local.remove(["shopifyToken"]);
}

async function shopifyApiRequest(path, options = {}, { rawResponse = false } = {}) {
  const token = await getAccessToken();
  if (!token) throw new Error("Not connected to Shopify");
  const { shop, apiVersion } = await getShopifyConfig();
  const url = `https://${shop}/admin/api/${apiVersion}${path}`;

  for (let attempt = 0; attempt <= SHOPIFY_API_MAX_RETRIES; attempt++) {
    const res = await enqueueShopifyApiCall(() =>
      fetch(url, {
        ...options,
        headers: {
          "X-Shopify-Access-Token": token,
          "Content-Type": "application/json",
          ...(options.headers || {})
        }
      })
    );

    if (res.status === 429 && attempt < SHOPIFY_API_MAX_RETRIES) {
      const delayMs = getRetryDelayMs(res, attempt);
      nextShopifyApiRequestAt = Math.max(nextShopifyApiRequestAt, Date.now() + delayMs);
      await sleep(delayMs);
      continue;
    }

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Shopify API ${res.status}: ${body}`);
    }

    return rawResponse ? res : res.json();
  }

  throw new Error("Shopify API 429: retry limit reached");
}

async function shopifyFetch(path, options = {}) {
  return shopifyApiRequest(path, options);
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
  if (data.product && isFreshCacheEntry(productsSnapshotCache)) {
    const baseSku = deriveBaseSkuFromVariants(data.product.variants || []);
    if (baseSku) {
      productsSnapshotCache = {
        value: [
          {
            id: data.product.id,
            title: data.product.title || "",
            status: data.product.status || "",
            sku: baseSku,
            variantSkus: (data.product.variants || []).map((variant) => String(variant?.sku || "").trim()).filter(Boolean),
            image: data.product.images?.[0]?.src || ""
          },
          ...productsSnapshotCache.value.filter((entry) => Number(entry.id) !== Number(data.product.id))
        ],
        timestamp: Date.now()
      };
    }
  }
  invalidateShopifyCaches({ metadata: true });
  return data.product;
}

export async function deleteProduct(productId) {
  await shopifyApiRequest(`/products/${productId}.json`, { method: "DELETE" }, { rawResponse: true });
  if (isFreshCacheEntry(productsSnapshotCache)) {
    productsSnapshotCache = {
      value: productsSnapshotCache.value.filter((entry) => Number(entry.id) !== Number(productId)),
      timestamp: Date.now()
    };
  }
  invalidateShopifyCaches({ metadata: true });
}

async function getProductById(productId) {
  if (!productId) return null;
  const data = await shopifyFetch(`/products/${productId}.json?fields=id,status,vendor,variants`);
  return data.product || null;
}

async function updateProductStatus(productId, status) {
  if (!productId || !status) return null;
  const data = await shopifyFetch(`/products/${productId}.json`, {
    method: "PUT",
    body: JSON.stringify({ product: { id: productId, status } })
  });
  return data.product || null;
}

async function updateProductVendor(productId, vendor) {
  if (!productId || !vendor) return null;
  const data = await shopifyFetch(`/products/${productId}.json`, {
    method: "PUT",
    body: JSON.stringify({ product: { id: productId, vendor } })
  });
  invalidateShopifyCaches({ metadata: true });
  return data.product || null;
}

async function getAllShopifyProductVendors() {
  const products = [];
  let pageInfo = null;
  let firstPage = true;
  while (firstPage || pageInfo) {
    firstPage = false;
    const path = pageInfo
      ? `/products.json?limit=250&fields=id,vendor&page_info=${pageInfo}`
      : `/products.json?limit=250&fields=id,vendor`;
    const res = await shopifyApiRequest(path, {}, { rawResponse: true });
    const link = res.headers.get("Link") || "";
    const data = await res.json();
    products.push(...(data.products || []));
    const next = link.match(/<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"/);
    pageInfo = next ? next[1] : null;
  }
  return products;
}

async function getProductsByIds(productIds, onBatch = null) {
  const ids = [...new Set((productIds || []).map((id) => Number(id)).filter(Boolean))];
  if (!ids.length) return [];
  const products = [];
  let fetchedCount = 0;
  for (const batch of chunkArray(ids, 100)) {
    const params = new URLSearchParams({
      ids: batch.join(","),
      fields: "id,variants"
    });
    const data = await shopifyFetch(`/products.json?${params.toString()}`);
    products.push(...(data.products || []));
    fetchedCount += batch.length;
    if (onBatch) onBatch(fetchedCount, ids.length);
  }
  return products;
}

function deriveBaseSkuFromVariants(variants = []) {
  const skus = variants.map((variant) => String(variant?.sku || "").trim()).filter(Boolean);
  if (!skus.length) return "";
  if (skus.length === 1) return skus[0];
  const prefixes = skus.map((sku) => {
    const idx = sku.lastIndexOf("-");
    return idx > 0 ? sku.slice(0, idx).trim() : "";
  }).filter(Boolean);
  if (prefixes.length === skus.length && prefixes.every((prefix) => prefix === prefixes[0])) {
    return prefixes[0];
  }
  return skus[0];
}

function findCachedProductBySkuPrefix(baseSku) {
  if (!baseSku || !isFreshCacheEntry(productsSnapshotCache)) return null;
  return productsSnapshotCache.value.find((product) =>
    product.sku === baseSku ||
    (product.variantSkus || []).some((sku) => sku === baseSku || sku.startsWith(`${baseSku}-`))
  ) || null;
}

async function getInventoryLevels(inventoryItemIds, locationIds) {
  const itemIds = [...new Set((inventoryItemIds || []).filter(Boolean))];
  const locIds = [...new Set((locationIds || []).filter(Boolean))];
  if (!itemIds.length || !locIds.length) return [];
  const params = new URLSearchParams({
    inventory_item_ids: itemIds.join(","),
    location_ids: locIds.join(",")
  });
  const data = await shopifyFetch(`/inventory_levels.json?${params.toString()}`);
  return data.inventory_levels || [];
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
    const res = await shopifyApiRequest(url, {}, { rawResponse: true });
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
  if (!await getAccessToken()) return null;
  let pageInfo = null;
  let firstPage = true;
  while (firstPage || pageInfo) {
    firstPage = false;
    const path = pageInfo
      ? `/products.json?limit=250&fields=id,variants&page_info=${pageInfo}`
      : `/products.json?limit=250&fields=id,variants`;
    const res = await shopifyApiRequest(path, {}, { rawResponse: true });
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

export async function getShopifyProductsSnapshot() {
  if (isFreshCacheEntry(productsSnapshotCache)) return productsSnapshotCache.value;
  const products = [];
  let pageInfo = null;
  let firstPage = true;
  while (firstPage || pageInfo) {
    firstPage = false;
    const path = pageInfo
      ? `/products.json?limit=250&fields=id,title,status,variants,images&page_info=${pageInfo}`
      : `/products.json?limit=250&fields=id,title,status,variants,images`;
    const res = await shopifyApiRequest(path, {}, { rawResponse: true });
    const link = res.headers.get("Link") || "";
    const data = await res.json();
    for (const product of (data.products || [])) {
      const baseSku = deriveBaseSkuFromVariants(product.variants || []);
      products.push({
        id: product.id,
        title: product.title || "",
        status: product.status || "",
        sku: baseSku || "",
        variantSkus: (product.variants || []).map((variant) => String(variant?.sku || "").trim()).filter(Boolean),
        image: product.images?.[0]?.src || ""
      });
    }
    const next = link.match(/<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"/);
    pageInfo = next ? next[1] : null;
  }
  productsSnapshotCache = { value: products, timestamp: Date.now() };
  return products;
}

export async function getFullyOutOfStockProductIds(productIds, onProgress = null) {
  const ids = [...new Set((productIds || []).map((id) => Number(id)).filter(Boolean))];
  if (!ids.length) return [];
  let lastReported = -1;
  const reportProgress = (current) => {
    if (!onProgress) return;
    const safeCurrent = Math.max(0, Math.min(ids.length, Math.round(current)));
    if (safeCurrent === lastReported) return;
    lastReported = safeCurrent;
    onProgress(safeCurrent, ids.length);
  };
  reportProgress(0);

  const locations = await getLocations().catch(() => []);
  const locationIds = locations.map((location) => location.id).filter(Boolean);
  const products = await getProductsByIds(ids, (fetchedCount, totalCount) => {
    reportProgress((fetchedCount / totalCount) * totalCount * 0.35);
  });
  const productsById = new Map(products.map((product) => [Number(product.id), product]));
  const inventoryItemIds = products.flatMap((product) =>
    (product?.variants || []).map((variant) => variant.inventory_item_id).filter(Boolean)
  );
  const totalsByInventoryItemId = new Map();

  if (locationIds.length && inventoryItemIds.length) {
    const inventoryBatches = chunkArray([...new Set(inventoryItemIds)], 50);
    for (let batchIndex = 0; batchIndex < inventoryBatches.length; batchIndex++) {
      const batch = inventoryBatches[batchIndex];
      const inventoryLevels = await getInventoryLevels(batch, locationIds);
      inventoryLevels.forEach((level) => {
        const inventoryItemId = level.inventory_item_id;
        const current = totalsByInventoryItemId.get(inventoryItemId) || 0;
        totalsByInventoryItemId.set(inventoryItemId, current + Number(level.available ?? 0));
      });
      reportProgress((0.35 + ((batchIndex + 1) / inventoryBatches.length) * 0.35) * ids.length);
    }
  }

  const outOfStockIds = [];
  for (let index = 0; index < ids.length; index++) {
    const productId = ids[index];
    reportProgress((0.7 + ((index + 1) / ids.length) * 0.3) * ids.length);
    const product = productsById.get(productId);
    const variants = product?.variants || [];
    if (!variants.length) continue;
    const isFullyOutOfStock = variants.every((variant) => {
      if (!locationIds.length) return Number(variant.inventory_quantity ?? 0) <= 0;
      const total = totalsByInventoryItemId.get(variant.inventory_item_id);
      const fallbackQty = Number(variant.inventory_quantity ?? 0);
      return (total ?? fallbackQty) <= 0;
    });
    if (isFullyOutOfStock) outOfStockIds.push(productId);
  }

  reportProgress(ids.length);
  return outOfStockIds;
}

export async function deleteShopifyProducts(productIds) {
  const ids = [...new Set((productIds || []).filter(Boolean))];
  await Promise.all(ids.map((productId) => deleteProduct(productId)));
}

export function clearShopifyProductsSnapshotCache() {
  invalidateShopifyCaches({ products: true });
}

export async function updateShopifyForMonitor(monitor, liveData) {
  try {
    if (!await isConnected()) return;
    const baseSku = monitor.productData?.sku;
    const canonicalBrand = canonicalizeBrand(monitor.productData?.brand || "");
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
    const shouldSetDraft = inStock.size === 0 && outOfStock.size > 0;
    const shouldSetActive = inStock.size > 0;
    const locations = await getLocations();
    const locationIds = locations.map((location) => location.id).filter(Boolean);
    const prefix = `${baseSku}-`;
    const errors = [];
    const inventoryLevels = await getInventoryLevels(
      product.variants.map((variant) => variant.inventory_item_id),
      locationIds
    ).catch(() => []);
    const inventoryLevelMap = new Map(
      inventoryLevels.map((level) => [`${level.inventory_item_id}:${level.location_id}`, Number(level.available ?? 0)])
    );

    await Promise.all(product.variants.map(async (variant) => {
      const tasks = [];
      const currentPrice = variant.price != null ? String(variant.price) : null;
      const currentCompareAt = variant.compare_at_price != null ? String(variant.compare_at_price) : null;
      const needsPriceUpdate =
        price !== null &&
        (currentPrice !== price || (hasCompareAt && currentCompareAt !== compareAt));
      if (needsPriceUpdate) {
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
      if (locationIds.length && variant.sku?.startsWith(prefix)) {
        const usSize = variant.sku.slice(prefix.length);
        const desiredQty = inStock.has(usSize) ? 10 : (outOfStock.has(usSize) ? 0 : null);
        if (desiredQty !== null) {
          for (const locationId of locationIds) {
            const currentQty = inventoryLevelMap.get(`${variant.inventory_item_id}:${locationId}`);
            if (currentQty === desiredQty) continue;
            tasks.push(
              setInventoryLevel(variant.inventory_item_id, locationId, desiredQty).catch((error) => {
                const label = desiredQty > 0 ? "In-stock" : "Out-of-stock";
                errors.push(`${label} sync failed for ${usSize} at location ${locationId}: ${formatShopifyError(error)}`);
              })
            );
          }
        }
      }
      await Promise.all(tasks);
    }));

    if (canonicalBrand && canonicalBrand !== product.vendor) {
      try {
        const updatedProduct = await updateProductVendor(product.id, canonicalBrand);
        product.vendor = updatedProduct?.vendor || canonicalBrand;
      } catch (error) {
        errors.push(`Vendor update failed (${canonicalBrand}) for ${baseSku || product.id}: ${formatShopifyError(error)}`);
      }
    }

    const desiredStatus = shouldSetDraft ? "draft" : (shouldSetActive ? "active" : null);
    if (desiredStatus && product.status !== desiredStatus) {
      try {
        const updatedProduct = await updateProductStatus(product.id, desiredStatus);
        if (updatedProduct?.status) {
          product.status = updatedProduct.status;
        } else {
          product.status = desiredStatus;
        }
      } catch (error) {
        errors.push(`Product status update failed (${desiredStatus}) for ${baseSku || product.id}: ${formatShopifyError(error)}`);
      }
    }

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

async function setImportHistory(history) {
  await chrome.storage.local.set({ [IMPORT_HISTORY_KEY]: history });
}

function normalizeImportHistory(history) {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  const seenProductIds = new Set();
  const result = [];
  for (const entry of Array.isArray(history) ? history : []) {
    const productId = Number(entry?.shopifyProductId);
    const importedAt = entry?.importedAt ? new Date(entry.importedAt).getTime() : 0;
    if (!productId || !importedAt || importedAt <= cutoff) continue;
    if (seenProductIds.has(productId)) continue;
    seenProductIds.add(productId);
    result.push(entry);
  }
  return result;
}

async function pushImportHistory(entry) {
  const history = normalizeImportHistory(await getImportHistory()).filter(
    (item) => Number(item?.shopifyProductId) !== Number(entry?.shopifyProductId)
  );
  history.unshift(entry);
  await setImportHistory(history);
}

export async function getRecentImports() {
  const history = normalizeImportHistory(await getImportHistory());
  await setImportHistory(history);
  return history;
}

export async function undoLastImport() {
  const history = normalizeImportHistory(await getImportHistory());
  const idx = history.findIndex(Boolean);
  if (idx === -1) throw new Error("No imports in the last 24 hours to undo");

  const entry = history[idx];

  // Delete from Shopify
  await deleteProduct(entry.shopifyProductId);

  // Restore monitor via background message
  await chrome.runtime.sendMessage({ type: "restore-monitor", monitor: entry.monitorData });

  // Remove from history
  history.splice(idx, 1);
  await setImportHistory(history);

  return entry;
}

export async function primeImportCaches() {
  await Promise.all([
    getShopifyMetadata().catch(() => ({ vendors: [], types: [], tags: [] })),
    getLocations().catch(() => []),
    getShopifyProductsSnapshot().catch(() => [])
  ]);
}

export async function syncMonitorBrandsToShopify(monitors = []) {
  if (!await isConnected()) return { updated: 0, checked: 0 };
  const mapping = await getSkuMapping();
  let updated = 0;
  let checked = 0;

  for (const monitor of monitors) {
    const canonicalBrand = canonicalizeBrand(monitor?.productData?.brand || "");
    const baseSku = String(monitor?.productData?.sku || "").trim();
    const productId = Number(monitor?.shopifyProductId || mapping[baseSku] || 0);
    if (!canonicalBrand || !productId) continue;
    checked += 1;
    try {
      const product = await getProductById(productId);
      if (product?.vendor !== canonicalBrand) {
        await updateProductVendor(productId, canonicalBrand);
        updated += 1;
      }
    } catch (_) {}
  }

  return { updated, checked };
}

export async function normalizeAllShopifyVendors() {
  if (!await isConnected()) return { updated: 0, checked: 0 };
  const products = await getAllShopifyProductVendors();
  let updated = 0;
  let checked = 0;

  for (const product of products) {
    const productId = Number(product?.id);
    const currentVendor = String(product?.vendor || "").trim();
    if (!productId || !currentVendor) continue;
    const canonicalVendor = canonicalizeBrand(currentVendor);
    checked += 1;
    if (!canonicalVendor || canonicalVendor === currentVendor) continue;
    try {
      await updateProductVendor(productId, canonicalVendor);
      updated += 1;
    } catch (_) {}
  }

  invalidateShopifyCaches({ metadata: true, products: true });
  return { updated, checked };
}

// ── Main import ────────────────────────────────────────────────────────────
export async function importMonitorProduct(monitor, context = {}) {
  const pd = monitor.productData || {};
  const live = monitor.lastExtractedData || {};

  // Fetch existing Shopify metadata to match exact casing
  const meta = context.meta || await getShopifyMetadata().catch(() => ({ vendors: [], types: [], tags: [] }));
  context.meta = meta;

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
    const mapping = context.mapping || await getSkuMapping();
    context.mapping = mapping;
    let existing = null;
    if (mapping[baseSku]) {
      existing = await getProductById(mapping[baseSku]).catch(() => null);
    }
    if (!existing) {
      const cached = findCachedProductBySkuPrefix(baseSku);
      if (cached?.id) existing = { id: cached.id };
    }
    if (!existing) {
      existing = await findProductBySkuPrefix(baseSku);
    }
    if (existing?.id) {
      throw new Error(`Already imported to Shopify for SKU ${baseSku}`);
    }
  }
  const brand = canonicalizeBrand(pd.brand || "");
  const gender = pd.gender || "Not defined";
  const genderDisplay = pd.genderDisplay || gender;
  const sizeGender = pd.extractedGender || "";
  const productType = matchExisting(pd.type || "", meta.types);
  const normalizedColor = normalizeColor(pd.color);

  // Always keep the extracted color as a Shopify tag.
  // If a normalized color also exists, include it too unless it's the same tag ignoring case.
  const genderTags =
    genderDisplay === "Both"
      ? ["Men", "Women"]
      : (genderDisplay && genderDisplay !== "Not defined" ? [genderDisplay] : []);
  const rawTags = [...genderTags, normalizedColor]
    .filter(Boolean)
    .filter((tag, index, arr) =>
      arr.findIndex((value) => String(value).toLowerCase() === String(tag).toLowerCase()) === index
    );
  const tags = rawTags.map((tag) => matchExisting(tag, meta.tags));

  const variants = allSizesUS.length
    ? allSizesUS.map(usSize => {
        const euSize = getEuSize(usSize, brand, sizeGender);
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
    const mapping = context.mapping || await getSkuMapping();
    context.mapping = mapping;
    mapping[baseSku] = created.id;
    await saveSkuMapping(mapping);
  }

  // Set inventory at primary location
  const locations = context.locations || await getLocations();
  context.locations = locations;
  if (locations.length && created.variants) {
      await Promise.all(created.variants.map(async (variant) => {
      const usSize = allSizesUS.find(us => {
        const eu = getEuSize(us, brand, sizeGender);
        return eu ? String(eu) === variant.option1 : us === variant.option1;
      });
      const qty = usSize
        ? (inStock.has(usSize) ? 10 : 0)
        : (variant.inventory_quantity ?? 10);
      if (qty <= 0) return;
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

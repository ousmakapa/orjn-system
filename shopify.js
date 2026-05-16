import { addLog, canonicalizeBrand, deriveShoesType, SHOES_TYPE_METAFIELD_ENABLED_KEY } from "./shared.js";

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
const SHOPIFY_API_MIN_INTERVAL_MS = 250;
const SHOPIFY_API_MAX_RETRIES = 4;
let envConfigPromise = null;
let locationsCache = null;
let metadataCache = null;
let productsSnapshotCache = null;
let productsSnapshotPromise = null;
let shopifyApiQueue = Promise.resolve();
let nextShopifyApiRequestAt = 0;
let productFilterMetafieldDefinitionsPromise = null;
let shoesTypeMetafieldEnabledCache = null;

chrome.storage?.onChanged?.addListener((changes, areaName) => {
  if (areaName === "local" && changes[SHOES_TYPE_METAFIELD_ENABLED_KEY]) {
    shoesTypeMetafieldEnabledCache = null;
  }
});

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

async function runOptionalShopifyStep(label, task, monitor = null) {
  try {
    return await task();
  } catch (error) {
    const details = [`${label}: ${formatShopifyError(error)}`];
    if (monitor) await logShopifyError(monitor, details);
    return null;
  }
}

function deferOptionalShopifyStep(label, task, monitor = null, delayMs = 15000) {
  setTimeout(() => {
    runOptionalShopifyStep(label, task, monitor);
  }, delayMs);
}

function invalidateShopifyCaches({ locations = false, metadata = false, products = false, definitions = false } = {}) {
  if (locations) locationsCache = null;
  if (metadata) { metadataCache = null; productFilterMetafieldDefinitionsPromise = null; }
  if (products) { productsSnapshotCache = null; productsSnapshotPromise = null; }
  if (definitions) productFilterMetafieldDefinitionsPromise = null;
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
  const normalized = raw.toLowerCase().replace(/[^a-z0-9\s-]/g, " ");
  const words = normalized.split(/\s+/).filter(Boolean);
  const compact = normalized.replace(/[^a-z0-9]/g, "");
  for (const word of words) {
    if (COLOR_MAP[word]) return COLOR_MAP[word];
  }
  for (const [key, val] of Object.entries(COLOR_MAP)) {
    const keyCompact = key.replace(/[^a-z0-9]/g, "");
    if (keyCompact.length >= 6 && compact.includes(keyCompact)) return val;
  }
  return "Multicolor";
}

// ── EU size charts ─────────────────────────────────────────────────────────
const SIZE_CHARTS = {
  Nike:   { Men: {"3.5":"35.5","4":"36","4.5":"36.5","5":"37.5","5.5":"38","6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"45.5","12":"46","12.5":"47","13":"47.5","13.5":"48","14":"48.5","14.5":"49","15":"49.5","15.5":"50","16":"50.5","16.5":"51","17":"51.5","17.5":"52","18":"52.5"}, Women: {"5":"35.5","5.5":"36","6":"36.5","6.5":"37.5","7":"38","7.5":"38.5","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"42","10.5":"42.5","11":"43","11.5":"44","12":"44.5","12.5":"45","13":"45.5","13.5":"46","14":"47","14.5":"47.5","15":"48"} },
  Jordan: { Men: {"3":"35.5","3.5":"36","4":"36.5","4.5":"37.5","5":"37.5","5.5":"38","6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"45.5","12":"46","12.5":"47","13":"47.5","13.5":"48","14":"48.5","14.5":"49","15":"49.5","15.5":"50","16":"50.5","16.5":"51","17":"51.5","17.5":"52","18":"52.5"}, Women: {"5":"35.5","5.5":"36","6":"36.5","6.5":"37.5","7":"38","7.5":"38.5","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"42","10.5":"42.5","11":"43","11.5":"44","12":"44.5","12.5":"45","13":"45.5","13.5":"46","14":"47","14.5":"47.5","15":"48"} },
  Adidas: { Men: {"4":"36","4.5":"36 2/3","5":"37 1/3","5.5":"38","6":"38 2/3","6.5":"39 1/3","7":"40","7.5":"40 2/3","8":"41 1/3","8.5":"42","9":"42 2/3","9.5":"43 1/3","10":"44","10.5":"44 2/3","11":"45 1/3","11.5":"46","12":"46 2/3","12.5":"47 1/3","13":"48","13.5":"48 2/3","14":"49 1/3","14.5":"50","15":"50 2/3","16":"51 1/3","17":"52 2/3","18":"53 1/3","19":"54 2/3","20":"55 2/3"}, Women: {"5":"36","5.5":"36 2/3","6":"37 1/3","6.5":"38","7":"38 2/3","7.5":"39 1/3","8":"40","8.5":"40 2/3","9":"41 1/3","9.5":"42","10":"42 2/3","10.5":"43 1/3","11":"44","11.5":"44 2/3","12":"45 1/3","12.5":"46","13":"46 2/3"} },
  Asics: { Men: {"4":"36","4.5":"37","5":"37.5","5.5":"38","6":"39","6.5":"39.5","7":"40","7.5":"40.5","8":"41.5","8.5":"42","9":"42.5","9.5":"43.5","10":"44","10.5":"44.5","11":"45","11.5":"46","12":"47"}, Women: {"4":"34.5","4.5":"35","5":"35.5","5.5":"36","6":"37","6.5":"37.5","7":"38","7.5":"39","8":"39.5","8.5":"40","9":"40.5","9.5":"41.5","10":"42","10.5":"42.5","11":"43.5","11.5":"44","12":"44.5","12.5":"45","13":"46","14":"47"} },
  "New Balance": { Men: {"4":"36","4.5":"37","5":"37.5","5.5":"38","6":"38.5","6.5":"39.5","7":"40","7.5":"40.5","8":"41.5","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"45.5","12":"46.5","12.5":"47","13":"47.5","14":"49","15":"50","16":"51","17":"52","18":"53"}, Women: {"4":"34","5":"35","5.5":"36","6":"36.5","6.5":"37","7":"37.5","7.5":"38","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"41.5","10.5":"42.5","11":"43","11.5":"43.5","12":"44","13":"45.5"} },
  Reebok: { Men: {"4":"34.5","4.5":"35","5":"36","5.5":"36.5","6":"37.5","6.5":"38.5","7":"39","7.5":"40","8":"40.5","8.5":"41","9":"42","9.5":"42.5","10":"43","10.5":"44","11":"44.5","11.5":"45","12":"45.5","12.5":"46","13":"47","13.5":"48","14":"48.5","15":"50","16":"52","17":"53.5","18":"55"}, Women: {"5.5":"34.5","6":"35","6.5":"36","7":"36.5","7.5":"37.5","8":"38.5","8.5":"39","9":"40","9.5":"40.5","10":"41","10.5":"42","11":"42.5","11.5":"43","12":"44"} },
  Puma:     { Men: {"6":"38","6.5":"38.5","7":"39","7.5":"40","8":"40.5","8.5":"41","9":"42","9.5":"42.5","10":"43","10.5":"44","11":"44.5","11.5":"45","12":"46","12.5":"46.5","13":"47","14":"48.5","15":"49.5","16":"51"}, Women: {"5.5":"35.5","6":"36","6.5":"37","7":"37.5","7.5":"38","8":"38.5","8.5":"39","9":"40","9.5":"40.5","10":"41","10.5":"42","11":"42.5"} },
  Converse: { Men: {"6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"46","12":"46.5","13":"47.5","14":"49","15":"50","16":"51.5"} },
  On: { Men: {"7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"46","12":"47","12.5":"47.5","13":"48","14":"49"}, Women: {"5":"36","5.5":"36.5","6":"37","6.5":"37.5","7":"38","7.5":"38.5","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"42","10.5":"42.5","11":"43"} },
  Hoka: { Men: {"5":"37 1/3","5.5":"38","6":"38 2/3","6.5":"39 1/3","7":"40","7.5":"40 2/3","8":"41 1/3","8.5":"42","9":"42 2/3","9.5":"43 1/3","10":"44","10.5":"44 2/3","11":"45 1/3","11.5":"46","12":"46 2/3","12.5":"47 1/3","13":"48","13.5":"48 2/3","14":"49 1/3","14.5":"50","15":"50 2/3"}, Women: {"5":"36","5.5":"36 2/3","6":"37 1/3","6.5":"38","7":"38 2/3","7.5":"39 1/3","8":"40","8.5":"40 2/3","9":"41 1/3","9.5":"42","10":"42 2/3","10.5":"43 1/3","11":"44","11.5":"44 2/3","12":"45 1/3","12.5":"46","13":"46 2/3","13.5":"47 1/3","14":"48","14.5":"48 2/3","15":"49 1/3","15.5":"50"} },
  "Way of Wade": { Men: {"4.5":"36 1/3","5":"37","5.5":"37 2/3","6":"38 1/3","6.5":"39","7":"39 2/3","7.5":"40 1/3","8":"41","8.5":"41 2/3","9":"42 1/3","9.5":"43","10":"43 2/3","10.5":"44 1/3","11":"45","11.5":"45 2/3","12":"46 1/3","12.5":"47","13":"47 2/3","13.5":"48 1/3","14":"49","14.5":"49 2/3","15":"50 1/3"} }
};

function normalizeUsSizeChartKey(value) {
  const match = String(value || "").replace(",", ".").match(/\d+(?:\.\d+)?/);
  if (!match) return "";
  const n = Number(match[0]);
  if (!Number.isFinite(n) || n <= 0 || n > 30) return "";
  return Number.isInteger(n) ? String(n) : String(n).replace(/0+$/, "").replace(/\.$/, "");
}

export function getEuSize(usSize, brand, gender) {
  if (!usSize) return null;
  const rawLabel = String(usSize).replace(/\s+/g, " ").trim();
  const b = String(brand || "").toLowerCase();
  const isWayOfWadeBrand = /way\s*of\s*wade|li[\s-]*ning|lining/i.test(b);
  const isFractionalEuBrand = /adidas|yeezy|hoka|way\s*of\s*wade|li[\s-]*ning|lining/i.test(b);
  if (/^(?:EU|EUR)?\s*\d{2}(?:[.,]\d+)?(?:\s+(?:1\/2|[12]\/3))?$/i.test(rawLabel)) {
    const euLabel = rawLabel.replace(/^(?:EU|EUR)\s*/i, "").replace(",", ".").trim();
    const n = parseFloat(euLabel);
    if (isFractionalEuBrand && n > 30 && /\d+[.,]\d+/.test(rawLabel) && !/\b[12]\/3\b/.test(rawLabel)) return null;
    if (!isWayOfWadeBrand && Number.isFinite(n) && n > 30 && n < 60) return euLabel;
  }
  if (!brand) return null;
  let chartBrand = null;
  if (/nike/i.test(b)) chartBrand = "Nike";
  else if (/jordan/i.test(b)) chartBrand = "Jordan";
  else if (/adidas|yeezy/i.test(b)) chartBrand = "Adidas";
  else if (/asics/i.test(b)) chartBrand = "Asics";
  else if (/new\s*balance/i.test(b)) chartBrand = "New Balance";
  else if (/reebok/i.test(b)) chartBrand = "Reebok";
  else if (/puma/i.test(b)) chartBrand = "Puma";
  else if (/converse/i.test(b)) chartBrand = "Converse";
  else if (/^on(?:\s+cloud|\s+running|\s+cloud\s+running)?$/i.test(b)) chartBrand = "On";
  else if (/hoka/i.test(b)) chartBrand = "Hoka";
  else if (/way\s*of\s*wade|li[\s-]*ning|lining/i.test(b)) chartBrand = "Way of Wade";
  if (!chartBrand) return null;
  const genderText = String(gender || "").trim();
  let chartGender = null;
  if (/women|girl|female/i.test(genderText)) chartGender = "Women";
  else if (/men|boy|male/i.test(genderText)) chartGender = "Men";
  // Unisex / Both / Not defined / empty → fall back to Men's if available
  if (!chartGender && SIZE_CHARTS[chartBrand]?.Men) chartGender = "Men";
  if (!chartGender) return null;
  const chart = SIZE_CHARTS[chartBrand]?.[chartGender];
  if (!chart) return null;
  const sizeKey = normalizeUsSizeChartKey(usSize);
  const n = Number(sizeKey);
  if (isNaN(n) || n <= 0 || n > 30) return null;
  return chart[sizeKey] || null;
}

function buildSizeEntriesFromLiveData(liveData = {}, brand = "", gender = "") {
  const inStock = new Set((liveData.inStock || []).map((size) => String(size)));
  const outOfStock = new Set((liveData.outOfStock || []).map((size) => String(size)));
  const allSizesUS = [...new Set([...inStock, ...outOfStock])];
  const usedEuLabels = new Set();
  const sizeEntries = [];
  for (const usSize of allSizesUS) {
    const usLabel = String(usSize);
    const euSize = getEuSize(usLabel, brand, gender);
    if (!euSize) continue;
    const option1 = String(euSize);
    const optionKey = option1.toLowerCase();
    if (usedEuLabels.has(optionKey)) continue;
    usedEuLabels.add(optionKey);
    sizeEntries.push({
      usSize: usLabel,
      euSize: option1,
      preferredLabel: option1,
      option1
    });
  }
  return sizeEntries;
}

function normalizeSizeLabel(value) {
  return String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
}

function normalizeSizeNumber(value) {
  const text = String(value || "").replace(",", ".").trim();
  const match = text.match(/\d+(?:\.\d+)?/);
  if (!match) return "";
  return String(parseFloat(match[0]));
}

function extractExplicitUsSizeNumber(value) {
  const text = String(value || "").replace(",", ".").trim();
  const match = text.match(/(?:^|[\/\s-])US\s*(\d+(?:\.\d+)?)/i);
  if (!match) return "";
  return String(parseFloat(match[1]));
}

function isLikelyRawUsSizeLabel(value) {
  const text = cleanFilterValue(value);
  if (!text || /^default title$/i.test(text)) return false;
  const match = text.match(/\d+(?:[.,]\d+)?/);
  if (!match) return false;
  const n = Number(match[0].replace(",", "."));
  if (!Number.isFinite(n) || n <= 0 || n > 30) return false;
  return /^(?:us\s*)?\d+(?:[.,]\d+)?(?:\s*(?:us|m|w|y|c|kids?|men'?s?|women'?s?))?$/i.test(text);
}

function normalizeSku(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeLookupText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/['’]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function buildSkuSuffix(value, fallback = "") {
  const suffix = String(value || fallback || "")
    .trim()
    .replace(/^us\s+/i, "")
    .replace(/[^a-z0-9.]+/gi, "-")
    .replace(/^-+|-+$/g, "");
  return suffix || String(fallback || "").trim();
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

async function shopifyGraphQL(query, variables = {}) {
  const data = await shopifyFetch("/graphql.json", {
    method: "POST",
    body: JSON.stringify({ query, variables })
  });
  if (Array.isArray(data.errors) && data.errors.length) {
    throw new Error(data.errors.map((error) => error.message || String(error)).join(" | "));
  }
  return data.data || {};
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
            vendor: data.product.vendor || "",
            productType: data.product.product_type || "",
            sku: baseSku,
            variantSkus: (data.product.variants || []).map((variant) => {
              const sku = String(variant?.sku || "").trim();
              if (!sku) return "";
              return sku === baseSku || !sku.startsWith(`${baseSku}-`) ? sku : sku.slice(baseSku.length + 1);
            }).filter(Boolean),
            rawVariantSkus: (data.product.variants || []).map((variant) => String(variant?.sku || "").trim()).filter(Boolean),
            variantOptions: (data.product.variants || []).map((variant) => String(variant?.option1 || "").trim()).filter(Boolean),
            variantDetails: (data.product.variants || []).map((variant) => ({
              id: variant.id,
              option1: String(variant?.option1 || "").trim(),
              sku: String(variant?.sku || "").trim(),
              title: String(variant?.title || "").trim()
            })),
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
  const data = await shopifyFetch(`/products/${productId}.json?fields=id,title,body_html,status,vendor,product_type,tags,variants`);
  return data.product || null;
}

async function updateProductStatus(productId, status) {
  if (!productId || !status) return null;
  const data = await shopifyFetch(`/products/${productId}.json`, {
    method: "PUT",
    body: JSON.stringify({ product: { id: productId, status } })
  });
  invalidateShopifyCaches({ products: true });
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

async function updateProductCoreFields(productId, product, { invalidateProducts = true } = {}) {
  if (!productId || !product) return null;
  const data = await shopifyFetch(`/products/${productId}.json`, {
    method: "PUT",
    body: JSON.stringify({ product: { id: productId, ...product } })
  });
  invalidateShopifyCaches({ metadata: true, products: invalidateProducts });
  return data.product || null;
}

async function updateVariantFields(variantId, fields = {}) {
  if (!variantId || !fields || !Object.keys(fields).length) return null;
  const data = await shopifyFetch(`/variants/${variantId}.json`, {
    method: "PUT",
    body: JSON.stringify({ variant: { id: variantId, ...fields } })
  });
  invalidateShopifyCaches({ products: true });
  return data.variant || null;
}

async function updateVariantSku(variantId, sku) {
  return updateVariantFields(variantId, { sku });
}

async function deleteVariant(variantId) {
  if (!variantId) return null;
  await shopifyApiRequest(`/variants/${variantId}.json`, { method: "DELETE" }, { rawResponse: true });
  invalidateShopifyCaches({ products: true });
  return true;
}

export async function deleteShopifyVariantsByIds(variantIds = []) {
  const ids = [...new Set((variantIds || []).map((id) => Number(id)).filter(Boolean))];
  if (!ids.length) return { deleted: 0 };
  await Promise.all(ids.map((id) => deleteVariant(id)));
  invalidateShopifyCaches({ products: true });
  return { deleted: ids.length };
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

export async function getProductsByIds(productIds, onBatch = null) {
  const ids = [...new Set((productIds || []).map((id) => Number(id)).filter(Boolean))];
  if (!ids.length) return [];
  const products = [];
  let fetchedCount = 0;
  for (const batch of chunkArray(ids, 100)) {
    const params = new URLSearchParams({
      ids: batch.join(","),
      fields: "id,title,vendor,product_type,tags,variants"
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
    (product.rawVariantSkus || []).some((sku) => sku === baseSku || sku.startsWith(`${baseSku}-`)) ||
    (product.variantSkus || []).some((sku) => sku === baseSku || sku.startsWith(`${baseSku}-`))
  ) || null;
}

function findCachedProductByMonitorIdentity(monitor) {
  if (!isFreshCacheEntry(productsSnapshotCache)) return null;
  const pd = monitor?.productData || {};
  const title = normalizeLookupText(pd.name || monitor?.name || "");
  if (!title) return null;
  const brand = canonicalizeBrand(pd.brand || "");
  const matches = productsSnapshotCache.value.filter((product) => {
    if (normalizeLookupText(product.title) !== title) return false;
    if (!brand) return true;
    return canonicalizeBrand(product.vendor || "") === brand;
  });
  return matches.length === 1 ? matches[0] : null;
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

export async function getAllProductMetafieldDefinitions() {
  let allNodes = [];
  let cursor = null;
  let hasNextPage = true;
  while (hasNextPage) {
    const data = await shopifyGraphQL(`
      query GetMetafieldDefs($cursor: String) {
        metafieldDefinitions(first: 200, ownerType: PRODUCT, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          nodes { id namespace key name description type { name } }
        }
      }
    `, { cursor });
    const page = data.metafieldDefinitions || {};
    allNodes = allNodes.concat(page.nodes || []);
    hasNextPage = page.pageInfo?.hasNextPage || false;
    cursor = page.pageInfo?.endCursor || null;
  }
  return allNodes;
}

// Match a value case-insensitively against an existing Shopify list, return exact casing if found
function matchExisting(value, list) {
  if (!value || !list.length) return value;
  const lower = value.toLowerCase();
  return list.find(v => v.toLowerCase() === lower) || value;
}

// ── SKU → Shopify product ID mapping ──────────────────────────────────────
function cleanFilterValue(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function getShoesTypeToggleKey(typeOrCombined = "", model = "") {
  if (model) {
    const t = cleanFilterValue(typeOrCombined).toLowerCase();
    const m = cleanFilterValue(model).toLowerCase();
    return t && m ? `${t}||${m}` : (m || t);
  }
  return cleanFilterValue(typeOrCombined).toLowerCase();
}

async function getShoesTypeMetafieldEnabledSet() {
  if (shoesTypeMetafieldEnabledCache) return shoesTypeMetafieldEnabledCache;
  const stored = await chrome.storage.local.get(SHOES_TYPE_METAFIELD_ENABLED_KEY).catch(() => ({}));
  const names = Array.isArray(stored?.[SHOES_TYPE_METAFIELD_ENABLED_KEY])
    ? stored[SHOES_TYPE_METAFIELD_ENABLED_KEY]
    : [];
  shoesTypeMetafieldEnabledCache = new Set(names.map(getShoesTypeToggleKey).filter(Boolean));
  return shoesTypeMetafieldEnabledCache;
}


function getGenderFilterValues(pd = {}) {
  const gender = cleanFilterValue(pd.gender || "Not defined");
  const genderDisplay = cleanFilterValue(pd.genderDisplay || gender);
  if (!genderDisplay || genderDisplay === "Not defined") return [];
  if (genderDisplay === "Both" || /unisex/i.test(genderDisplay) || /men\s*[,/&+]\s*women|women\s*[,/&+]\s*men/i.test(genderDisplay)) return ["Men", "Women"];
  return [genderDisplay];
}

async function buildProductFilterData(pd = {}, meta = {}, monitorUrl = "") {
  const brand = canonicalizeBrand(pd.brand || "");
  const productType = matchExisting(cleanFilterValue(pd.type), meta.types || []);
  const rawColor = cleanFilterValue(pd.color || pd.colorFinal || pd.colorRaw || "");
  const normalizedColor = rawColor ? normalizeColor(rawColor) : "";
  const shoesType = deriveShoesType(pd);
  const shoesTypeModel = cleanFilterValue(typeof shoesType === "string" ? shoesType : shoesType?.model || "");
  const isFootball = /^football$/i.test(String(pd.type || ""));
  const enabledSet = await getShoesTypeMetafieldEnabledSet();
  const genderValues = getGenderFilterValues(pd);
  const colorValues = normalizedColor ? [normalizedColor] : [];

  let metafields;
  let disabledShoesTypeMetafield = false;
  let disabledCleatsMetafield = false;

  if (isFootball) {
    const cleatsValue = pd.cleatType || detectDsgCleatType(monitorUrl) || detectCleatTypeFromName(pd.name) || shoesTypeModel || "Unknown";
    const cleatsEnabled = enabledSet.has(getShoesTypeToggleKey("Football", cleatsValue));
    metafields = [
      genderValues.length ? { namespace: "custom", key: "gender", value: genderValues.join(", "), type: "single_line_text_field" } : null,
      colorValues.length ? { namespace: "custom", key: "color", value: colorValues[0], type: "single_line_text_field" } : null,
      productType ? { namespace: "custom", key: "product_type", value: productType, type: "single_line_text_field" } : null,
      cleatsEnabled ? { namespace: "custom", key: "cleats", value: cleatsValue, type: "single_line_text_field" } : null
    ].filter(Boolean);
    disabledCleatsMetafield = !cleatsEnabled;
  } else {
    const shoesTypeEnabled = shoesTypeModel && enabledSet.has(getShoesTypeToggleKey(pd.type || "", shoesTypeModel));
    metafields = [
      genderValues.length ? { namespace: "custom", key: "gender", value: genderValues.join(", "), type: "single_line_text_field" } : null,
      colorValues.length ? { namespace: "custom", key: "color", value: colorValues[0], type: "single_line_text_field" } : null,
      productType ? { namespace: "custom", key: "product_type", value: productType, type: "single_line_text_field" } : null,
      shoesTypeEnabled ? { namespace: "custom", key: "shoes_type", value: shoesTypeModel, type: "single_line_text_field" } : null
    ].filter(Boolean);
    disabledShoesTypeMetafield = !!shoesTypeModel && !shoesTypeEnabled;
  }

  return { brand, productType, metafields, disabledShoesTypeMetafield, disabledCleatsMetafield, isFootball };
}


function buildProductMetadataPayload(monitor = {}, product = {}, filterData = {}, { includeContent = false } = {}) {
  const pd = monitor.productData || {};
  const payload = {
    vendor: filterData.brand || product.vendor || "",
    product_type: filterData.productType || product.product_type || ""
  };
  if (includeContent) {
    payload.title = pd.name || monitor.name || product.title || "Imported Product";
    payload.body_html = pd.description ? `<p>${pd.description}</p>` : (product.body_html || "");
  }
  return payload;
}

function findSizeEntryForVariant(variant, index, sizeEntries = []) {
  if (!sizeEntries.length) return null;
  const skuSuffix = String(variant?.sku || "").split("-").pop();
  const skuNumber = normalizeSizeNumber(skuSuffix);
  if (skuNumber) {
    const bySku = sizeEntries.find((entry) => normalizeSizeNumber(entry.usSize) === skuNumber);
    if (bySku) return bySku;
  }
  const option = normalizeSizeLabel(variant?.option1);
  if (option) {
    const byOption = sizeEntries.find((entry) => normalizeSizeLabel(entry.option1) === option);
    if (byOption) return byOption;
    const byPreferred = sizeEntries.find((entry) => normalizeSizeLabel(entry.preferredLabel) === option);
    if (byPreferred) return byPreferred;
    const optionNumber = extractExplicitUsSizeNumber(option) || normalizeSizeNumber(option);
    const byUsSize = sizeEntries.find((entry) => normalizeSizeNumber(entry.usSize) === optionNumber);
    if (byUsSize) return byUsSize;
  }
  return sizeEntries[index] || null;
}

async function repairProductVariantSkus(product, monitor, liveData = {}) {
  const pd = monitor?.productData || {};
  const baseSku = cleanFilterValue(pd.sku);
  const variants = product?.variants || [];
  if (!baseSku || !variants.length) return { updated: 0, errors: [] };

  const brand = canonicalizeBrand(pd.brand || "");
  const sizeGender = pd.extractedGender || pd.genderDisplay || pd.gender || "";
  const sizeEntries = buildSizeEntriesFromLiveData(liveData || monitor?.lastExtractedData || {}, brand, sizeGender);

  const errors = [];
  let updated = 0;
  for (let index = 0; index < variants.length; index++) {
    const variant = variants[index];
    const sizeEntry = findSizeEntryForVariant(variant, index, sizeEntries);
    const fallbackSuffix = buildSkuSuffix(variant?.option1 || variant?.title, String(index + 1));
    const desiredSku = variants.length === 1
      ? baseSku
      : `${baseSku}-${buildSkuSuffix(sizeEntry?.usSize, fallbackSuffix)}`;
    const desiredOption1 = sizeEntry?.option1;
    const fields = {};
    if (desiredSku && cleanFilterValue(variant?.sku) !== desiredSku) fields.sku = desiredSku;
    if (desiredOption1 && normalizeSizeLabel(variant?.option1) !== normalizeSizeLabel(desiredOption1)) fields.option1 = desiredOption1;
    if (!Object.keys(fields).length) continue;
    try {
      await updateVariantFields(variant.id, fields);
      if (fields.sku) variant.sku = fields.sku;
      if (fields.option1) variant.option1 = fields.option1;
      updated += 1;
    } catch (error) {
      errors.push(`Variant SKU repair failed for ${variant.sku || variant.id}: ${formatShopifyError(error)}`);
    }
  }
  return { updated, errors };
}

function findDirectSizeEntryForVariant(variant, sizeEntries = []) {
  if (!sizeEntries.length) return null;
  const optionNumber = extractExplicitUsSizeNumber(variant?.option1) || normalizeSizeNumber(variant?.option1);
  const skuSuffix = String(variant?.sku || "").split("-").pop();
  const skuNumber = normalizeSizeNumber(skuSuffix);
  return sizeEntries.find((entry) => {
    const usNumber = normalizeSizeNumber(entry.usSize);
    return usNumber && (usNumber === optionNumber || usNumber === skuNumber);
  }) || null;
}

function getMonitorSizeNumberSet(liveData = {}) {
  return new Set([
    ...(Array.isArray(liveData.inStock) ? liveData.inStock : []),
    ...(Array.isArray(liveData.outOfStock) ? liveData.outOfStock : [])
  ].map(normalizeSizeNumber).filter(Boolean));
}

function getVariantSizeNumber(variant) {
  const optionNumber = extractExplicitUsSizeNumber(variant?.option1) || normalizeSizeNumber(variant?.option1);
  if (optionNumber) return optionNumber;
  return normalizeSizeNumber(String(variant?.sku || "").split("-").pop());
}

function isLikelyUsShoeSizeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 && n <= 30;
}

function shouldDeleteUnconvertedVariant(variant, sizeEntries = [], brand = "", gender = "", liveData = {}) {
  const publicSizeLabel = cleanFilterValue(variant?.option1 || variant?.title || "");
  if (isLikelyRawUsSizeLabel(publicSizeLabel)) {
    return normalizeSizeNumber(publicSizeLabel) || publicSizeLabel;
  }

  const directEntry = findDirectSizeEntryForVariant(variant, sizeEntries);
  if (directEntry) return !directEntry.euSize ? directEntry.usSize : "";

  const variantNumber = getVariantSizeNumber(variant);
  if (!variantNumber || !isLikelyUsShoeSizeNumber(variantNumber)) return "";

  const knownMonitorSizes = getMonitorSizeNumberSet(liveData);
  if (knownMonitorSizes.size && !knownMonitorSizes.has(variantNumber)) return "";

  return getEuSize(variantNumber, brand, gender) ? "" : variantNumber;
}

async function findExistingShopifyProductForMonitor(monitor, context = {}) {
  const pd = monitor?.productData || {};
  const baseSku = String(pd.sku || "").trim();
  const mapping = context.mapping || await getSkuMapping();
  context.mapping = mapping;
  let productId = monitor?.shopifyProductId || (baseSku ? mapping[baseSku] : null);
  let product = null;

  if (productId) {
    product = context.productsById?.get(Number(productId)) || await getProductById(productId).catch(() => null);
    if (!product) productId = null;
  }
  if (!product && baseSku) {
    product = context.productsByBaseSku?.get(normalizeSku(baseSku)) || null;
  }
  if (!product && baseSku) {
    const cached = findCachedProductBySkuPrefix(baseSku);
    if (cached?.id) product = context.productsById?.get(Number(cached.id)) || await getProductById(cached.id).catch(() => null);
  }
  if (!product && baseSku) product = await findProductBySkuPrefix(baseSku);
  if (!product) {
    const cached = findCachedProductByMonitorIdentity(monitor);
    if (cached?.id) product = await getProductById(cached.id).catch(() => null);
  }
  if (!product) product = await findProductByMonitorIdentity(monitor);
  if (product?.id && baseSku) {
    mapping[baseSku] = product.id;
    await saveSkuMapping(mapping);
  }
  return product;
}

const PRODUCT_FILTER_METAFIELD_DEFINITIONS = [
  { namespace: "custom", key: "gender",       name: "Gender",       type: "single_line_text_field", storefront: "PUBLIC_READ" },
  { namespace: "custom", key: "color",        name: "Color",        type: "single_line_text_field", storefront: "PUBLIC_READ" },
  { namespace: "custom", key: "product_type", name: "Product Type", type: "single_line_text_field", storefront: "PUBLIC_READ" },
  { namespace: "custom", key: "shoes_type",   name: "Shoes Type",   type: "single_line_text_field", storefront: "PUBLIC_READ" },
  { namespace: "custom", key: "cleats",       name: "Cleats",       type: "single_line_text_field", storefront: "PUBLIC_READ" },
  { namespace: "custom", key: "notes",        name: "Monitor URL",  type: "multi_line_text_field",  storefront: null },
];

function getMetafieldDefinitionUserErrors(payload) {
  const errors =
    payload?.metafieldDefinitionCreate?.userErrors ||
    payload?.metafieldDefinitionUpdate?.userErrors ||
    [];
  return errors.map((error) => error.message || String(error)).filter(Boolean);
}

async function getProductMetafieldDefinition(namespace, key) {
  const data = await shopifyGraphQL(`
    query ProductMetafieldDefinition($namespace: String!, $key: String!) {
      metafieldDefinitions(first: 1, ownerType: PRODUCT, namespace: $namespace, key: $key) {
        nodes { id namespace key }
      }
    }
  `, { namespace, key });
  return data.metafieldDefinitions?.nodes?.[0] || null;
}

async function createProductMetafieldDefinition(definition) {
  const fieldType = definition.type || "single_line_text_field";
  const payload = {
    namespace: definition.namespace,
    key: definition.key,
    name: definition.name,
    ownerType: "PRODUCT",
    type: fieldType,
    ...(definition.storefront ? { access: { storefront: definition.storefront } } : {})
  };
  try {
    const data = await shopifyGraphQL(`
      mutation CreateProductMetafieldDefinition($definition: MetafieldDefinitionInput!) {
        metafieldDefinitionCreate(definition: $definition) {
          createdDefinition { id namespace key }
          userErrors { field message code }
        }
      }
    `, { definition: payload });
    const errors = getMetafieldDefinitionUserErrors(data);
    if (errors.length && !errors.some((message) => /already exists/i.test(message))) {
      throw new Error(errors.join(" | "));
    }
    return data.metafieldDefinitionCreate?.createdDefinition || null;
  } catch (_) {
    // Retry without access block (some Shopify plans don't support storefront access control)
    const fallback = { namespace: definition.namespace, key: definition.key, name: definition.name, ownerType: "PRODUCT", type: fieldType };
    const data = await shopifyGraphQL(`
      mutation CreateProductMetafieldDefinition($definition: MetafieldDefinitionInput!) {
        metafieldDefinitionCreate(definition: $definition) {
          createdDefinition { id namespace key }
          userErrors { field message code }
        }
      }
    `, { definition: fallback });
    const errors = getMetafieldDefinitionUserErrors(data);
    if (errors.length && !errors.some((message) => /already exists/i.test(message))) {
      throw new Error(errors.join(" | "));
    }
    return data.metafieldDefinitionCreate?.createdDefinition || null;
  }
}

async function updateProductMetafieldDefinitionAccess(id, storefront) {
  if (!id || !storefront) return null;
  try {
    const data = await shopifyGraphQL(`
      mutation UpdateProductMetafieldDefinition($id: ID!, $definition: MetafieldDefinitionUpdateInput!) {
        metafieldDefinitionUpdate(id: $id, definition: $definition) {
          updatedDefinition { id namespace key }
          userErrors { field message code }
        }
      }
    `, { id, definition: { access: { storefront } } });
    const errors = getMetafieldDefinitionUserErrors(data);
    if (errors.length) throw new Error(errors.join(" | "));
    return data.metafieldDefinitionUpdate?.updatedDefinition || null;
  } catch (_) {
    return null;
  }
}

async function ensureProductFilterMetafieldDefinitions() {
  if (productFilterMetafieldDefinitionsPromise) return productFilterMetafieldDefinitionsPromise;
  productFilterMetafieldDefinitionsPromise = (async () => {
    for (const definition of PRODUCT_FILTER_METAFIELD_DEFINITIONS) {
      const existing = await getProductMetafieldDefinition(definition.namespace, definition.key);
      if (existing?.id) {
        if (definition.storefront) await updateProductMetafieldDefinitionAccess(existing.id, definition.storefront);
      } else {
        await createProductMetafieldDefinition(definition);
      }
    }
  })();
  try {
    await productFilterMetafieldDefinitionsPromise;
  } catch (error) {
    productFilterMetafieldDefinitionsPromise = null;
    throw error;
  }
}

async function upsertProductMetafields(productId, metafields = []) {
  if (!productId || !metafields.length) return [];
  await ensureProductFilterMetafieldDefinitions();
  const ownerId = `gid://shopify/Product/${productId}`;
  const data = await shopifyGraphQL(`
    mutation SetProductMetafields($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id namespace key value }
        userErrors { field message code }
      }
    }
  `, {
    metafields: metafields.map((metafield) => ({
      ownerId,
      namespace: metafield.namespace,
      key: metafield.key,
      type: metafield.type,
      value: String(metafield.value ?? "")
    }))
  });
  const errors = data.metafieldsSet?.userErrors || [];
  if (errors.length) {
    throw new Error(errors.map((error) => error.message || String(error)).join(" | "));
  }
  return data.metafieldsSet?.metafields || [];
}

async function deleteProductMetafieldByKey(productId, namespace, key) {
  if (!productId || !namespace || !key) return null;
  const ownerId = `gid://shopify/Product/${productId}`;
  const lookup = await shopifyGraphQL(`
    query ProductMetafield($id: ID!, $namespace: String!, $key: String!) {
      product(id: $id) {
        metafield(namespace: $namespace, key: $key) { id }
      }
    }
  `, { id: ownerId, namespace, key });
  const metafieldId = lookup.product?.metafield?.id;
  if (!metafieldId) return null;
  const data = await shopifyGraphQL(`
    mutation DeleteProductMetafield($input: MetafieldDeleteInput!) {
      metafieldDelete(input: $input) {
        deletedId
        userErrors { field message code }
      }
    }
  `, { input: { id: metafieldId } });
  const errors = data.metafieldDelete?.userErrors || [];
  if (errors.length) {
    throw new Error(errors.map((error) => error.message || String(error)).join(" | "));
  }
  return data.metafieldDelete?.deletedId || null;
}

async function applyProductFilterMetafields(productId, filterData, extraMetafields = []) {
  const seen = new Set();
  const metafields = [
    ...(filterData?.metafields || []),
    ...(extraMetafields || [])
  ].filter(Boolean).filter(mf => {
    const k = `${mf.namespace}:${mf.key}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
  if (metafields.length) await upsertProductMetafields(productId, metafields);

  // Delete the explicitly-disabled toggle metafield
  if (filterData?.disabledShoesTypeMetafield) {
    await deleteProductMetafieldByKey(productId, "custom", "shoes_type");
  }
  if (filterData?.disabledCleatsMetafield) {
    await deleteProductMetafieldByKey(productId, "custom", "cleats");
  }

  // Always delete the cross-type metafield so type changes (Football ↔ other) stay clean.
  // deleteProductMetafieldByKey queries first and skips the delete mutation if not found,
  // so this is just one lightweight lookup per product when the metafield doesn't exist.
  if (filterData?.isFootball) {
    await deleteProductMetafieldByKey(productId, "custom", "shoes_type");
  } else {
    await deleteProductMetafieldByKey(productId, "custom", "cleats");
  }
}

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

async function findProductByMonitorIdentity(monitor) {
  if (!await getAccessToken()) return null;
  const pd = monitor?.productData || {};
  const title = normalizeLookupText(pd.name || monitor?.name || "");
  if (!title) return null;
  const brand = canonicalizeBrand(pd.brand || "");
  const matches = [];
  let pageInfo = null;
  let firstPage = true;
  while (firstPage || pageInfo) {
    firstPage = false;
    const path = pageInfo
      ? `/products.json?limit=250&fields=id,title,vendor,product_type,tags,variants&page_info=${pageInfo}`
      : `/products.json?limit=250&fields=id,title,vendor,product_type,tags,variants`;
    const res = await shopifyApiRequest(path, {}, { rawResponse: true });
    const link = res.headers.get("Link") || "";
    const data = await res.json();
    for (const product of (data.products || [])) {
      if (normalizeLookupText(product.title) !== title) continue;
      if (brand && canonicalizeBrand(product.vendor || "") !== brand) continue;
      matches.push(product);
      if (matches.length > 1) return null;
    }
    const next = link.match(/<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"/);
    pageInfo = next ? next[1] : null;
  }
  return matches.length === 1 ? matches[0] : null;
}

export async function getShopifyProductsSnapshot() {
  if (isFreshCacheEntry(productsSnapshotCache)) return productsSnapshotCache.value;
  if (productsSnapshotPromise) return productsSnapshotPromise;
  productsSnapshotPromise = (async () => {
    const products = [];
    let pageInfo = null;
    let firstPage = true;
    while (firstPage || pageInfo) {
      firstPage = false;
      const path = pageInfo
        ? `/products.json?limit=250&fields=id,title,status,vendor,product_type,variants,images&page_info=${pageInfo}`
        : `/products.json?limit=250&fields=id,title,status,vendor,product_type,variants,images`;
      const res = await shopifyApiRequest(path, {}, { rawResponse: true });
      const link = res.headers.get("Link") || "";
      const data = await res.json();
      for (const product of (data.products || [])) {
        const baseSku = deriveBaseSkuFromVariants(product.variants || []);
        products.push({
          id: product.id,
          title: product.title || "",
          status: product.status || "",
          vendor: product.vendor || "",
          productType: product.product_type || "",
          sku: baseSku || "",
          variantSkus: (product.variants || []).map((variant) => {
            const sku = String(variant?.sku || "").trim();
            if (!sku) return "";
            return sku === baseSku || !baseSku || !sku.startsWith(`${baseSku}-`) ? sku : sku.slice(baseSku.length + 1);
          }).filter(Boolean),
          rawVariantSkus: (product.variants || []).map((variant) => String(variant?.sku || "").trim()).filter(Boolean),
          variantOptions: (product.variants || []).map((variant) => String(variant?.option1 || "").trim()).filter(Boolean),
          variantDetails: (product.variants || []).map((variant) => ({
            id: variant.id,
            option1: String(variant?.option1 || "").trim(),
            sku: String(variant?.sku || "").trim(),
            title: String(variant?.title || "").trim()
          })),
          image: product.images?.[0]?.src || ""
        });
      }
      const next = link.match(/<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"/);
      pageInfo = next ? next[1] : null;
    }
    productsSnapshotCache = { value: products, timestamp: Date.now() };
    return products;
  })().finally(() => { productsSnapshotPromise = null; });
  return productsSnapshotPromise;
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

    const price = liveData.price != null ? String(Math.round(Number(liveData.price))) : null;
    const hasCompareAt = Object.prototype.hasOwnProperty.call(liveData || {}, "compareAt");
    const compareAt = liveData.compareAt != null ? String(Math.round(Number(liveData.compareAt))) : null;
    const inStock = new Set((liveData.inStock || []).map(String));
    const outOfStock = new Set((liveData.outOfStock || []).map(String));
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
    const skuRepair = await repairProductVariantSkus(product, monitor, liveData);
    errors.push(...skuRepair.errors);

    // Build size entries up front so we can rename existing variants with stale option1 labels
    const pd = monitor.productData || {};
    const sizeGender = pd.extractedGender || pd.genderDisplay || pd.gender || "";
    const sizeEntries = buildSizeEntriesFromLiveData(liveData, canonicalBrand, sizeGender);
    const sizeEntryByUsSize = new Map(sizeEntries.map(e => [e.usSize, e]));

    await Promise.all(product.variants.map(async (variant) => {
      const tasks = [];
      const currentPrice = variant.price != null ? String(Math.round(parseFloat(variant.price))) : null;
      const currentCompareAt = variant.compare_at_price != null ? String(Math.round(parseFloat(variant.compare_at_price))) : null;
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
      if (variant.sku?.startsWith(prefix)) {
        const usSize = variant.sku.slice(prefix.length);
        // Rename option1 if it doesn't match the expected EU-only public label.
        // Use getEuSize directly so variants absent from current liveData are also fixed.
        const euSize = getEuSize(usSize, canonicalBrand, sizeGender);
        if (euSize) {
          const expectedOption1 = String(euSize);
          if (variant.option1 !== expectedOption1) {
            tasks.push(
              shopifyFetch(`/variants/${variant.id}.json`, {
                method: "PUT",
                body: JSON.stringify({ variant: { id: variant.id, option1: expectedOption1 } })
              }).catch((error) => {
                errors.push(`Label rename failed for ${usSize}: ${formatShopifyError(error)}`);
              })
            );
          }
        }
        if (locationIds.length) {
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
      }
      await Promise.all(tasks);
    }));

    const coveredUsSizes = new Set(
      product.variants
        .filter((v) => v.sku?.startsWith(prefix))
        .map((v) => v.sku.slice(prefix.length))
    );
    const fallbackPrice = price ?? (product.variants[0]?.price != null ? String(Math.round(parseFloat(product.variants[0].price))) : "0");
    for (const entry of sizeEntries) {
      if (!entry.euSize) continue;
      if (coveredUsSizes.has(entry.usSize)) continue;
      const desiredQty = inStock.has(entry.usSize) ? 10 : 0;
      const newVariant = {
        option1: entry.option1,
        sku: `${baseSku}-${entry.usSize}`,
        price: fallbackPrice,
        inventory_management: "shopify",
        inventory_quantity: desiredQty,
        fulfillment_service: "manual",
        requires_shipping: true,
        taxable: true
      };
      if (compareAt) newVariant.compare_at_price = compareAt;
      try {
        const data = await shopifyFetch(`/products/${product.id}/variants.json`, {
          method: "POST",
          body: JSON.stringify({ variant: newVariant })
        });
        const created = data.variant;
        if (created && locationIds.length) {
          await Promise.all(locationIds.map((locationId) =>
            setInventoryLevel(created.inventory_item_id, locationId, desiredQty).catch(() => {})
          ));
        }
        coveredUsSizes.add(entry.usSize);
        invalidateShopifyCaches({ products: true });
      } catch (error) {
        errors.push(`Variant creation failed for ${entry.usSize}: ${formatShopifyError(error)}`);
      }
    }

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

export async function reapplyMonitorDataToShopify(monitor, liveData = {}, options = {}) {
  const { createIfMissing = true } = options;
  if (!await isConnected()) throw new Error("Connect Shopify first.");
  const pd = monitor?.productData || {};
  const baseSku = String(pd.sku || "").trim();
  if (!baseSku) throw new Error("Monitor has no SKU.");

  const mapping = await getSkuMapping();
  let productId = monitor?.shopifyProductId || mapping[baseSku];
  let product = null;

  if (productId) {
    try {
      product = await getProductById(productId);
      if (product && mapping[baseSku] !== product.id) {
        mapping[baseSku] = product.id;
        await saveSkuMapping(mapping);
      }
    } catch (_) {
      productId = null;
    }
  }

  if (!product) {
    product = await findProductBySkuPrefix(baseSku);
    if (product) {
      mapping[baseSku] = product.id;
      await saveSkuMapping(mapping);
    }
  }

  if (!product?.id && createIfMissing) {
    const created = await importMonitorProduct(
      {
        ...monitor,
        shopifyProductId: null,
        lastExtractedData: liveData && typeof liveData === "object" ? liveData : (monitor?.lastExtractedData || {})
      },
      { mapping, meta: await getShopifyMetadata().catch(() => ({ vendors: [], types: [], tags: [] })), locations: await getLocations().catch(() => []) }
    );
    return { id: created.id, created: true };
  }
  if (!product?.id) {
    throw new Error(`No existing Shopify product found for SKU ${baseSku}`);
  }

  const meta = await getShopifyMetadata().catch(() => ({ vendors: [], types: [], tags: [] }));
  const filterData = await buildProductFilterData(pd, meta, monitor.url);

  const productPayload = buildProductMetadataPayload(monitor, product, filterData, { includeContent: true });

  await updateProductCoreFields(product.id, productPayload);
  await applyProductFilterMetafields(product.id, filterData, [
    {
      namespace: "custom",
      key: "notes",
      value: monitor.url || "",
      type: "multi_line_text_field"
    }
  ]);
  await updateShopifyForMonitor(
    { ...monitor, shopifyProductId: product.id },
    liveData && typeof liveData === "object" ? liveData : (monitor?.lastExtractedData || {})
  );
  return { id: product.id, created: false };
}

export async function updateMonitorShopifyMetadata(monitor, context = {}) {
  if (!await isConnected()) throw new Error("Connect Shopify first.");
  const pd = monitor?.productData || {};
  const baseSku = String(pd.sku || "").trim();
  if (!baseSku && !monitor?.shopifyProductId) throw new Error("Monitor has no SKU or Shopify product ID.");

  const product = await findExistingShopifyProductForMonitor(monitor, context);

  if (!product?.id) {
    throw new Error(`No existing Shopify product found for SKU ${baseSku || monitor?.shopifyProductId}. If this product exists in Shopify with duplicate/blank SKUs and a non-unique title, link the monitor to its Shopify product ID or import it again.`);
  }

  const meta = context.meta || await getShopifyMetadata().catch(() => ({ vendors: [], types: [], tags: [] }));
  context.meta = meta;
  const filterData = await buildProductFilterData(pd, meta, monitor.url);
  const liveData = monitor?.lastExtractedData || {};
  const skuRepair = await repairProductVariantSkus(product, monitor, liveData);
  if (skuRepair.errors.length) {
    throw new Error(skuRepair.errors.join(" | "));
  }

  // Rename any existing variants whose option1 doesn't match the expected EU-only label.
  // We compute the expected label directly from the variant's SKU suffix — no liveData needed,
  // so variants that were out of stock at last capture are also fixed.
  const canonicalBrand = canonicalizeBrand(pd.brand || "");
  const sizeGender = pd.extractedGender || pd.genderDisplay || pd.gender || "";
  const prefix = `${baseSku}-`;
  const renameErrors = [];
  await Promise.all((product.variants || []).map(async (variant) => {
    if (!variant.sku?.startsWith(prefix)) return;
    const usSize = variant.sku.slice(prefix.length);
    const euSize = getEuSize(usSize, canonicalBrand, sizeGender);
    if (!euSize) return; // no EU mapping for this brand/gender — leave as-is
    const expectedOption1 = String(euSize);
    if (variant.option1 === expectedOption1) return;
    await shopifyFetch(`/variants/${variant.id}.json`, {
      method: "PUT",
      body: JSON.stringify({ variant: { id: variant.id, option1: expectedOption1 } })
    }).catch(err => renameErrors.push(`Label rename failed for ${usSize}: ${formatShopifyError(err)}`));
  }));

  await updateProductCoreFields(
    product.id,
    buildProductMetadataPayload(monitor, product, filterData),
    { invalidateProducts: false }
  );
  await applyProductFilterMetafields(product.id, filterData, [
    {
      namespace: "custom",
      key: "notes",
      value: monitor.url || "",
      type: "multi_line_text_field"
    }
  ]);
  if (renameErrors.length) throw new Error(renameErrors.join(" | "));
  return { id: product.id, updated: true, repairedVariantSkus: skuRepair.updated };
}

export async function deleteUnconvertedShopifyVariantsForMonitor(monitor, context = {}) {
  if (!await isConnected()) throw new Error("Connect Shopify first.");
  const pd = monitor?.productData || {};
  const product = await findExistingShopifyProductForMonitor(monitor, context);
  if (!product?.id) {
    throw new Error(`No existing Shopify product found for SKU ${pd.sku || monitor?.shopifyProductId || monitor?.id}`);
  }

  const liveData = monitor?.lastExtractedData || {};
  const brand = canonicalizeBrand(pd.brand || "");
  const sizeGender = pd.extractedGender || pd.genderDisplay || pd.gender || "";
  const sizeEntries = buildSizeEntriesFromLiveData(liveData, brand, sizeGender);

  const toDelete = [];
  for (const variant of (product.variants || [])) {
    const usSize = shouldDeleteUnconvertedVariant(variant, sizeEntries, brand, sizeGender, liveData);
    if (!usSize) continue;
    toDelete.push({ variant, usSize });
  }

  if (!toDelete.length) return { id: product.id, deleted: 0, sizes: [] };
  if ((product.variants || []).length - toDelete.length < 1) {
    throw new Error(`Skipped deleting unconverted sizes because it would leave "${product.title || product.id}" with no variants.`);
  }

  await Promise.all(toDelete.map((item) => deleteVariant(item.variant.id)));
  return {
    id: product.id,
    deleted: toDelete.length,
    sizes: toDelete.map((item) => item.usSize)
  };
}

export async function syncMonitorShopifyStatus(monitor, desiredStatus) {
  if (!desiredStatus || !await isConnected()) return null;
  const baseSku = String(monitor?.productData?.sku || "").trim();
  if (!baseSku) return null;

  const mapping = await getSkuMapping();
  let productId = Number(monitor?.shopifyProductId || mapping[baseSku] || 0);
  let product = null;

  if (productId) {
    try {
      product = await getProductById(productId);
      if (product && mapping[baseSku] !== product.id) {
        mapping[baseSku] = product.id;
        await saveSkuMapping(mapping);
      }
    } catch (_) {
      productId = 0;
    }
  }

  if (!product) {
    product = await findProductBySkuPrefix(baseSku);
    if (product) {
      mapping[baseSku] = product.id;
      await saveSkuMapping(mapping);
    }
  }

  if (!product?.id) return null;
  if (String(product.status || "").toLowerCase() === String(desiredStatus).toLowerCase()) {
    return product;
  }
  return updateProductStatus(product.id, desiredStatus);
}

export async function setInventoryLevel(inventoryItemId, locationId, available) {
  return shopifyFetch("/inventory_levels/set.json", {
    method: "POST",
    body: JSON.stringify({ inventory_item_id: inventoryItemId, location_id: locationId, available })
  });
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

function detectDsgCleatType(url) {
  if (!url || !/dickssportinggoods\.com/i.test(url)) return null;
  const slug = (url.split('?')[0] || '').toLowerCase();
  const types = new Set();
  // Compound abbreviations (e.g. mxsg = MX/SG = Molded + Soft Ground)
  if (/-mxsg-/.test(slug)) { types.add('Molded'); types.add('Soft Ground'); }
  if (/-fgmg-/.test(slug)) { types.add('Firm Ground'); types.add('Molded'); }
  if (/-agfg-/.test(slug)) { types.add('Artificial Grass'); types.add('Firm Ground'); }
  if (/-hgmg-/.test(slug)) { types.add('Hard Ground'); types.add('Molded'); }
  // Individual surface codes
  if (/-\bfg\b-/.test(slug)) types.add('Firm Ground');
  if (/-\bmg\b-/.test(slug)) types.add('Molded');
  if (/-\bsg\b-/.test(slug)) types.add('Soft Ground');
  if (/-\bag\b-/.test(slug)) types.add('Artificial Grass');
  if (/-\bhg\b-/.test(slug)) types.add('Hard Ground');
  if (/-turf-|-\btf\b-/.test(slug)) types.add('Turf');
  if (/-indoor-|-\bic\b-/.test(slug)) types.add('Indoor');
  return types.size ? [...types].join(', ') : null;
}

function detectCleatTypeFromName(name) {
  if (!name) return null;
  const n = ` ${name} `;
  const types = new Set();
  if (/\bMXSG\b/.test(n)) { types.add('Molded'); types.add('Soft Ground'); }
  if (/\bFG\/MG\b|\bFG\/AG\b/.test(n)) {
    types.add('Firm Ground');
    if (/\bFG\/MG\b/.test(n)) types.add('Molded');
    if (/\bFG\/AG\b/.test(n)) types.add('Artificial Grass');
  }
  if (/\bAG[-\s]?Pro\b/i.test(n)) types.add('Artificial Grass');
  if (/\bFxG\b/i.test(n)) types.add('Firm Ground');
  if (!types.size) {
    if (/\bFG\b/.test(n)) types.add('Firm Ground');
    if (/\bMG\b/.test(n)) types.add('Molded');
    if (/\bAG\b/.test(n)) types.add('Artificial Grass');
    if (/\bSG\b/.test(n)) types.add('Soft Ground');
    if (/\bHG\b/.test(n)) types.add('Hard Ground');
  }
  if (/\bTurf\b/i.test(n)) types.add('Turf');
  if (/\bIndoor\b|\bSala\b|\bIC\b/i.test(n)) types.add('Indoor');
  return types.size ? [...types].join(', ') : null;
}

function resolveCleatType(monitor) {
  const pd = monitor.productData || {};
  return pd.cleatType || detectDsgCleatType(monitor.url) || detectCleatTypeFromName(pd.name) || (String(pd.type || '') === 'Football' ? 'Unknown' : null);
}

export function transformDsgImageSrc(src) {
  try {
    if (!/^https?:\/\/dks\.scene7\.com\/is\/image\//i.test(src)) return src;
    const base = src.split('?')[0];
    return `${base}?wid=680&hei=680&extend=60,60,60,60&bgc=255,255,255&fmt=jpg&qlt=85`;
  } catch (_) { return src; }
}

// ── Main import ────────────────────────────────────────────────────────────
export async function importMonitorProduct(monitor, context = {}) {
  const pd = monitor.productData || {};
  const live = monitor.lastExtractedData || {};
  const optionalDelayMs = context.optionalDelayMs ?? 15000;

  // Fetch existing Shopify metadata to match exact casing
  const meta = context.meta || await getShopifyMetadata().catch(() => ({ vendors: [], types: [], tags: [] }));
  context.meta = meta;

  const price = live.price != null ? String(Math.round(Number(live.price))) : (pd.price != null ? String(Math.round(Number(pd.price))) : "0");
  const compareAt = live.compareAt != null ? String(Math.round(Number(live.compareAt))) : null;

  const allSizesRaw = [...(live.inStock || []), ...(live.outOfStock || [])];
  const baseSku = pd.sku || "";
  const filterData = await buildProductFilterData(pd, meta, monitor.url);
  const brand = filterData.brand;
  const sizeGender = pd.extractedGender || pd.genderDisplay || pd.gender || "";
  const missingEuSizes = [...new Set(allSizesRaw.map(cleanFilterValue).filter(Boolean))]
    .filter((size) => {
      const numeric = Number((size.match(/\d+(?:[.,]\d+)?/) || [""])[0].replace(",", "."));
      if (!/way\s*of\s*wade|li[\s-]*ning|lining/i.test(brand) && Number.isFinite(numeric) && numeric > 30) return false;
      return !/\bEU\b/i.test(size) && !getEuSize(size, brand, sizeGender);
    });
  if (missingEuSizes.length > 0) {
    throw Object.assign(
      new Error(`Sizes with no EU conversion: ${missingEuSizes.join(", ")}. Remove those sizes before importing.`),
      { code: "US_ONLY_SIZES", sizes: missingEuSizes }
    );
  }

  const inStock = new Set((live.inStock || []).map((size) => String(size)));
  const outOfStock = new Set((live.outOfStock || []).map((size) => String(size)));

  const relinkExistingProduct = async (existingProductId) => {
    if (!existingProductId) return null;
    if (baseSku) {
      const mapping = context.mapping || await getSkuMapping();
      context.mapping = mapping;
      mapping[baseSku] = existingProductId;
      await runOptionalShopifyStep("SKU mapping save failed", () => saveSkuMapping(mapping), monitor);
    }
    const existingProduct = await getProductById(existingProductId).catch(() => null);
    const filterData = await buildProductFilterData(pd, meta, monitor.url);
    deferOptionalShopifyStep("Existing product metadata update failed", () => updateProductCoreFields(
      existingProductId,
      buildProductMetadataPayload(monitor, existingProduct || {}, filterData, { includeContent: true })
    ), monitor, optionalDelayMs);
    deferOptionalShopifyStep("Existing product metafield update failed", () => applyProductFilterMetafields(existingProductId, filterData, [
      {
        namespace: "custom",
        key: "notes",
        value: monitor.url || "",
        type: "multi_line_text_field"
      }
    ]), monitor, optionalDelayMs);
    return existingProduct
      ? { ...existingProduct, relinkedExisting: true }
      : { id: existingProductId, relinkedExisting: true };
  };
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
    if (!existing && !isFreshCacheEntry(productsSnapshotCache)) {
      existing = await findProductBySkuPrefix(baseSku);
    }
    if (existing?.id) {
      return relinkExistingProduct(existing.id);
    }
  }
  if (!pd.description) throw new Error("Product description is required for import.");
  const sizeEntries = buildSizeEntriesFromLiveData(live, brand, sizeGender);
  if (!sizeEntries.length) throw new Error("No sizes available for import. Capture sizes before importing.");

  const variants = sizeEntries.map(({ usSize, option1 }) => {
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
  });

  const product = {
    title: pd.name || monitor.name || "Imported Product",
    body_html: `<p>${pd.description}</p>`,
    vendor: brand,
    product_type: filterData.productType,
    status: "active",
    published_scope: "global",
    options: [{ name: "Size" }],
    variants,
    images: (pd.images || []).map(src => ({ src: transformDsgImageSrc(src) }))
  };

  let created = null;
  try {
    created = await createProduct(product);
  } catch (error) {
    if (!product.images?.length) throw error;
    await logShopifyError(monitor, [`Product image import failed, retrying without images: ${formatShopifyError(error)}`]);
    created = await createProduct({ ...product, images: [] });
  }
  deferOptionalShopifyStep("Product metafield update failed", () => applyProductFilterMetafields(created.id, filterData, [
    {
      namespace: "custom",
      key: "notes",
      value: monitor.url || "",
      type: "multi_line_text_field"
    }
  ]), monitor, optionalDelayMs);

  // Cache SKU → product ID for future auto-updates
  if (baseSku) {
    const mapping = context.mapping || await getSkuMapping();
    context.mapping = mapping;
    mapping[baseSku] = created.id;
    await runOptionalShopifyStep("SKU mapping save failed", () => saveSkuMapping(mapping), monitor);
  }

  // Set inventory at all locations via single GraphQL batch call
  const inventoryTask = async () => {
    const locations = context.locations || await runOptionalShopifyStep("Location lookup failed", () => getLocations(), monitor) || [];
    context.locations = locations;
    if (!locations.length || !created.variants) return;
    const quantities = [];
    for (const variant of created.variants) {
      if (!variant.inventory_item_id) continue;
      const sizeEntry = sizeEntries.find((e) => e.option1 === variant.option1);
      const qty = sizeEntry?.usSize
        ? (inStock.has(sizeEntry.usSize) ? 10 : 0)
        : (variant.inventory_quantity ?? 10);
      for (const location of locations) {
        quantities.push({
          inventoryItemId: `gid://shopify/InventoryItem/${variant.inventory_item_id}`,
          locationId: `gid://shopify/Location/${location.id}`,
          quantity: qty
        });
      }
    }
    if (quantities.length) {
      await runOptionalShopifyStep("Inventory quantity update failed", () => shopifyGraphQL(`
        mutation SetInventory($input: InventorySetQuantitiesInput!) {
          inventorySetQuantities(input: $input) {
            userErrors { field message }
          }
        }
      `, { input: { reason: "correction", name: "available", quantities } }), monitor);
    }
  };
  deferOptionalShopifyStep("Inventory update failed", inventoryTask, monitor, optionalDelayMs);

  // Save to import history for undo
  runOptionalShopifyStep("Import history save failed", () => pushImportHistory({
    shopifyProductId: created.id,
    monitorData: compactMonitorForImportHistory(monitor),
    importedAt: new Date().toISOString(),
    monitorName: monitor.name
  }), monitor);

  return created;
}

export async function updateVariantPrice(variantId, price, compareAtPrice = null) {
  const body = { variant: { id: variantId, price: String(Math.round(Number(price))) } };
  if (compareAtPrice != null) body.variant.compare_at_price = String(Math.round(Number(compareAtPrice)));
  return shopifyFetch(`/variants/${variantId}.json`, {
    method: "PUT",
    body: JSON.stringify(body)
  });
}

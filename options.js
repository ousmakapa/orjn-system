import { buildDiffRows, summarizeDiff, getLogs, clearLogs, addLog, canonicalizeBrand, deriveShoesType, getMissingProductDataFields, SHOES_TYPE_METAFIELD_ENABLED_KEY, SHOES_TYPE_METAFIELD_DISABLED_KEY } from "./shared.js";
import { isConnected, connectShopify, disconnectShopify, verifyConnection, importMonitorProduct, getShopifyMetadata, getShopifyProductsSnapshot, getProductsByIds, deleteShopifyProducts, deleteProduct, deleteShopifyVariantsByIds, clearShopifyProductsSnapshotCache, getFullyOutOfStockProductIds, primeImportCaches, reapplyMonitorDataToShopify, updateMonitorShopifyMetadata, deleteUnconvertedShopifyVariantsForMonitor, transformDsgImageSrc, getAllProductMetafieldDefinitions } from "./shopify.js";
import { storeDirectoryHandle, clearDirectoryHandle, requestLocalBackupPermission, readLocalBackup } from "./local-backup.js";

const monitorGrid = document.getElementById("monitor-grid");
const monitorDetail = document.getElementById("monitor-detail");
const dashboardStats = document.getElementById("dashboard-stats");
const shopifySyncStatus = document.getElementById("shopify-sync-status");
const searchInput = document.getElementById("dashboard-search");
const changedOnlyInput = document.getElementById("changed-only");
const bulkCheckBtn = document.getElementById("bulk-check");
const bulkFirstCaptureBtn = document.getElementById("bulk-first-capture");
const bulkImportBtn = document.getElementById("bulk-import");
const smartImportAllBtn = document.getElementById("smart-import-all");
const bulkUpdateShopifyBtn = document.getElementById("bulk-update-shopify");
const bulkDeleteUnconvertedVariantsBtn = document.getElementById("bulk-delete-unconverted-variants");
const stopImportBtn = document.getElementById("stop-import");
const bulkDeleteBtn = document.getElementById("bulk-delete");
const bulkDeleteShopifyBtn = document.getElementById("bulk-delete-shopify");
const bulkDeleteShopifyOnlyBtn = document.getElementById("bulk-delete-shopify-only");
const selectAllBtn = document.getElementById("select-all");
const deselectBtn = document.getElementById("deselect-all");
const openBulkEditorBtn = document.getElementById("open-bulk-editor-btn");
const checkAllBtn = document.getElementById("check-all");
const checkSavedBtn = document.getElementById("check-saved");
const checkBatchGrid = document.getElementById("check-batch-grid");
const checkBatchSummary = document.getElementById("check-batch-summary");
const selectAllBatchesBtn = document.getElementById("select-all-batches-btn");
const clearBatchesBtn = document.getElementById("clear-batches-btn");
const continueCheckToggle = document.getElementById("continue-check-toggle");
const batchCheckStatus = document.getElementById("batch-check-status");
const openErrorSectionBtn = document.getElementById("open-error-section-btn");
const stopChecksBtn = document.getElementById("stop-checks");
const undoLastBtn = document.getElementById("undo-last");
const filterBrand = document.getElementById("filter-brand");
const filterType = document.getElementById("filter-type");
const filterSort = document.getElementById("filter-sort");
const clearAllFiltersBtn = document.getElementById("clear-all-filters");
const openShopifyUnmonitoredBtn = document.getElementById("open-shopify-unmonitored-btn");
const openMonitorNotShopifyBtn = document.getElementById("open-monitor-not-shopify-btn");
const openMonitorDuplicatesBtn = document.getElementById("open-monitor-duplicates-btn");
const openShopifyOutOfStockBtn = document.getElementById("open-shopify-out-of-stock-btn");
const checkShopifyOutOfStockBtn = document.getElementById("check-shopify-out-of-stock-btn");
const shopifyUnmonitoredPanel = document.getElementById("shopify-unmonitored-panel");
const shopifyUnmonitoredStatus = document.getElementById("shopify-unmonitored-status");
const shopifyUnmonitoredList = document.getElementById("shopify-unmonitored-list");
const shopifyUnmonitoredRefreshBtn = document.getElementById("shopify-unmonitored-refresh-btn");
const shopifyUnmonitoredClearSelectedBtn = document.getElementById("shopify-unmonitored-clear-selected-btn");
const shopifyUnmonitoredClearBtn = document.getElementById("shopify-unmonitored-clear-btn");
const shopifyUnmonitoredDeleteSelectedBtn = document.getElementById("shopify-unmonitored-delete-selected-btn");
const shopifyUnmonitoredDeleteAllBtn = document.getElementById("shopify-unmonitored-delete-all-btn");
const closeShopifyUnmonitoredBtn = document.getElementById("close-shopify-unmonitored-btn");
const monitorNotShopifyPanel = document.getElementById("monitor-not-shopify-panel");
const monitorNotShopifyStatus = document.getElementById("monitor-not-shopify-status");
const monitorNotShopifyList = document.getElementById("monitor-not-shopify-list");
const monitorNotShopifyRefreshBtn = document.getElementById("monitor-not-shopify-refresh-btn");
const monitorNotShopifyClearSelectedBtn = document.getElementById("monitor-not-shopify-clear-selected-btn");
const monitorNotShopifyClearBtn = document.getElementById("monitor-not-shopify-clear-btn");
const closeMonitorNotShopifyBtn = document.getElementById("close-monitor-not-shopify-btn");
let monitorNotShopifyDeleteDuplicatesBtn = document.getElementById("monitor-not-shopify-delete-duplicates-btn");
const monitorDuplicatesPanel = document.getElementById("monitor-duplicates-panel");
const monitorDuplicatesStatus = document.getElementById("monitor-duplicates-status");
const monitorDuplicatesList = document.getElementById("monitor-duplicates-list");
const monitorDuplicatesRefreshBtn = document.getElementById("monitor-duplicates-refresh-btn");
const monitorDuplicatesDeleteBtn = document.getElementById("monitor-duplicates-delete-btn");
const closeMonitorDuplicatesBtn = document.getElementById("close-monitor-duplicates-btn");
const skuDuplicatesPanel = document.getElementById("sku-duplicates-panel");
const skuDuplicatesStatus = document.getElementById("sku-duplicates-status");
const skuDuplicatesList = document.getElementById("sku-duplicates-list");
const skuDuplicatesRefreshBtn = document.getElementById("sku-duplicates-refresh-btn");
const skuDuplicatesSelectAllBtn = document.getElementById("sku-duplicates-select-all-btn");
const skuDuplicatesDeselectBtn = document.getElementById("sku-duplicates-deselect-btn");
const skuDuplicatesDeleteBtn = document.getElementById("sku-duplicates-delete-btn");
const closeSkuDuplicatesBtn = document.getElementById("close-sku-duplicates-btn");
const openSkuDuplicatesBtn = document.getElementById("open-sku-duplicates-btn");
const openUsSizeAuditBtn = document.getElementById("open-us-size-audit-btn");
const usSizeAuditPanel = document.getElementById("us-size-audit-panel");
const usSizeAuditStatus = document.getElementById("us-size-audit-status");
const usSizeAuditList = document.getElementById("us-size-audit-list");
const usSizeAuditRefreshBtn = document.getElementById("us-size-audit-refresh-btn");
const usSizeAuditSelectAllBtn = document.getElementById("us-size-audit-select-all-btn");
const usSizeAuditDeselectBtn = document.getElementById("us-size-audit-deselect-btn");
const usSizeAuditDeleteBtn = document.getElementById("us-size-audit-delete-btn");
const usSizeAuditUpdateMetadataBtn = document.getElementById("us-size-audit-update-metadata-btn");
const closeUsSizeAuditBtn = document.getElementById("close-us-size-audit-btn");
const metaSection = document.getElementById("shopify-meta-section");
const metaContent = document.getElementById("shopify-meta-content");
const metaStatus = document.getElementById("fetch-meta-status");
const fetchMetaBtn = document.getElementById("fetch-meta-btn");
const clearMetaFilterBtn = document.getElementById("clear-meta-filter-btn");
const monitorMetaSection = document.getElementById("monitor-meta-section");
const monitorMetaContent = document.getElementById("monitor-meta-content");
const monitorMetaStatus = document.getElementById("monitor-meta-status");
const refreshMonitorMetaBtn = document.getElementById("refresh-monitor-meta-btn");
const clearMonitorMetaFilterBtn = document.getElementById("clear-monitor-meta-filter-btn");
const bulkEditorPanel = document.getElementById("bulk-editor-panel");
const bulkEditorStatus = document.getElementById("bulk-editor-status");
const bulkEditorApplyBtn = document.getElementById("bulk-editor-apply-btn");
const bulkEditorCloseBtn = document.getElementById("bulk-editor-close-btn");

let allMonitors = [];
let selectedMonitorId = null;

// Cache LCS diff results keyed by "monitorId:entryKey" to avoid recomputing on every renderAll.
const diffRowsCache = new Map();
function getCachedDiffRows(cacheKey, prev, curr) {
  if (!diffRowsCache.has(cacheKey)) {
    if (diffRowsCache.size >= 120) diffRowsCache.clear();
    diffRowsCache.set(cacheKey, buildDiffRows(prev, curr));
  }
  return diffRowsCache.get(cacheKey);
}
function invalidateDiffCache(monitorId) {
  for (const key of diffRowsCache.keys()) {
    if (key.startsWith(monitorId + ":")) diffRowsCache.delete(key);
  }
}
const checkedIds = new Set();
const CHECK_BATCH_SIZE = 50;
const AUDIT_RENDER_CHUNK_SIZE = 30;
let selectedCheckBatchIndex = 0;
const selectedCheckBatchIndexes = new Set();
let batchCheckResults = new Map();
let activeBatchRun = null;
let auditRenderToken = 0;
const collapsedGroups = new Set();
let lastCheckedId = null;
let lastCheckedGroupKey = null;
let canCheck = true;
let autoEnabled = false; // mirrors chrome.storage autoIntervalEnabled
let shopifyUnmonitoredProducts = [];
let monitorNotShopifyMonitors = [];
let monitorOnlyMonitorIds = null;
let shopifyOutOfStockMonitorIds = null;
let monitorDuplicateSkuGroups = [];
let skuMismatchedLinkedMonitors = [];
let shopifyProductCount = null;
let shopifyUniqueSkuCount = null;
let shopifyMissingSkuCount = null;
let shopifyComparableSkuSet = null;
let shopifyMissingSkuProducts = [];
let shopifySkuGapDetails = [];
let shopifySkuGapProducts = [];
let shopifyBadSizeVariantCount = null;
let outOfStockSectionHidden = false;
let lastOutOfStockSignature = "";
const dismissedOutOfStockMonitorIds = new Set();
const selectedShopifyUnmonitoredIds = new Set();
const selectedMonitorNotShopifyIds = new Set();
const selectedDuplicateMonitorIds = new Set();
const selectedDuplicateIds = new Set();
let skuDuplicateGroups = []; // [{ sku, keepProduct, keepReason, extraProducts, protectedProducts, blockedReasons }]
let skuDuplicatesAdminBase = ""; // https://admin.shopify.com/store/{name}/products
let usSizeAuditRows = [];
const selectedUsSizeVariantIds = new Set();
const selectedMonitorBadSizeKeys = new Set();
let dashboardStorageRefreshTimer = null;
let dashboardStorageLastRenderTime = 0;
let shoesTypeMetafieldEnabledNames = new Set();
let shoesTypeMetafieldDisabledNames = new Set();
let dashboardAutoRefreshTimer = null;
let dashboardAutoRefreshRunning = false;
let dashboardAutoRefreshPending = false;
let _silentRefreshRunning = false;
let _silentRefreshPending = false;
let _lastDetailSig = "";
let _lastGridSig = "";
let _monitorMetaLastSig = null;
let _monitorMetaLastData = null;
let _lastGridStructureSig = "";
const _renderedMonitorSigs = new Map(); // monitorId → last-rendered card sig
// O(1) monitor lookup — kept in sync with allMonitors array
let _monitorIndex = new Map(); // id → index in allMonitors
// Batched patch queue — coalesces concurrent completions into one RAF pass
let _pendingPatches = new Map(); // id → updated monitor
let _patchRafId = null;
// O(1) card DOM lookup — updated whenever cards are swapped or grid rebuilt
const _cardIndex = new Map(); // id → HTMLElement
// O(1) monitor lookup helper — never returns undefined for missing ids
function _getMonitor(id) { const i = _monitorIndex.get(id); return i !== undefined ? allMonitors[i] : undefined; }
function invalidateMonitorOrderCaches() {
  _firstCreatedCacheSig = "";
  _firstCreatedCache = [];
  _checkBatchesCacheSig = "";
  _checkBatchesCache = [];
  _batchRenderSig = "";
}
// Incremental stat counters — O(1) reads instead of O(n) array scans
let _statErrors = 0;
let _statPending = 0;
let _statChanged = 0;
let _statMissingSku = 0;
let _statShopifyLinked = 0; // monitors with shopifyProductId (show Shopify badge)
let _skuFreq = new Map(); // sku → number of monitors with that sku
let _cachedErrorSig = null; // null = needs recompute; invalidated when any error monitor changes
let _firstCreatedCacheSig = "";
let _firstCreatedCache = [];
let _checkBatchesCacheSig = "";
let _checkBatchesCache = [];
let _batchRenderSig = "";
// O(1) grid change detection — incremented whenever any monitor's card-visible data changes.
// Replaces O(n) string builds in getGridSig for the common no-change case.
let _gridDataVersion = 0;
// Set of monitor IDs changed since last incremental card pass — limits tryIncrementalCardUpdates to O(dirty).
const _dirtyMonitorIds = new Set();
const monitorMetaValueCache = new Map();
const monitorImageCandidateCache = new Map();
const shopifyProductImageById = new Map();
const shopifyProductImageBySku = new Map();
let shopifyProductImageCacheVersion = 0;
let dashboardUiStateSaveTimer = null;
let stopImportRequested = false;
let importInProgress = false;
const selectedBrands = new Set();
const selectedTypes = new Set();
let _multiFilterKeyHeld = false;
const selectedShoesTypeKeys = new Set();
let _lastShoesTypeClickIdx = -1;
let bulkEditorBusy = false;
let dashboardRenderPaused = false;
let shopifySyncStatusTimer = null;
let activeStoreMetaFilters = []; // [{kind, value}, ...]
let storeVariantMonitorIdsByValue = new Map();
let cachedStoreMetaRenderData = null;
let cachedMonitorMetaRenderData = null;
const metaExpandedGroups = new Set();
const monitorMetaExpandedGroups = new Set();
let monitorMetaLoaded = false;

const PAGE_SIZE = 20; // keep rendered cards light; groups are paged
const THUMB_LAZY_ROOT_MARGIN = "700px 0px";
const THUMB_CANDIDATE_LIMIT = 4;
const SHOPIFY_OOS_GROUP_KEY = "__shopify_oos__";
const META_CHIP_LIMIT = 80;
const PRODUCT_NAME_SECTION_ITEM_LIMIT = 12;
const DASHBOARD_UI_STATE_KEY = "dashboardUiState";
const groupPages = new Map();   // groupKey → currentPage (0-indexed)
const groupFilters = new Map(); // groupKey → { brand: string|null, type: string|null }

function escapeHtml(v) {
  return (v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function ensureMonitorDuplicateDeleteBtn() {
  if (monitorNotShopifyDeleteDuplicatesBtn) return monitorNotShopifyDeleteDuplicatesBtn;
  const actionsWrap = monitorNotShopifyRefreshBtn?.parentElement;
  if (!actionsWrap) return null;
  const btn = document.createElement("button");
  btn.id = "monitor-not-shopify-delete-duplicates-btn";
  btn.className = "inline-button";
  btn.style.fontSize = "11px";
  btn.style.padding = "4px 10px";
  btn.textContent = "Delete selected duplicate monitors";
  actionsWrap.insertBefore(btn, monitorNotShopifyClearBtn || closeMonitorNotShopifyBtn || null);
  monitorNotShopifyDeleteDuplicatesBtn = btn;
  return btn;
}

function formatTimestamp(v, fallback = "Never") {
  return v ? new Date(v).toLocaleString() : fallback;
}

function timeAgo(iso) {
  if (!iso) return "never";
  const d = Date.now() - new Date(iso).getTime();
  if (d < 60000) return "just now";
  if (d < 3600000) return `${Math.floor(d / 60000)}m ago`;
  if (d < 86400000) return `${Math.floor(d / 3600000)}h ago`;
  return `${Math.floor(d / 86400000)}d ago`;
}

function getDomain(url) {
  try {
    return new URL(url).hostname.toLowerCase().replace(/^www\./, "");
  } catch {
    return String(url || "");
  }
}

function getWebsiteShortName(url) {
  const domain = getDomain(url).replace(/^www\./, "").toLowerCase();
  if (!domain) return "";
  if (/dickssportinggoods/.test(domain)) return "dicks";
  if (/wayofwade|lining|li-ning/.test(domain)) return "way of wade";
  if (/footlocker/.test(domain)) return "footlocker";
  return domain
    .split(".")
    .filter((part) => part && !/^(com|net|org|co|us|shop|store|ca|uk|eu)$/.test(part))
    .slice(-1)[0] || domain.replace(/\..*$/, "");
}

function normalizeSku(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeMetaValue(value) {
  return String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
}

function normalizeLookupText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/['’]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function getProductSkuSet(product) {
  const skus = new Set();
  const baseSku = normalizeSku(product?.sku);
  if (baseSku) skus.add(baseSku);
  for (const sku of (product?.rawVariantSkus || [])) {
    const normalized = normalizeSku(sku);
    if (normalized) {
      skus.add(normalized);
      const dash = normalized.lastIndexOf("-");
      if (dash > 0) skus.add(normalized.slice(0, dash));
    }
  }
  for (const sku of (product?.variantSkus || [])) {
    const normalized = normalizeSku(sku);
    if (normalized) skus.add(normalized);
  }
  return skus;
}

function getMonitorGenderValues(monitor = {}) {
  const pd = monitor.productData || {};
  const gender = String(pd.genderDisplay || pd.gender || "").trim();
  if (!gender || /^not defined$/i.test(gender)) return [];
  if (/both|unisex/i.test(gender) || /men\s*[,/&+]\s*women|women\s*[,/&+]\s*men/i.test(gender)) return ["Men", "Women"];
  return [gender];
}

function uniqueCaseInsensitiveValues(values = []) {
  const seen = new Set();
  return values
    .map((value) => String(value || "").replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .filter((value) => {
      const key = value.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

function simplifyMonitorColor(value) {
  const text = String(value || "").toLowerCase();
  if (!text) return "";
  const colorWords = {
    black: "Black", coreblack: "Black", tripleblack: "Black", anthracite: "Black", carbon: "Black", phantom: "Black", noir: "Black", onyx: "Black", obsidian: "Black",
    white: "White", offwhite: "White", sail: "White", ivory: "White", cream: "White", bone: "White", chalk: "White", pearl: "White",
    gray: "Gray", grey: "Gray", silver: "Gray", ash: "Gray", charcoal: "Gray", graphite: "Gray", smoke: "Gray", cement: "Gray", stone: "Gray",
    blue: "Blue", navy: "Blue", royal: "Blue", cobalt: "Blue", indigo: "Blue", denim: "Blue", aqua: "Blue", teal: "Blue", turquoise: "Blue", cyan: "Blue", sky: "Blue",
    red: "Red", burgundy: "Red", maroon: "Red", crimson: "Red", scarlet: "Red", cherry: "Red", wine: "Red", oxblood: "Red", ruby: "Red",
    green: "Green", olive: "Green", sage: "Green", forest: "Green", mint: "Green", jade: "Green", emerald: "Green", lime: "Green", volt: "Green",
    yellow: "Yellow", gold: "Yellow", golden: "Yellow", mustard: "Yellow", lemon: "Yellow",
    orange: "Orange", amber: "Orange", tangerine: "Orange", rust: "Orange", copper: "Orange", peach: "Orange", apricot: "Orange",
    brown: "Brown", tan: "Brown", beige: "Brown", sand: "Brown", mocha: "Brown", chocolate: "Brown", khaki: "Brown", taupe: "Brown", camel: "Brown", wheat: "Brown",
    pink: "Pink", rose: "Pink", blush: "Pink", fuchsia: "Pink", magenta: "Pink", salmon: "Pink",
    violet: "Violet", purple: "Violet", lavender: "Violet", lilac: "Violet", plum: "Violet", mauve: "Violet"
  };
  const words = text.replace(/[^a-z0-9]+/g, " ").split(/\s+/).filter(Boolean);
  for (const word of words) {
    if (colorWords[word]) return colorWords[word];
  }
  const compact = words.join("");
  for (const [key, color] of Object.entries(colorWords)) {
    if (key.length >= 6 && compact.includes(key)) return color;
  }
  return "Multicolor";
}

function cleanSizeValue(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function extractUsSizeValue(value) {
  const text = cleanSizeValue(value);
  if (!text) return "";
  if (/^(?:EU|EUR)?\s*\d{2}(?:[.,]\d+)?(?:\s+(?:1\/2|[12]\/3))?$/i.test(text)) {
    return text.replace(/^(?:EU|EUR)\s*/i, "").replace(",", ".").trim();
  }
  const explicit = text.match(/(?:^|[\s/:-])US\s*(\d+(?:[.,]\d+)?)/i);
  if (explicit) return explicit[1].replace(",", ".");
  const firstNumber = text.match(/\d+(?:[.,]\d+)?/);
  return firstNumber ? firstNumber[0].replace(",", ".") : text;
}

function cleanShopifyVariantLabel(value) {
  return cleanSizeValue(value)
    .replace(/\b(?:size|eu|eur|us)\b\.?/gi, "")
    .replace(/\s*\/\s*/g, " / ")
    .replace(/\s+/g, " ")
    .trim();
}

function isLikelyRawUsSize(value) {
  const text = cleanSizeValue(value);
  if (!text || /^default title$/i.test(text)) return false;
  if (/^(?:EU|EUR)?\s*\d{2}(?:[.,]\d+)?(?:\s+(?:1\/2|[12]\/3))?$/i.test(text)) return false;
  if (/\bUS\b/i.test(text)) return true;
  if (/\bM\s*\d+(?:[.,]\d+)?\s*\/\s*W\s*\d+(?:[.,]\d+)?/i.test(text)) return true;
  if (/\d+(?:[.,]\d+)?\s*\/\s*\d+(?:[.,]\d+)?/.test(text)) return true;
  const match = text.match(/\d+(?:[.,]\d+)?/);
  if (!match) return false;
  const n = Number(match[0].replace(",", "."));
  if (!Number.isFinite(n) || n <= 0) return false;
  return n <= 30;
}

function isFractionalSizeBrandText(value = "") {
  return /adidas|yeezy|hoka|way\s*of\s*wade|li[\s-]*ning|lining/i.test(String(value || ""));
}

function isStaleFractionalEuSizeLabel(value = "", brandText = "") {
  if (!isFractionalSizeBrandText(brandText)) return false;
  const text = cleanSizeValue(value).replace(",", ".");
  if (!text || /\//.test(text)) return false;
  const match = text.match(/\d+(?:\.\d+)?/);
  if (!match || !/\d+\.\d+/.test(match[0])) return false;
  const n = Number(match[0]);
  if (!Number.isFinite(n) || n < 30 || n > 60) return false;
  const decimal = match[0].split(".")[1] || "";
  return /^(3|33|5|6|66|67)$/.test(decimal);
}

function getSizeSortNumber(value) {
  const text = cleanSizeValue(value);
  const match = text.match(/\d+(?:[.,]\d+)?/);
  if (!match) return Number.POSITIVE_INFINITY;
  let n = Number(match[0].replace(",", "."));
  const fraction = text.match(/\b(1\/2|[12]\/3)\b/);
  if (fraction) {
    const [numerator, denominator] = fraction[1].split("/").map(Number);
    n += numerator / denominator;
  }
  return n;
}

function sortSizeLabels(values = []) {
  return [...values].sort((a, b) => {
    const an = getSizeSortNumber(a);
    const bn = getSizeSortNumber(b);
    if (an !== bn) return an - bn;
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" });
  });
}

function normalizeIgnoredAuditSize(value) {
  return String(value || "").replace(/,/g, ".").replace(/\s+/g, " ").trim().toLowerCase();
}

function getIgnoredAuditSizeNumber(value) {
  const match = normalizeIgnoredAuditSize(value).match(/\d+(?:\.\d+)?/);
  if (!match) return "";
  const n = Number(match[0]);
  return Number.isFinite(n) ? String(n) : "";
}

function isIgnoredAuditSizeMatch(size, ignored) {
  const normalizedSize = normalizeIgnoredAuditSize(size);
  const normalizedIgnored = normalizeIgnoredAuditSize(ignored);
  if (!normalizedSize || !normalizedIgnored) return false;
  if (normalizedSize === normalizedIgnored) return true;
  const sizeNumber = getIgnoredAuditSizeNumber(normalizedSize);
  const ignoredNumber = getIgnoredAuditSizeNumber(normalizedIgnored);
  return !!sizeNumber && sizeNumber === ignoredNumber;
}

function filterIgnoredAuditSizes(values = [], ignoredSizes = []) {
  const ignored = (ignoredSizes || []).filter(Boolean);
  if (!ignored.length) return Array.isArray(values) ? values : [];
  return (Array.isArray(values) ? values : []).filter((size) => !ignored.some((ignoredSize) => isIgnoredAuditSizeMatch(size, ignoredSize)));
}

function countBadSizeVariants(products = []) {
  let count = 0;
  for (const product of (Array.isArray(products) ? products : [])) {
    if (String(product.status || "").toLowerCase() !== "active") continue;
    for (const option of (product.variantOptions || [])) {
      if (isLikelyRawUsSize(option) || isStaleFractionalEuSizeLabel(option, [product.vendor, product.title, product.productType].join(" "))) count++;
    }
  }
  return count;
}

function getBadSizeVariantRows(products = []) {
  const rows = [];
  for (const product of (Array.isArray(products) ? products : [])) {
    if (String(product.status || "").toLowerCase() !== "active") continue;
    const variants = Array.isArray(product.variantDetails) ? product.variantDetails : [];
    for (const variant of variants) {
      const publicSize = cleanSizeValue(variant.option1 || variant.title || "");
      const brandText = [product.vendor, product.title, product.productType].join(" ");
      const staleFractional = isStaleFractionalEuSizeLabel(publicSize, brandText);
      if (!isLikelyRawUsSize(publicSize) && !staleFractional) continue;
      rows.push({
        source: "shopify",
        reason: staleFractional ? "Fraction EU size showing as decimal" : "Raw US size in Store Data",
        expectedSize: getChartExpectedEuLabel(publicSize, product.vendor || product.title || "", ""),
        productId: Number(product.id || 0),
        productTitle: product.title || "Untitled Shopify product",
        vendor: product.vendor || "",
        productType: product.productType || "",
        productSku: product.sku || "",
        image: product.image || "",
        variantId: Number(variant.id || 0),
        variantSku: variant.sku || "",
        size: publicSize
      });
    }
  }
  return rows.sort((a, b) =>
    getSizeSortNumber(a.size) - getSizeSortNumber(b.size) ||
    a.size.localeCompare(b.size, undefined, { numeric: true }) ||
    a.productTitle.localeCompare(b.productTitle, undefined, { sensitivity: "base" })
  );
}

function getMonitorBadSizeRows(monitors = []) {
  const rows = [];
  for (const monitor of (Array.isArray(monitors) ? monitors : [])) {
    if (!monitor?.productData || isSavedPendingMonitor(monitor)) continue;
    const pd = monitor.productData || {};
    const live = monitor.lastExtractedData || {};
    const ignoredSizes = monitor.ignoredSizes || [];
    const brand = canonicalizeBrand(pd.brand || getBrandFromMonitor(monitor) || "");
    const gender = pd.extractedGender || pd.genderDisplay || pd.gender || "";
    const rawSizes = filterIgnoredAuditSizes(uniqueCaseInsensitiveValues([
      ...(Array.isArray(live.inStock) ? live.inStock : []),
      ...(Array.isArray(live.outOfStock) ? live.outOfStock : []),
      ...(Array.isArray(pd.sizes) ? pd.sizes : []),
      ...(Array.isArray(pd.inStock) ? pd.inStock : []),
      ...(Array.isArray(pd.outOfStock) ? pd.outOfStock : [])
    ]), ignoredSizes).map(cleanSizeValue).filter(Boolean);
    const badSizes = sortSizeLabels(rawSizes.filter((size) => {
      if (isStaleFractionalEuSizeLabel(size, `${brand} ${pd.name || ""} ${monitor.name || ""} ${monitor.url || ""}`)) return true;
      if (/\bEU\b/i.test(size)) return false;
      const numeric = getSizeSortNumber(size);
      if (!/way\s*of\s*wade|li[\s-]*ning|lining/i.test(brand) && Number.isFinite(numeric) && numeric > 30) return false;
      return !getEuSize(size, brand, gender);
    }));
    if (!badSizes.length) continue;
    rows.push({
      source: "monitor",
      productId: `monitor:${monitor.id}`,
      monitorId: monitor.id,
      productTitle: pd.name || monitor.name || "Untitled monitor",
      vendor: pd.brand || getBrandFromMonitor(monitor) || "",
      productType: pd.type || "",
      productSku: pd.sku || "",
      image: getMonitorThumb(monitor),
      variantId: 0,
      variantSku: pd.sku || "",
      size: badSizes.join(", "),
      badSizes
    });
  }
  return rows.sort((a, b) =>
    getSizeSortNumber(a.badSizes?.[0] || a.size) - getSizeSortNumber(b.badSizes?.[0] || b.size) ||
    String(a.productTitle || "").localeCompare(String(b.productTitle || ""), undefined, { sensitivity: "base" })
  );
}

function getMonitorBadSizeKey(monitorId, size) {
  return `${monitorId}::${cleanSizeValue(size).toLowerCase()}`;
}

function getChartExpectedEuLabel(size, brand = "", gender = "") {
  const usSize = extractUsSizeValue(size);
  const eu = getEuSize(usSize, brand, gender);
  return eu ? cleanShopifyVariantLabel(String(eu)) : "";
}

function getBadSizeTargetKey(source, id, size) {
  return `${source}:${id}:${cleanSizeValue(size).toLowerCase()}`;
}

function updateBadSizeAuditButtons() {
  const shopifyRows = usSizeAuditRows.filter((row) => row.source === "shopify" && row.variantId);
  const monitorSizeCount = usSizeAuditRows.reduce((n, row) => n + (row.source === "monitor" ? (row.badSizes?.length || 0) : 0), 0);
  const validShopifyIds = new Set(shopifyRows.map((row) => String(row.variantId)));
  const validMonitorKeys = new Set(usSizeAuditRows
    .filter((row) => row.source === "monitor")
    .flatMap((row) => (row.badSizes || []).map((size) => getMonitorBadSizeKey(row.monitorId, size))));
  for (const id of [...selectedUsSizeVariantIds]) {
    if (!validShopifyIds.has(String(id))) selectedUsSizeVariantIds.delete(String(id));
  }
  for (const key of [...selectedMonitorBadSizeKeys]) {
    if (!validMonitorKeys.has(key)) selectedMonitorBadSizeKeys.delete(key);
  }
  const selectedCount = selectedUsSizeVariantIds.size + selectedMonitorBadSizeKeys.size;
  const totalCount = shopifyRows.length + monitorSizeCount;
  usSizeAuditDeleteBtn.disabled = selectedCount === 0;
  usSizeAuditDeleteBtn.textContent = selectedCount > 0 ? `Remove selected (${selectedCount})` : "Remove selected bad sizes";
  usSizeAuditSelectAllBtn.disabled = totalCount === 0;
  usSizeAuditSelectAllBtn.textContent = totalCount > 0 && selectedCount === totalCount ? "All selected" : "Select all";
  usSizeAuditDeselectBtn.disabled = selectedCount === 0;
  usSizeAuditUpdateMetadataBtn.disabled = usSizeAuditRows.length === 0;
}

function getVisibleBadSizeAuditSelection() {
  usSizeAuditList?.querySelectorAll(".us-size-check").forEach((input) => {
    if (input.checked) selectedUsSizeVariantIds.add(input.dataset.id);
    else selectedUsSizeVariantIds.delete(input.dataset.id);
  });
  usSizeAuditList?.querySelectorAll(".monitor-bad-size-check").forEach((input) => {
    const key = getMonitorBadSizeKey(input.dataset.monitorId, input.dataset.size);
    if (input.checked) selectedMonitorBadSizeKeys.add(key);
    else selectedMonitorBadSizeKeys.delete(key);
  });
  const shopifyIds = [...selectedUsSizeVariantIds].map(Number).filter(Boolean);
  const monitorSelections = new Map();
  const selectedMonitorTargets = [];
  for (const row of usSizeAuditRows) {
    if (row.source !== "monitor") continue;
    for (const size of (row.badSizes || [])) {
      const key = getMonitorBadSizeKey(row.monitorId, size);
      if (!selectedMonitorBadSizeKeys.has(key)) continue;
      if (!monitorSelections.has(row.monitorId)) monitorSelections.set(row.monitorId, []);
      monitorSelections.get(row.monitorId).push(size);
      selectedMonitorTargets.push({ monitorId: row.monitorId, size });
    }
  }
  const validShopifyIds = new Set(usSizeAuditRows
    .filter((row) => row.source === "shopify" && row.variantId)
    .map((row) => String(row.variantId)));
  for (const id of [...selectedUsSizeVariantIds]) {
    if (!validShopifyIds.has(String(id))) selectedUsSizeVariantIds.delete(String(id));
  }
  const validMonitorKeys = new Set(usSizeAuditRows
    .filter((row) => row.source === "monitor")
    .flatMap((row) => (row.badSizes || []).map((size) => getMonitorBadSizeKey(row.monitorId, size))));
  for (const key of [...selectedMonitorBadSizeKeys]) {
    if (!validMonitorKeys.has(key)) selectedMonitorBadSizeKeys.delete(key);
  }
  usSizeAuditList?.querySelectorAll(".us-size-check").forEach((input) => {
    input.checked = selectedUsSizeVariantIds.has(String(input.dataset.id));
  });
  usSizeAuditList?.querySelectorAll(".monitor-bad-size-check").forEach((input) => {
    input.checked = selectedMonitorBadSizeKeys.has(getMonitorBadSizeKey(input.dataset.monitorId, input.dataset.size));
  });
  const selectedShopifyRows = usSizeAuditRows.filter((row) =>
    row.source === "shopify" && selectedUsSizeVariantIds.has(String(row.variantId))
  );
  return { shopifyIds: [...selectedUsSizeVariantIds].map(Number).filter(Boolean), monitorSelections, selectedMonitorTargets, selectedShopifyRows };
}

function getMonitorVariantValues(monitor = {}) {
  const pd = monitor.productData || {};
  const live = monitor.lastExtractedData || {};
  const rawSizes = filterIgnoredAuditSizes([
    ...(Array.isArray(live.inStock) ? live.inStock : []),
    ...(Array.isArray(live.outOfStock) ? live.outOfStock : []),
    ...(Array.isArray(pd.sizes) ? pd.sizes : [])
  ], monitor.ignoredSizes || []).map(cleanSizeValue).filter(Boolean);
  const brand = pd.brand || getBrandFromMonitor(monitor) || "";
  const gender = pd.extractedGender || pd.gender || pd.genderDisplay || "";
  const values = new Set();
  for (const size of rawSizes) {
    const eu = getEuSize(size, brand, gender);
    if (eu) values.add(String(eu));
  }
  return sortSizeLabels([...values]);
}

function getExpectedShopifyVariantValuesForMonitor(monitor = {}) {
  const pd = monitor.productData || {};
  const live = monitor.lastExtractedData || {};
  const rawSizes = filterIgnoredAuditSizes(uniqueCaseInsensitiveValues([
    ...(Array.isArray(live.inStock) ? live.inStock : []),
    ...(Array.isArray(live.outOfStock) ? live.outOfStock : []),
    ...(Array.isArray(pd.sizes) ? pd.sizes : [])
  ]), monitor.ignoredSizes || []);
  const brand = String(pd.brand || getBrandFromMonitor(monitor) || "").replace(/\s+/g, " ").trim();
  const gender = pd.extractedGender || pd.genderDisplay || pd.gender || "";
  const used = new Set();
  return sortSizeLabels(rawSizes.map((rawSize) => {
    const usSize = extractUsSizeValue(rawSize);
    const euSize = getEuSize(usSize, brand, gender);
    if (!euSize) return "";
    const preferred = cleanShopifyVariantLabel(String(euSize));
    const key = preferred.toLowerCase();
    if (used.has(key)) return "";
    const option1 = preferred;
    used.add(String(option1).toLowerCase());
    return option1;
  }).filter(Boolean));
}

function getMonitorStoreMetaValues(monitor = {}) {
  const pd = monitor.productData || {};
  const id = String(monitor.id || "");
  const sig = [
    pd.brand || "",
    pd.type || "",
    pd.colorFinal || "",
    pd.color || "",
    pd.colorRaw || "",
    pd.genderDisplay || "",
    pd.gender || "",
    pd.extractedGender || "",
    (Array.isArray(monitor.ignoredSizes) ? monitor.ignoredSizes.join("|") : ""),
    (Array.isArray(pd.sizes) ? pd.sizes.join("|") : ""),
    (Array.isArray(monitor.lastExtractedData?.inStock) ? monitor.lastExtractedData.inStock.join("|") : ""),
    (Array.isArray(monitor.lastExtractedData?.outOfStock) ? monitor.lastExtractedData.outOfStock.join("|") : "")
  ].join("::");
  const cached = id ? monitorMetaValueCache.get(id) : null;
  if (cached?.sig === sig) return cached.values;
  const brand = canonicalizeBrand(pd.brand || getBrandFromMonitor(monitor) || "");
  const type = String(pd.type || "").trim();
  const simpleColor = simplifyMonitorColor(pd.colorFinal || pd.color || pd.colorRaw || "");
  const tags = [
    type,
    ...getMonitorGenderValues(monitor),
    simpleColor
  ].filter(Boolean);
  const values = {
    vendors: brand ? [brand] : [],
    types: type ? [type] : [],
    tags,
    variants: getExpectedShopifyVariantValuesForMonitor(monitor)
  };
  if (id) monitorMetaValueCache.set(id, { sig, values });
  return values;
}

function renderAuditHtmlInChunks(htmlParts = [], emptyHtml = "") {
  if (!usSizeAuditList) return;
  const token = ++auditRenderToken;
  usSizeAuditList.textContent = "";
  if (!htmlParts.length) {
    usSizeAuditList.innerHTML = emptyHtml;
    return;
  }
  let cursor = 0;
  const appendChunk = () => {
    if (token !== auditRenderToken) return;
    const html = htmlParts.slice(cursor, cursor + AUDIT_RENDER_CHUNK_SIZE).join("");
    if (html) usSizeAuditList.insertAdjacentHTML("beforeend", html);
    cursor += AUDIT_RENDER_CHUNK_SIZE;
    if (cursor < htmlParts.length) requestAnimationFrame(appendChunk);
  };
  appendChunk();
}

function getMonitorExpectedMetafields(monitor = {}) {
  const pd = monitor.productData || {};
  const genderValues = getMonitorGenderValues(monitor);
  const color = simplifyMonitorColor(pd.colorFinal || pd.color || pd.colorRaw || "");
  const type = String(pd.type || "").replace(/\s+/g, " ").trim();
  const shoesType = getMonitorShoesTypeInfo(monitor);
  const isFootball = /^football$/i.test(type);
  const cleatValue = isFootball
    ? (pd.cleatType || detectDsgCleatTypeLocal(monitor.url) || detectCleatTypeFromName(pd.name) || shoesType.model || "Unknown")
    : null;
  return [
    genderValues.length ? { namespace: "custom", key: "gender", value: genderValues.join(", "), type: "single_line_text_field" } : null,
    color ? { namespace: "custom", key: "color", value: color, type: "single_line_text_field" } : null,
    type ? { namespace: "custom", key: "product_type", value: type, type: "single_line_text_field" } : null,
    !isFootball && shoesType.model && isShoesTypeMetafieldEnabled(shoesType.model) ? { namespace: "custom", key: "shoes_type", value: shoesType.model, type: "single_line_text_field" } : null,
    isFootball && cleatValue && isShoesTypeMetafieldEnabled(cleatValue) ? { namespace: "custom", key: "cleats", value: cleatValue, type: "single_line_text_field" } : null
  ].filter(Boolean);
}

function getMonitorShoesTypeInfo(monitor = {}) {
  const derived = deriveShoesType(monitor.productData || {});
  if (!derived) return { category: "", model: "" };
  if (typeof derived === "string") return { category: "", model: derived };
  return {
    category: String(derived.category || "").replace(/\s+/g, " ").trim(),
    model: String(derived.model || "").replace(/\s+/g, " ").trim()
  };
}

function getShoesTypeToggleKey(value = "") {
  return normalizeMetaValue(value);
}

function isShoesTypeMetafieldEnabled(value = "") {
  const key = getShoesTypeToggleKey(value);
  return !!key && shoesTypeMetafieldEnabledNames.has(key);
}

async function loadShoesTypeMetafieldToggles() {
  const stored = await chrome.storage.local.get([SHOES_TYPE_METAFIELD_ENABLED_KEY, SHOES_TYPE_METAFIELD_DISABLED_KEY]).catch(() => ({}));
  const enabledNames = Array.isArray(stored?.[SHOES_TYPE_METAFIELD_ENABLED_KEY])
    ? stored[SHOES_TYPE_METAFIELD_ENABLED_KEY]
    : [];
  shoesTypeMetafieldEnabledNames = new Set(enabledNames.map(getShoesTypeToggleKey).filter(Boolean));
  const disabledNames = Array.isArray(stored?.[SHOES_TYPE_METAFIELD_DISABLED_KEY])
    ? stored[SHOES_TYPE_METAFIELD_DISABLED_KEY]
    : [];
  shoesTypeMetafieldDisabledNames = new Set(disabledNames.map(getShoesTypeToggleKey).filter(Boolean));
}

async function saveShoesTypeMetafieldToggles() {
  await chrome.storage.local.set({
    [SHOES_TYPE_METAFIELD_ENABLED_KEY]: [...shoesTypeMetafieldEnabledNames].sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" })),
    [SHOES_TYPE_METAFIELD_DISABLED_KEY]: [...shoesTypeMetafieldDisabledNames].sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }))
  });
}

function monitorMatchesStoreMeta(monitor, kind, value) {
  const needle = normalizeMetaValue(value);
  if (!needle) return false;
  if (kind === "variant" && storeVariantMonitorIdsByValue.size) {
    return storeVariantMonitorIdsByValue.get(needle)?.has(String(monitor?.id || "")) || false;
  }
  const values = getMonitorStoreMetaValues(monitor);
  if (kind === "vendor") {
    return values.vendors.some((item) => normalizeMetaValue(canonicalizeBrand(item)) === normalizeMetaValue(canonicalizeBrand(value)));
  }
  if (kind === "type") {
    return values.types.some((item) => normalizeMetaValue(item) === needle);
  }
  if (kind === "name") {
    return normalizeMetaValue(monitor.productData?.name || monitor.name || "") === needle;
  }
  if (kind === "category") {
    return normalizeMetaValue(getMonitorShoesTypeInfo(monitor).category) === needle;
  }
  if (kind === "shoes_type") {
    return normalizeMetaValue(getMonitorShoesTypeInfo(monitor).model) === needle;
  }
  if (kind?.startsWith("metafield:")) {
    const key = kind.slice("metafield:".length);
    return getMonitorExpectedMetafields(monitor).some((field) =>
      field.key === key && normalizeMetaValue(field.value) === needle
    );
  }
  if (kind === "tag") {
    if (values.tags.some((item) => normalizeMetaValue(item) === needle)) return true;
    const pd = monitor.productData || {};
    const searchable = [pd.name, monitor.name, pd.description].filter(Boolean).join(" ");
    return normalizeLookupText(searchable).includes(normalizeLookupText(value));
  }
  if (kind === "variant") {
    return values.variants.some((item) => normalizeMetaValue(item) === needle);
  }
  return false;
}

function findUniqueProductByMonitorIdentity(products, monitor) {
  const title = normalizeLookupText(monitor?.productData?.name || monitor?.name || "");
  if (!title) return null;
  const brand = normalizeLookupText(canonicalizeBrand(monitor?.productData?.brand || ""));
  const matches = (products || []).filter((product) => {
    if (normalizeLookupText(product?.title) !== title) return false;
    if (!brand) return true;
    return normalizeLookupText(canonicalizeBrand(product?.vendor || "")) === brand;
  });
  return matches.length === 1 ? matches[0] : null;
}

function isMonitorFullyOutOfStock(monitor) {
  const live = monitor?.lastExtractedData || {};
  const inStock = Array.isArray(live.inStock) ? live.inStock.filter(Boolean) : [];
  const outOfStock = Array.isArray(live.outOfStock) ? live.outOfStock.filter(Boolean) : [];
  return inStock.length === 0 && outOfStock.length > 0;
}

let _oosCache = null;
let _oosCacheVersion = -1;
function getCombinedOutOfStockMonitorIds(monitors = allMonitors) {
  if (_oosCacheVersion === _gridDataVersion && monitors === allMonitors) return _oosCache;
  const ids = new Set((monitors || []).filter(isMonitorFullyOutOfStock).map((monitor) => monitor.id));
  if (shopifyOutOfStockMonitorIds) {
    shopifyOutOfStockMonitorIds.forEach((id) => ids.add(id));
  }
  if (monitors === allMonitors) { _oosCache = ids; _oosCacheVersion = _gridDataVersion; }
  return ids;
}

function getVisibleOutOfStockMonitorIds(monitors = allMonitors) {
  const ids = getCombinedOutOfStockMonitorIds(monitors);
  dismissedOutOfStockMonitorIds.forEach((id) => ids.delete(id));
  return ids;
}

function getOutOfStockSignature(monitors = allMonitors) {
  return [...getCombinedOutOfStockMonitorIds(monitors)].sort().join(",");
}

function updateOutOfStockButtons() {
  const count = getVisibleOutOfStockMonitorIds(allMonitors).size;
  openShopifyOutOfStockBtn.textContent = count
    ? `Open out of stock (${count})`
    : "Open out of stock";
  if (!checkShopifyOutOfStockBtn.disabled) {
    checkShopifyOutOfStockBtn.textContent = "Check out of stock";
  }
}

function collectDashboardUiState() {
  return {
    search: searchInput.value || "",
    changedOnly: !!changedOnlyInput.checked,
    filterBrands: [...selectedBrands],
    filterTypes: [...selectedTypes],
    filterSort: filterSort.value || "created-desc",
    collapsedGroups: [...collapsedGroups],
    groupPages: [...groupPages.entries()],
    groupFilters: [...groupFilters.entries()],
    outOfStockSectionHidden: !!outOfStockSectionHidden,
    dismissedOutOfStockMonitorIds: [...dismissedOutOfStockMonitorIds]
  };
}

function captureDashboardFilters() {
  return {
    search: searchInput.value || "",
    changedOnly: !!changedOnlyInput.checked,
    filterBrands: [...selectedBrands],
    filterTypes: [...selectedTypes],
    filterSort: filterSort.value || "created-desc",
    activeStoreMetaFilters: activeStoreMetaFilters.map(f => ({ ...f })),
    groupFilters: new Map(groupFilters),
    groupPages: new Map(groupPages)
  };
}

function restoreDashboardFilters(snapshot) {
  if (!snapshot) return;
  searchInput.value = snapshot.search || "";
  changedOnlyInput.checked = !!snapshot.changedOnly;
  filterSort.value = snapshot.filterSort || "created-desc";

  selectedBrands.clear();
  selectedTypes.clear();
  // Support both new array format and old single-value format
  if (Array.isArray(snapshot.filterBrands)) snapshot.filterBrands.forEach(b => b && selectedBrands.add(b));
  else if (snapshot.filterBrand) selectedBrands.add(snapshot.filterBrand);
  if (Array.isArray(snapshot.filterTypes)) snapshot.filterTypes.forEach(t => t && selectedTypes.add(t));
  else if (snapshot.filterType) selectedTypes.add(snapshot.filterType);

  // Ensure selected values exist as options
  for (const b of selectedBrands) {
    if (![...filterBrand.options].some(o => o.value === b)) filterBrand.add(new Option(b, b));
  }
  for (const t of selectedTypes) {
    if (![...filterType.options].some(o => o.value === t)) filterType.add(new Option(t, t));
  }
  syncBrandSelectDisplay();
  syncTypeSelectDisplay();

  activeStoreMetaFilters = Array.isArray(snapshot.activeStoreMetaFilters)
    ? snapshot.activeStoreMetaFilters
    : (snapshot.activeStoreMetaFilter ? [snapshot.activeStoreMetaFilter] : []);
  const _hasMF = activeStoreMetaFilters.length > 0;
  if (clearMetaFilterBtn) clearMetaFilterBtn.style.display = _hasMF ? "" : "none";
  if (clearMonitorMetaFilterBtn) clearMonitorMetaFilterBtn.style.display = _hasMF ? "" : "none";

  groupFilters.clear();
  for (const [key, value] of snapshot.groupFilters || []) groupFilters.set(key, value);
  groupPages.clear();
  for (const [key, value] of snapshot.groupPages || []) groupPages.set(key, value);
}

function showShopifySyncStatus(message, tone = "working", { autoHide = tone === "success" } = {}) {
  if (!shopifySyncStatus) return;
  clearTimeout(shopifySyncStatusTimer);
  shopifySyncStatus.textContent = message;
  shopifySyncStatus.className = `dashboard-sync-status visible ${tone}`;
  if (autoHide) {
    shopifySyncStatusTimer = setTimeout(() => {
      shopifySyncStatus.className = "dashboard-sync-status";
      shopifySyncStatus.textContent = "";
    }, 12000);
  }
}

function scheduleSaveDashboardUiState() {
  clearTimeout(dashboardUiStateSaveTimer);
  dashboardUiStateSaveTimer = setTimeout(() => {
    chrome.storage.local.set({ [DASHBOARD_UI_STATE_KEY]: collectDashboardUiState() }).catch(() => {});
  }, 120);
}

async function restoreDashboardUiState() {
  const { [DASHBOARD_UI_STATE_KEY]: state } = await chrome.storage.local.get(DASHBOARD_UI_STATE_KEY);
  if (!state || typeof state !== "object") return;

  // Do not restore transient dashboard filters across sessions.
  // Persisting them has been causing large portions of the grid to look "missing"
  // when the user reopens the dashboard later.
  searchInput.value = "";
  changedOnlyInput.checked = false;
  selectedBrands.clear();
  selectedTypes.clear();
  activeStoreMetaFilters = [];
  syncBrandSelectDisplay();
  syncTypeSelectDisplay();
  filterSort.value = state.filterSort || "created-desc";

  collapsedGroups.clear();
  for (const key of (state.collapsedGroups || [])) {
    if (key) collapsedGroups.add(key);
  }

  groupPages.clear();
  for (const entry of (state.groupPages || [])) {
    if (Array.isArray(entry) && entry.length === 2) {
      groupPages.set(entry[0], entry[1]);
    }
  }

  // Group-level pills are also treated as transient view state.
  groupFilters.clear();

  outOfStockSectionHidden = !!state.outOfStockSectionHidden;
  dismissedOutOfStockMonitorIds.clear();
  for (const id of (state.dismissedOutOfStockMonitorIds || [])) {
    if (id) dismissedOutOfStockMonitorIds.add(id);
  }
}

function reconcileOutOfStockMonitorUpdates(previousMonitors = [], nextMonitors = []) {
  const previousById = new Map(previousMonitors.map((monitor) => [monitor.id, monitor]));
  let shouldOpenOutOfStock = false;

  for (const monitor of nextMonitors) {
    const previous = previousById.get(monitor.id);
    const previousCheckedAt = previous?.lastCheckedAt || "";
    const nextCheckedAt = monitor?.lastCheckedAt || "";
    if (nextCheckedAt && nextCheckedAt !== previousCheckedAt && isMonitorFullyOutOfStock(monitor)) {
      dismissedOutOfStockMonitorIds.delete(monitor.id);
      shouldOpenOutOfStock = true;
    }
  }

  if (shouldOpenOutOfStock) {
    outOfStockSectionHidden = false;
    collapsedGroups.delete(SHOPIFY_OOS_GROUP_KEY);
  }
}

function getMonitorSavedSizeValues(monitor = {}) {
  const lists = [
    monitor.lastExtractedData?.inStock,
    monitor.lastExtractedData?.outOfStock,
    monitor.previousExtractedData?.inStock,
    monitor.previousExtractedData?.outOfStock,
    monitor.productData?.sizes,
    monitor.productData?.inStock,
    monitor.productData?.outOfStock
  ];
  return lists.flatMap((list) => Array.isArray(list) ? list : []);
}

function getMonitorUnconvertedSizeValues(monitor = {}) {
  const pd = monitor.productData || {};
  const brand = canonicalizeBrand(pd.brand || getBrandFromMonitor(monitor) || "");
  const gender = pd.extractedGender || pd.genderDisplay || pd.gender || "";
  const rawSizes = filterIgnoredAuditSizes(
    uniqueCaseInsensitiveValues(getMonitorSavedSizeValues(monitor)),
    monitor.ignoredSizes || []
  );
  return sortSizeLabels(rawSizes.filter((size) => {
    if (isStaleFractionalEuSizeLabel(size, `${brand} ${pd.name || ""} ${monitor.name || ""} ${monitor.url || ""}`)) return false;
    if (/\bEU\b/i.test(size)) return false;
    const numeric = getSizeSortNumber(size);
    if (!/way\s*of\s*wade|li[\s-]*ning|lining/i.test(brand) && Number.isFinite(numeric) && numeric > 30) return false;
    return !getEuSize(size, brand, gender);
  }));
}

function normalizeMonitorSizeConversionState(monitor = {}) {
  const unconverted = getMonitorUnconvertedSizeValues(monitor);
  const hadBadSizeError = /Sizes with no EU conversion/i.test(String(monitor.lastError || ""));

  if (unconverted.length) {
    const lastError = `Sizes with no EU conversion: ${unconverted.join(", ")} - remove them before importing`;
    return {
      ...monitor,
      hasUsOnlySizes: true,
      usOnlySizesList: unconverted,
      status: "error",
      lastError
    };
  }

  if (monitor.hasUsOnlySizes || monitor.usOnlySizesList?.length || hadBadSizeError) {
    return {
      ...monitor,
      hasUsOnlySizes: false,
      usOnlySizesList: [],
      status: hadBadSizeError && monitor.status === "error" ? "ok" : monitor.status,
      lastError: hadBadSizeError ? "" : monitor.lastError
    };
  }

  return monitor;
}

function persistSizeConversionRepair(previous, next) {
  if (previous === next) return;
  if (previous.hasUsOnlySizes === next.hasUsOnlySizes &&
      previous.status === next.status &&
      previous.lastError === next.lastError) return;
  chrome.runtime.sendMessage({
    type: "update-monitor",
    payload: {
      id: next.id,
      hasUsOnlySizes: next.hasUsOnlySizes,
      usOnlySizesList: next.usOnlySizesList,
      status: next.status,
      lastError: next.lastError
    }
  }).catch(() => {});
}

function applyMonitorsUpdate(nextMonitors = []) {
  const previousMonitors = allMonitors;

  // ── Fast O(n) change-detection pass ─────────────────────────────────────
  // If nothing meaningful changed, skip ALL expensive downstream work.
  // This is the common case on auto-refresh ticks with no new checks.
  if (previousMonitors.length === nextMonitors.length) {
    let anythingChanged = false;
    const prevById = new Map(previousMonitors.map((m) => [m.id, m]));
    for (const m of nextMonitors) {
      const prev = prevById.get(m.id);
      if (!prev ||
          prev.lastCheckedAt !== m.lastCheckedAt ||
          prev.lastChangedAt !== m.lastChangedAt ||
          prev.status !== m.status ||
          prev.changeCount !== m.changeCount ||
          prev.lastError !== m.lastError ||
          prev.shopifyProductId !== m.shopifyProductId ||
          prev.shopifySyncStatus !== m.shopifySyncStatus ||
          prev.pendingInitialCheck !== m.pendingInitialCheck ||
          prev.hiddenFromNew48h !== m.hiddenFromNew48h) {
        anythingChanged = true;
        break;
      }
    }
    if (!anythingChanged) {
      // Data is identical — replace array reference so the rest of the app
      // always holds the latest object references, but skip all heavy work.
      allMonitors = nextMonitors;
      return;
    }
  }

  // ── Something changed — full update path ────────────────────────────────
  _invalidateMetaSig();
  _invalidateFilterOpts();
  reconcileOutOfStockMonitorUpdates(previousMonitors, nextMonitors);
  const prevById = new Map(previousMonitors.map((m) => [m.id, m]));
  nextMonitors = nextMonitors.map((monitor) => {
    const prev = prevById.get(monitor.id);
    // Skip normalization when check data, error, and size-override fields are all unchanged.
    if (prev &&
        prev.lastCheckedAt === monitor.lastCheckedAt &&
        prev.lastChangedAt === monitor.lastChangedAt &&
        prev.lastError === monitor.lastError &&
        prev.ignoredSizes === monitor.ignoredSizes &&
        prev.hasUsOnlySizes === monitor.hasUsOnlySizes) {
      return monitor;
    }
    const normalized = normalizeMonitorSizeConversionState(monitor);
    persistSizeConversionRepair(monitor, normalized);
    return normalized;
  });
  const importedNewMonitors = nextMonitors.filter((monitor) =>
    !monitor.hiddenFromNew48h &&
    !isMonitorMissingFromShopify(monitor)
  );
  if (importedNewMonitors.length) {
    const importedNewIds = new Set(importedNewMonitors.map((m) => m.id));
    nextMonitors = nextMonitors.map((monitor) =>
      importedNewIds.has(monitor.id)
        ? { ...monitor, hiddenFromNew48h: true }
        : monitor
    );
    importedNewMonitors.forEach((monitor) => {
      chrome.runtime.sendMessage({
        type: "update-monitor",
        payload: { id: monitor.id, hiddenFromNew48h: true }
      }).catch(() => {});
    });
  }
  // Invalidate diff cache and track dirty monitors for incremental card updates
  let _anyChanged = previousMonitors.length !== nextMonitors.length;
  for (const m of nextMonitors) {
    const prev = prevById.get(m.id);
    if (!prev) { _anyChanged = true; _dirtyMonitorIds.add(m.id); invalidateDiffCache(m.id); continue; }
    if (prev.lastChangedAt !== m.lastChangedAt) invalidateDiffCache(m.id);
    if (prev.status !== m.status || prev.lastCheckedAt !== m.lastCheckedAt ||
        prev.changeCount !== m.changeCount || prev.lastError !== m.lastError ||
        prev.shopifyProductId !== m.shopifyProductId || prev.shopifySyncStatus !== m.shopifySyncStatus ||
        prev.pendingInitialCheck !== m.pendingInitialCheck) {
      _anyChanged = true;
      _dirtyMonitorIds.add(m.id);
    }
  }
  if (_anyChanged) _gridDataVersion++;
  const nextIds = new Set(nextMonitors.map((m) => String(m.id || "")));
  for (const id of monitorMetaValueCache.keys()) {
    if (!nextIds.has(id)) monitorMetaValueCache.delete(id);
  }
  monitorImageCandidateCache.clear();
  allMonitors = nextMonitors;
  _monitorIndex = new Map(allMonitors.map((m, i) => [m.id, i]));
  invalidateMonitorOrderCaches();
  cachedMonitorMetaRenderData = null;
  _rebuildStatCaches();
  // Migrate any WoW monitors that just arrived without the image swap applied
  migrateWayOfWadeImages().catch(() => {});
  // Fix colorFinal for WoW monitors with CamelCase colorRaw — once per session
  if (!_wowColorMigrationDone) {
    _wowColorMigrationDone = true;
    chrome.runtime.sendMessage({ type: "fix-wow-colorfinal" })
      .then(() => silentRefresh())
      .catch(() => {});
  }
}

function _rebuildStatCaches() {
  _statErrors = 0; _statPending = 0; _statChanged = 0; _statMissingSku = 0; _statShopifyLinked = 0;
  _skuFreq = new Map();
  const errorParts = [];
  for (const m of allMonitors) {
    if (isErrorMonitor(m)) { _statErrors++; errorParts.push(`${m.id}:${m.lastError || ""}`); }
    if (isSavedPendingMonitor(m)) _statPending++;
    if (m.changeCount > 0) _statChanged++;
    if (m.shopifyProductId) _statShopifyLinked++;
    const sku = normalizeSku(m?.productData?.sku);
    if (sku) _skuFreq.set(sku, (_skuFreq.get(sku) || 0) + 1);
    else if (!isSavedPendingMonitor(m)) _statMissingSku++;
  }
  _cachedErrorSig = errorParts.join(",");
}

function _removeFromStatCaches(m) {
  if (isErrorMonitor(m)) { _statErrors--; _cachedErrorSig = null; }
  if (isSavedPendingMonitor(m)) _statPending--;
  if (m.changeCount > 0) _statChanged--;
  if (m.shopifyProductId) _statShopifyLinked--;
  const sku = normalizeSku(m?.productData?.sku);
  if (sku) {
    const c = (_skuFreq.get(sku) || 1) - 1;
    if (c <= 0) _skuFreq.delete(sku); else _skuFreq.set(sku, c);
  } else if (!isSavedPendingMonitor(m)) _statMissingSku--;
}

function _updateStatCaches(prev, updated) {
  const wasError = isErrorMonitor(prev), isError = isErrorMonitor(updated);
  if (wasError !== isError) { _statErrors += isError ? 1 : -1; _cachedErrorSig = null; }
  else if ((wasError || isError) && prev.lastError !== updated.lastError) { _cachedErrorSig = null; }
  const wasPending = isSavedPendingMonitor(prev), isPending = isSavedPendingMonitor(updated);
  if (wasPending !== isPending) _statPending += isPending ? 1 : -1;
  const wasChanged = prev.changeCount > 0, isChanged = updated.changeCount > 0;
  if (wasChanged !== isChanged) _statChanged += isChanged ? 1 : -1;
  const wasLinked = !!prev.shopifyProductId, isLinked = !!updated.shopifyProductId;
  if (wasLinked !== isLinked) _statShopifyLinked += isLinked ? 1 : -1;
  const oldSku = normalizeSku(prev?.productData?.sku);
  const newSku = normalizeSku(updated?.productData?.sku);
  if (oldSku !== newSku || wasPending !== isPending) {
    if (oldSku) {
      const c = (_skuFreq.get(oldSku) || 1) - 1;
      if (c <= 0) _skuFreq.delete(oldSku); else _skuFreq.set(oldSku, c);
    } else if (!wasPending) _statMissingSku--;
    if (newSku) _skuFreq.set(newSku, (_skuFreq.get(newSku) || 0) + 1);
    else if (!isPending) _statMissingSku++;
  }
}

function createStatCard(label, value, tone = "") {
  return `<article class="stat-card ${tone}"><span>${escapeHtml(label)}</span><strong>${escapeHtml(String(value))}</strong></article>`;
}

function getProductPrimarySku(product) {
  return normalizeSku(product?.sku) || [...getProductSkuSet(product)][0] || "";
}

function getShopifyDuplicateProductCount(products = []) {
  const safeProducts = Array.isArray(products) ? products : [];
  const productsWithSku = safeProducts.filter((product) => getProductPrimarySku(product));
  const uniqueSkus = new Set(productsWithSku.map(getProductPrimarySku).filter(Boolean));
  return productsWithSku.length - uniqueSkus.size;
}

function getShopifyMissingSkuCount(products = []) {
  const safeProducts = Array.isArray(products) ? products : [];
  return safeProducts.filter((product) => !getProductPrimarySku(product)).length;
}

function getMonitorUniqueSkuCount(monitors = allMonitors) {
  return new Set(
    (Array.isArray(monitors) ? monitors : [])
      .map((monitor) => normalizeSku(monitor?.productData?.sku))
      .filter(Boolean)
  ).size;
}

function getMonitorMissingSkuCount(monitors = allMonitors) {
  return (Array.isArray(monitors) ? monitors : []).filter((monitor) => !isSavedPendingMonitor(monitor) && !normalizeSku(monitor?.productData?.sku)).length;
}

function getMonitorsMissingSku(monitors = allMonitors) {
  return (Array.isArray(monitors) ? monitors : []).filter((monitor) => !isSavedPendingMonitor(monitor) && !normalizeSku(monitor?.productData?.sku));
}

function getShopifyComparableSkuSet(products = [], monitors = allMonitors) {
  const comparableSkus = new Set();
  const productById = new Map();
  for (const product of (Array.isArray(products) ? products : [])) {
    const productId = Number(product?.id || 0);
    if (productId) productById.set(productId, product);
    const primarySku = getProductPrimarySku(product);
    if (primarySku) comparableSkus.add(primarySku);
  }

  for (const monitor of (Array.isArray(monitors) ? monitors : [])) {
    const monitorSku = normalizeSku(monitor?.productData?.sku);
    if (!monitorSku) continue;
    const linkedProductId = Number(monitor?.shopifyProductId || 0);
    const linkedProduct = linkedProductId ? productById.get(linkedProductId) : null;
    if (linkedProduct) {
      // Product already counted via its own primary SKU in the product loop above.
      // Only add the monitor's SKU if the linked product has no primary SKU at all.
      if (!getProductPrimarySku(linkedProduct)) comparableSkus.add(monitorSku);
      continue;
    }
    const matchedBySku = (Array.isArray(products) ? products : []).some((product) => getProductSkuSet(product).has(monitorSku));
    if (matchedBySku) comparableSkus.add(monitorSku);
  }

  return comparableSkus;
}

function getShopifyComparableSkuCount(products = [], monitors = allMonitors) {
  return getShopifyComparableSkuSet(products, monitors).size;
}

function applyStoredGroupFilter(groupKey, monitors) {
  const filters = groupFilters.get(groupKey) || {};
  let result = Array.isArray(monitors) ? monitors : [];
  if (filters.brand) {
    result = result.filter((monitor) => canonicalizeBrand(getBrandFromMonitor(monitor) || "") === filters.brand);
  }
  if (filters.type) {
    result = result.filter((monitor) => (monitor.productData?.type || "").toLowerCase() === filters.type.toLowerCase());
  }
  return result;
}

function isErrorMonitor(monitor = {}) {
  if (monitor.pendingInitialCheck) return false;
  return monitor.status === "error" || !!String(monitor.lastError || "").trim() || getMissingProductDataFields(monitor).length > 0;
}

function isSavedPendingMonitor(monitor) {
  return !!monitor?.pendingInitialCheck;
}

function isMonitorMissingFromShopify(monitor) {
  if (isMonitorImportedToShopify(monitor)) {
    return false;
  }
  const monitorSku = normalizeSku(monitor?.productData?.sku);
  if (monitorSku && shopifyComparableSkuSet instanceof Set && shopifyComparableSkuSet.has(monitorSku)) {
    return false;
  }
  if (monitorOnlyMonitorIds instanceof Set) {
    return monitorOnlyMonitorIds.has(String(monitor?.id || ""));
  }
  return true;
}

function isMonitorImportedToShopify(monitor) {
  return !!(
    monitor?.shopifyProductId ||
    monitor?.shopifyImportedAt ||
    monitor?.shopifyProductHandle ||
    monitor?.shopifySyncStatus === "ok" ||
    (monitor?.shopifyLastSyncAt && monitor?.shopifySyncStatus !== "error")
  );
}

function getNewNotImportedMonitors(monitors) {
  return (Array.isArray(monitors) ? monitors : []).filter((monitor) =>
    isMonitorMissingFromShopify(monitor) &&
    !monitor.hiddenFromNew48h
  );
}

function markImportedNew48hMonitorsHidden() {
  const importedNewMonitors = allMonitors.filter((monitor) =>
    !monitor.hiddenFromNew48h &&
    !isMonitorMissingFromShopify(monitor)
  );
  if (!importedNewMonitors.length) return;
  const ids = new Set(importedNewMonitors.map((monitor) => monitor.id));
  allMonitors = allMonitors.map((monitor) =>
    ids.has(monitor.id) ? { ...monitor, hiddenFromNew48h: true } : monitor
  );
  importedNewMonitors.forEach((monitor) => {
    chrome.runtime.sendMessage({
      type: "update-monitor",
      payload: { id: monitor.id, hiddenFromNew48h: true }
    }).catch(() => {});
  });
}

let _lastStatsSig = "";
function renderStats(monitors) {
  const totalMonitorCount = allMonitors.length;
  // Use O(1) cached counters — maintained incrementally by _updateStatCaches/_rebuildStatCaches
  const savedPendingCount = _statPending;
  const changed = monitors === allMonitors ? _statChanged : monitors.filter(m => m.changeCount > 0).length;
  const errors = _statErrors;
  const uniqueMonitorSkuCount = _skuFreq.size;
  const missingMonitorSkuCount = _statMissingSku;
  const activeMetaSig = activeStoreMetaFilters.map(f => `${f.kind}:${f.value}`).sort().join("|");
  const shopifyLinked = _statShopifyLinked;
  const statsSig = `${totalMonitorCount}|${monitors.length}|${savedPendingCount}|${uniqueMonitorSkuCount}|${missingMonitorSkuCount}|${changed}|${errors}|${shopifyProductCount}|${shopifyUniqueSkuCount}|${shopifyMissingSkuCount}|${shopifyBadSizeVariantCount}|${monitorOnlyMonitorIds instanceof Set ? monitorOnlyMonitorIds.size : ""}|${activeMetaSig}|${shopifyLinked}`;
  if (statsSig === _lastStatsSig) return;
  _lastStatsSig = statsSig;

  const filteredErrors = applyStoredGroupFilter("__errors__", allMonitors.filter(isErrorMonitor)).length;
  const dupeCount = (shopifyProductCount != null && shopifyUniqueSkuCount != null)
    ? Math.max(0, shopifyProductCount - shopifyUniqueSkuCount - (shopifyMissingSkuCount || 0))
    : 0;
  const shopifyCountLabel = shopifyProductCount == null ? "..." : shopifyProductCount;
  const shopifySkuLabel = shopifyUniqueSkuCount == null ? "..." : shopifyUniqueSkuCount;
  const shopifyMissingLabel = shopifyMissingSkuCount == null ? "..." : shopifyMissingSkuCount;
  const shopifyProductsCard = shopifyProductCount == null
    ? createStatCard("Shopify Products", "...")
    : dupeCount > 0
      ? `<article class="stat-card" title="${dupeCount} product${dupeCount !== 1 ? "s" : ""} share a SKU with another Shopify product (duplicates). The diff buttons compare by unique SKU."><span>Shopify Products</span><strong>${shopifyCountLabel}</strong><div style="font-size:11px;color:#ef4444;margin-top:2px">+${dupeCount} duplicate${dupeCount !== 1 ? "s" : ""}</div></article>`
      : (shopifyMissingSkuCount || 0) > 0
        ? `<article class="stat-card" title="${shopifyMissingLabel} Shopify product${shopifyMissingLabel !== "1" ? "s" : ""} do not have a SKU."><span>Shopify Products</span><strong>${shopifyCountLabel}</strong><div style="font-size:11px;color:#ef4444;margin-top:2px">+${shopifyMissingLabel} missing SKU</div></article>`
      : createStatCard("Shopify Products", shopifyCountLabel);
    const changedActionsHtml = changed > 0 ? `
      <div class="stat-card-actions">
        <button class="stat-action-btn" data-stat-action="filter-changed">Filter</button>
        <button class="stat-action-btn" data-stat-action="mark-read">Mark all as read</button>
      </div>` : "";
    const errorActionsHtml = filteredErrors > 0 ? `
      <div class="stat-card-actions">
        <button class="stat-action-btn" data-stat-action="select-errors">Select all errors</button>
      </div>` : "";
    dashboardStats.innerHTML = [
      (() => {
        const isFiltered = monitors.length !== totalMonitorCount;
        if (!isFiltered) return createStatCard("Monitors", totalMonitorCount);
        return `<article class="stat-card warn" title="Showing ${monitors.length} of ${totalMonitorCount} monitors — filter is active"><span>Monitors</span><strong>${monitors.length} <span style="font-size:14px;font-weight:500;color:var(--muted)">/ ${totalMonitorCount}</span></strong><div style="font-size:11px;margin-top:2px;color:#f59e0b">filtered</div></article>`;
      })(),
      `<article class="stat-card ${savedPendingCount ? "warm" : ""}"><span>Saved to check</span><strong>${savedPendingCount}</strong>${savedPendingCount ? `<div class="stat-card-actions"><button class="stat-action-btn" data-stat-action="check-saved">Check saved</button><button class="stat-action-btn danger" data-stat-action="delete-saved">Delete all</button></div>` : ""}</article>`,
      createStatCard("Monitor Unique SKUs", uniqueMonitorSkuCount),
      missingMonitorSkuCount > 0
        ? `<article class="stat-card"><span>Monitors Without SKU</span><strong>${missingMonitorSkuCount}</strong><div class="stat-card-actions"><button class="stat-action-btn" data-stat-action="select-no-sku">Select all</button></div></article>`
        : createStatCard("Monitors Without SKU", missingMonitorSkuCount),
      shopifyProductsCard,
      (() => {
        if (shopifyProductCount == null) return createStatCard("Badged / In Store", "...");
        const diff = shopifyProductCount - shopifyLinked;
        const tone = diff > 0 ? "warn" : diff < 0 ? "danger" : "";
        const diffLabel = diff > 0 ? `+${diff} unlinked` : diff < 0 ? `${diff} ghost` : "in sync";
        return `<article class="stat-card ${tone}" title="${shopifyLinked} monitor${shopifyLinked !== 1 ? "s" : ""} have a Shopify badge · ${shopifyProductCount} product${shopifyProductCount !== 1 ? "s" : ""} in store · ${Math.abs(diff)} ${diff > 0 ? "store product(s) not linked to any monitor" : diff < 0 ? "monitor(s) with badge but no matching store product" : "perfectly in sync"}"><span>Badged / In Store</span><strong>${shopifyLinked} / ${shopifyProductCount}</strong><div style="font-size:11px;margin-top:2px;color:${diff === 0 ? "var(--muted)" : "inherit"}">${diffLabel}</div></article>`;
      })(),
      createStatCard("Shopify Matched SKUs", shopifySkuLabel),
      `<article class="stat-card warm"><span>Changed</span><strong>${changed}</strong>${changedActionsHtml}</article>`,
    `<article class="stat-card ${filteredErrors ? "danger" : ""}"><span>Errors</span><strong>${filteredErrors}</strong>${errorActionsHtml}</article>`
  ].join("");
}

function renderGridKeepingGroupPosition(groupKey) {
  const savedY = window.scrollY;
  renderGrid(getFilteredMonitors());
  window.scrollTo({ top: savedY, behavior: "instant" });
}

dashboardStats.addEventListener("click", async (e) => {
  const action = e.target.dataset.statAction;
  if (!action) return;
  if (action === "filter-changed") {
    changedOnlyInput.checked = !changedOnlyInput.checked;
    renderGrid(getFilteredMonitors());
  }
  if (action === "mark-read") {
    e.target.textContent = "…";
    e.target.disabled = true;
    await chrome.runtime.sendMessage({ type: "reset-all-change-counts" });
    if (changedOnlyInput.checked) {
      changedOnlyInput.checked = false;
      scheduleSaveDashboardUiState();
    }
    await silentRefresh();
  }
  if (action === "check-saved") {
    const ids = allMonitors.filter(isSavedPendingMonitor).map((monitor) => monitor.id);
    runBulkFirstCapture(ids);
  }
  if (action === "delete-saved") {
    const saved = allMonitors.filter(isSavedPendingMonitor);
    if (!saved.length) return;
    if (!window.confirm(`Delete ${saved.length} "Saved to check" monitor${saved.length !== 1 ? "s" : ""}? This cannot be undone.`)) return;
    const btn = e.target;
    btn.disabled = true;
    btn.textContent = "Deleting…";
    for (let i = 0; i < saved.length; i++) {
      await chrome.runtime.sendMessage({ type: "delete-monitor", monitorId: saved[i].id });
      if (saved[i].id === selectedMonitorId) {
        selectedMonitorId = null;
        _lastDetailSig = "";
        monitorDetail.style.display = "none";
      }
      removeMonitorFromDom(saved[i].id);
    }
    refreshUndoCount();
    scheduleDashboardAutoRefresh({ forceShopifyRefresh: false });
  }
  if (action === "select-errors") {
    allMonitors.filter(isErrorMonitor).forEach((monitor) => checkedIds.add(monitor.id));
    updateBulkBtn();
    const _f1 = getFilteredMonitors(); renderStats(_f1); renderGrid(_f1);
  }
  if (action === "select-no-sku") {
    getMonitorsMissingSku(allMonitors).forEach((monitor) => checkedIds.add(monitor.id));
    updateBulkBtn();
    const _f2 = getFilteredMonitors(); renderStats(_f2); renderGrid(_f2);
  }
});

function renderDiffViewer(rows) {
  const content = rows.length
    ? rows.map((r) => `
        <div class="diff-row">
          <div class="diff-cell ${r.leftType}">${escapeHtml(r.left || "")}</div>
          <div class="diff-cell ${r.rightType}">${escapeHtml(r.right || "")}</div>
        </div>`).join("")
    : `<div class="diff-row"><div class="diff-cell same" style="grid-column:1/-1;text-align:center;color:#5f6c7b">No differences detected</div></div>`;

  return `
    <section class="diff-viewer">
      <div class="diff-head"><span>Before</span><span>After</span></div>
      <div class="diff-grid">${content}</div>
    </section>`;
}

function buildHistoryEntry(entry, index, monitorId) {
  const liveChangeHtml = (entry.liveChanges && entry.liveChanges.length)
    ? `<div class="live-change-pills">${entry.liveChanges.map((c) => `<span class="live-change-pill">${escapeHtml(c)}</span>`).join("")}</div>`
    : "";

  return `
    <details class="history-entry" data-history-index="${index}" data-monitor-id="${escapeHtml(monitorId)}">
      <summary>
        <div>
          <strong>${escapeHtml(formatTimestamp(entry.changedAt, "Change recorded"))}</strong>
          <p class="subtle">${escapeHtml(entry.reason || "scheduled")}</p>
        </div>
        <div class="pill-row">
          <span class="pill neutral">expand to view</span>
        </div>
      </summary>
      ${liveChangeHtml}
      <div class="history-diff-placeholder"></div>
    </details>`;
}

function truncateHtml(str, maxLen = 150000) {
  if (!str || str.length <= maxLen) return escapeHtml(str || "");
  const kb = Math.round(str.length / 1024);
  return escapeHtml(str.slice(0, maxLen)) + `\n\n… [truncated — full size: ${kb} KB]`;
}

function renderSelectedOuterHtmlPanel(monitor, selectors) {
  if (!selectors.length) return "";
  const hasSnapshot = !!(
    monitor.hasLastSelectedOuterHtmlSnapshot ||
    monitor.lastSelectedOuterHtmlSnapshot
  );
  return `
    <details class="html-section selected-html-section" data-monitor-id="${escapeHtml(monitor.id)}">
      <summary>Selected parts outerHTML</summary>
      <p class="subtle" style="margin:12px 12px 8px">Raw HTML captured from the selected page parts.</p>
      <div class="selected-html-placeholder" style="padding:0 12px 12px;color:var(--muted);font-size:12px">${hasSnapshot ? "Open to load selected outerHTML." : "Click Check now to capture selected outerHTML."}</div>
    </details>
  `;
}

function detectDsgCleatTypeLocal(url) {
  if (!url || !/dickssportinggoods\.com/i.test(url)) return null;
  const slug = (url.split('?')[0] || '').toLowerCase();
  const types = new Set();
  if (/-mxsg-/.test(slug)) { types.add('Molded'); types.add('Soft Ground'); }
  if (/-fgmg-/.test(slug)) { types.add('Firm Ground'); types.add('Molded'); }
  if (/-agfg-/.test(slug)) { types.add('Artificial Grass'); types.add('Firm Ground'); }
  if (/-hgmg-/.test(slug)) { types.add('Hard Ground'); types.add('Molded'); }
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

function renderProductData(p, monitorId, priceAdjustment = 80, monitorUrl = "") {
  if (!p) return "";

  const field = (label, value) => value
    ? `<div class="product-field"><span class="product-field-label">${escapeHtml(label)}</span><span class="product-field-value">${escapeHtml(String(value))}</span></div>`
    : "";

  const priceStr = p.price != null
    ? (p.currency ? `${p.currency} ${p.price}` : String(p.price))
    : null;

  const sourceTags = (p.source || []).map((s) => `<span class="source-tag">${escapeHtml(s)}</span>`).join("");

  const adjustmentStr = Number.isFinite(Number(priceAdjustment)) ? String(Number(priceAdjustment)) : "80";

  const hasAnyField = p.name || p.brand || p.type || p.color || p.colorRaw || p.colorFinal || priceStr || p.sku || p.description || adjustmentStr;
  if (!hasAnyField) return "";

  const _rawImgs = p.images || [];
  const _displayImgs = (() => {
    if (_rawImgs.length < 2 || p._wowImgSwapped) return _rawImgs;
    if (!/way\s*of\s*wade|li[\s-]*ning|lining/i.test(String(p.brand || ""))) return _rawImgs;
    const [a, b, ...rest] = _rawImgs;
    return [b, a, ...rest];
  })();
  const _isWoWBrand = /way\s*of\s*wade|li[\s-]*ning|lining/i.test(String(p.brand || ""));
  const imagesHtml = _displayImgs.length ? `
    <div class="product-images-row">
      ${_displayImgs.map((src) => { const transformed = _isWoWBrand ? transformWayOfWadeImageSrc(src, 680) : transformDsgImageSrc(src); return `<a href="${escapeHtml(transformed)}" target="_blank" rel="noopener" class="product-thumb-link"><img class="product-thumb" src="${escapeHtml(transformed)}" alt="" loading="lazy"></a>`; }).join("")}
    </div>` : "";

  const editBtn = monitorId
    ? `<button class="ghost pdata-edit-btn" data-monitor-id="${escapeHtml(monitorId)}" style="font-size:11px;padding:4px 10px;border-radius:8px;line-height:1">Edit</button>`
    : "";

  return `
    <div class="product-data-section">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <p class="product-data-title">Extracted product data</p>
        ${editBtn}
      </div>
      ${imagesHtml}
      <div class="product-fields">
        ${field("Name", p.name)}
        ${field("Brand", p.brand)}
        ${field("Player", p.player)}
        ${field("Type", p.type)}
        ${field("Gender", p.gender)}
        ${field("Gender display", p.genderDisplay)}
        ${field("Color raw", p.colorRaw)}
        ${field("Color", p.color)}
        ${field("Color final", p.colorFinal)}
        ${field("Added amount", adjustmentStr)}
        ${field("Price", priceStr)}
        ${field("SKU / Code", p.sku)}
        ${field("Cleats", p.cleatType || detectDsgCleatTypeLocal(monitorUrl))}
      </div>
      ${p.description ? `<div class="product-description-block"><p class="product-field-label">Description</p><p class="product-description">${escapeHtml(p.description)}</p></div>` : ""}
      ${sourceTags ? `<div class="source-row">${sourceTags}</div>` : ""}
    </div>`;
}

function renderProductDataEdit(p, monitorId) {
  const val = (v) => escapeHtml(v || "");
  const imagesEditHtml = Array.isArray(p.images) && p.images.length
    ? `<div class="pdata-image-editor">
        <p class="product-field-label" style="margin-bottom:6px">Images</p>
        <div class="pdata-image-list">
          ${p.images.map((src, i) => {
            const t = transformDsgImageSrc(src);
            return `<div class="pdata-image-row" data-url="${escapeHtml(src)}">
              <img class="pdata-img-thumb" src="${escapeHtml(t)}" alt="">
              <span class="pdata-img-url" title="${escapeHtml(src)}">${escapeHtml(src.replace(/^https?:\/\//, "").slice(0, 60))}…</span>
              <div class="pdata-img-btns">
                <button class="ghost pdata-img-up" title="Move up" ${i === 0 ? "disabled" : ""}>↑</button>
                <button class="ghost pdata-img-down" title="Move down" ${i === p.images.length - 1 ? "disabled" : ""}>↓</button>
                <button class="ghost pdata-img-remove" title="Remove" style="color:#ef4444">Ã—</button>
              </div>
            </div>`;
          }).join("")}
        </div>
      </div>`
    : "";
  return `
    <div class="product-data-section">
      <p class="product-data-title">Edit product data</p>
      ${imagesEditHtml}
      <div class="product-fields pdata-edit-fields">
        <div class="product-field">
          <span class="product-field-label">Name</span>
          <input class="pdata-input" data-field="name" type="text" value="${val(p.name)}">
        </div>
        <div class="product-field">
          <span class="product-field-label">Brand</span>
          <input class="pdata-input" data-field="brand" type="text" value="${val(p.brand)}">
        </div>
        <div class="product-field">
          <span class="product-field-label">Type</span>
          <input class="pdata-input" data-field="type" type="text" value="${val(p.type)}">
        </div>
        <div class="product-field">
          <span class="product-field-label">Gender</span>
          <input class="pdata-input" data-field="gender" type="text" value="${val(p.gender)}">
        </div>
        <div class="product-field">
          <span class="product-field-label">Color</span>
          <input class="pdata-input" data-field="color" type="text" value="${val(p.color)}">
        </div>
        <div class="product-field">
          <span class="product-field-label">SKU / Code</span>
          <input class="pdata-input" data-field="sku" type="text" value="${val(p.sku)}">
        </div>
        <div class="product-field" style="grid-column:1/-1">
          <span class="product-field-label">Description</span>
          <textarea class="pdata-input pdata-textarea" data-field="description" rows="4">${val(p.description)}</textarea>
        </div>
      </div>
      <div style="display:flex;gap:8px;margin-top:4px">
        <button class="primary pdata-save-btn" data-monitor-id="${escapeHtml(monitorId)}" style="font-size:13px;padding:8px 16px">Save</button>
        <button class="ghost pdata-cancel-btn" style="font-size:13px;padding:8px 16px">Cancel</button>
      </div>
    </div>`;
}

function refreshImageEditButtons(list) {
  if (!list) return;
  const rows = list.querySelectorAll(".pdata-image-row");
  rows.forEach((row, i) => {
    row.querySelector(".pdata-img-up").disabled = i === 0;
    row.querySelector(".pdata-img-down").disabled = i === rows.length - 1;
  });
}

function getIntervalDisplay(minutes) {
  const m = minutes || 1440;
  if (m >= 1440 && m % 1440 === 0) return { value: m / 1440, unit: 1440 };
  if (m >= 60 && m % 60 === 0) return { value: m / 60, unit: 60 };
  return { value: m, unit: 1 };
}

function getMonitorThumb(monitor) {
  const images = getMonitorImageCandidates(monitor);
  if (!Array.isArray(images) || !images.length) return "";
  return transformDsgImageSrc(images[0]);
}

function transformWayOfWadeImageSrc(src, width = 680) {
  try {
    if (!/^https?:\/\/([a-z0-9-]+\.)*wayofwade\.com\/cdn\//i.test(src)) return src;
    const base = src.split("?")[0];
    return `${base}?width=${width}&height=${width}&crop=center`;
  } catch (_) { return src; }
}

function transformMonitorCardThumbSrc(src) {
  try {
    if (/^https?:\/\/dks\.scene7\.com\/is\/image\//i.test(src)) {
      const base = src.split("?")[0];
      return `${base}?wid=220&hei=160&extend=30,30,30,30&bgc=255,255,255&fmt=jpg&qlt=60`;
    }
    if (/^https?:\/\/([a-z0-9-]+\.)*wayofwade\.com\/cdn\//i.test(src)) {
      return transformWayOfWadeImageSrc(src, 400);
    }
  } catch (_) {}
  return transformDsgImageSrc(src);
}

function getMonitorCardThumbCandidates(monitor) {
  const images = getMonitorImageCandidates(monitor);
  if (!Array.isArray(images) || !images.length) return [];
  const seen = new Set();
  const candidates = [];
  for (const image of images) {
    for (const src of [transformMonitorCardThumbSrc(image), image]) {
      const clean = String(src || "").trim();
      if (!clean) continue;
      const key = clean.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      candidates.push(clean);
      if (candidates.length >= THUMB_CANDIDATE_LIMIT) return candidates;
    }
  }
  return candidates;
}

function getMonitorCardThumb(monitor) {
  return getMonitorCardThumbCandidates(monitor)[0] || "";
}

function clearShopifyProductImageCache() {
  shopifyProductImageById.clear();
  shopifyProductImageBySku.clear();
  shopifyProductImageCacheVersion++;
  _gridDataVersion++;
  monitorImageCandidateCache.clear();
}

function rebuildShopifyProductImageCache(products = []) {
  shopifyProductImageById.clear();
  shopifyProductImageBySku.clear();
  for (const product of (Array.isArray(products) ? products : [])) {
    const image = String(product.image || "").trim();
    if (!image) continue;
    const productId = Number(product.id || 0);
    if (productId) shopifyProductImageById.set(String(productId), image);
    for (const sku of getProductSkuSet(product)) {
      if (sku) shopifyProductImageBySku.set(sku, image);
    }
  }
  shopifyProductImageCacheVersion++;
  _gridDataVersion++;
  monitorImageCandidateCache.clear();
}

function getMonitorImageCandidates(monitor = {}) {
  const cacheKey = [
    monitor.id || "",
    (monitor.productData?.images || []).join("|"),
    (monitor.productDataOverrides?.images || []).join("|"),
    shopifyProductImageCacheVersion,
    monitor.shopifyProductId || "",
    monitor.productData?.sku || "",
    monitor.productData?.brand || "",
    monitor.productData?.name || ""
  ].join("::");
  if (monitorImageCandidateCache.has(cacheKey)) return monitorImageCandidateCache.get(cacheKey);
  const finish = (images = []) => {
    const seen = new Set();
    const clean = (Array.isArray(images) ? images : [])
      .map((src) => String(src || "").trim())
      .filter(Boolean)
      .filter((src) => {
        const key = src.toLowerCase();
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    monitorImageCandidateCache.set(cacheKey, clean);
    return clean;
  };
  const pd = monitor.productData || {};
  // Way of Wade: swap images[0] <-> images[1] at display time (only before storage migration runs)
  const _wowRaw = monitor.productData?.images;
  const directImages = (() => {
    if (!Array.isArray(_wowRaw) || _wowRaw.length < 2 || pd._wowImgSwapped) return _wowRaw;
    const brand = String(pd.brand || getBrandFromMonitor(monitor) || "");
    if (!/way\s*of\s*wade|li[\s-]*ning|lining/i.test(brand)) return _wowRaw;
    const [a, b, ...rest] = _wowRaw;
    return [b, a, ...rest];
  })();
  const overrideImages = monitor.productDataOverrides?.images;
  const sku = normalizeSku(pd.sku || "");
  const nameKey = normalizeLookupText([pd.brand, pd.name].filter(Boolean).join(" "));
  const shopifyProductId = Number(monitor.shopifyProductId || 0);
  const shopifyImages = [];
  if (shopifyProductId && shopifyProductImageById.has(String(shopifyProductId))) {
    shopifyImages.push(shopifyProductImageById.get(String(shopifyProductId)));
  }
  if (sku && shopifyProductImageBySku.has(sku)) {
    shopifyImages.push(shopifyProductImageBySku.get(sku));
  }

  const match = allMonitors.find((candidate) => {
    if (!candidate || candidate.id === monitor.id) return false;
    const images = candidate.productData?.images || candidate.productDataOverrides?.images;
    if (!Array.isArray(images) || !images.length) return false;
    const candidateSku = normalizeSku(candidate.productData?.sku || "");
    if (sku && candidateSku && sku === candidateSku) return true;
    const candidateNameKey = normalizeLookupText([candidate.productData?.brand, candidate.productData?.name].filter(Boolean).join(" "));
    return !!nameKey && nameKey === candidateNameKey;
  });
  const fallbackImages = match?.productData?.images || match?.productDataOverrides?.images;
  return finish([
    ...(Array.isArray(directImages) ? directImages : []),
    ...(Array.isArray(overrideImages) ? overrideImages : []),
    ...shopifyImages,
    ...(Array.isArray(fallbackImages) ? fallbackImages : [])
  ]);
}

function renderSquareLiveData(monitor) {
  const d = monitor.lastExtractedData;
  if (!d) return "";
  const rows = [];
  if (d.price != null) rows.push(`<span class="square-live-line">price: ${escapeHtml(`$${Math.round(Number(d.price))}`)}</span>`);
  if (d.compareAt != null) rows.push(`<span class="square-live-line">compare: ${escapeHtml(`$${Math.round(Number(d.compareAt))}`)}</span>`);
  if (d.inStock?.length) rows.push(`<span class="square-live-line">in: ${escapeHtml((d.inStock || []).join(", "))}</span>`);
  if (d.outOfStock?.length) rows.push(`<span class="square-live-line">out: ${escapeHtml((d.outOfStock || []).join(", "))}</span>`);
  if (!rows.length) return "";
  const title = [
    d.price != null ? `price: $${Math.round(Number(d.price))}` : "",
    d.compareAt != null ? `compare: $${Math.round(Number(d.compareAt))}` : "",
    d.inStock?.length ? `in: ${(d.inStock || []).join(", ")}` : "",
    d.outOfStock?.length ? `out: ${(d.outOfStock || []).join(", ")}` : ""
  ].filter(Boolean).join(" | ");
  return `<div class="square-live" title="${escapeHtml(title)}">${rows.join("")}</div>`;
}

let _thumbObserver = null;

function hydrateThumbImage(img) {
  if (!(img instanceof HTMLImageElement) || !img.classList.contains("square-thumb")) return;
  const src = img.dataset.src;
  if (!src || img.hasAttribute("src")) return;
  img.src = src;
  delete img.dataset.src;
}

function initThumbLazyLoading(root = monitorGrid) {
  const scope = root instanceof Element || root instanceof Document ? root : monitorGrid;
  const imgs = scope.querySelectorAll("img.square-thumb[data-src]");
  if (!imgs.length) return;
  if (!("IntersectionObserver" in window)) {
    imgs.forEach(hydrateThumbImage);
    return;
  }
  if (!_thumbObserver) {
    _thumbObserver = new IntersectionObserver((entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue;
        _thumbObserver.unobserve(entry.target);
        hydrateThumbImage(entry.target);
      }
    }, { rootMargin: THUMB_LAZY_ROOT_MARGIN });
  }
  imgs.forEach((img) => _thumbObserver.observe(img));
}

function isMonitorWithoutImage(monitor) {
  return getMissingProductDataFields(monitor).includes("pictures") || getMonitorCardThumbCandidates(monitor).length === 0;
}

function renderSquare(monitor, groupKey = "") {
  const isActive = selectedMonitorId === monitor.id;
  const isChecked = checkedIds.has(monitor.id);
  const num = _monitorNumMap.get(monitor.id) ?? 0;
  const thumbCandidates = getMonitorCardThumbCandidates(monitor);
  const thumb = thumbCandidates[0] || "";
  const thumbCandidatesAttr = escapeHtml(JSON.stringify(thumbCandidates));
  const importedBadge = monitor.shopifyProductId ? `<span class="square-imported">Shopify</span>` : "";
  const savedBadge = isSavedPendingMonitor(monitor) ? `<span class="square-saved">Saved</span>` : "";
  const outOfStockBadge = isMonitorFullyOutOfStock(monitor) ? `<span class="square-oos">Out of stock</span>` : "";
  const usOnlyBadge = monitor.hasUsOnlySizes ? `<span class="square-us-only" title="Has sizes with no EU conversion — import blocked">Bad sizes</span>` : "";
  const staleFractionBadge = !monitor.hasUsOnlySizes && monitor.hasStaleFractionalEuSizes
    ? `<span class="square-us-only" title="Fraction EU sizes are showing as decimals; recheck to refresh">Recheck sizes</span>`
    : "";
  const showWebsiteBadge = groupKey === "__errors__" || groupKey === SHOPIFY_OOS_GROUP_KEY;
  const websiteName = showWebsiteBadge ? getWebsiteShortName(monitor.url) : "";
  const websiteBadge = websiteName ? `<span class="square-website" title="${escapeHtml(getDomain(monitor.url))}">${escapeHtml(websiteName)}</span>` : "";
  const adjustment = Number.isFinite(Number(monitor.priceAdjustment)) ? Number(monitor.priceAdjustment) : 80;
  const pd = monitor.productData || {};
  const cardMeta = [pd.type, pd.genderDisplay || pd.gender, pd.colorFinal || pd.color, pd.player]
    .map((value) => String(value || "").trim())
    .filter(Boolean);
  const metafields = getMonitorExpectedMetafields(monitor);
  const missingFields = getMissingProductDataFields(monitor);
  const derivedError = monitor.lastError || (missingFields.length ? `Missing product data: ${missingFields.join(", ")}` : "");
  const displayStatus = derivedError ? "error" : (monitor.status || "idle");
  const errorHtml = derivedError
    ? `<p class="square-error" title="${escapeHtml(derivedError)}">${escapeHtml(derivedError)}</p>`
    : "";

  return `
    <article class="monitor-square${isActive ? " active" : ""}" data-id="${monitor.id}">
      <span class="monitor-num">#${num}</span>
      <input type="checkbox" class="square-check" data-id="${monitor.id}"${isChecked ? " checked" : ""} title="Select for bulk check">
      <div class="status-dot ${escapeHtml(displayStatus)}"></div>
      ${importedBadge}
      ${savedBadge}
      ${outOfStockBadge}
      ${usOnlyBadge}
      ${staleFractionBadge}
      ${websiteBadge}
      ${thumb ? `<div class="square-thumb-wrap square-thumb-loading" data-thumb-candidates="${thumbCandidatesAttr}" data-thumb-index="0"><span class="square-thumb-state-text">Loading</span><img class="square-thumb" data-src="${escapeHtml(thumb)}" alt="${escapeHtml(monitor.productData?.name || monitor.name || "Product image")}" loading="lazy" decoding="async" fetchpriority="low"></div>` : `<div class="square-thumb-wrap square-thumb-placeholder"><span class="square-thumb-state-text">No image</span></div>`}
      ${(() => {
        const brand = pd?.brand || "";
        const sku = pd?.sku || "";
        const cardTitle = [brand, sku].filter(Boolean).join(" ") || monitor.name;
        const rawName = pd?.name || "";
        const productName = rawName.replace(/\u00C2/g, "");
        const mfHtml = metafields.length
          ? `<p class="square-mf-line">${metafields.map(f => `<span class="square-mf-badge">${escapeHtml(f.key)}: ${escapeHtml(f.value)}</span>`).join("")}</p>`
          : "";
        return `<p class="square-name" title="${escapeHtml(cardTitle)}">${escapeHtml(cardTitle)}</p>
      ${productName ? `<p class="square-domain" title="${escapeHtml(productName)}" style="white-space:normal;line-height:1.3;font-size:10px">${escapeHtml(productName)}</p>` : `<p class="square-domain" title="${escapeHtml(getDomain(monitor.url))}">${escapeHtml(getDomain(monitor.url))}</p>`}
      ${cardMeta.length ? `<p class="square-data-line" title="${escapeHtml(cardMeta.join(" / "))}">${escapeHtml(cardMeta.join(" / "))}</p>` : ""}
      ${mfHtml}
      <p class="square-domain" title="Added amount: ${escapeHtml(String(adjustment))}" style="font-size:10px">Added: ${escapeHtml(String(adjustment))}</p>`;
      })()}
      ${errorHtml}
      ${renderSquareLiveData(monitor)}
      <div class="square-meta">
        ${monitor.changeCount > 0 ? `<span class="square-changes">${monitor.changeCount}</span>` : ""}
        <span class="square-time">${escapeHtml(timeAgo(monitor.lastCheckedAt))}</span>
        <span class="auto-badge ${autoEnabled ? "auto-on" : "auto-off"}">${autoEnabled ? "AUTO" : "MANUAL"}</span>
      </div>
    </article>`;
}

document.addEventListener("load", (event) => {
  const img = event.target;
  if (!(img instanceof HTMLImageElement) || !img.classList.contains("square-thumb")) return;
  img.closest(".square-thumb-wrap")?.classList.remove("square-thumb-loading");
}, true);

document.addEventListener("error", (event) => {
  const img = event.target;
  if (!(img instanceof HTMLImageElement) || !img.classList.contains("square-thumb")) return;
  const wrap = img.closest(".square-thumb-wrap");
  if (!wrap) {
    img.remove();
    return;
  }
  let candidates = [];
  try {
    candidates = JSON.parse(wrap.dataset.thumbCandidates || "[]");
  } catch (_) {
    candidates = [];
  }
  const nextIndex = Number(wrap.dataset.thumbIndex || "0") + 1;
  if (Array.isArray(candidates) && candidates[nextIndex]) {
    wrap.dataset.thumbIndex = String(nextIndex);
    wrap.classList.add("square-thumb-loading");
    wrap.classList.remove("square-thumb-placeholder");
    const label = wrap.querySelector(".square-thumb-state-text");
    if (label) label.textContent = "Loading";
    img.src = candidates[nextIndex];
    return;
  }
  if (wrap) {
    wrap.classList.remove("square-thumb-loading");
    wrap.classList.add("square-thumb-placeholder");
    const label = wrap.querySelector(".square-thumb-state-text");
    if (label) label.textContent = "No image";
  }
  img.remove();
}, true);

const SIZE_CHARTS = {
  Nike: {
    Men: {"3.5":"35.5","4":"36","4.5":"36.5","5":"37.5","5.5":"38","6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"45.5","12":"46","12.5":"47","13":"47.5","13.5":"48","14":"48.5","14.5":"49","15":"49.5","15.5":"50","16":"50.5","16.5":"51","17":"51.5","17.5":"52","18":"52.5"},
    Women: {"5":"35.5","5.5":"36","6":"36.5","6.5":"37.5","7":"38","7.5":"38.5","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"42","10.5":"42.5","11":"43","11.5":"44","12":"44.5","12.5":"45","13":"45.5","13.5":"46","14":"47","14.5":"47.5","15":"48"}
  },
  Jordan: {
    Men: {"3":"35.5","3.5":"36","4":"36.5","4.5":"37.5","5":"37.5","5.5":"38","6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"45.5","12":"46","12.5":"47","13":"47.5","13.5":"48","14":"48.5","14.5":"49","15":"49.5","15.5":"50","16":"50.5","16.5":"51","17":"51.5","17.5":"52","18":"52.5"},
    Women: {"5":"35.5","5.5":"36","6":"36.5","6.5":"37.5","7":"38","7.5":"38.5","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"42","10.5":"42.5","11":"43","11.5":"44","12":"44.5","12.5":"45","13":"45.5","13.5":"46","14":"47","14.5":"47.5","15":"48"}
  },
  Adidas: {
    Men: {"4":"36","4.5":"36 2/3","5":"37 1/3","5.5":"38","6":"38 2/3","6.5":"39 1/3","7":"40","7.5":"40 2/3","8":"41 1/3","8.5":"42","9":"42 2/3","9.5":"43 1/3","10":"44","10.5":"44 2/3","11":"45 1/3","11.5":"46","12":"46 2/3","12.5":"47 1/3","13":"48","13.5":"48 2/3","14":"49 1/3","14.5":"50","15":"50 2/3","16":"51 1/3","17":"52 2/3","18":"53 1/3","19":"54 2/3","20":"55 2/3"},
    Women: {"5":"36","5.5":"36 2/3","6":"37 1/3","6.5":"38","7":"38 2/3","7.5":"39 1/3","8":"40","8.5":"40 2/3","9":"41 1/3","9.5":"42","10":"42 2/3","10.5":"43 1/3","11":"44","11.5":"44 2/3","12":"45 1/3","12.5":"46","13":"46 2/3"}
  },
  Asics: {
    Men: {"4":"36","4.5":"37","5":"37.5","5.5":"38","6":"39","6.5":"39.5","7":"40","7.5":"40.5","8":"41.5","8.5":"42","9":"42.5","9.5":"43.5","10":"44","10.5":"44.5","11":"45","11.5":"46","12":"47"},
    Women: {"4":"34.5","4.5":"35","5":"35.5","5.5":"36","6":"37","6.5":"37.5","7":"38","7.5":"39","8":"39.5","8.5":"40","9":"40.5","9.5":"41.5","10":"42","10.5":"42.5","11":"43.5","11.5":"44","12":"44.5","12.5":"45","13":"46","14":"47"}
  },
  "New Balance": {
    Men: {"4":"36","4.5":"37","5":"37.5","5.5":"38","6":"38.5","6.5":"39.5","7":"40","7.5":"40.5","8":"41.5","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"45.5","12":"46.5","12.5":"47","13":"47.5","14":"49","15":"50","16":"51","17":"52","18":"53"},
    Women: {"4":"34","5":"35","5.5":"36","6":"36.5","6.5":"37","7":"37.5","7.5":"38","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"41.5","10.5":"42.5","11":"43","11.5":"43.5","12":"44","13":"45.5"}
  },
  Reebok: {
    Men: {"4":"34.5","4.5":"35","5":"36","5.5":"36.5","6":"37.5","6.5":"38.5","7":"39","7.5":"40","8":"40.5","8.5":"41","9":"42","9.5":"42.5","10":"43","10.5":"44","11":"44.5","11.5":"45","12":"45.5","12.5":"46","13":"47","13.5":"48","14":"48.5","15":"50","16":"52","17":"53.5","18":"55"},
    Women: {"5.5":"34.5","6":"35","6.5":"36","7":"36.5","7.5":"37.5","8":"38.5","8.5":"39","9":"40","9.5":"40.5","10":"41","10.5":"42","11":"42.5","11.5":"43","12":"44"}
  },
  Puma: {
    Men: {"6":"38","6.5":"38.5","7":"39","7.5":"40","8":"40.5","8.5":"41","9":"42","9.5":"42.5","10":"43","10.5":"44","11":"44.5","11.5":"45","12":"46","12.5":"46.5","13":"47","14":"48.5","15":"49.5","16":"51"},
    Women: {"5.5":"35.5","6":"36","6.5":"37","7":"37.5","7.5":"38","8":"38.5","8.5":"39","9":"40","9.5":"40.5","10":"41","10.5":"42","11":"42.5"}
  },
  Converse: {
    Men: {"6":"38.5","6.5":"39","7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"46","12":"46.5","13":"47.5","14":"49","15":"50","16":"51.5"}
  },
  On: {
    Men: {"7":"40","7.5":"40.5","8":"41","8.5":"42","9":"42.5","9.5":"43","10":"44","10.5":"44.5","11":"45","11.5":"46","12":"47","12.5":"47.5","13":"48","14":"49"},
    Women: {"5":"36","5.5":"36.5","6":"37","6.5":"37.5","7":"38","7.5":"38.5","8":"39","8.5":"40","9":"40.5","9.5":"41","10":"42","10.5":"42.5","11":"43"}
  },
  Hoka: {
    Men: {"5":"37 1/3","5.5":"38","6":"38 2/3","6.5":"39 1/3","7":"40","7.5":"40 2/3","8":"41 1/3","8.5":"42","9":"42 2/3","9.5":"43 1/3","10":"44","10.5":"44 2/3","11":"45 1/3","11.5":"46","12":"46 2/3","12.5":"47 1/3","13":"48","13.5":"48 2/3","14":"49 1/3","14.5":"50","15":"50 2/3"},
    Women: {"5":"36","5.5":"36 2/3","6":"37 1/3","6.5":"38","7":"38 2/3","7.5":"39 1/3","8":"40","8.5":"40 2/3","9":"41 1/3","9.5":"42","10":"42 2/3","10.5":"43 1/3","11":"44","11.5":"44 2/3","12":"45 1/3","12.5":"46","13":"46 2/3","13.5":"47 1/3","14":"48","14.5":"48 2/3","15":"49 1/3","15.5":"50"}
  },
  "Way of Wade": {
    Men: {"4.5":"36 1/3","5":"37","5.5":"37 2/3","6":"38 1/3","6.5":"39","7":"39 2/3","7.5":"40 1/3","8":"41","8.5":"41 2/3","9":"42 1/3","9.5":"43","10":"43 2/3","10.5":"44 1/3","11":"45","11.5":"45 2/3","12":"46 1/3","12.5":"47","13":"47 2/3","13.5":"48 1/3","14":"49","14.5":"49 2/3","15":"50 1/3"}
  }
};

function normalizeUsSizeChartKey(value) {
  const match = String(value || "").replace(",", ".").match(/\d+(?:\.\d+)?/);
  if (!match) return "";
  const n = Number(match[0]);
  if (!Number.isFinite(n) || n <= 0 || n > 30) return "";
  return Number.isInteger(n) ? String(n) : String(n).replace(/0+$/, "").replace(/\.$/, "");
}

function getEuSize(usSize, brand, gender) {
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
  else if (/new\s*balance|^nb$/i.test(b)) chartBrand = "New Balance";
  else if (/reebok/i.test(b)) chartBrand = "Reebok";
  else if (/puma/i.test(b)) chartBrand = "Puma";
  else if (/converse/i.test(b)) chartBrand = "Converse";
  else if (/^on(?:\s+cloud|\s+running|\s+cloud\s+running)?$/i.test(b)) chartBrand = "On";
  else if (/hoka/i.test(b)) chartBrand = "Hoka";
  else if (/way\s*of\s*wade|li[\s-]*ning|lining/i.test(b)) chartBrand = "Way of Wade";
  if (!chartBrand) return null;
  const chartGender = /women|girl|female/i.test(String(gender || "")) ? "Women" : "Men";
  const chart = SIZE_CHARTS[chartBrand]?.[chartGender];
  if (!chart) return null;
  const sizeKey = normalizeUsSizeChartKey(usSize);
  const n = Number(sizeKey);
  if (isNaN(n) || n <= 0 || n > 30) return null;
  return chart[sizeKey] || null;
}

function formatLiveSizeLabel(size, brand, gender) {
  const eu = getEuSize(size, brand, gender);
  if (!eu) return String(size);
  return `${size} US / ${eu} EU`;
}

function renderLiveExtracted(monitor) {
  const d = monitor.lastExtractedData;
  const prev = monitor.previousExtractedData;
  const hasData = d && (d.price != null || d.compareAt != null || d.inStock?.length || d.outOfStock?.length);
  if (!hasData) return "";

  const fmt = (n) => n != null ? `$${Math.round(Number(n))}` : null;

  const prevInSet = new Set(prev?.inStock || []);
  const prevOosSet = new Set(prev?.outOfStock || []);
  const prevAllSet = new Set([...prevInSet, ...prevOosSet]);

  const priceChanged = prev != null && prev.price !== d.price;
  const compareChanged = prev != null && prev.compareAt !== d.compareAt;

  const brand = monitor.productData?.brand || null;
  const gender = monitor.productData?.gender || null;

  const sizeChip = (s, cls, refInSet, refOosSet, refAllSet) => {
    let changed = false;
    if (refAllSet && refAllSet.size > 0) {
      if (cls === "available") changed = refOosSet.has(s) || !refAllSet.has(s);
      if (cls === "oos") changed = refInSet.has(s) || !refAllSet.has(s);
    }
    const label = formatLiveSizeLabel(s, brand, gender);
    return `<span class="size-chip ${cls}${changed ? " live-changed" : ""}">${escapeHtml(label)}</span>`;
  };

  const onSale = d.compareAt != null && d.price != null && d.compareAt > d.price;
  const priceHtml = d.price != null || d.compareAt != null ? `
    <div class="live-price-row">
      ${d.price != null ? `<span class="live-price-main${priceChanged ? " live-changed-price" : ""}">${escapeHtml(fmt(d.price))}</span>` : ""}
      ${d.compareAt != null ? `<span class="live-price-compare${compareChanged ? " live-changed-price" : ""}">Compare at ${escapeHtml(fmt(d.compareAt))}</span>` : ""}
      ${onSale ? `<span class="live-price-badge">Sale</span>` : ""}
    </div>` : "";

  const sizesHtml = (d.inStock?.length || d.outOfStock?.length) ? `
    <div class="live-sizes">
      <p class="field-label">In stock</p>
      <div class="size-chips">${(d.inStock || []).map(s => sizeChip(s, "available", prevInSet, prevOosSet, prevAllSet)).join("") || '<span class="subtle" style="font-size:12px">None detected</span>'}</div>
      ${d.outOfStock?.length ? `
        <p class="field-label" style="margin-top:8px">Out of stock</p>
        <div class="size-chips">${(d.outOfStock || []).map(s => sizeChip(s, "oos", prevInSet, prevOosSet, prevAllSet)).join("")}</div>` : ""}
    </div>` : "";

  let prevHtml = "";
  if (prev && (prev.price != null || prev.compareAt != null || prev.inStock?.length || prev.outOfStock?.length)) {
    const curInSet = new Set(d.inStock || []);
    const curOosSet = new Set(d.outOfStock || []);
    const curAllSet = new Set([...curInSet, ...curOosSet]);
    const prevOnSale = prev.compareAt != null && prev.price != null && prev.compareAt > prev.price;

    const prevPriceHtml = prev.price != null || prev.compareAt != null ? `
      <div class="live-price-row">
        ${prev.price != null ? `<span class="live-price-main${priceChanged ? " live-changed-price" : ""}">${escapeHtml(fmt(prev.price))}</span>` : ""}
        ${prev.compareAt != null ? `<span class="live-price-compare${compareChanged ? " live-changed-price" : ""}">Compare at ${escapeHtml(fmt(prev.compareAt))}</span>` : ""}
        ${prevOnSale ? `<span class="live-price-badge">Sale</span>` : ""}
      </div>` : "";

    const prevSizesHtml = (prev.inStock?.length || prev.outOfStock?.length) ? `
      <div class="live-sizes">
        <p class="field-label">In stock</p>
        <div class="size-chips">${(prev.inStock || []).map(s => sizeChip(s, "available", curInSet, curOosSet, curAllSet)).join("") || '<span class="subtle" style="font-size:12px">None detected</span>'}</div>
        ${prev.outOfStock?.length ? `
          <p class="field-label" style="margin-top:8px">Out of stock</p>
          <div class="size-chips">${(prev.outOfStock || []).map(s => sizeChip(s, "oos", curInSet, curOosSet, curAllSet)).join("")}</div>` : ""}
      </div>` : "";

    prevHtml = `
      <details class="html-section live-prev-section" open>
        <summary>Previous size &amp; price</summary>
        ${prevPriceHtml}
        ${prevSizesHtml}
      </details>`;
  }

  return `
    <details class="html-section live-extracted-section" open>
      <summary>Live data — last check · ${escapeHtml(formatTimestamp(monitor.lastExtractedAt))}</summary>
      ${priceHtml}
      ${sizesHtml}
    </details>
    ${prevHtml}`;
}

function renderDetail(monitor) {
  const selectors = monitor.selectors || (monitor.selector ? [monitor.selector] : []);
  const interval = getIntervalDisplay(monitor.intervalMinutes);

  const selectorsHtml = selectors.map((sel) => `
    <span class="selector-chip" data-selector="${escapeHtml(sel)}">
      <span class="chip-text">${escapeHtml(sel)}</span>
      <button class="chip-remove" title="Remove">Ã—</button>
    </span>`).join("");

  const historyEntries = monitor.changeHistory || [];
  const historyHtml = historyEntries.length
    ? historyEntries.map((e, i) => buildHistoryEntry(e, i, monitor.id)).join("")
    : `<div class="empty-panel">No changes recorded yet.</div>`;

  const hasSnapshot = !!(monitor.hasLastHtmlSnapshot || monitor.lastHtmlSnapshot);
  const canManualSyncShopify = !!(monitor.shopifyProductId || monitor.productData?.sku);
  const manualSyncLabel = monitor.shopifyProductId ? "Update Shopify" : "Find & update Shopify";
  const productDataHtml = renderProductData(monitor.productData, monitor.id, monitor.priceAdjustment, monitor.url);

  monitorDetail.innerHTML = `
    <div style="display:flex;justify-content:flex-end">
      <button class="inline-button back-to-monitor-btn" style="font-size:12px;padding:5px 12px" title="Scroll to monitor card">&#8593; Back to card</button>
    </div>
    <div class="monitor-top">
      <div>
        <h2>${escapeHtml(monitor.name)}</h2>
        <p class="url"><a href="${escapeHtml(monitor.url)}" target="_blank" rel="noopener">${escapeHtml(monitor.url)}</a></p>
      </div>
      <span class="status ${escapeHtml(monitor.status || "idle")}">${escapeHtml(monitor.status || "idle")}</span>
    </div>

    <div class="selectors-section">
      <p class="field-label">Selectors <span class="subtle">(empty = full page)</span></p>
      <div class="selectors-list">${selectorsHtml}</div>
      <div class="add-selector-row">
        <input class="selector-input" type="text" placeholder="CSS selector e.g. #price, .heading">
        <button class="add-selector-btn inline-button">Add</button>
        <button class="pick-selector-btn inline-button" title="Click an element on the active tab to pick its selector">Pick from tab</button>
      </div>
    </div>

    <div class="settings-row">
      <label class="toggle">
        <input type="checkbox" class="autocheck-toggle"${monitor.autoCheck ? " checked" : ""}>
        <span>Auto-check</span>
      </label>
      <div class="interval-control">
        <input class="interval-value" type="number" min="1" value="${interval.value}">
        <select class="interval-unit">
          <option value="1"${interval.unit === 1 ? " selected" : ""}>minutes</option>
          <option value="60"${interval.unit === 60 ? " selected" : ""}>hours</option>
          <option value="1440"${interval.unit === 1440 ? " selected" : ""}>days</option>
        </select>
      </div>
    </div>

    <div class="details-row">
      <div><p class="field-label">Last check</p><p>${escapeHtml(formatTimestamp(monitor.lastCheckedAt))}</p></div>
      <div><p class="field-label">Last changed</p><p>${escapeHtml(formatTimestamp(monitor.lastChangedAt, "Never"))}</p></div>
      <div><p class="field-label">Changes</p><p style="display:flex;align-items:center;gap:8px">${monitor.changeCount || 0}<button class="inline-button reset-count-btn" data-id="${escapeHtml(monitor.id)}" style="font-size:11px;padding:3px 8px;border-radius:6px">Reset</button></p></div>
      ${(() => {
        const missingFields = getMissingProductDataFields(monitor);
        const derivedError = monitor.lastError || (missingFields.length ? `Missing product data: ${missingFields.join(", ")}` : "");
        return derivedError ? `<div><p class="field-label">Error</p><p class="danger-text">${escapeHtml(derivedError)}</p></div>` : "";
      })()}
      ${monitor.hasUsOnlySizes ? `<div><p class="field-label" style="color:var(--infrared)">Sizes with no EU conversion</p><p style="font-size:12px;color:var(--muted);margin:2px 0 6px">Sizes: ${escapeHtml((monitor.usOnlySizesList || []).join(", "))}</p><button class="inline-button danger remove-us-sizes-btn" data-id="${escapeHtml(monitor.id)}">Remove bad sizes</button></div>` : ""}
      ${monitor.hasStaleFractionalEuSizes ? `<div><p class="field-label" style="color:var(--infrared)">Decimal fraction sizes</p><p style="font-size:12px;color:var(--muted);margin:2px 0 6px">Sizes: ${escapeHtml((monitor.staleFractionalEuSizesList || []).join(", "))}</p><p class="subtle">Recheck this card to refresh 1/3 and 2/3 labels.</p></div>` : ""}
    </div>

    ${productDataHtml || `<div class="product-data-section product-data-empty">
      <p class="product-data-title">Extracted product data</p>
      <p class="subtle">No saved product metadata yet. Run "Check now" while the product page is available to refresh this monitor.</p>
    </div>`}

    ${monitor.initialFullPageText ? `
      <details class="html-section first-capture" open>
        <summary>First-visit capture · ${escapeHtml(formatTimestamp(monitor.initialCapturedAt))}</summary>
        <p class="first-capture-hint">Everything extracted on the first visit — product name, description, code, brand, price.</p>
        <details class="raw-html-panel" style="margin:0 12px 12px">
          <summary>Full page text (readable)</summary>
          <pre>${truncateHtml(monitor.initialFullPageText)}</pre>
        </details>
      </details>
    ` : ""}

    ${renderLiveExtracted(monitor)}

    ${renderSelectedOuterHtmlPanel(monitor, selectors)}

    ${hasSnapshot ? `
      <details class="html-section main-diff-section" data-monitor-id="${escapeHtml(monitor.id)}">
        <summary>Changes — before vs after</summary>
        <div class="main-diff-placeholder" style="padding:12px;color:var(--muted);font-size:12px">Open to load the diff.</div>
      </details>
    ` : `<p class="subtle">No snapshot yet — click "Check now" to capture one.</p>`}

    <details class="history-panel"${historyEntries.length ? " open" : ""}>
      <summary>Change history (${historyEntries.length})</summary>
      <div class="history-list">${historyHtml}</div>
    </details>

    <div class="card-actions">
      <button data-save="${monitor.id}" class="primary">Save</button>
      <button data-refresh="${monitor.id}" class="secondary"${canCheck ? "" : " disabled"}>Check now</button>
      <button data-first-capture="${monitor.id}" class="secondary"${canCheck ? "" : " disabled"} title="Re-capture all product metadata: name, brand, images, SKU, color, description">First capture</button>
      <button data-resync-shopify="${monitor.id}" class="secondary"${canManualSyncShopify ? "" : " disabled"} title="Push the current monitor data back to Shopify">${manualSyncLabel}</button>
      <button class="inline-button back-to-monitor-btn" title="Scroll to monitor card">&#8593; Back to card</button>
      <button data-delete="${monitor.id}" class="inline-button danger">Delete</button>
    </div>
  `;

  monitorDetail.style.display = "";
}

function getFilteredMonitors() {
  const query = searchInput.value.trim().toLowerCase();
  const changedOnly = changedOnlyInput.checked;
  const sort = filterSort.value;

  let result = allMonitors.filter((m) => {
    if (changedOnly && !m.changeCount) return false;
    if (selectedBrands.size > 0) {
      const b = canonicalizeBrand(getBrandFromMonitor(m) || "");
      if (![...selectedBrands].some(fb => canonicalizeBrand(fb) === b)) return false;
    }
    if (selectedTypes.size > 0) {
      const t = (m.productData?.type || "").trim().toLowerCase();
      if (![...selectedTypes].some(ft => ft.trim().toLowerCase() === t)) return false;
    }
    if (activeStoreMetaFilters.length > 0 && !activeStoreMetaFilters.some(f => monitorMatchesStoreMeta(m, f.kind, f.value))) return false;
    if (!query) return true;
    const sels = (m.selectors || (m.selector ? [m.selector] : [])).join(" ");
    const pd = m.productData;
    const shoesInfo = getMonitorShoesTypeInfo(m);
    const pdText = pd ? [pd.name, pd.sku, pd.description, pd.brand, pd.type, shoesInfo.category, shoesInfo.model].filter(Boolean).join(" ") : "";
    return [m.name, m.url, sels, m.lastSnapshot, pdText].join(" ").toLowerCase().includes(query);
  });

  const ts = (v) => v ? new Date(v).getTime() : 0;
  if (sort === "created-desc") result = result.slice().sort((a, b) => ts(b.createdAt) - ts(a.createdAt));
  else if (sort === "created-asc") result = result.slice().sort((a, b) => ts(a.createdAt) - ts(b.createdAt));
  else if (sort === "changed-desc") result = result.slice().sort((a, b) => ts(b.lastChangedAt) - ts(a.lastChangedAt));
  else if (sort === "checked-desc") result = result.slice().sort((a, b) => ts(b.lastCheckedAt) - ts(a.lastCheckedAt));

  return result;
}

const KNOWN_BRANDS = [
  "New Balance","Under Armour","Dr. Martens","Air Jordan",
  "Nike","Jordan","Adidas","Asics","Puma","Reebok","Converse","Vans",
  "Saucony","Brooks","Hoka","Salomon","Merrell","Timberland","UGG",
  "Birkenstock","Clarks","Crocs","Fila","Kappa","Ellesse","Diadora",
  "New Era","Champion","Carhartt","The North Face"
];

function getBrandFromMonitor(m) {
  if (m.productData?.brand) return m.productData.brand;
  try {
    if (/(^|\.)wayofwade\.com\b/i.test(new URL(m.url || "").hostname)) return "Way of Wade";
  } catch (_) {}
  const title = (m.name || "").trim();
  const lower = title.toLowerCase();
  // match against known brand list (longest first to prefer "New Balance" over "New")
  for (const b of KNOWN_BRANDS) {
    if (lower.startsWith(b.toLowerCase())) return b;
  }
  // generic: first word(s) before a model number pattern (letters followed by digits)
  const match = title.match(/^([A-Za-z][A-Za-z\s]{1,18}?)\s+[A-Za-z]{1,3}[0-9]/);
  return match ? match[1].trim() : null;
}

let _lastFilterOptSig = "";
let _cachedFilterBrands = [];
let _cachedFilterTypes = [];
let _filterOptsDirty = true;
function _invalidateFilterOpts() { _filterOptsDirty = true; }
function populateFilterOptions() {
  if (_filterOptsDirty) {
    _cachedFilterBrands = [...new Set(allMonitors.map(m => canonicalizeBrand(getBrandFromMonitor(m) || "")).filter(Boolean))].sort();
    _cachedFilterTypes = [...new Set(allMonitors.map(m => m.productData?.type).filter(Boolean))].sort();
    _filterOptsDirty = false;
  }
  const brands = _cachedFilterBrands;
  const types = _cachedFilterTypes;
  const brandSig = [...selectedBrands].sort().join(",");
  const typeSig = [...selectedTypes].sort().join(",");
  const sig = brands.join(",") + "|" + types.join(",") + "|" + brandSig + "|" + typeSig;
  if (sig === _lastFilterOptSig) return;
  _lastFilterOptSig = sig;

  // Include any selected values not in the current list
  const brandExtras = [...selectedBrands].filter(b => !brands.includes(b));
  const typeExtras = [...selectedTypes].filter(t => !types.includes(t));
  const brandOptions = [...brandExtras, ...brands];
  const typeOptions = [...typeExtras, ...types];

  filterBrand.innerHTML = `<option value="">All brands</option>` + brandOptions.map(b => `<option value="${escapeHtml(b)}">${escapeHtml(b)}</option>`).join("");
  filterType.innerHTML = `<option value="">All types</option>` + typeOptions.map(t => `<option value="${escapeHtml(t)}">${escapeHtml(t)}</option>`).join("");

  syncBrandSelectDisplay();
  syncTypeSelectDisplay();
}

// Precomputed per-render: avoids O(n²) findIndex inside renderSquare
let _monitorNumMap = new Map();

function getGridSig(_monitors) {
  const oosSig = [...(shopifyOutOfStockMonitorIds || [])].sort().join(",");
  if (_cachedErrorSig === null) {
    _cachedErrorSig = allMonitors.filter(isErrorMonitor).map(m => `${m.id}:${m.lastError || ""}`).join(",");
  }
  // O(1) — version bumped by applyMonitorsUpdate / _flushPatches whenever monitor data changes
  return `v${_gridDataVersion}|auto:${autoEnabled}|oos:${outOfStockSectionHidden}|oosIds:${oosSig}|errors:${_cachedErrorSig}|img:${shopifyProductImageCacheVersion}`;
}

// Captures which monitors appear on which group pages — changes here require full renderGrid.
// Per-monitor data changes (status, price, stock) are NOT included; those use getMonitorCardSig.
// Not cached — only called after getGridSig already detected a change, so it runs rarely.
function getGridStructureSig(monitors) {
  const newMons = getNewNotImportedMonitors(monitors);
  const errorMons = allMonitors.filter(isErrorMonitor);
  const oosIds = getVisibleOutOfStockMonitorIds(monitors);
  const monMap = new Map(monitors.map(m => [m.id, m]));
  const oosMons = [...oosIds].map(id => monMap.get(id)).filter(Boolean);
  const domainGroups = new Map();
  for (const m of monitors) {
    const d = getDomain(m.url);
    if (!domainGroups.has(d)) domainGroups.set(d, []);
    domainGroups.get(d).push(m);
  }
  function getPageSig(key, mons) {
    if (collapsedGroups.has(key)) return `${key}:collapsed`;
    const filtered = applyGroupFilter(key, mons);
    const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
    const page = Math.min(groupPages.get(key) || 0, totalPages - 1);
    return `${key}:[${filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE).map(m => m.id).join(",")}]`;
  }
  return [
    `oosh:${outOfStockSectionHidden}`,
    newMons.length ? getPageSig("__new48h__", newMons) : "new:[]",
    errorMons.length ? getPageSig("__errors__", errorMons) : "err:[]",
    (oosMons.length && !outOfStockSectionHidden) ? getPageSig(SHOPIFY_OOS_GROUP_KEY, oosMons) : `${SHOPIFY_OOS_GROUP_KEY}:[]`,
    ...[...domainGroups.entries()].map(([d, mons]) => getPageSig(d, mons))
  ].join("|");
}

// Minimal per-card sig — only fields visible in the card tile.
function getMonitorCardSig(m) {
  return [
    m.status || "idle",
    m.pendingInitialCheck ? "pending" : "",
    m.lastCheckedAt || 0,
    m.changeCount || 0,
    m.shopifyProductId || "",
    m.lastError || "",
    m.shopifySyncStatus || "",
    m.lastExtractedData?.price ?? "",
    (m.lastExtractedData?.inStock?.length || 0) + (m.lastExtractedData?.outOfStock?.length || 0),
    shopifyOutOfStockMonitorIds?.has(m.id) ? "1" : "0",
    getMonitorCardThumb(m) || "",
    (m.productData?.type || "") + "|" + (m.productData?.genderDisplay || m.productData?.gender || "") + "|" + (m.productData?.colorFinal || m.productData?.color || ""),
  ].join(":");
}

// Try to update only changed card elements instead of rebuilding the entire grid.
// Falls back to full renderGrid when group structure changes (monitors added/removed/moved).
function tryIncrementalCardUpdates(monitors) {
  const structSig = getGridStructureSig(monitors);
  if (structSig !== _lastGridStructureSig) {
    _lastGridStructureSig = structSig;
    _renderedMonitorSigs.clear();
    renderGrid(monitors);
    return;
  }
  // Same groups, same pages — only update cards whose data changed.
  // _dirtyMonitorIds tracks which monitors changed since the last render pass (O(dirty) vs O(n)).
  _monitorNumMap = new Map(allMonitors.map((m, i) => [m.id, i + 1]));
  const tpl = document.createElement("template");
  const candidates = _dirtyMonitorIds.size > 0 && _dirtyMonitorIds.size < monitors.length
    ? monitors.filter(m => _dirtyMonitorIds.has(m.id))
    : monitors;
  _dirtyMonitorIds.clear();
  for (const m of candidates) {
    const newSig = getMonitorCardSig(m);
    if (_renderedMonitorSigs.get(m.id) === newSig) continue;
    const cardEl = _cardIndex.get(m.id);
    if (!cardEl) continue;
    tpl.innerHTML = renderSquare(m).trim();
    const newEl = tpl.content.firstElementChild;
    if (newEl) {
      cardEl.replaceWith(newEl);
      _cardIndex.set(m.id, newEl);
      _renderedMonitorSigs.set(m.id, newSig);
      initThumbLazyLoading(newEl);
    }
  }
}

// Remove one monitor from the DOM and allMonitors immediately, without a full reload.
// Keeps _lastGridSig/_lastGridStructureSig in sync so the next renderAll is a no-op.
function removeMonitorFromDom(id) {
  const prev = _monitorIndex.get(id) !== undefined ? allMonitors[_monitorIndex.get(id)] : null;
  if (prev) { _removeFromStatCaches(prev); }
  allMonitors = allMonitors.filter(m => m.id !== id);
  _monitorIndex = new Map(allMonitors.map((m, i) => [m.id, i]));
  _gridDataVersion++;
  _dirtyMonitorIds.delete(id);
  invalidateMonitorOrderCaches();
  checkedIds.delete(id);
  _renderedMonitorSigs.delete(id);
  const card = _cardIndex.get(id) || monitorGrid.querySelector(`.monitor-square[data-id="${CSS.escape(id)}"]`);
  _cardIndex.delete(id);
  // Determine which group elements need their count updated.
  // An error monitor shows in __errors__; all monitors show in their domain group.
  const affectedKeys = new Set();
  if (prev) {
    affectedKeys.add(getDomain(prev.url));
    if (isErrorMonitor(prev)) affectedKeys.add("__errors__");
    if (prev.shopifyOutOfStock) affectedKeys.add(SHOPIFY_OOS_GROUP_KEY);
  }
  if (card) card.remove();
  // Compute once — used for both count labels and emptiness checks.
  const visibleMons = getFilteredMonitors();
  for (const key of affectedKeys) {
    const groupEl = monitorGrid.querySelector(`.site-group[data-group-key="${CSS.escape(key)}"]`);
    if (!groupEl) continue;
    if (groupEl.dataset.virtualized) {
      _restoreGroupGrid(groupEl);
    }
    // Header count matches what buildGroup receives: filtered set for domain groups,
    // allMonitors for __errors__ (shown regardless of filter).
    const allInGroup = key === "__errors__"
      ? allMonitors.filter(isErrorMonitor).length
      : key === SHOPIFY_OOS_GROUP_KEY
        ? getVisibleOutOfStockMonitorIds(visibleMons).size
        : visibleMons.filter(m => getDomain(m.url) === key).length;
    const countEl = groupEl.querySelector(".site-group-count");
    if (countEl) countEl.textContent = `${allInGroup} monitor${allInGroup !== 1 ? "s" : ""}`;
    if (allInGroup === 0) {
      if (_groupObserver) _groupObserver.unobserve(groupEl);
      groupEl.remove();
    }
  }
  _lastGridSig = getGridSig(visibleMons);
  _lastGridStructureSig = getGridStructureSig(visibleMons);
  renderStats(allMonitors);
  updateBulkBtn();
}

// Module-level group helpers (used by renderGrid and renderGroupInPlace)
function applyGroupFilter(key, mons) {
  const f = groupFilters.get(key) || {};
  let result = mons;
  if (f.brand) result = result.filter(m => canonicalizeBrand(getBrandFromMonitor(m) || "") === f.brand);
  if (f.type) result = result.filter(m => (m.productData?.type || "").toLowerCase() === f.type.toLowerCase());
  return result;
}

function buildGroupFilterPills(key, allMons) {
  const brandCounts = new Map();
  const typeCounts = new Map();
  for (const monitor of allMons) {
    const brand = canonicalizeBrand(getBrandFromMonitor(monitor) || "");
    if (brand) brandCounts.set(brand, (brandCounts.get(brand) || 0) + 1);
    const type = monitor.productData?.type;
    if (type) typeCounts.set(type, (typeCounts.get(type) || 0) + 1);
  }
  const brands = [...brandCounts.keys()].sort();
  const types  = [...typeCounts.keys()].sort();
  const f = groupFilters.get(key) || {};
  if (!brands.length && !types.length) return "";
  const bPills = brands.map(b => {
    const active = f.brand === b;
    const count = brandCounts.get(b) || 0;
    return `<button class="group-filter-pill${active?" active":""}" data-group-key="${escapeHtml(key)}" data-filter-brand="${escapeHtml(b)}">${escapeHtml(b)} <span>${count}</span></button>`;
  }).join("");
  const tPills = types.map(t => {
    const active = f.type === t;
    const count = typeCounts.get(t) || 0;
    return `<button class="group-filter-pill type-pill${active?" active":""}" data-group-key="${escapeHtml(key)}" data-filter-type="${escapeHtml(t)}">${escapeHtml(t)} <span>${count}</span></button>`;
  }).join("");
  const hasActive = f.brand || f.type;
  return `<div class="group-filter-bar">
    ${bPills}${types.length ? `<span class="group-filter-sep"></span>${tPills}` : ""}
    ${hasActive ? `<button class="group-filter-clear" data-group-key="${escapeHtml(key)}">✕ Clear</button>` : ""}
  </div>`;
}

function buildWebsiteSelectActions(mons = []) {
  const byDomain = new Map();
  for (const monitor of mons) {
    const domain = getDomain(monitor.url || "");
    if (!byDomain.has(domain)) byDomain.set(domain, []);
    byDomain.get(domain).push(monitor);
  }
  const domains = [...byDomain.keys()].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
  if (domains.length <= 1) return "";
  const buttons = domains.map((domain) => {
    const ids = byDomain.get(domain).map((monitor) => monitor.id);
    const allChecked = ids.length > 0 && ids.every((id) => checkedIds.has(id));
    const label = `${allChecked ? "Deselect" : "Select"} ${domain} (${ids.length})`;
    return `<button class="site-select-btn inline-button site-website-select-btn" data-ids="${escapeHtml(ids.join(","))}">${escapeHtml(label)}</button>`;
  }).join("");
  return `<div class="site-website-actions">${buttons}</div>`;
}

function buildGroup(key, titleHtml, allMons, extraActions = "") {
  const collapsed = collapsedGroups.has(key);
  const arrow = collapsed ? "&#9654;" : "&#9660;";
  const filterPills = collapsed ? "" : buildGroupFilterPills(key, allMons);
  let filtered = applyGroupFilter(key, allMons);
  const groupFilterState = groupFilters.get(key) || {};
  if (!filtered.length && allMons.length && (groupFilterState.brand || groupFilterState.type)) {
    groupFilters.delete(key);
    scheduleSaveDashboardUiState();
    filtered = allMons;
  }
  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const currentPage = Math.min(groupPages.get(key) || 0, totalPages - 1);
  groupPages.set(key, currentPage);
  const pageSlice = filtered.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE);

  const pagination = totalPages > 1 ? `
    <div class="group-pagination">
      <button class="inline-button group-page-btn" data-group-key="${escapeHtml(key)}" data-page="${currentPage - 1}" ${currentPage === 0 ? "disabled" : ""}>&#8592; Prev</button>
      <span class="group-page-label">Page ${currentPage + 1} of ${totalPages} &nbsp;·&nbsp; ${filtered.length} monitors</span>
      <button class="inline-button group-page-btn" data-group-key="${escapeHtml(key)}" data-page="${currentPage + 1}" ${currentPage >= totalPages - 1 ? "disabled" : ""}>Next &#8594;</button>
      <label class="group-page-jump">
        <span>Go to</span>
        <input class="group-page-input" data-group-key="${escapeHtml(key)}" min="1" max="${totalPages}" type="number" value="${currentPage + 1}" inputmode="numeric">
      </label>
    </div>` : "";

  return `
  <div class="site-group" data-group-key="${escapeHtml(key)}">
    <div class="site-group-header">
      <button class="group-toggle-btn" data-group-key="${escapeHtml(key)}" title="${collapsed ? "Expand" : "Collapse"}">${arrow}</button>
      ${titleHtml}
      <span class="site-group-count">${allMons.length} monitor${allMons.length !== 1 ? "s" : ""}</span>
      ${extraActions}
    </div>
    ${filterPills}
    ${collapsed ? "" : `<div class="site-group-grid">${pageSlice.map((monitor) => renderSquare(monitor, key)).join("")}</div>${pagination}`}
  </div>`;
}

// Rebuild only one domain group in-place — used by filter pill clicks to avoid full grid rebuild
function renderGroupInPlace(key) {
  const groupEl = monitorGrid.querySelector(`.site-group[data-group-key="${CSS.escape(key)}"]`);
  if (!groupEl) { renderGrid(getFilteredMonitors()); return; }

  const visibleMonitors = getFilteredMonitors();
  const mons = visibleMonitors.filter(m => getDomain(m.url) === key);
  const ids = mons.map(m => m.id);
  const allChecked = ids.length > 0 && ids.every(id => checkedIds.has(id));
  const actions = `
    <div class="site-group-actions">
      <button class="site-select-btn inline-button" data-ids="${escapeHtml(ids.join(","))}">${allChecked ? "Deselect site" : "Select site"}</button>
      <button class="site-check-btn inline-button" data-ids="${escapeHtml(ids.join(","))}">Check site</button>
    </div>`;
  // Clear stale _cardIndex entries for the group being replaced.
  groupEl.querySelectorAll(".monitor-square[data-id]").forEach(el => _cardIndex.delete(el.dataset.id));
  if (_groupObserver) _groupObserver.unobserve(groupEl);
  const newHtml = buildGroup(key, `<span class="site-group-title">${escapeHtml(key)}</span>`, mons, actions);
  groupEl.outerHTML = newHtml;
  // Sync structure sig so the next renderAll doesn't trigger a full grid rebuild
  _lastGridStructureSig = getGridStructureSig(visibleMonitors);
  // Re-query the replacement element (outerHTML doesn't return a reference).
  const newGroupEl = monitorGrid.querySelector(`.site-group[data-group-key="${CSS.escape(key)}"]`);
  if (newGroupEl) {
    // Update _cardIndex for newly rendered cards.
    newGroupEl.querySelectorAll(".monitor-square[data-id]").forEach(el => {
      const mid = el.dataset.id;
      _cardIndex.set(mid, el);
      const idx = _monitorIndex.get(mid);
      if (idx !== undefined) _renderedMonitorSigs.set(mid, getMonitorCardSig(allMonitors[idx]));
    });
    if (_groupObserver) _groupObserver.observe(newGroupEl);
    initThumbLazyLoading(newGroupEl);
  }
}

// ── Virtual scrolling ────────────────────────────────────────────────────────
let _groupObserver = null;

function _virtualizeGroupGrid(groupEl) {
  const grid = groupEl.querySelector(".site-group-grid");
  if (!grid || groupEl.dataset.virtualized) return;
  // Remember height so the container keeps its scroll space
  groupEl.dataset.virtualized = "1";
  groupEl.dataset.virtualHeight = grid.scrollHeight + "px";
  // Remove cards from DOM and clear indexes
  grid.querySelectorAll(".monitor-square[data-id]").forEach(el => {
    const id = el.dataset.id;
    _cardIndex.delete(id);
    _renderedMonitorSigs.delete(id);
  });
  if (_thumbObserver) {
    grid.querySelectorAll("img.square-thumb").forEach((img) => _thumbObserver.unobserve(img));
  }
  grid.style.minHeight = groupEl.dataset.virtualHeight;
  grid.innerHTML = "";
}

function _restoreGroupGrid(groupEl) {
  const grid = groupEl.querySelector(".site-group-grid");
  if (!grid || !groupEl.dataset.virtualized) return;
  delete groupEl.dataset.virtualized;
  grid.style.minHeight = "";

  // Reconstruct the same monitor set renderGrid would use for this group key.
  const key = groupEl.dataset.groupKey;
  const filtered = getFilteredMonitors();
  let mons;
  if (key === "__errors__") {
    mons = allMonitors.filter(isErrorMonitor);
  } else if (key === SHOPIFY_OOS_GROUP_KEY) {
    const oosIds = getVisibleOutOfStockMonitorIds(filtered);
    const filteredMap = new Map(filtered.map(m => [m.id, m]));
    mons = [...oosIds].map(id => filteredMap.get(id)).filter(Boolean);
  } else if (key === "__new48h__") {
    mons = getNewNotImportedMonitors(filtered);
  } else {
    mons = filtered.filter(m => getDomain(m.url) === key);
  }

  // Apply same group filter + pagination as buildGroup
  mons = applyGroupFilter(key, mons);
  const totalPages = Math.max(1, Math.ceil(mons.length / PAGE_SIZE));
  const currentPage = Math.min(groupPages.get(key) || 0, totalPages - 1);
  const pageSlice = mons.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE);

  // Re-render current page cards
  const tpl = document.createElement("template");
  pageSlice.forEach(m => {
    tpl.innerHTML = renderSquare(m, key).trim();
    const el = tpl.content.firstElementChild;
    grid.appendChild(el);
    _cardIndex.set(m.id, el);
    _renderedMonitorSigs.set(m.id, getMonitorCardSig(m));
  });
  initThumbLazyLoading(grid);
}

function _observeGroups() {
  if (_groupObserver) _groupObserver.disconnect();
  _groupObserver = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      const groupEl = entry.target;
      if (entry.isIntersecting) {
        _restoreGroupGrid(groupEl);
      } else {
        _virtualizeGroupGrid(groupEl);
      }
    }
  }, { rootMargin: "400px 0px" });

  monitorGrid.querySelectorAll(".site-group[data-group-key]").forEach(el => {
    _groupObserver.observe(el);
  });
}

function renderGrid(monitors) {
  // Keep sig in sync so subsequent renderAll calls can skip redundant rebuilds
  _lastGridSig = getGridSig(monitors);
  const ERROR_GROUP_KEY = "__errors__";
  const errorMons = allMonitors.filter(isErrorMonitor);

  if (!monitors.length && !errorMons.length) {
    if (_thumbObserver) _thumbObserver.disconnect();
    monitorGrid.innerHTML = `<section class="empty-state large"><p>No monitors match this view.</p><p class="subtle">Try clearing the search or filters.</p></section>`;
    return;
  }

  // Build number map once per render pass so renderSquare is O(1)
  _monitorNumMap = new Map(allMonitors.map((m, i) => [m.id, i + 1]));

  const NEW_GROUP_KEY = "__new48h__";

  const newMons = getNewNotImportedMonitors(monitors);
  const outOfStockIdSet = getVisibleOutOfStockMonitorIds(monitors);
  const monitorsById = new Map(monitors.map(m => [m.id, m]));
  const shopifyOutOfStockMons = [...outOfStockIdSet].map((id) => monitorsById.get(id)).filter(Boolean);
  const selectedNewMons = newMons.filter((m) => checkedIds.has(m.id));
  const selectedErrorMons = errorMons.filter((m) => checkedIds.has(m.id));
  const errorNoImageMons = errorMons.filter(isMonitorWithoutImage);
  const selectedErrorNoImageMons = errorNoImageMons.filter((m) => checkedIds.has(m.id));
  const selectedShopifyOutOfStockMons = shopifyOutOfStockMons.filter((m) => checkedIds.has(m.id));

  const newSection = newMons.length
    ? buildGroup(
        NEW_GROUP_KEY,
        `<span class="site-group-title new-group-title">New not imported yet</span>`,
        newMons,
        `<div class="site-group-actions">
          <button class="site-select-btn inline-button" data-ids="${escapeHtml(newMons.map((m) => m.id).join(","))}">${selectedNewMons.length === newMons.length && newMons.length ? "Deselect shown" : "Select shown"}</button>
          <button class="site-clear-selected-btn inline-button" data-ids="${escapeHtml(selectedNewMons.map((m) => m.id).join(","))}" ${selectedNewMons.length ? "" : "disabled"}>Clear selected</button>
          <button class="site-clear-btn inline-button danger" data-ids="${escapeHtml(newMons.map((m) => m.id).join(","))}">Clear</button>
        </div>${buildWebsiteSelectActions(newMons)}`
      )
    : "";

  const errorSection = errorMons.length
    ? buildGroup(
        ERROR_GROUP_KEY,
        `<span class="site-group-title error-group-title">Errors</span>`,
        errorMons,
        `<div class="site-group-actions">
          <button class="site-select-btn inline-button" data-ids="${escapeHtml(errorMons.map((m) => m.id).join(","))}">${selectedErrorMons.length === errorMons.length && errorMons.length ? "Deselect all errors" : "Select all errors"}</button>
          <button class="site-select-btn inline-button" data-ids="${escapeHtml(errorNoImageMons.map((m) => m.id).join(","))}" ${errorNoImageMons.length ? "" : "disabled"}>${selectedErrorNoImageMons.length === errorNoImageMons.length && errorNoImageMons.length ? "Deselect" : "Select"} no images (${errorNoImageMons.length})</button>
          <button class="site-check-btn inline-button" data-ids="${escapeHtml(errorMons.map((m) => m.id).join(","))}">Check errors</button>
        </div>${buildWebsiteSelectActions(errorMons)}`
      )
    : "";

  const shopifyOutOfStockSection = shopifyOutOfStockMons.length && !outOfStockSectionHidden
    ? buildGroup(
        SHOPIFY_OOS_GROUP_KEY,
        `<span class="site-group-title error-group-title">Out of stock</span>`,
        shopifyOutOfStockMons,
        `<div class="site-group-actions">
          <button class="site-select-btn inline-button" data-ids="${escapeHtml(shopifyOutOfStockMons.map((m) => m.id).join(","))}">${selectedShopifyOutOfStockMons.length === shopifyOutOfStockMons.length && shopifyOutOfStockMons.length ? "Deselect all out of stock" : "Select all out of stock"}</button>
          <button class="shopify-oos-clear-btn inline-button">Clear</button>
        </div>${buildWebsiteSelectActions(shopifyOutOfStockMons)}`
      )
    : "";

  const groups = new Map();
  for (const m of monitors) {
    const d = getDomain(m.url);
    if (!groups.has(d)) groups.set(d, []);
    groups.get(d).push(m);
  }
  const domainSections = [...groups.entries()].map(([domain, mons]) => {
    const ids = mons.map((m) => m.id);
    const allChecked = ids.every((id) => checkedIds.has(id));
    const actions = `
      <div class="site-group-actions">
        <button class="site-select-btn inline-button" data-ids="${escapeHtml(ids.join(","))}">${allChecked ? "Deselect site" : "Select site"}</button>
        <button class="site-check-btn inline-button" data-ids="${escapeHtml(ids.join(","))}">Check site</button>
      </div>`;
    return buildGroup(domain, `<span class="site-group-title">${escapeHtml(domain)}</span>`, mons, actions);
  }).join("");

  if (_thumbObserver) _thumbObserver.disconnect();
  monitorGrid.innerHTML = errorSection + shopifyOutOfStockSection + newSection + domainSections;

  // Populate per-card sig map and structure sig so tryIncrementalCardUpdates can diff future updates.
  _dirtyMonitorIds.clear();
  _lastGridStructureSig = getGridStructureSig(monitors);
  _renderedMonitorSigs.clear();
  _cardIndex.clear();
  monitorGrid.querySelectorAll(".monitor-square[data-id]").forEach(el => {
    const id = el.dataset.id;
    const idx = _monitorIndex.get(id);
    if (idx !== undefined) {
      const m = allMonitors[idx];
      _renderedMonitorSigs.set(id, getMonitorCardSig(m));
      _cardIndex.set(id, el);
    }
  });
  _observeGroups();
  initThumbLazyLoading(monitorGrid);
}

function getMonitorsFirstCreated() {
  const sig = allMonitors.map((monitor) => `${monitor.id}:${monitor.createdAt || ""}`).join("|");
  if (sig === _firstCreatedCacheSig) return _firstCreatedCache;
  _firstCreatedCacheSig = sig;
  _firstCreatedCache = allMonitors.slice().sort((a, b) => {
    const at = new Date(a.createdAt || 0).getTime() || 0;
    const bt = new Date(b.createdAt || 0).getTime() || 0;
    if (at !== bt) return at - bt;
    return String(a.id || "").localeCompare(String(b.id || ""), undefined, { numeric: true, sensitivity: "base" });
  });
  return _firstCreatedCache;
}

function getCheckBatches() {
  const ordered = getMonitorsFirstCreated();
  const sig = `${_firstCreatedCacheSig}|size:${CHECK_BATCH_SIZE}`;
  if (sig === _checkBatchesCacheSig) return _checkBatchesCache;
  _checkBatchesCacheSig = sig;
  const batches = [];
  for (let index = 0; index < ordered.length; index += CHECK_BATCH_SIZE) {
    const monitors = ordered.slice(index, index + CHECK_BATCH_SIZE);
    batches.push({
      index: batches.length,
      from: index + 1,
      to: index + monitors.length,
      monitors,
      ids: monitors.map((monitor) => monitor.id)
    });
  }
  _checkBatchesCache = batches;
  return _checkBatchesCache;
}

function getSelectedCheckBatch() {
  const batches = getCheckBatches();
  if (!batches.length) return null;
  const index = Math.max(0, Math.min(Number(selectedCheckBatchIndex) || 0, batches.length - 1));
  return batches[index] || batches[0];
}

function reconcileBatchCheckResultsWithCurrentState() {
  let changed = false;
  for (const [index, result] of batchCheckResults.entries()) {
    const ids = Array.isArray(result.ids) ? result.ids : [];
    if (!ids.length) continue;
    const completedIds = Array.isArray(result.completedIds) ? result.completedIds : ids;
    const currentErrors = completedIds.filter((id) => {
      const monitor = _getMonitor(id);
      return !monitor || isErrorMonitor(monitor);
    });
    const success = Math.max(0, completedIds.length - currentErrors.length);
    const sameErrors = currentErrors.length === (result.errors || []).length &&
      currentErrors.every((id, i) => id === result.errors[i]);
    if (sameErrors && success === result.success) continue;
    batchCheckResults.set(index, { ...result, completedIds, success, errors: currentErrors });
    changed = true;
  }
  if (changed) _batchRenderSig = "";
  return changed;
}

function renderCheckBatchOptions() {
  if (!checkBatchGrid) return;
  reconcileBatchCheckResultsWithCurrentState();
  const previous = Number(selectedCheckBatchIndex) || 0;
  const batches = getCheckBatches();
  selectedCheckBatchIndex = Math.max(0, Math.min(previous, Math.max(0, batches.length - 1)));
  const validBatchIndexes = new Set(batches.map((batch) => batch.index));
  for (const index of [...selectedCheckBatchIndexes]) {
    if (!validBatchIndexes.has(index)) selectedCheckBatchIndexes.delete(index);
  }
  const selectedCount = selectedCheckBatchIndexes.size;
  const selectedCardCount = [...selectedCheckBatchIndexes].reduce((total, index) => total + (batches[index]?.ids.length || 0), 0);
  if (checkBatchSummary) {
    checkBatchSummary.textContent = selectedCount
      ? `${selectedCount} batch${selectedCount !== 1 ? "es" : ""} selected - ${selectedCardCount} cards`
      : `${batches.length} batch${batches.length !== 1 ? "es" : ""} available - no batches selected`;
  }
  if (selectAllBatchesBtn) selectAllBatchesBtn.disabled = !batches.length || importInProgress || !!activeBatchRun || selectedCount === batches.length;
  if (clearBatchesBtn) clearBatchesBtn.disabled = !selectedCount || importInProgress || !!activeBatchRun;
  const resultsSig = [...batchCheckResults.entries()]
    .map(([index, result]) => `${index}:${result.success}:${result.errors.length}:${result.completedIds?.length || 0}:${result.ids?.length || 0}`)
    .join("|");
  const renderSig = [
    _checkBatchesCacheSig,
    [...selectedCheckBatchIndexes].sort((a, b) => a - b).join(","),
    resultsSig,
    importInProgress ? "import" : "",
    activeBatchRun ? "running" : ""
  ].join("::");
  if (renderSig === _batchRenderSig) return;
  _batchRenderSig = renderSig;
  checkBatchGrid.innerHTML = batches.length
    ? batches.map((batch) => {
        const result = batchCheckResults.get(batch.index);
        const isSelected = selectedCheckBatchIndexes.has(batch.index);
        const isDisabled = importInProgress || !!activeBatchRun;
        const resultHtml = result
          ? `<span class="check-batch-result">${result.success} ok - ${result.errors.length} errors</span>`
          : `<span class="check-batch-result">${batch.ids.length} cards</span>`;
        return `<button type="button" class="check-batch-card${isSelected ? " selected" : ""}" data-check-batch-index="${batch.index}"${isDisabled ? " disabled" : ""}>
          <span class="check-batch-name">Batch ${batch.from}-${batch.to}</span>
          ${resultHtml}
        </button>`;
      }).join("")
    : `<span class="check-batch-empty">No batches yet</span>`;
}

function syncSelectedCheckBatchesToCards(message = "") {
  checkedIds.clear();
  const batches = getCheckBatches();
  for (const index of [...selectedCheckBatchIndexes].sort((a, b) => a - b)) {
    const batch = batches[index];
    if (!batch) {
      selectedCheckBatchIndexes.delete(index);
      continue;
    }
    batch.ids.forEach((id) => checkedIds.add(id));
  }
  updateBulkBtn();
  syncCheckedStates();
  if (message) renderBatchCheckStatus(message);
}

function clearSelectedCheckBatches() {
  selectedCheckBatchIndexes.clear();
}

function toggleCheckBatch(batchIndex = Number(selectedCheckBatchIndex) || 0) {
  const batches = getCheckBatches();
  const batch = batches[Math.max(0, Math.min(batchIndex, batches.length - 1))];
  if (!batch) return;
  selectedCheckBatchIndex = batch.index;
  if (selectedCheckBatchIndexes.has(batch.index)) selectedCheckBatchIndexes.delete(batch.index);
  else selectedCheckBatchIndexes.add(batch.index);
  const selected = [...selectedCheckBatchIndexes].sort((a, b) => a - b);
  const count = selected.reduce((total, index) => total + (batches[index]?.ids.length || 0), 0);
  syncSelectedCheckBatchesToCards(selected.length
    ? `Selected ${selected.length} batch${selected.length !== 1 ? "es" : ""}: ${count} cards, locked first-created order.`
    : "No batches selected.");
}

function isDsgMonitorUrl(url = "") {
  try {
    return /(^|\.)dickssportinggoods\.com\b/i.test(new URL(url || "").hostname || "");
  } catch (_) {
    return /(^|\.)dickssportinggoods\.com\b/i.test(String(url || ""));
  }
}

function renderBatchCheckStatus(message = "") {
  if (!batchCheckStatus) return;
  reconcileBatchCheckResultsWithCurrentState();
  const parts = [];
  if (message) parts.push(`<span>${escapeHtml(message)}</span>`);
  const batches = getCheckBatches();
  for (const batch of batches) {
    const result = batchCheckResults.get(batch.index);
    if (!result) continue;
    const label = `Batch ${batch.from}-${batch.to}`;
    const errorButton = result.errors.length
      ? `<button type="button" class="inline-button batch-error-btn" data-select-batch-errors="${batch.index}">Select errors</button>`
      : "";
    parts.push(`<span><strong>${escapeHtml(label)}</strong>: ${result.success} successful, ${result.errors.length} errors ${errorButton}</span>`);
  }
  batchCheckStatus.innerHTML = parts.join("");
  batchCheckStatus.classList.toggle("active", parts.length > 0);
}

function sendRuntimeMessageWithTimeout(message, timeoutMs = 10000) {
  return Promise.race([
    chrome.runtime.sendMessage(message),
    new Promise((_, reject) => setTimeout(() => reject(new Error("Timed out")), timeoutMs))
  ]);
}

function updateBulkBtn() {
  const n = checkedIds.size;
  const savedPendingCount = _statPending;
  checkAllBtn.disabled = importInProgress || !!activeBatchRun;
  if (checkSavedBtn) {
    checkSavedBtn.disabled = savedPendingCount === 0 || !canCheck || importInProgress || !!activeBatchRun;
    checkSavedBtn.textContent = savedPendingCount > 0 ? `Check saved (${savedPendingCount})` : "Check saved";
  }
  bulkCheckBtn.disabled = n === 0 || !canCheck || importInProgress || !!activeBatchRun;
  bulkFirstCaptureBtn.disabled = n === 0 || !canCheck || importInProgress || !!activeBatchRun;
  bulkDeleteBtn.disabled = n === 0 || importInProgress;
  bulkDeleteShopifyBtn.disabled = n === 0 || importInProgress;
  const selectedWithShopify = n > 0 && allMonitors.some((m) => checkedIds.has(m.id) && m.shopifyProductId);
  bulkDeleteShopifyOnlyBtn.disabled = !selectedWithShopify || importInProgress;
  bulkImportBtn.disabled = n === 0 || importInProgress;
  if (smartImportAllBtn) smartImportAllBtn.disabled = importInProgress || !allMonitors.some(m => !m.shopifyProductId);
  if (openBulkEditorBtn) {
    openBulkEditorBtn.disabled = n === 0 || importInProgress || bulkEditorBusy;
    openBulkEditorBtn.textContent = n > 0 ? `Bulk edit (${n})` : "Bulk edit";
  }
  if (bulkEditorApplyBtn) bulkEditorApplyBtn.disabled = n === 0 || importInProgress || bulkEditorBusy;
  bulkUpdateShopifyBtn.disabled = importInProgress || !allMonitors.some((monitor) => monitor.shopifyProductId || monitor.productData?.sku);
  bulkDeleteUnconvertedVariantsBtn.disabled = importInProgress || !allMonitors.some((monitor) => monitor.shopifyProductId || monitor.productData?.sku);
  bulkCheckBtn.textContent = n > 0 ? `Check selected (${n})` : "Check selected";
  bulkFirstCaptureBtn.textContent = n > 0 ? `First capture selected (${n})` : "First capture selected";
  bulkDeleteBtn.textContent = n > 0 ? `Delete selected (${n})` : "Delete selected";
  bulkDeleteShopifyBtn.textContent = n > 0 ? `Shopify + Monitors (${n})` : "Shopify + Monitors";
  if (!importInProgress) {
    bulkImportBtn.textContent = n > 0 ? `Import to Shopify (${n})` : "Import to Shopify";
    const selectedMetadataCount = allMonitors.filter((monitor) =>
      checkedIds.has(monitor.id) && (monitor.shopifyProductId || monitor.productData?.sku)
    ).length;
    const metadataCount = selectedMetadataCount || allMonitors.filter((monitor) => monitor.shopifyProductId || monitor.productData?.sku).length;
    const badSizeLabel = shopifyBadSizeVariantCount == null ? "..." : shopifyBadSizeVariantCount;
    bulkUpdateShopifyBtn.textContent = selectedMetadataCount > 0 ? `Update Shopify (${selectedMetadataCount})` : `Update Shopify (${metadataCount})`;
    bulkDeleteUnconvertedVariantsBtn.textContent = selectedMetadataCount > 0 ? `Bad sizes in selected (${selectedMetadataCount})` : `Bad sizes (${badSizeLabel})`;
    stopImportBtn.textContent = "Stop Import";
  }
  stopImportBtn.style.display = importInProgress ? "" : "none";
  stopImportBtn.disabled = !importInProgress;
  const bulkIntervalRow = document.getElementById("bulk-interval-row");
  if (bulkIntervalRow) bulkIntervalRow.style.display = n > 0 ? "" : "none";
  if (bulkEditorStatus && bulkEditorPanel?.style.display !== "none") {
    const hasDraft = bulkEditorPanel.querySelector("[data-bulk-toggle]:checked");
    if (!hasDraft) {
      bulkEditorStatus.textContent = n > 0
        ? `${n} selected monitor${n !== 1 ? "s" : ""}. Check each field you want to overwrite.`
        : "Select monitors first.";
    }
  }
  selectAllBtn.style.display = "";
  renderCheckBatchOptions();
}

function setProgressLabel(button, action, current, total) {
  button.textContent = `${action} ${current}/${total}...`;
}

async function runButtonProgress(button, items, action, worker) {
  const total = items.length;
  const originalText = button.textContent;
  const originalDisabled = button.disabled;
  button.disabled = true;
  try {
    for (let index = 0; index < total; index++) {
      setProgressLabel(button, action, index + 1, total);
      await worker(items[index], index, total);
    }
  } finally {
    button.textContent = originalText;
    button.disabled = originalDisabled;
  }
}

function renderAll() {
  const currentOutOfStockIds = getCombinedOutOfStockMonitorIds(allMonitors);
  const currentOutOfStockSignature = [...currentOutOfStockIds].sort().join(",");
  [...dismissedOutOfStockMonitorIds].forEach((id) => {
    if (!currentOutOfStockIds.has(id)) dismissedOutOfStockMonitorIds.delete(id);
  });
  if (currentOutOfStockSignature !== lastOutOfStockSignature) {
    outOfStockSectionHidden = false;
    if (currentOutOfStockSignature) {
      collapsedGroups.delete(SHOPIFY_OOS_GROUP_KEY);
    }
    lastOutOfStockSignature = currentOutOfStockSignature;
  }
  const hasActiveTopLevelFilters = !!(
    searchInput.value.trim() ||
    changedOnlyInput.checked ||
    selectedBrands.size ||
    selectedTypes.size ||
    activeStoreMetaFilters.length
  );

  let visibleMonitors = hasActiveTopLevelFilters ? getFilteredMonitors() : allMonitors.slice();
  if (!visibleMonitors.length && allMonitors.length) {
    resetDashboardFiltersWithoutRendering();
    visibleMonitors = allMonitors.slice();
  }
  if (!visibleMonitors.length && allMonitors.length && changedOnlyInput.checked && !allMonitors.some((monitor) => monitor.changeCount > 0)) {
    changedOnlyInput.checked = false;
    scheduleSaveDashboardUiState();
    visibleMonitors = getFilteredMonitors();
  }
  renderStats(visibleMonitors);
  populateFilterOptions();
  const gridSig = getGridSig(visibleMonitors);
  if (gridSig !== _lastGridSig) {
    _lastGridSig = gridSig;
    tryIncrementalCardUpdates(visibleMonitors);
  } else {
    syncCheckedStates();
  }
  if (monitorDuplicatesPanel.style.display !== "none") {
    loadDuplicateMonitorGroups().catch(() => {});
  }
  updateOutOfStockButtons();

  if (selectedMonitorId) {
    const m = _getMonitor(selectedMonitorId);
    if (m) {
      const sig = [m.lastCheckedAt, m.lastChangedAt, m.status, m.lastError, m.shopifyProductId, m.shopifySyncStatus, m.changeCount].join("|");
      if (sig !== _lastDetailSig) {
        _lastDetailSig = sig;
        renderDetail(m);
      }
    } else {
      selectedMonitorId = null;
      _lastDetailSig = "";
      monitorDetail.style.display = "none";
    }
  }

  updateBulkBtn();
  scheduleSaveDashboardUiState();
}

let recoveredJsonImportAttempted = false;
let recoveryBackupMode = false;
function setRecoveryStatus(text, isError = false) {
  const el = document.getElementById("recovery-status");
  if (!el) return;
  el.textContent = text;
  el.style.color = isError ? "var(--infrared)" : "var(--muted)";
}

async function loadRecoveredLocalBackupForDashboard() {
  const fileResponse = await fetch(chrome.runtime.getURL("recovered/local-monitors-backup.json"), { cache: "no-store" });
  if (!fileResponse.ok) throw new Error("Recovered local backup file not found.");
  const monitors = await fileResponse.json();
  if (!Array.isArray(monitors) || !monitors.length) throw new Error("Recovered local backup is empty or invalid.");
  return monitors;
}

async function renderRecoveredBackupDirectly(forceShopifyRefresh = false) {
  const monitors = await loadRecoveredLocalBackupForDashboard();
  setRecoveryStatus(`Loaded ${monitors.length} monitors from local backup - rendering...`);
  searchInput.value = "";
  changedOnlyInput.checked = false;
  selectedBrands.clear();
  selectedTypes.clear();
  activeStoreMetaFilters = [];
  groupFilters.clear();
  applyMonitorsUpdate(monitors);
  renderAll();
  monitorDetail.style.display = "none";
  chrome.runtime.sendMessage({
    type: "import-recovered-supabase-backup",
    payload: { replace: true }
  }).catch(() => {});
  return monitors.length;
}

async function tryRestoreFromLocalFileBackup(forceShopifyRefresh = false) {
  try {
    const monitors = await readLocalBackup();
    if (!monitors || !monitors.length) return false;
    setRecoveryStatus(`Loaded ${monitors.length} monitors from local backup file - restoring...`);
    const resp = await chrome.runtime.sendMessage({ type: "import-monitors", payload: { monitors, replace: true } });
    if (!resp?.ok) return false;
    applyMonitorsUpdate(monitors);
    renderAll();
    monitorDetail.style.display = "none";
    setRecoveryStatus(`Recovered ${monitors.length} monitors from local backup file.`);
    setTimeout(() => refreshMonitorMetaContent(true), 600);
    return true;
  } catch {
    return false;
  }
}

async function tryImportRecoveredSupabaseJson() {
  if (recoveredJsonImportAttempted) return false;
  recoveredJsonImportAttempted = true;
  try {
    const monitors = await loadRecoveredLocalBackupForDashboard();
    const importResponse = await chrome.runtime.sendMessage({
      type: "import-recovered-supabase-backup",
      payload: { replace: false }
    });
    if (!importResponse?.ok) throw new Error(importResponse?.error || "Recovered monitor import failed.");
    setRecoveryStatus(`Recovered ${importResponse.count || monitors.length} monitors from local backup`);
    return true;
  } catch (error) {
    setRecoveryStatus(error.message || String(error), true);
    return false;
  }
}

const WOW_IMG_RE = /way\s*of\s*wade|li[\s-]*ning|lining/i;
let _wowMigrationRunning = false;
let _wowColorMigrationDone = false;
async function migrateWayOfWadeImages() {
  if (_wowMigrationRunning) return;
  const toMigrate = allMonitors.filter((m) => {
    const imgs = m.productData?.images;
    if (!Array.isArray(imgs) || imgs.length < 2) return false;
    if (!WOW_IMG_RE.test(String(m.productData?.brand || getBrandFromMonitor(m) || ""))) return false;
    // Needs migration if not yet swapped, OR swapped but URLs not yet transformed
    if (!m.productData._wowImgSwapped) return true;
    return imgs.some((src) => /^https?:\/\/([a-z0-9-]+\.)*wayofwade\.com\/cdn\//i.test(src) && !src.includes("?width="));
  });
  if (!toMigrate.length) return;
  _wowMigrationRunning = true;
  const migratedIds = new Set();
  for (const m of toMigrate) {
    const pd = m.productData;
    const alreadySwapped = !!pd._wowImgSwapped;
    const [a, b, ...rest] = pd.images;
    // Only swap if not already done; always apply URL transform
    const ordered = alreadySwapped ? pd.images : [b, a, ...rest];
    const newImages = ordered.map((src) => transformWayOfWadeImageSrc(src, 680));
    const newPd = { ...pd, images: newImages, _wowImgSwapped: true };
    await chrome.runtime.sendMessage({
      type: "update-monitor",
      payload: { id: m.id, productData: newPd }
    }).catch(() => {});
    // Update in-memory immediately so Shopify import and display use correct data
    const idx = allMonitors.findIndex((x) => x.id === m.id);
    if (idx !== -1) allMonitors[idx] = { ...allMonitors[idx], productData: newPd };
    migratedIds.add(m.id);
  }
  monitorImageCandidateCache.clear();
  _renderedMonitorSigs.clear();
  migratedIds.forEach((id) => _dirtyMonitorIds.add(id));
  _wowMigrationRunning = false;
  renderAll();
}

async function loadDashboard(forceShopifyRefresh = false) {
  const filterSnapshot = captureDashboardFilters();
  monitorGrid.innerHTML = `<p style="grid-column:1/-1;padding:20px;color:var(--muted)">Loading monitors…</p>`;
  if (recoveryBackupMode) {
    try {
      const count = await renderRecoveredBackupDirectly(forceShopifyRefresh);
      recoveryBackupMode = false;
      setRecoveryStatus(`Restored ${count} monitors from local backup.`);
      setTimeout(() => refreshMonitorMetaContent(true), 600);
      return;
    } catch (error) {
      setRecoveryStatus(error.message || String(error), true);
    }
  }

  let response;
  try {
    response = await chrome.runtime.sendMessage({ type: "get-monitors" });
  } catch (connErr) {
    restoreDashboardFilters(filterSnapshot);
    monitorGrid.innerHTML = `<p style="grid-column:1/-1;padding:20px;color:#b41f1f">Extension background is not responding. <button id="dashboard-retry-btn" style="margin-left:8px;padding:4px 12px;cursor:pointer;border-radius:6px;border:1px solid #ccc">Retry</button></p>`;
    document.getElementById("dashboard-retry-btn")?.addEventListener("click", () => loadDashboard(false), { once: true });
    return;
  }

  if (!response?.ok) {
    restoreDashboardFilters(filterSnapshot);
    monitorGrid.innerHTML = `<p style="grid-column:1/-1;padding:20px;color:#b41f1f">${escapeHtml(response?.error || "Unable to load monitors.")}</p>`;
    return;
  }

  applyMonitorsUpdate(response.monitors || []);
  restoreDashboardFilters(filterSnapshot);
  if (allMonitors.length && !getFilteredMonitors().length) {
    resetDashboardFiltersWithoutRendering();
  }

  if (!allMonitors.length) {
    if (await tryRestoreFromLocalFileBackup(forceShopifyRefresh)) return;
    try {
      await renderRecoveredBackupDirectly(forceShopifyRefresh);
      return;
    } catch (_) {
      if (await tryImportRecoveredSupabaseJson()) {
        await loadDashboard(forceShopifyRefresh);
        return;
      }
    }
    renderStats([]);
    monitorGrid.innerHTML = `<section class="empty-state large" style="grid-column:1/-1"><p>No monitors yet.</p><p class="subtle">If you had monitors before, your backup file may still be at <strong>D:\\orjn backup\\monitors-backup.json</strong>.</p><p class="subtle"><label style="cursor:pointer;text-decoration:underline;color:var(--accent,#2563eb)">Click here to import it<input type="file" accept=".json" style="display:none" id="empty-state-import-input"></label></p></section>`;
    document.getElementById("empty-state-import-input")?.addEventListener("change", async (e) => {
      const file = e.target.files[0];
      if (!file) return;
      try {
        const monitors = JSON.parse(await file.text());
        if (!Array.isArray(monitors) || !monitors.length) { alert("File appears empty or invalid."); return; }
        const resp = await chrome.runtime.sendMessage({ type: "import-monitors", payload: { monitors, replace: true } });
        if (!resp?.ok) { alert("Import failed: " + (resp?.error || "unknown error")); return; }
        applyMonitorsUpdate(monitors);
        renderAll();
        monitorDetail.style.display = "none";
        setRecoveryStatus(`Restored ${monitors.length} monitors from backup file.`);
        setTimeout(() => refreshMonitorMetaContent(true), 600);
      } catch (err) {
        alert("Could not read file: " + err.message);
      }
    }, { once: true });
    monitorDetail.style.display = "none";
    refreshMonitorMetaContent();
    scheduleDashboardAutoRefresh({ forceShopifyRefresh, delayMs: forceShopifyRefresh ? 1200 : 2500 });
    return;
  }

  renderAll();
  scheduleDashboardAutoRefresh({ forceShopifyRefresh, delayMs: forceShopifyRefresh ? 1200 : 2500 });
  // Defer monitor meta load so it doesn't compete with the initial grid render
  setTimeout(() => refreshMonitorMetaContent(true), 600);
}

// Tracks the SW's _monitorsVersion — lets silentRefresh skip the IDB read when nothing changed.
let _lastKnownSwVersion = -1;

// Refresh data and re-render without the loading spinner flash.
// Guards against concurrent invocations: a second call while one is in-flight queues one
// more run (not many), so rapid back-to-back callers always produce exactly two renders max.
async function silentRefresh(forceShopifyRefresh = false) {
  if (importInProgress || dashboardRenderPaused) return;
  if (_silentRefreshRunning) {
    _silentRefreshPending = true;
    return;
  }
  _silentRefreshRunning = true;
  try {
    const filterSnapshot = captureDashboardFilters();
    let response;
    try {
      response = await chrome.runtime.sendMessage({ type: "get-monitors", knownVersion: _lastKnownSwVersion });
    } catch (_) { return; }
    if (!response?.ok) return;
    if (response.version !== undefined) _lastKnownSwVersion = response.version;
    // SW says nothing changed since our last fetch — skip IDB transfer and heavy processing.
    if (!response.unchanged) {
      applyMonitorsUpdate(response.monitors || []);
      restoreDashboardFilters(filterSnapshot);
      renderAll();
      refreshMonitorMetaContent();
      refreshUndoCount().catch(() => {});
      refreshLogCount().catch(() => {});
      updateCheckState().catch(() => {});
    }
    scheduleDashboardAutoRefresh({ forceShopifyRefresh, delayMs: forceShopifyRefresh ? 1200 : 2500 });
    // After any Shopify-modifying operation, silently refresh the store meta panel
    // so vendors/types/variants/tags stay in sync without requiring a manual Refresh click.
    if (forceShopifyRefresh && cachedStoreMetaRenderData) {
      fetchAndShowMeta(false).catch(() => {});
    }
  } finally {
    _silentRefreshRunning = false;
    if (_silentRefreshPending) {
      _silentRefreshPending = false;
      silentRefresh(forceShopifyRefresh).catch(() => {});
    }
  }
}

async function refreshShopifyProductCount(forceRefresh = false) {
  if (!await isConnected()) {
    clearShopifyProductImageCache();
    shopifyProductCount = null;
    shopifyUniqueSkuCount = null;
    shopifyMissingSkuCount = null;
    shopifyComparableSkuSet = null;
    shopifySkuGapDetails = [];
    shopifySkuGapProducts = [];
    shopifyBadSizeVariantCount = null;
    monitorOnlyMonitorIds = null;
    skuMismatchedLinkedMonitors = [];
    if (!importInProgress) renderStats(getFilteredMonitors());
    return;
  }
    try {
      if (forceRefresh) clearShopifyProductsSnapshotCache();
      const products = await getShopifyProductsSnapshot();
      rebuildShopifyProductImageCache(products);
      shopifyComparableSkuSet = getShopifyComparableSkuSet(products, allMonitors);
      shopifyProductCount = products.length;
      shopifyUniqueSkuCount = shopifyComparableSkuSet.size;
      shopifyMissingSkuCount = getShopifyMissingSkuCount(products);
      shopifySkuGapDetails = findMonitorShopifySkuGapDetails(products, allMonitors);
      shopifySkuGapProducts = findShopifySkuGapProducts(products, allMonitors);
      shopifyBadSizeVariantCount = countBadSizeVariants(products);
      monitorOnlyMonitorIds = computeMonitorMissingFromShopifyIds(products, allMonitors);
      skuMismatchedLinkedMonitors = findSkuMismatchedLinkedMonitors(products, allMonitors);
      markImportedNew48hMonitorsHidden();
    } catch (_) {
      clearShopifyProductImageCache();
      shopifyProductCount = null;
      shopifyUniqueSkuCount = null;
      shopifyMissingSkuCount = null;
      shopifyComparableSkuSet = null;
      shopifySkuGapDetails = [];
      shopifySkuGapProducts = [];
      shopifyBadSizeVariantCount = null;
      monitorOnlyMonitorIds = null;
  }
  if (!importInProgress) renderAll();
}

async function refreshOpenShopifyPanels(forceRefresh = false, { manual = false } = {}) {
  if (!manual) return;
  if (shopifyUnmonitoredPanel.style.display !== "none") {
    await loadShopifyUnmonitoredProducts(forceRefresh);
  }
  if (monitorNotShopifyPanel.style.display !== "none") {
    await loadMonitorNotShopifyMonitors(forceRefresh);
  }
  if (skuDuplicatesPanel.style.display !== "none") {
    await loadDuplicateSkuProducts(forceRefresh);
  }
  if (usSizeAuditPanel.style.display !== "none") {
    await loadBadSizeAudit(forceRefresh);
  }
}

async function refreshShopifyDashboardState(forceRefresh = false) {
  await refreshShopifyProductCount(forceRefresh);
  await refreshOpenShopifyPanels(forceRefresh, { manual: true });
}

async function runDashboardAutoRefresh(forceShopifyRefresh = false, { includePanels = false } = {}) {
  if (importInProgress) {
    dashboardAutoRefreshPending = true;
    return;
  }
  if (dashboardAutoRefreshRunning) {
    dashboardAutoRefreshPending = true;
    return;
  }
  dashboardAutoRefreshRunning = true;
  try {
    await refreshShopifyProductCount(forceShopifyRefresh);
    if (includePanels) await refreshOpenShopifyPanels(forceShopifyRefresh, { manual: true });
    refreshUndoCount().catch(() => {});
    refreshLogCount().catch(() => {});
  } finally {
    dashboardAutoRefreshRunning = false;
    if (dashboardAutoRefreshPending) {
      dashboardAutoRefreshPending = false;
      scheduleDashboardAutoRefresh({ forceShopifyRefresh: true, delayMs: 800 });
    }
  }
}

function scheduleDashboardAutoRefresh({ forceShopifyRefresh = false, delayMs = 800, includePanels = false } = {}) {
  clearTimeout(dashboardAutoRefreshTimer);
  dashboardAutoRefreshTimer = setTimeout(() => {
    runDashboardAutoRefresh(forceShopifyRefresh, { includePanels }).catch(() => {});
  }, delayMs);
}

function addSelectorChip(list, value) {
  const chip = document.createElement("span");
  chip.className = "selector-chip";
  chip.dataset.selector = value;
  chip.innerHTML = `<span class="chip-text">${escapeHtml(value)}</span><button class="chip-remove" title="Remove">Ã—</button>`;
  list.appendChild(chip);
}

// Sync only checkbox states in-place — avoids full grid re-render for check/select actions
function syncCheckedStates() {
  monitorGrid.querySelectorAll(".square-check").forEach((cb) => {
    cb.checked = checkedIds.has(cb.dataset.id);
  });
}

// Grid clicks: checkbox, site-select, site-check, or square-to-expand
monitorGrid.addEventListener("click", async (event) => {
  const target = event.target;

  if (target.classList.contains("group-toggle-btn")) {
    const key = target.dataset.groupKey;
    if (collapsedGroups.has(key)) collapsedGroups.delete(key);
    else collapsedGroups.add(key);
    renderGrid(getFilteredMonitors());
    return;
  }

  if (target.classList.contains("square-check")) {
    if (selectedCheckBatchIndexes.size) {
      event.preventDefault();
      syncSelectedCheckBatchesToCards();
      return;
    }
    const id = target.dataset.id;
    const groupKey = target.closest(".site-group")?.querySelector(".group-toggle-btn")?.dataset.groupKey || null;
    if (event.shiftKey && lastCheckedId && lastCheckedId !== id && groupKey && groupKey === lastCheckedGroupKey) {
      const visible = getFilteredMonitors();
      const groupMonitors = groupKey === "__new48h__"
        ? getNewNotImportedMonitors(visible)
        : groupKey === "__shopify_oos__"
          ? visible.filter((m) => isMonitorFullyOutOfStock(m) || shopifyOutOfStockMonitorIds?.has(m.id))
        : groupKey === "__errors__"
          ? allMonitors.filter(isErrorMonitor)
          : visible.filter((m) => getDomain(m.url) === groupKey);
      const a = groupMonitors.findIndex((m) => m.id === lastCheckedId);
      const b = groupMonitors.findIndex((m) => m.id === id);
      if (a !== -1 && b !== -1) {
        const [lo, hi] = a < b ? [a, b] : [b, a];
        const shouldCheck = target.checked;
        groupMonitors.slice(lo, hi + 1).forEach((m) => {
          if (shouldCheck) checkedIds.add(m.id);
          else checkedIds.delete(m.id);
        });
      }
    } else {
      if (target.checked) checkedIds.add(id);
      else checkedIds.delete(id);
      lastCheckedId = target.checked ? id : null;
      lastCheckedGroupKey = target.checked ? groupKey : null;
    }
    updateBulkBtn();
    syncCheckedStates();
    return;
  }

  if (target.classList.contains("site-select-btn")) {
    if (selectedCheckBatchIndexes.size) {
      clearSelectedCheckBatches();
    }
    const ids = target.dataset.ids.split(",").filter(Boolean);
    const allSelected = ids.every((id) => checkedIds.has(id));
    ids.forEach((id) => allSelected ? checkedIds.delete(id) : checkedIds.add(id));
    updateBulkBtn();
    syncCheckedStates();
    return;
  }

  if (target.classList.contains("shopify-oos-clear-btn")) {
    getVisibleOutOfStockMonitorIds(allMonitors).forEach((id) => dismissedOutOfStockMonitorIds.add(id));
    outOfStockSectionHidden = false;
    updateOutOfStockButtons();
    renderGrid(getFilteredMonitors());
    return;
  }

  if (target.classList.contains("site-check-btn")) {
    const ids = target.dataset.ids.split(",").filter(Boolean);
    runBulkCheck(ids);
    return;
  }

  if (target.classList.contains("site-clear-selected-btn")) {
    const ids = target.dataset.ids.split(",").filter(Boolean);
    if (!ids.length) return;
    if (!window.confirm(`Clear ${ids.length} selected monitor${ids.length > 1 ? "s" : ""} from New not imported yet?`)) return;
    await runButtonProgress(target, ids, "Clearing", async (id) => {
      await chrome.runtime.sendMessage({ type: "update-monitor", payload: { id, hiddenFromNew48h: true } });
      checkedIds.delete(id);
    });
    await silentRefresh();
    return;
  }

  if (target.classList.contains("site-clear-btn")) {
    const ids = target.dataset.ids.split(",").filter(Boolean);
    if (!ids.length) return;
    if (!window.confirm(`Clear ${ids.length} monitor${ids.length > 1 ? "s" : ""} from New not imported yet?`)) return;
    await runButtonProgress(target, ids, "Clearing", async (id) => {
      await chrome.runtime.sendMessage({ type: "update-monitor", payload: { id, hiddenFromNew48h: true } });
      checkedIds.delete(id);
    });
    await silentRefresh();
    return;
  }

  // Pagination
  if (target.classList.contains("group-page-btn")) {
    const key = target.dataset.groupKey;
    const page = parseInt(target.dataset.page, 10);
    if (!isNaN(page) && page >= 0) {
      groupPages.set(key, page);
      renderGridKeepingGroupPosition(key);
    }
    return;
  }

  // Group filter pill — brand/type
  if (target.closest(".group-filter-pill")) {
    const pill = target.closest(".group-filter-pill");
    const key = pill.dataset.groupKey;
    const f = groupFilters.get(key) || {};
    if (pill.dataset.filterBrand !== undefined) {
      const brand = pill.dataset.filterBrand;
      groupFilters.set(key, { ...f, brand: f.brand === brand ? null : brand });
    } else if (pill.dataset.filterType !== undefined) {
      const type = pill.dataset.filterType;
      groupFilters.set(key, { ...f, type: f.type === type ? null : type });
    }
    groupPages.set(key, 0);
    renderGroupInPlace(key);
    scheduleSaveDashboardUiState();
    return;
  }

  // Clear group filter
  if (target.classList.contains("group-filter-clear")) {
    const key = target.dataset.groupKey;
    groupFilters.delete(key);
    groupPages.set(key, 0);
    renderGroupInPlace(key);
    scheduleSaveDashboardUiState();
    return;
  }

  const square = target.closest(".monitor-square");
  if (!square) return;
  const id = square.dataset.id;
  const monitor = _getMonitor(id);
  if (!monitor) return;

  if (selectedMonitorId === id) {
    selectedMonitorId = null;
    monitorDetail.style.display = "none";
    square.classList.remove("active");
    return;
  }

  monitorGrid.querySelector(".monitor-square.active")?.classList.remove("active");
  selectedMonitorId = id;
  _lastDetailSig = "";
  square.classList.add("active");
  renderDetail(monitor);
  monitorDetail.scrollIntoView({ behavior: "smooth", block: "nearest" });
});

function setGroupPageFromInput(input) {
  const key = input.dataset.groupKey;
  if (!key) return;
  const min = Number(input.min || 1);
  const max = Number(input.max || 1);
  const value = Number(input.value || min);
  const page = Math.max(min, Math.min(max, Number.isFinite(value) ? value : min)) - 1;
  groupPages.set(key, page);
  renderGridKeepingGroupPosition(key);
}

monitorGrid.addEventListener("keydown", (event) => {
  const target = event.target;
  if (!target.classList?.contains("group-page-input")) return;
  if (event.key !== "Enter") return;
  event.preventDefault();
  setGroupPageFromInput(target);
});

monitorGrid.addEventListener("change", (event) => {
  const target = event.target;
  if (!target.classList?.contains("group-page-input")) return;
  setGroupPageFromInput(target);
});

// Detail panel: chip-remove, add-selector, save, check, delete
monitorDetail.addEventListener("click", async (event) => {
  const target = event.target;

  if (target.classList.contains("back-to-monitor-btn")) {
    const card = monitorGrid.querySelector(".monitor-square.active");
    if (card) { card.scrollIntoView({ behavior: "smooth", block: "center" }); return; }
    // Card may be virtualized (off-screen group) — scroll to its group header instead
    if (selectedMonitorId) {
      const m = _getMonitor(selectedMonitorId);
      if (m) {
        const gk = isErrorMonitor(m) ? "__errors__" : getDomain(m.url);
        const groupEl = monitorGrid.querySelector(`.site-group[data-group-key="${CSS.escape(gk)}"]`);
        if (groupEl) groupEl.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }
    return;
  }

  if (target.classList.contains("reset-count-btn")) {
    target.textContent = "…";
    target.disabled = true;
    await chrome.runtime.sendMessage({ type: "reset-change-count", monitorId: target.dataset.id });
    await silentRefresh();
    return;
  }

  if (target.classList.contains("remove-us-sizes-btn")) {
    target.textContent = "Removing…";
    target.disabled = true;
    const resp = await chrome.runtime.sendMessage({ type: "remove-us-sizes", monitorId: target.dataset.id });
    if (!resp?.ok) { alert("Failed: " + (resp?.error || "unknown")); target.textContent = "Remove bad sizes"; target.disabled = false; }
    return;
  }

  if (target.classList.contains("chip-remove")) {
    target.closest(".selector-chip")?.remove();
    return;
  }

  if (target.classList.contains("copy-selected-html-btn")) {
    const panel = target.closest(".selected-html-section");
    const html = panel?.querySelector(".raw-html-panel pre")?.textContent || "";
    if (!html) return;
    await navigator.clipboard.writeText(html);
    const original = target.textContent;
    target.textContent = "Copied";
    setTimeout(() => { target.textContent = original || "Copy outerHTML"; }, 1200);
    return;
  }

  if (target.classList.contains("pdata-img-remove")) {
    target.closest(".pdata-image-row")?.remove();
    refreshImageEditButtons(target.closest(".pdata-image-list"));
    return;
  }

  if (target.classList.contains("pdata-img-up") || target.classList.contains("pdata-img-down")) {
    const row = target.closest(".pdata-image-row");
    const list = row?.parentElement;
    if (!list) return;
    if (target.classList.contains("pdata-img-up") && row.previousElementSibling) {
      list.insertBefore(row, row.previousElementSibling);
    } else if (target.classList.contains("pdata-img-down") && row.nextElementSibling) {
      list.insertBefore(row.nextElementSibling, row);
    }
    refreshImageEditButtons(list);
    return;
  }

  if (target.classList.contains("pdata-edit-btn")) {
    const monitorId = target.dataset.monitorId;
    const monitor = _getMonitor(monitorId);
    if (!monitor?.productData) return;
    target.closest(".product-data-section").outerHTML = renderProductDataEdit(monitor.productData, monitorId);
    return;
  }

  if (target.classList.contains("pdata-cancel-btn")) {
    const monitor = _getMonitor(selectedMonitorId);
    if (monitor) renderDetail(monitor);
    return;
  }

  if (target.dataset.resyncShopify) {
    const monitorId = target.dataset.resyncShopify;
    const monitor = _getMonitor(monitorId);
    if (!monitor) return;
    if (!await isConnected()) {
      window.alert("Connect Shopify first.");
      return;
    }
    target.textContent = "Updating…";
    target.disabled = true;
    importInProgress = true;
    updateBulkBtn();
    showShopifySyncStatus(`Updating Shopify for ${monitor.productData?.sku || monitor.name || monitor.id}...`, "working", { autoHide: false });
    try {
      const result = await reapplyMonitorDataToShopify(monitor, monitor.lastExtractedData || {});
      const syncVerb = result?.created ? "Created Shopify product from monitor data" : "Updated Shopify product from monitor data";
      await Promise.all([
        chrome.runtime.sendMessage({
          type: "update-monitor",
          payload: {
            id: monitorId,
            shopifyProductId: result?.id || monitor.shopifyProductId || null,
            shopifyLastSyncAt: new Date().toISOString(),
            shopifySyncStatus: "ok"
          }
        }),
        addLog({
          type: "shopify-sync",
          title: [monitor.productData?.brand, monitor.productData?.sku].filter(Boolean).join(" ") || monitor.name,
          productName: monitor.productData?.name || monitor.name || "",
          brand: monitor.productData?.brand || "",
          sku: monitor.productData?.sku || "",
          details: [syncVerb],
          monitorId: monitor.id,
          url: monitor.url
        }).catch(() => {})
      ]);
      await silentRefresh(true);
      const refreshedMonitor = _getMonitor(monitorId);
      if (refreshedMonitor) renderDetail(refreshedMonitor);
      showShopifySyncStatus(`${syncVerb}: ${monitor.productData?.sku || monitor.name || monitor.id}.`, "success");
    } catch (error) {
      const reason = error?.message || String(error);
      await chrome.runtime.sendMessage({
        type: "update-monitor",
        payload: {
          id: monitorId,
          shopifySyncStatus: "error"
        }
      }).catch(() => {});
      target.textContent = "Update Shopify";
      target.disabled = false;
      showShopifySyncStatus(`Shopify update failed for ${monitor.productData?.sku || monitor.name || monitor.id}: ${reason}`, "error", { autoHide: false });
      window.alert(reason);
    } finally {
      importInProgress = false;
      updateBulkBtn();
    }
    return;
  }

  if (target.classList.contains("pdata-save-btn")) {
    const monitorId = target.dataset.monitorId;
    const monitor = _getMonitor(monitorId);
    if (!monitor) return;
    const section = target.closest(".product-data-section");
    const updated = { ...(monitor.productData || {}), source: monitor.productData?.source || [] };
    section.querySelectorAll(".pdata-input").forEach((inp) => {
      const f = inp.dataset.field;
      if (f) updated[f] = inp.value.trim() || null;
    });
    const imageRows = section.querySelectorAll(".pdata-image-row");
    if (imageRows.length > 0 || section.querySelector(".pdata-image-list")) {
      updated.images = Array.from(imageRows).map((r) => r.dataset.url).filter(Boolean);
    }
    target.textContent = "Saving…";
    target.disabled = true;
    chrome.runtime.sendMessage({
      type: "update-monitor",
      payload: { id: monitorId, productData: updated }
    }).then(async () => { await silentRefresh(); });
    return;
  }

  if (target.classList.contains("add-selector-btn")) {
    const input = monitorDetail.querySelector(".selector-input");
    const value = input.value.trim();
    if (!value) return;
    addSelectorChip(monitorDetail.querySelector(".selectors-list"), value);
    input.value = "";
    return;
  }

  if (target.classList.contains("pick-selector-btn")) {
    const monitor = _getMonitor(selectedMonitorId);
    const [activeTab] = await chrome.tabs.query({ active: true, currentWindow: true }).catch(() => []);
    const tabs = await chrome.tabs.query({ currentWindow: true }).catch(() => []);
    let tab = activeTab;
    try {
      const monitorUrl = new URL(monitor?.url || "");
      const activeUrl = new URL(activeTab?.url || "");
      if (monitorUrl.hostname.replace(/^www\./i, "") !== activeUrl.hostname.replace(/^www\./i, "")) {
        tab = tabs.find((candidate) => {
          try {
            const candidateUrl = new URL(candidate.url || "");
            return candidateUrl.href === monitorUrl.href ||
              candidateUrl.hostname.replace(/^www\./i, "") === monitorUrl.hostname.replace(/^www\./i, "");
          } catch (_) {
            return false;
          }
        }) || activeTab;
      }
    } catch (_) {}
    if (!tab?.id || /^chrome-extension:\/\//i.test(tab.url || "")) {
      window.alert("Open the product page tab first, then click Pick from tab again.");
      return;
    }
    target.textContent = "Picking…";
    target.disabled = true;
    await chrome.tabs.update(tab.id, { active: true }).catch(() => {});
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ["content-script.js"] }).catch(() => {});
    const response = await chrome.tabs.sendMessage(tab.id, { type: "start-simple-picker" }).catch((error) => ({ ok: false, error: error?.message || String(error) }));
    if (!response?.ok) {
      target.textContent = "Pick from tab";
      target.disabled = false;
      window.alert(response?.error || "Could not start picker on that tab.");
      return;
    }
    setTimeout(() => { if (target.isConnected) { target.textContent = "Pick from tab"; target.disabled = false; } }, 30000);
    return;
  }

  if (!(target instanceof HTMLButtonElement)) return;

  if (target.dataset.save) {
    const chips = monitorDetail.querySelectorAll(".selectors-list .selector-chip");
    const selectors = Array.from(chips)
      .map((c) => c.dataset.selector || c.querySelector(".chip-text")?.textContent?.trim() || "")
      .filter(Boolean);
    const autoCheck = monitorDetail.querySelector(".autocheck-toggle").checked;
    const intervalValue = Number(monitorDetail.querySelector(".interval-value").value) || 1;
    const intervalUnit = Number(monitorDetail.querySelector(".interval-unit").value) || 1440;

    target.textContent = "Saving…";
    target.disabled = true;
    await chrome.runtime.sendMessage({
      type: "update-monitor",
      payload: { id: target.dataset.save, selectors, autoCheck, intervalMinutes: Math.max(1, intervalValue * intervalUnit) }
    });
    await silentRefresh();
    return;
  }

  if (target.dataset.refresh) {
    const originalText = target.textContent;
    target.textContent = "Checking...";
    target.disabled = true;
    try {
      await chrome.runtime.sendMessage({ type: "resume-checks" }).catch(() => {});
      const monitor = _getMonitor(target.dataset.refresh);
      const [activeTab] = await chrome.tabs.query({ active: true, currentWindow: true }).catch(() => []);
      let tabId = null;
      try {
        const monitorHost = new URL(monitor?.url || "").hostname.replace(/^www\./i, "");
        const tabHost = new URL(activeTab?.url || "").hostname.replace(/^www\./i, "");
        if (monitorHost && tabHost && monitorHost === tabHost) tabId = activeTab.id;
      } catch (_) {}
      const response = await sendRuntimeMessageWithTimeout({ type: "refresh-monitor", monitorId: target.dataset.refresh, tabId }, 240000);
      if (response?.monitor) {
        applyCheckedMonitorUpdate(response.monitor);
        scheduleDashboardAutoRefresh({ forceShopifyRefresh: false });
      } else if (response?.error) {
        window.alert(response.error);
        await silentRefresh(true);
      } else {
        await silentRefresh(true);
      }
    } catch (error) {
      window.alert(`Check failed: ${error?.message || error || "unknown error"}`);
      await silentRefresh(true).catch(() => {});
    } finally {
      if (target.isConnected) {
        target.textContent = originalText;
        target.disabled = false;
      }
    }
    return;
  }

  if (target.dataset.firstCapture) {
    target.textContent = "Capturing...";
    target.disabled = true;
    try {
      const response = await sendRuntimeMessageWithTimeout({ type: "first-capture", monitorId: target.dataset.firstCapture }, 240000);
      if (response?.monitor) {
        applyCheckedMonitorUpdate(response.monitor);
        scheduleDashboardAutoRefresh({ forceShopifyRefresh: false });
      } else if (response?.error) {
        window.alert(response.error);
        await silentRefresh(true);
      } else {
        await silentRefresh(true);
      }
    } catch (error) {
      window.alert(`First capture failed: ${error?.message || error || "unknown error"}`);
      await silentRefresh(true).catch(() => {});
    } finally {
      if (target.isConnected) {
        target.textContent = "First capture";
        target.disabled = false;
      }
    }
    return;
  }

  if (target.dataset.delete) {
    const deleteId = target.dataset.delete;
    await chrome.runtime.sendMessage({ type: "delete-monitor", monitorId: deleteId });
    selectedMonitorId = null;
    _lastDetailSig = "";
    monitorDetail.style.display = "none";
    removeMonitorFromDom(deleteId);
    refreshUndoCount();
    scheduleDashboardAutoRefresh({ forceShopifyRefresh: false });
  }
});

// Lazy-load diff viewer when a collapsed history entry is expanded
monitorDetail.addEventListener("toggle", (event) => {
  const details = event.target;
  if (details.classList.contains("selected-html-section") && details.open) {
    const placeholder = details.querySelector(".selected-html-placeholder");
    if (!placeholder) return;
    const monitorId = details.dataset.monitorId;
    const monitor = _getMonitor(monitorId);
    if (!monitor) return;
    placeholder.textContent = "Loading selected outerHTML...";
    (async () => {
      let currSnap = monitor.lastSelectedOuterHtmlSnapshot || "";
      if (!currSnap && monitor.hasLastSelectedOuterHtmlSnapshot) {
        const res = await chrome.runtime.sendMessage({ type: "get-monitor-snapshots", payload: { id: monitor.id } }).catch(() => null);
        if (res?.ok) currSnap = res.lastSelectedOuterHtmlSnapshot || "";
      }
      const html = currSnap || "No selected outerHTML captured yet. Click Check now to capture the current selected page parts.";
      placeholder.outerHTML = `
        <div style="display:flex;justify-content:flex-end;margin:0 12px 8px">
          <button class="inline-button copy-selected-html-btn" type="button">Copy outerHTML</button>
        </div>
        <details class="raw-html-panel" open style="margin:0 12px 12px">
          <summary>Current selected outerHTML</summary>
          <pre>${truncateHtml(html)}</pre>
        </details>`;
    })();
    return;
  }
  if (details.classList.contains("main-diff-section") && details.open) {
    const placeholder = details.querySelector(".main-diff-placeholder");
    if (!placeholder) return;
    const monitorId = details.dataset.monitorId;
    const monitor = _getMonitor(monitorId);
    if (!monitor) return;
    placeholder.textContent = "Loading diff...";
    (async () => {
      let prevSnap = monitor.previousHtmlSnapshot || "";
      let currSnap = monitor.lastHtmlSnapshot || "";
      if (!prevSnap && !currSnap && monitor.hasLastHtmlSnapshot) {
        const res = await chrome.runtime.sendMessage({ type: "get-monitor-snapshots", payload: { id: monitor.id } }).catch(() => null);
        if (res?.ok) { prevSnap = res.previousHtmlSnapshot || ""; currSnap = res.lastHtmlSnapshot || ""; }
      }
      const rows = getCachedDiffRows(`${monitor.id}:main:${monitor.lastChangedAt || ""}`, prevSnap, currSnap);
      placeholder.outerHTML = renderDiffViewer(rows);
    })();
    return;
  }
  if (!details.classList.contains("history-entry") || !details.open) return;
  const placeholder = details.querySelector(".history-diff-placeholder");
  if (!placeholder) return;
  const index = parseInt(details.dataset.historyIndex, 10);
  const monitorId = details.dataset.monitorId;
  const monitor = _getMonitor(monitorId);
  const entry = monitor?.changeHistory?.[index];
  if (!entry) return;
  placeholder.textContent = "Loading diff...";
  (async () => {
    let prevHtml = entry.previousHtml || "";
    let currHtml = entry.currentHtml || "";
    if (!prevHtml && !currHtml && (entry.hasPreviousHtml || entry.hasCurrentHtml)) {
      const res = await chrome.runtime.sendMessage({ type: "get-monitor-snapshots", payload: { id: monitorId, historyIndex: index } }).catch(() => null);
      if (res?.ok) { prevHtml = res.previousHtml || ""; currHtml = res.currentHtml || ""; }
    }
    const cacheKey = `${monitorId}:hist:${index}:${entry.changedAt || ""}`;
    const rows = getCachedDiffRows(cacheKey, prevHtml, currHtml);
    const s = summarizeDiff(rows);
    const summary = details.querySelector("summary .pill-row");
    if (summary) {
      summary.innerHTML = `<span class="pill add">+${s.added}</span><span class="pill remove">-${s.removed}</span><span class="pill neutral">~${s.changed}</span>`;
    }
    if (placeholder.isConnected) placeholder.outerHTML = renderDiffViewer(rows);
  })();
}, true);

// Enter to add selector
monitorDetail.addEventListener("keydown", (event) => {
  if (event.key !== "Enter" || !event.target.classList.contains("selector-input")) return;
  const value = event.target.value.trim();
  if (!value) return;
  addSelectorChip(monitorDetail.querySelector(".selectors-list"), value);
  event.target.value = "";
});

function applyCheckedMonitorUpdate(monitor) {
  if (!monitor?.id) return;
  patchMonitorInPlace(monitor);
  if (reconcileBatchCheckResultsWithCurrentState()) {
    renderCheckBatchOptions();
    renderBatchCheckStatus();
  }
}

async function runBulkCheck(ids) {
  if (!ids.length) return;
  if (activeBatchRun) return;

  const requestedIdSet = new Set(ids);
  const orderedIds = getMonitorsFirstCreated()
    .map((monitor) => monitor.id)
    .filter((id) => requestedIdSet.has(id));
  const batches = getCheckBatches();
  const matchingBatch = batches.find((batch) =>
    batch.ids.length === orderedIds.length && batch.ids.every((id, index) => id === orderedIds[index])
  );
  const singleRunBatch = matchingBatch || {
    index: -1,
    from: 1,
    to: orderedIds.length,
    ids: orderedIds,
    monitors: orderedIds.map((id) => _getMonitor(id)).filter(Boolean)
  };
  const selectedBatchIndexes = [...selectedCheckBatchIndexes]
    .map(Number)
    .filter((index) => batches[index])
    .sort((a, b) => a - b);
  const hasSelectedBatches = selectedBatchIndexes.length > 0;
  const shouldContinue = !!continueCheckToggle?.checked && hasSelectedBatches;
  const runBatches = shouldContinue
    ? batches.slice(selectedBatchIndexes[0])
    : (hasSelectedBatches ? selectedBatchIndexes.map((index) => batches[index]) : [singleRunBatch]);
  const runIds = runBatches.flatMap((batch) => batch.ids);
  if (!runIds.length) return;

  const total = runIds.length;
  let totalDone = 0;
  let totalSuccess = 0;
  let totalErrors = 0;
  let stopped = false;
  activeBatchRun = { stopped: false };

  const freeze = (label = "Checking") => {
    checkAllBtn.disabled = true;
    if (checkSavedBtn) checkSavedBtn.disabled = true;
    bulkCheckBtn.disabled = true;
    bulkFirstCaptureBtn.disabled = true;
    bulkDeleteBtn.disabled = true;
    selectAllBtn.disabled = true;
    deselectBtn.disabled = true;
    stopChecksBtn.style.display = "";
    const row = document.getElementById("bulk-interval-row");
    if (row) row.style.display = "none";
    const text = `${label} ${totalDone}/${total} (${totalSuccess} successful, ${totalErrors} errors)...`;
    bulkCheckBtn.textContent = text;
    checkAllBtn.textContent = text;
    if (checkSavedBtn) checkSavedBtn.textContent = text;
  };

  const unfreeze = () => {
    checkAllBtn.disabled = false;
    checkAllBtn.textContent = "Check all";
    if (checkSavedBtn) checkSavedBtn.textContent = "Check saved";
    selectAllBtn.disabled = false;
    deselectBtn.disabled = false;
    stopChecksBtn.disabled = false;
    stopChecksBtn.textContent = "Stop checks";
    stopChecksBtn.style.display = "none";
    checkedIds.clear();
    clearSelectedCheckBatches();
    activeBatchRun = null;
    updateBulkBtn();
  };

  try {

  renderCheckBatchOptions();
  freeze(); // freeze immediately before any await so UI reflects running state

  // Detect invalidated context (extension was reloaded while this tab was open).
  if (!chrome.runtime?.id) {
    showShopifySyncStatus(
      "This page is outdated — the extension was reloaded. Please close this tab and reopen the dashboard from the extension icon.",
      "error", { autoHide: false }
    );
    unfreeze();
    return;
  }

  // Reset any stale stop flag in SW. Non-blocking — if it fails we still try checks.
  const swAlive = await Promise.race([
    chrome.runtime.sendMessage({ type: "resume-checks" }).then(r => r?.ok === true).catch(() => false),
    new Promise(r => setTimeout(() => r(false), 5000))
  ]);
  if (!swAlive) {
    showShopifySyncStatus(
      "Warning: background worker may be unresponsive — checks may fail. If all checks error, reload the extension from chrome://extensions then reopen this dashboard.",
      "error", { autoHide: false }
    );
    // Don't bail — let the checks attempt anyway so the user gets error feedback
  }

  const stopOnce = () => {
    stopped = true;
    if (activeBatchRun) activeBatchRun.stopped = true;
    stopChecksBtn.disabled = true;
    stopChecksBtn.textContent = "Stopping...";
    renderBatchCheckStatus(`Stopping after current in-flight checks finish...`);
    chrome.runtime.sendMessage({ type: "stop-checks" }).catch(() => {});
  };
  stopChecksBtn.addEventListener("click", stopOnce, { once: true });

  const allSelectedMonitors = runIds.map((id) => _getMonitor(id)).filter(Boolean);
  const hasDsgMonitor = allSelectedMonitors.some((monitor) => isDsgMonitorUrl(monitor.url || ""));
  const hasRetailLimitedMonitor = allSelectedMonitors.some((monitor) => /footlocker\.com/i.test(monitor.url || ""));
  if (hasDsgMonitor || hasRetailLimitedMonitor) {
    showShopifySyncStatus("Clearing cookies...", "working", { autoHide: false });
    try {
      await sendRuntimeMessageWithTimeout({ type: "prepare-dsg-session" }, 15000);
      showShopifySyncStatus("Cookies cleared.", "working", { autoHide: true });
    } catch (_) {
      showShopifySyncStatus("Cookie clear timed out - continuing with current cookies...", "working", { autoHide: true });
    }
    if (stopped || activeBatchRun?.stopped) {
      unfreeze();
      return;
    }
  }

  const runChunk = async (chunkIds, batch, batchResult, concurrency) => {
    let cursor = 0;
    const workers = Array.from({ length: Math.min(concurrency, chunkIds.length) }, async () => {
      while (!stopped && cursor < chunkIds.length) {
        const id = chunkIds[cursor++];
        let completed = false;
        try {
          const m = _getMonitor(id);
          const monitorHint = m ? {
            id: m.id, url: m.url, name: m.name,
            selectors: m.selectors, selector: m.selector,
            priceAdjustment: m.priceAdjustment,
            productData: m.productData,
            lastExtractedData: m.lastExtractedData,
            lastHtmlSnapshot: m.lastHtmlSnapshot,
            previousSnapshot: m.previousSnapshot,
            status: m.status,
            pendingInitialCheck: m.pendingInitialCheck
          } : null;
          const response = await sendRuntimeMessageWithTimeout({ type: "refresh-monitor", monitorId: id, isBatch: total > 1, monitorHint }, 300000);
          if (response?.stopped || stopped || activeBatchRun?.stopped) {
            stopped = true;
            continue;
          }
          completed = true;
          if (response?.monitor) applyCheckedMonitorUpdate(response.monitor);
          if (response?.ok && response?.monitor && !isErrorMonitor(response.monitor)) {
            totalSuccess++;
            batchResult.success++;
          } else {
            totalErrors++;
            batchResult.errors.push(id);
          }
        } catch (_) {
          completed = true;
          totalErrors++;
          batchResult.errors.push(id);
          showShopifySyncStatus("Monitor check failed or timed out. The dashboard is still responsive.", "error", { autoHide: true });
        } finally {
          if (completed && !batchResult.completedIds.includes(id)) batchResult.completedIds.push(id);
          if (completed) totalDone++;
          freeze(`Batch ${batch.from}-${batch.to}`);
          renderBatchCheckStatus(`Checking Batch ${batch.from}-${batch.to}: ${batchResult.success} successful, ${batchResult.errors.length} errors. Total ${totalDone}/${total}.`);
        }
      }
    });
    await Promise.all(workers);
  };

  try {
    for (const batch of runBatches) {
      if (stopped) break;
      selectedCheckBatchIndex = batch.index >= 0 ? batch.index : selectedCheckBatchIndex;
      batchCheckResults.set(batch.index, { success: 0, errors: [], ids: batch.ids.slice(), completedIds: [] });
      const batchResult = batchCheckResults.get(batch.index);
      const batchMonitors = batch.ids.map((id) => _getMonitor(id)).filter(Boolean);
      const hasBatchDsg = batchMonitors.some((monitor) => isDsgMonitorUrl(monitor.url || ""));
      const hasBatchRetail = batchMonitors.some((monitor) => /footlocker\.com/i.test(monitor.url || ""));
      const concurrency = hasBatchDsg ? 4 : (hasBatchRetail ? 8 : 10);
      renderBatchCheckStatus(`Starting Batch ${batch.from}-${batch.to}: 0 successful, 0 errors.`);
      await runChunk(batch.ids, batch, batchResult, concurrency);
      renderBatchCheckStatus(`Finished Batch ${batch.from}-${batch.to}: ${batchResult.success} successful, ${batchResult.errors.length} errors.`);
      renderCheckBatchOptions();
      if (!shouldContinue) break;
    }
  } finally {
    stopChecksBtn.removeEventListener("click", stopOnce);
    await refreshUndoCount();
    await chrome.runtime.sendMessage({ type: "resume-checks" }).catch(() => {});
    unfreeze();
    scheduleDashboardAutoRefresh({ forceShopifyRefresh: false });
    renderBatchCheckStatus(stopped
      ? `Stopped at ${totalDone}/${total}: ${totalSuccess} successful, ${totalErrors} errors.`
      : `Finished checks: ${totalSuccess} successful, ${totalErrors} errors.`);
    if (totalErrors) {
      showShopifySyncStatus(`${totalErrors} monitor check${totalErrors !== 1 ? "s" : ""} failed.`, "error", { autoHide: false });
    }
  }

  } finally {
    // Emergency cleanup: clears activeBatchRun if anything threw before the inner try/finally ran
    if (activeBatchRun) { activeBatchRun = null; updateBulkBtn(); }
  }
}

async function refreshUndoCount() {
  const resp = await chrome.runtime.sendMessage({ type: "get-undo-count" }).catch(() => null);
  const count = resp?.count ?? 0;
  undoLastBtn.textContent = `Undo (${count})`;
  undoLastBtn.disabled = count === 0;
}

stopChecksBtn.addEventListener("click", async () => {
  if (activeBatchRun) {
    activeBatchRun.stopped = true;
    stopChecksBtn.disabled = true;
    stopChecksBtn.textContent = "Stopping...";
    renderBatchCheckStatus("Stopping checks...");
    await chrome.runtime.sendMessage({ type: "stop-checks" }).catch(() => {});
    return;
  }
  await chrome.runtime.sendMessage({ type: "stop-checks" });
  await silentRefresh();
});

undoLastBtn.addEventListener("click", async () => {
  undoLastBtn.disabled = true;
  const resp = await chrome.runtime.sendMessage({ type: "undo-last" });
  if (resp?.ok) {
    await silentRefresh();
  } else {
    alert(resp?.error || "Nothing to undo");
  }
  await refreshUndoCount();
});

// Bulk check selected monitors
bulkCheckBtn.addEventListener("click", () => {
  if (!checkedIds.size) return;
  runBulkCheck(Array.from(checkedIds));
});

// Bulk first capture selected monitors
async function runBulkFirstCapture(ids) {
  if (!ids.length) return;
  if (activeBatchRun) return;

  const requestedIdSet = new Set(ids);
  const orderedIds = getMonitorsFirstCreated().map((m) => m.id).filter((id) => requestedIdSet.has(id));
  if (!orderedIds.length) return;

  const total = orderedIds.length;
  let totalDone = 0;
  let totalSuccess = 0;
  let totalErrors = 0;
  let stopped = false;
  activeBatchRun = { stopped: false };

  const freeze = () => {
    checkAllBtn.disabled = true;
    if (checkSavedBtn) checkSavedBtn.disabled = true;
    bulkCheckBtn.disabled = true;
    bulkFirstCaptureBtn.disabled = true;
    bulkDeleteBtn.disabled = true;
    selectAllBtn.disabled = true;
    deselectBtn.disabled = true;
    stopChecksBtn.style.display = "";
    const row = document.getElementById("bulk-interval-row");
    if (row) row.style.display = "none";
    const text = `First capturing ${totalDone}/${total} (${totalSuccess} successful, ${totalErrors} errors)...`;
    bulkFirstCaptureBtn.textContent = text;
    checkAllBtn.textContent = text;
    if (checkSavedBtn) checkSavedBtn.textContent = text;
    bulkCheckBtn.textContent = text;
  };

  const unfreeze = () => {
    checkAllBtn.disabled = false;
    checkAllBtn.textContent = "Check all";
    if (checkSavedBtn) checkSavedBtn.textContent = "Check saved";
    selectAllBtn.disabled = false;
    deselectBtn.disabled = false;
    stopChecksBtn.disabled = false;
    stopChecksBtn.textContent = "Stop checks";
    stopChecksBtn.style.display = "none";
    checkedIds.clear();
    activeBatchRun = null;
    updateBulkBtn();
  };

  try {
    freeze();

    if (!chrome.runtime?.id) {
      showShopifySyncStatus("This page is outdated — the extension was reloaded. Please close this tab and reopen the dashboard.", "error", { autoHide: false });
      unfreeze();
      return;
    }

    const swAlive = await Promise.race([
      chrome.runtime.sendMessage({ type: "resume-checks" }).then(r => r?.ok === true).catch(() => false),
      new Promise(r => setTimeout(() => r(false), 5000))
    ]);
    if (!swAlive) {
      showShopifySyncStatus("Warning: background worker may be unresponsive — captures may fail.", "error", { autoHide: false });
    }

    const stopOnce = () => {
      stopped = true;
      if (activeBatchRun) activeBatchRun.stopped = true;
      stopChecksBtn.disabled = true;
      stopChecksBtn.textContent = "Stopping...";
      renderBatchCheckStatus("Stopping after current in-flight captures finish...");
      chrome.runtime.sendMessage({ type: "stop-checks" }).catch(() => {});
    };
    stopChecksBtn.addEventListener("click", stopOnce, { once: true });

    const allSelectedMonitors = orderedIds.map((id) => _getMonitor(id)).filter(Boolean);
    const hasDsg = allSelectedMonitors.some((m) => isDsgMonitorUrl(m.url || ""));
    const hasRetail = allSelectedMonitors.some((m) => /footlocker\.com/i.test(m.url || ""));

    if (hasDsg || hasRetail) {
      showShopifySyncStatus("Clearing cookies...", "working", { autoHide: false });
      try {
        await sendRuntimeMessageWithTimeout({ type: "prepare-dsg-session" }, 15000);
        showShopifySyncStatus("Cookies cleared.", "working", { autoHide: true });
      } catch (_) {
        showShopifySyncStatus("Cookie clear timed out - continuing with current cookies...", "working", { autoHide: true });
      }
    }

    const concurrency = hasDsg ? 4 : (hasRetail ? 8 : 10);
    let cursor = 0;

    try {
      const workers = Array.from({ length: Math.min(concurrency, orderedIds.length) }, async () => {
        while (!stopped && cursor < orderedIds.length) {
          const id = orderedIds[cursor++];
          let completed = false;
          try {
            const response = await sendRuntimeMessageWithTimeout({ type: "first-capture", monitorId: id, isBatch: true }, 300000);
            if (response?.stopped || stopped || activeBatchRun?.stopped) {
              stopped = true;
              continue;
            }
            completed = true;
            if (response?.monitor) applyCheckedMonitorUpdate(response.monitor);
            if (response?.ok && response?.monitor && !isErrorMonitor(response.monitor)) {
              totalSuccess++;
            } else {
              totalErrors++;
            }
          } catch (_) {
            completed = true;
            totalErrors++;
            showShopifySyncStatus("First capture failed or timed out. The dashboard is still responsive.", "error", { autoHide: true });
          } finally {
            if (completed) totalDone++;
            freeze();
          }
        }
      });
      await Promise.all(workers);
    } finally {
      stopChecksBtn.removeEventListener("click", stopOnce);
      await refreshUndoCount();
      await chrome.runtime.sendMessage({ type: "resume-checks" }).catch(() => {});
      unfreeze();
      scheduleDashboardAutoRefresh({ forceShopifyRefresh: false });
      renderBatchCheckStatus(stopped
        ? `Stopped at ${totalDone}/${total}: ${totalSuccess} successful, ${totalErrors} errors.`
        : `Finished first capture: ${totalSuccess} successful, ${totalErrors} errors.`);
      if (totalErrors) {
        showShopifySyncStatus(`${totalErrors} first capture${totalErrors !== 1 ? "s" : ""} failed.`, "error", { autoHide: false });
      }
    }

  } finally {
    if (activeBatchRun) { activeBatchRun = null; updateBulkBtn(); }
    await silentRefresh();
  }
}

bulkFirstCaptureBtn.addEventListener("click", () => {
  if (!checkedIds.size) return;
  runBulkFirstCapture(Array.from(checkedIds));
});

function buildSmartImportOrder(monitors) {
  const toImport = monitors.filter(m => !m.shopifyProductId);

  function getSite(m) {
    const url = String(m.url || "");
    try {
      const host = new URL(url).hostname;
      if (/(^|\.)footlocker\.com\b/i.test(host)) return "FL";
      if (/(^|\.)dickssportinggoods\.com\b/i.test(host)) return "DSG";
      if (/(^|\.)wayofwade\.com\b/i.test(host)) return "WoW";
    } catch (_) {}
    if (/footlocker\.com/i.test(url)) return "FL";
    if (/dickssportinggoods\.com/i.test(url)) return "DSG";
    if (/wayofwade\.com/i.test(url)) return "WoW";
    return "Other";
  }

  // pools[brand][site] = [...monitors sorted by createdAt asc]
  const pools = {};
  for (const m of toImport) {
    const brand = canonicalizeBrand(m.productData?.brand || "") || "Unknown";
    const site = getSite(m);
    if (!pools[brand]) pools[brand] = {};
    if (!pools[brand][site]) pools[brand][site] = [];
    pools[brand][site].push(m);
  }
  for (const brand of Object.keys(pools)) {
    for (const site of Object.keys(pools[brand])) {
      pools[brand][site].sort((a, b) => (a.createdAt || "") < (b.createdAt || "") ? -1 : 1);
    }
  }

  function countBrand(brand) {
    if (!pools[brand]) return 0;
    return Object.values(pools[brand]).reduce((s, a) => s + a.length, 0);
  }

  function pickFromBrand(brand, preferSite) {
    if (!pools[brand]) return null;
    if (pools[brand][preferSite]?.length) return pools[brand][preferSite].shift();
    // steal oldest across all sites
    let oldestSite = null, oldestTs = null;
    for (const [site, arr] of Object.entries(pools[brand])) {
      if (!arr.length) continue;
      const ts = arr[0].createdAt || "";
      if (oldestSite === null || ts < oldestTs) { oldestTs = ts; oldestSite = site; }
    }
    if (oldestSite) return pools[brand][oldestSite].shift();
    return null;
  }

  const OTHER_BRAND_ORDER = [
    "Jordan", "Way of Wade", "Hoka", "New Balance", "ON Cloud",
    "Puma", "Asics", "Converse", "Reebok"
  ];
  const allBrandKeys = new Set(Object.keys(pools));
  allBrandKeys.delete("Nike");
  allBrandKeys.delete("Adidas");
  for (const b of OTHER_BRAND_ORDER) allBrandKeys.delete(b);
  const otherBrandRotation = [...OTHER_BRAND_ORDER, ...allBrandKeys].filter(b => pools[b]);

  let otherBrandIdx = 0;
  function pickOther(preferSite) {
    const len = otherBrandRotation.length;
    for (let i = 0; i < len; i++) {
      const idx = (otherBrandIdx + i) % len;
      if (countBrand(otherBrandRotation[idx]) > 0) {
        const m = pickFromBrand(otherBrandRotation[idx], preferSite);
        otherBrandIdx = (idx + 1) % len;
        return m;
      }
    }
    return null;
  }

  function countOthers() {
    return otherBrandRotation.reduce((s, b) => s + countBrand(b), 0);
  }

  const SITES = ["FL", "DSG", "WoW"];
  let siteIdx = 0;
  const result = [];

  // Phase 1: Nike+Adidas per round until Nike <= Others total
  while (countBrand("Nike") > countOthers()) {
    const site = SITES[siteIdx++ % SITES.length];
    const nike = pickFromBrand("Nike", site);
    const adidas = pickFromBrand("Adidas", site);
    if (nike) result.push(nike);
    if (adidas) result.push(adidas);
    if (!nike && !adidas) break;
  }

  // Phase 2: Nike+Other per round until Nike <= Adidas
  while (countBrand("Nike") > countBrand("Adidas")) {
    const site = SITES[siteIdx++ % SITES.length];
    const nike = pickFromBrand("Nike", site);
    const other = pickOther(site);
    if (nike) result.push(nike);
    if (other) result.push(other);
    if (!nike && !other) break;
  }

  // Phase 3: Nike+Adidas+Other triplets until all empty
  while (countBrand("Nike") > 0 || countBrand("Adidas") > 0 || countOthers() > 0) {
    const site = SITES[siteIdx++ % SITES.length];
    const nike = pickFromBrand("Nike", site);
    const adidas = pickFromBrand("Adidas", site);
    const other = pickOther(site);
    if (nike) result.push(nike);
    if (adidas) result.push(adidas);
    if (other) result.push(other);
    if (!nike && !adidas && !other) break;
  }

  return result;
}

if (smartImportAllBtn) {
  smartImportAllBtn.addEventListener("click", async () => {
    if (!await isConnected()) { alert("Connect to Shopify first (see top of page)."); return; }

    const toImport = buildSmartImportOrder(allMonitors);
    const n = toImport.length;
    if (!n) { alert("All monitors are already linked to Shopify."); return; }

    importInProgress = true;
    stopImportRequested = false;
    smartImportAllBtn.disabled = true;
    bulkImportBtn.disabled = true;
    bulkDeleteBtn.disabled = true;
    bulkDeleteShopifyBtn.disabled = true;
    bulkCheckBtn.disabled = true;
    stopImportBtn.textContent = "Stop Import";
    stopImportBtn.disabled = false;
    setProgressLabel(smartImportAllBtn, "Importing", 0, n);
    updateBulkBtn();

    const succeeded = [];
    const failed = [];
    let processed = 0;

    try {
      await primeImportCaches();
    } catch (_) {}

    const importContext = { optionalDelayMs: 120000 };
    let importIdx = 0;
    const importWorker = async () => {
      while (importIdx < toImport.length && !stopImportRequested) {
        const monitor = toImport[importIdx++];
        try {
          const created = await importMonitorProduct(monitor, importContext);
          const importVerb = created?.relinkedExisting ? "Re-linked to existing Shopify product" : "Imported to Shopify";
          await Promise.all([
            chrome.runtime.sendMessage({
              type: "update-monitor",
              payload: {
                id: monitor.id,
                shopifyProductId: created.id,
                shopifyImportedAt: new Date().toISOString(),
                shopifyLastSyncAt: new Date().toISOString(),
                shopifySyncStatus: "ok",
                hiddenFromNew48h: true
              }
            }),
            addLog({
              type: "import",
              title: [monitor.productData?.brand, monitor.productData?.sku].filter(Boolean).join(" ") || monitor.name,
              productName: monitor.productData?.name || monitor.name || "",
              brand: monitor.productData?.brand || "",
              sku: monitor.productData?.sku || "",
              details: [importVerb, "Monitor kept for ongoing price/size sync"],
              monitorId: monitor.id, url: monitor.url
            }).catch(() => {})
          ]);
          succeeded.push(monitor.id);
        } catch (e) {
          let reason = e.message || "Unknown error";
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
          const pd = monitor.productData || {};
          await addLog({
            type: "error",
            title: [pd.brand, pd.sku].filter(Boolean).join(" ") || monitor.name,
            productName: pd.name || monitor.name || "",
            brand: pd.brand || "",
            sku: pd.sku || "",
            details: [reason],
            monitorId: monitor.id,
            url: monitor.url
          }).catch(() => {});
          failed.push({ name: monitor.name, url: monitor.url, reason });
        } finally {
          processed++;
          setProgressLabel(smartImportAllBtn, "Importing", processed, n);
        }
      }
    };

    try {
      await Promise.all(Array.from({ length: Math.min(6, n) }, importWorker));

      stopImportRequested = false;
      renderAll();
      scheduleDashboardAutoRefresh({ forceShopifyRefresh: true });
      updateBulkBtn();

      if (processed < n) {
        alert(`Stopped after ${processed}/${n} imports.\n\nImported: ${succeeded.length}\nFailed: ${failed.length}`);
      } else if (failed.length && succeeded.length) {
        alert(`✓ Imported ${succeeded.length} / ${n}.\n\nImported monitors were kept for ongoing Shopify sync.\n\n❌ Failed (${failed.length}):\n\n${failed.map(f => `• ${f.name}\n  ${f.reason}`).join("\n\n")}`);
      } else if (failed.length) {
        alert(`❌ All ${n} imports failed:\n\n${failed.map(f => `• ${f.name}\n  ${f.reason}`).join("\n\n")}`);
      } else {
        alert(`✓ Imported ${succeeded.length} product${succeeded.length !== 1 ? "s" : ""} to Shopify. Monitors kept for ongoing sync.`);
      }
    } finally {
      importInProgress = false;
      stopImportRequested = false;
      updateBulkBtn();
    }
  });
}

// Bulk import selected monitors
bulkImportBtn.addEventListener("click", async () => {
  if (!checkedIds.size) return;
  if (!await isConnected()) { alert("Connect to Shopify first (see top of page)."); return; }

  const selectedMonitors = allMonitors.filter(m => checkedIds.has(m.id));
  const skippedAlreadyImported = selectedMonitors.filter(m => m.shopifyProductId).length;
  const toImport = selectedMonitors.filter(m => !m.shopifyProductId);
  const n = toImport.length;
  if (!n) {
    alert("All selected monitors are already linked to Shopify.");
    return;
  }
  importInProgress = true;
  stopImportRequested = false;
  bulkImportBtn.disabled = true;
  bulkDeleteBtn.disabled = true;
  bulkDeleteShopifyBtn.disabled = true;
  bulkCheckBtn.disabled = true;
  stopImportBtn.textContent = "Stop Import";
  stopImportBtn.disabled = false;
  setProgressLabel(bulkImportBtn, "Importing", 0, n);
  updateBulkBtn();

  const succeeded = [];
  const failed = [];
  let processed = 0;

  try {
    await primeImportCaches();
  } catch (_) {}

  const importContext = { optionalDelayMs: 120000 };
  let importIdx = 0;
  const importWorker = async () => {
    while (importIdx < toImport.length && !stopImportRequested) {
      const monitor = toImport[importIdx++];
      try {
        const created = await importMonitorProduct(monitor, importContext);
        const importVerb = created?.relinkedExisting ? "Re-linked to existing Shopify product" : "Imported to Shopify";
        await Promise.all([
          chrome.runtime.sendMessage({
            type: "update-monitor",
            payload: {
              id: monitor.id,
              shopifyProductId: created.id,
              shopifyImportedAt: new Date().toISOString(),
              shopifyLastSyncAt: new Date().toISOString(),
              shopifySyncStatus: "ok",
              hiddenFromNew48h: true
            }
          }),
          addLog({
            type: "import",
            title: [monitor.productData?.brand, monitor.productData?.sku].filter(Boolean).join(" ") || monitor.name,
            productName: monitor.productData?.name || monitor.name || "",
            brand: monitor.productData?.brand || "",
            sku: monitor.productData?.sku || "",
            details: [importVerb, "Monitor kept for ongoing price/size sync"],
            monitorId: monitor.id, url: monitor.url
          }).catch(() => {})
        ]);
        succeeded.push(monitor.id);
      } catch (e) {
        let reason = e.message || "Unknown error";
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
        const pd = monitor.productData || {};
        await addLog({
          type: "error",
          title: [pd.brand, pd.sku].filter(Boolean).join(" ") || monitor.name,
          productName: pd.name || monitor.name || "",
          brand: pd.brand || "",
          sku: pd.sku || "",
          details: [reason],
          monitorId: monitor.id,
          url: monitor.url
        }).catch(() => {});
        failed.push({ name: monitor.name, url: monitor.url, reason });
      } finally {
        processed++;
        setProgressLabel(bulkImportBtn, "Importing", processed, n);
      }
    }
  };
  try {
    await Promise.all(Array.from({ length: Math.min(6, n) }, importWorker));

    // Only delete successfully imported monitors — failed ones stay
    succeeded.forEach(id => checkedIds.delete(id));

    stopImportRequested = false;
    renderAll();
    scheduleDashboardAutoRefresh({ forceShopifyRefresh: true });
    updateBulkBtn();

    const skippedText = skippedAlreadyImported ? `\nAlready linked skipped: ${skippedAlreadyImported}` : "";
    if (processed < n) {
      alert(`Stopped after ${processed}/${n} imports.\n\nImported: ${succeeded.length}\nFailed: ${failed.length}${skippedText}`);
    } else if (failed.length && succeeded.length) {
      alert(`✓ Imported ${succeeded.length} / ${n}.\n\nImported monitors were kept for ongoing Shopify sync.\n\n❌ Failed (${failed.length}):\n\n${failed.map(f => `• ${f.name}\n  ${f.reason}`).join("\n\n")}`);
    } else if (failed.length) {
      alert(`❌ All ${n} imports failed:\n\n${failed.map(f => `• ${f.name}\n  ${f.reason}`).join("\n\n")}`);
    } else {
      alert(`✓ Imported ${succeeded.length} product${succeeded.length !== 1 ? "s" : ""} to Shopify as active products. Monitors were kept for ongoing sync.`);
    }
  } finally {
    importInProgress = false;
    stopImportRequested = false;
    updateBulkBtn();
  }
});

bulkUpdateShopifyBtn.addEventListener("click", async () => {
  const selectedTargets = allMonitors.filter((monitor) =>
    checkedIds.has(monitor.id) && (monitor.shopifyProductId || monitor.productData?.sku)
  );
  const fallbackTargets = allMonitors.filter((monitor) => monitor.shopifyProductId || monitor.productData?.sku);
  const targets = checkedIds.size ? selectedTargets : fallbackTargets;
  const n = targets.length;
  if (!n) {
    alert(checkedIds.size ? "None of the selected monitors have a Shopify ID or SKU." : "No dashboard monitors have a Shopify ID or SKU.");
    return;
  }
  if (!await isConnected()) { alert("Connect to Shopify first (see top of page)."); return; }

  const scopeLabel = selectedTargets.length ? `${n} selected monitor${n !== 1 ? "s" : ""}` : `all ${n} dashboard monitor${n !== 1 ? "s" : ""} with Shopify IDs or SKUs`;
  if (!window.confirm(`Update Shopify for ${scopeLabel}?`)) return;

  importInProgress = true;
  stopImportRequested = false;
  bulkUpdateShopifyBtn.disabled = true;
  bulkImportBtn.disabled = true;
  bulkDeleteBtn.disabled = true;
  bulkDeleteShopifyBtn.disabled = true;
  bulkCheckBtn.disabled = true;
  stopImportBtn.textContent = "Stop";
  stopImportBtn.disabled = false;
  setProgressLabel(bulkUpdateShopifyBtn, "Updating", 0, n);
  updateBulkBtn();
  showShopifySyncStatus(`Updating Shopify for ${n} monitor${n !== 1 ? "s" : ""}...`, "working", { autoHide: false });

  const succeeded = [];
  const failed = [];
  let processed = 0;

  try {
    await primeImportCaches();
  } catch (_) {}

  for (let i = 0; i < targets.length; i++) {
    if (stopImportRequested) break;
    const monitor = targets[i];
    try {
      const result = await reapplyMonitorDataToShopify(monitor, monitor.lastExtractedData || {});
      const syncVerb = result?.created ? "Created Shopify product from monitor data" : "Updated Shopify product from monitor data";
      await Promise.all([
        chrome.runtime.sendMessage({
          type: "update-monitor",
          payload: {
            id: monitor.id,
            shopifyProductId: result?.id || monitor.shopifyProductId || null,
            shopifyLastSyncAt: new Date().toISOString(),
            shopifySyncStatus: "ok"
          }
        }),
        addLog({
          type: "shopify-sync",
          title: [monitor.productData?.brand, monitor.productData?.sku].filter(Boolean).join(" ") || monitor.name,
          productName: monitor.productData?.name || monitor.name || "",
          brand: monitor.productData?.brand || "",
          sku: monitor.productData?.sku || "",
          details: [syncVerb],
          monitorId: monitor.id,
          url: monitor.url
        }).catch(() => {})
      ]);
      succeeded.push(monitor.id);
    } catch (error) {
      const pd = monitor.productData || {};
      const reason = error?.message || String(error) || "Unknown error";
      await addLog({
        type: "error",
        title: [pd.brand, pd.sku].filter(Boolean).join(" ") || monitor.name,
        productName: pd.name || monitor.name || "",
        brand: pd.brand || "",
        sku: pd.sku || "",
        details: [reason],
        monitorId: monitor.id,
        url: monitor.url
      }).catch(() => {});
      failed.push({ name: monitor.productData?.sku || monitor.name || monitor.id, reason });
    } finally {
      processed++;
      setProgressLabel(bulkUpdateShopifyBtn, "Updating", processed, n);
    }
  }

  try {
    stopImportRequested = false;
    await silentRefresh(true);
    updateBulkBtn();

    if (processed < n) {
      showShopifySyncStatus(`Stopped after ${processed}/${n}. Updated ${succeeded.length}; failed ${failed.length}.`, failed.length ? "error" : "success", { autoHide: !failed.length });
      alert(`Stopped after ${processed}/${n}.\n\nUpdated: ${succeeded.length}\nFailed: ${failed.length}`);
    } else if (failed.length) {
      showShopifySyncStatus(`Updated ${succeeded.length}/${n}; ${failed.length} failed.`, "error", { autoHide: false });
      alert(`Updated ${succeeded.length}/${n}.\n\nFailed (${failed.length}):\n\n${failed.map((f) => `- ${f.name}\n  ${f.reason}`).join("\n\n")}`);
    } else {
      showShopifySyncStatus(`Updated Shopify for ${succeeded.length} monitor${succeeded.length !== 1 ? "s" : ""}.`, "success");
    }
  } finally {
    importInProgress = false;
    stopImportRequested = false;
    updateBulkBtn();
  }
});

bulkDeleteUnconvertedVariantsBtn.addEventListener("click", async () => {
  const selectedTargets = allMonitors.filter((monitor) =>
    checkedIds.has(monitor.id) && (monitor.shopifyProductId || monitor.productData?.sku)
  );
  const fallbackTargets = allMonitors.filter((monitor) => monitor.shopifyProductId || monitor.productData?.sku);
  const targets = checkedIds.size ? selectedTargets : fallbackTargets;
  const n = targets.length;
  if (!n) {
    alert(checkedIds.size ? "None of the selected monitors have a Shopify ID or SKU." : "No dashboard monitors have a Shopify ID or SKU.");
    return;
  }
  if (!await isConnected()) { alert("Connect to Shopify first (see top of page)."); return; }
  const scopeLabel = selectedTargets.length ? `${n} selected monitor${n !== 1 ? "s" : ""}` : `all ${n} dashboard monitor${n !== 1 ? "s" : ""} with Shopify IDs or SKUs`;
  if (!window.confirm(`Delete Shopify size variants that do not have a size conversion for ${scopeLabel}? This deletes only variant/size rows, not products.`)) return;

  importInProgress = true;
  stopImportRequested = false;
  bulkDeleteUnconvertedVariantsBtn.disabled = true;
  bulkImportBtn.disabled = true;
  bulkDeleteBtn.disabled = true;
  bulkDeleteShopifyBtn.disabled = true;
  bulkCheckBtn.disabled = true;
  stopImportBtn.textContent = "Stop";
  stopImportBtn.disabled = false;
  setProgressLabel(bulkDeleteUnconvertedVariantsBtn, "Deleting sizes", 0, n);
  updateBulkBtn();

  const succeeded = [];
  const failed = [];
  let deleted = 0;
  let skipped = 0;
  const deletedSizes = [];
  let processed = 0;
  const context = {};

  try {
    setProgressLabel(bulkDeleteUnconvertedVariantsBtn, "Loading Shopify", 0, n);
    const snapshot = await getShopifyProductsSnapshot();
    const productIds = new Set();
    const idBySku = new Map();
    for (const product of snapshot) {
      const productId = Number(product.id || 0);
      if (!productId) continue;
      productIds.add(productId);
      const productSkus = [
        product.sku,
        ...(Array.isArray(product.rawVariantSkus) ? product.rawVariantSkus : []),
        ...(Array.isArray(product.variantSkus) ? product.variantSkus : [])
      ];
      for (const sku of productSkus) {
        const cleanSku = normalizeSku(sku);
        if (cleanSku && !idBySku.has(cleanSku)) idBySku.set(cleanSku, productId);
        const dash = cleanSku.lastIndexOf("-");
        const prefix = dash > 0 ? cleanSku.slice(0, dash) : "";
        if (prefix && !idBySku.has(prefix)) idBySku.set(prefix, productId);
      }
    }
    for (const monitor of targets) {
      const linkedId = Number(monitor.shopifyProductId || 0);
      if (linkedId) productIds.add(linkedId);
      const baseSku = normalizeSku(monitor.productData?.sku);
      const mappedId = baseSku ? idBySku.get(baseSku) : 0;
      if (mappedId) productIds.add(mappedId);
    }
    const products = await getProductsByIds([...productIds], (current, total) => {
      setProgressLabel(bulkDeleteUnconvertedVariantsBtn, "Loading Shopify", current, total);
    });
    context.productsById = new Map(products.map((product) => [Number(product.id), product]));
    context.productsByBaseSku = new Map();
    for (const product of products) {
      const skus = [
        product.sku,
        ...(Array.isArray(product.variants) ? product.variants.map((variant) => variant?.sku) : [])
      ].map(normalizeSku).filter(Boolean);
      for (const sku of skus) {
        if (!context.productsByBaseSku.has(sku)) context.productsByBaseSku.set(sku, product);
        const dash = sku.lastIndexOf("-");
        const prefix = dash > 0 ? sku.slice(0, dash) : "";
        if (prefix && !context.productsByBaseSku.has(prefix)) context.productsByBaseSku.set(prefix, product);
      }
    }
  } catch (error) {
    importInProgress = false;
    stopImportRequested = false;
    updateBulkBtn();
    alert(`Could not load Shopify products for bad-size cleanup: ${error?.message || error}`);
    return;
  }

  for (let i = 0; i < targets.length; i++) {
    if (stopImportRequested) break;
    const monitor = targets[i];
    try {
      const result = await deleteUnconvertedShopifyVariantsForMonitor(monitor, context);
      if (result.skipped) skipped++;
      if (result.deleted) {
        deleted += result.deleted;
        deletedSizes.push(...(result.sizes || []));
      }
      succeeded.push(monitor.id);
    } catch (error) {
      failed.push({ name: monitor.productData?.sku || monitor.name || monitor.id, reason: error?.message || String(error) });
    } finally {
      processed++;
      setProgressLabel(bulkDeleteUnconvertedVariantsBtn, "Deleting sizes", processed, n);
    }
  }

  try {
    stopImportRequested = false;
    await refreshShopifyDashboardState(true);
    updateBulkBtn();

    const sizeText = [...new Set(deletedSizes)].slice(0, 20).join(", ");
    const skippedText = skipped ? `\nSkipped with no saved size data: ${skipped}` : "";
    if (processed < n) {
      alert(`Stopped after ${processed}/${n} monitors.\n\nDeleted size variants: ${deleted}${sizeText ? `\nSizes: ${sizeText}` : ""}${skippedText}\nFailed: ${failed.length}`);
    } else if (failed.length) {
      alert(`Deleted ${deleted} size variant${deleted !== 1 ? "s" : ""}.${skippedText}\n\nFailed (${failed.length}):\n\n${failed.map((f) => `- ${f.name}\n  ${f.reason}`).join("\n\n")}`);
    } else {
      alert(`Deleted ${deleted} unconverted size variant${deleted !== 1 ? "s" : ""}.${sizeText ? `\nSizes: ${sizeText}` : ""}${skippedText}`);
    }
  } finally {
    importInProgress = false;
    stopImportRequested = false;
    updateBulkBtn();
  }
});

stopImportBtn.addEventListener("click", () => {
  stopImportRequested = true;
  stopImportBtn.disabled = true;
  stopImportBtn.textContent = "Stopping…";
});

bulkDeleteBtn.addEventListener("click", async () => {
  const n = checkedIds.size;
  if (!n) return;
  if (!window.confirm(`Delete ${n} monitor${n > 1 ? "s" : ""}? This cannot be undone.`)) return;

  const ids = Array.from(checkedIds);
  bulkDeleteBtn.disabled = true;
  bulkDeleteBtn.textContent = `Deleting 0/${n}...`;
  for (let i = 0; i < ids.length; i++) {
    setProgressLabel(bulkDeleteBtn, "Deleting", i + 1, n);
    await chrome.runtime.sendMessage({ type: "delete-monitor", monitorId: ids[i] });
    if (ids[i] === selectedMonitorId) {
      selectedMonitorId = null;
      _lastDetailSig = "";
      monitorDetail.style.display = "none";
    }
    removeMonitorFromDom(ids[i]);
  }

  refreshUndoCount();
  scheduleDashboardAutoRefresh({ forceShopifyRefresh: false });
  updateBulkBtn();
});

bulkDeleteShopifyBtn.addEventListener("click", async () => {
  const n = checkedIds.size;
  if (!n) return;
  if (!window.confirm(`Delete ${n} selected item${n > 1 ? "s" : ""} from Shopify and from monitors? This cannot be undone.`)) return;

  const targets = allMonitors.filter((monitor) => checkedIds.has(monitor.id));
  const failures = [];
  bulkDeleteShopifyBtn.textContent = `Deleting 0/${n}...`;
  bulkDeleteShopifyBtn.disabled = true;
  bulkDeleteBtn.disabled = true;

  for (let i = 0; i < targets.length; i++) {
    const monitor = targets[i];
    setProgressLabel(bulkDeleteShopifyBtn, "Deleting", i + 1, n);
    try {
      if (monitor.shopifyProductId) {
        await deleteProduct(monitor.shopifyProductId);
      }
      await chrome.runtime.sendMessage({ type: "delete-monitor", monitorId: monitor.id });
      if (monitor.id === selectedMonitorId) {
        selectedMonitorId = null;
        _lastDetailSig = "";
        monitorDetail.style.display = "none";
      }
      removeMonitorFromDom(monitor.id);
    } catch (error) {
      failures.push({
        name: monitor.productData?.sku || monitor.name || monitor.id,
        reason: error?.message || String(error) || "Unknown error"
      });
    }
  }

  refreshUndoCount();
  scheduleDashboardAutoRefresh({ forceShopifyRefresh: true });
  updateBulkBtn();
  if (failures.length) {
    alert(`Deleted ${targets.length - failures.length}/${targets.length} selected item${targets.length - failures.length !== 1 ? "s" : ""}.\n\nThese stayed because Shopify delete failed:\n\n${failures.map((item) => `• ${item.name}\n  ${item.reason}`).join("\n\n")}`);
  }
});

bulkDeleteShopifyOnlyBtn.addEventListener("click", async () => {
  const targets = allMonitors.filter((m) => checkedIds.has(m.id) && m.shopifyProductId);
  if (!targets.length) return;
  const n = targets.length;
  if (!window.confirm(`Delete ${n} Shopify product${n > 1 ? "s" : ""} from Shopify? The monitors will stay. This cannot be undone.`)) return;

  const failures = [];
  const succeededIds = [];
  bulkDeleteShopifyOnlyBtn.disabled = true;
  const originalText = bulkDeleteShopifyOnlyBtn.textContent;

  for (let i = 0; i < targets.length; i++) {
    const monitor = targets[i];
    bulkDeleteShopifyOnlyBtn.textContent = `Deleting ${i + 1}/${n}...`;
    try {
      await deleteProduct(monitor.shopifyProductId);
      succeededIds.push(monitor.id);
    } catch (error) {
      failures.push({
        name: monitor.productData?.sku || monitor.name || monitor.id,
        reason: error?.message || String(error) || "Unknown error"
      });
    }
  }

  if (succeededIds.length) {
    await chrome.runtime.sendMessage({
      type: "bulk-update-monitors",
      payload: {
        ids: succeededIds,
        monitorPatch: { shopifyProductId: null, shopifyProductHandle: null, shopifyStatus: null }
      }
    }).catch(() => {});
  }

  bulkDeleteShopifyOnlyBtn.textContent = originalText;
  scheduleDashboardAutoRefresh({ forceShopifyRefresh: true });
  updateBulkBtn();
  if (failures.length) {
    alert(`Deleted ${n - failures.length}/${n} Shopify product${n - failures.length !== 1 ? "s" : ""}.\n\nFailed:\n\n${failures.map((f) => `• ${f.name}\n  ${f.reason}`).join("\n\n")}`);
  }
  await silentRefresh();
});

document.getElementById("apply-interval-selected").addEventListener("click", async () => {
  if (!checkedIds.size) return;
  const val = Number(document.getElementById("bulk-interval-value").value) || 1;
  const unit = Number(document.getElementById("bulk-interval-unit").value) || 1440;
  const minutes = Math.max(1, val * unit);
  const btn = document.getElementById("apply-interval-selected");
  const targets = allMonitors.filter((m) => checkedIds.has(m.id));
  await runButtonProgress(btn, targets, "Applying", async (monitor) => {
    await chrome.runtime.sendMessage({ type: "update-monitor", payload: { ...monitor, intervalMinutes: minutes } });
  });
  await silentRefresh();
});

document.getElementById("apply-interval-all").addEventListener("click", async () => {
  const val = Number(document.getElementById("global-interval-value").value) || 1;
  const unit = Number(document.getElementById("global-interval-unit").value) || 1440;
  const minutes = Math.max(1, val * unit);
  if (!allMonitors.length) return;
  const btn = document.getElementById("apply-interval-all");
  await runButtonProgress(btn, allMonitors, "Applying", async (monitor) => {
    await chrome.runtime.sendMessage({ type: "update-monitor", payload: { ...monitor, intervalMinutes: minutes } });
  });
  await silentRefresh();
});

function normalizeBulkEditorValue(field, value) {
  const clean = String(value || "").replace(/\s+/g, " ").trim();
  if (!clean) return "";
  if (field === "genderDisplay") {
    if (/men.*women|women.*men|both|unisex/i.test(clean)) return "Men, Women";
    if (/women|woman|female|girls?/i.test(clean)) return "Women";
    if (/men|man|male|boys?/i.test(clean)) return "Men";
    return "";
  }
  if (field === "gender") {
    if (/women|woman|female|girls?/i.test(clean)) return "Women";
    if (/men|man|male|boys?/i.test(clean)) return "Men";
    return "";
  }
  if (field === "color" || field === "colorFinal") return simplifyMonitorColor(clean);
  return clean;
}

function getBulkEditorPatch() {
  const productPatch = {};
  const monitorPatch = {};
  const enabled = [...bulkEditorPanel.querySelectorAll("[data-bulk-toggle]:checked")];
  for (const toggle of enabled) {
    const field = toggle.dataset.bulkToggle;
    const input = bulkEditorPanel.querySelector(`[data-bulk-field="${CSS.escape(field)}"]`);
    const rawValue = input?.value ?? "";
    if (!String(rawValue).trim()) continue;
    if (field === "priceAdjustment") {
      const amount = Number(rawValue);
      if (Number.isFinite(amount)) monitorPatch.priceAdjustment = amount;
      continue;
    }
    if (field === "gender") {
      const gender = normalizeBulkEditorValue(field, rawValue);
      if (!gender) continue;
      productPatch.gender = gender;
      productPatch.extractedGender = gender;
      continue;
    }
    productPatch[field] = normalizeBulkEditorValue(field, rawValue);
  }
  if (productPatch.color && !("colorFinal" in productPatch)) {
    productPatch.colorFinal = productPatch.color;
  }
  return { productPatch, monitorPatch };
}

async function applyBulkEditorChanges() {
  const targets = allMonitors.filter((monitor) => checkedIds.has(monitor.id));
  if (!targets.length) {
    bulkEditorStatus.textContent = "Select monitors first.";
    return;
  }
  const { productPatch, monitorPatch } = getBulkEditorPatch();
  const changedFields = [...Object.keys(productPatch), ...Object.keys(monitorPatch)];
  if (!changedFields.length) {
    bulkEditorStatus.textContent = "Check at least one field and enter a value.";
    return;
  }
  const preview = changedFields.map((field) => `${field}: ${productPatch[field] ?? monitorPatch[field]}`).join(", ");
  bulkEditorStatus.textContent = `Applying ${preview} to ${targets.length} selected monitor${targets.length !== 1 ? "s" : ""}...`;
  bulkEditorBusy = true;
  bulkEditorApplyBtn.textContent = "Applying...";
  updateBulkBtn();
  try {
    const response = await chrome.runtime.sendMessage({
      type: "bulk-update-monitors",
      payload: {
        ids: targets.map((monitor) => monitor.id),
        productPatch,
        monitorPatch
      }
    });
    if (!response?.ok) throw new Error(response?.error || "Bulk update failed");
    await silentRefresh(true);
    bulkEditorStatus.textContent = `Updated ${response.count || targets.length} selected monitor${(response.count || targets.length) !== 1 ? "s" : ""}.`;
  } catch (error) {
    bulkEditorStatus.textContent = `Bulk edit failed: ${error?.message || error}`;
    alert(`Bulk edit failed: ${error?.message || error}`);
  } finally {
    bulkEditorBusy = false;
    bulkEditorApplyBtn.textContent = "Apply to selected";
    updateBulkBtn();
  }
}

openBulkEditorBtn?.addEventListener("click", () => {
  if (!checkedIds.size) return;
  bulkEditorPanel.style.display = bulkEditorPanel.style.display === "none" ? "" : "none";
  updateBulkBtn();
  if (bulkEditorPanel.style.display !== "none") {
    bulkEditorPanel.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

bulkEditorCloseBtn?.addEventListener("click", () => {
  bulkEditorPanel.style.display = "none";
});

bulkEditorApplyBtn?.addEventListener("click", applyBulkEditorChanges);

bulkEditorPanel?.addEventListener("input", (event) => {
  const field = event.target?.dataset?.bulkField;
  if (!field) return;
  const toggle = bulkEditorPanel.querySelector(`[data-bulk-toggle="${CSS.escape(field)}"]`);
  if (toggle && String(event.target.value || "").trim()) toggle.checked = true;
  const { productPatch, monitorPatch } = getBulkEditorPatch();
  const changedFields = [...Object.keys(productPatch), ...Object.keys(monitorPatch)];
  bulkEditorStatus.textContent = changedFields.length
    ? `Ready: ${changedFields.map((item) => `${item}: ${productPatch[item] ?? monitorPatch[item]}`).join(", ")}`
    : `${checkedIds.size} selected monitor${checkedIds.size !== 1 ? "s" : ""}. Check each field you want to overwrite.`;
});

bulkEditorPanel?.addEventListener("change", (event) => {
  const field = event.target?.dataset?.bulkField;
  if (field) {
    const toggle = bulkEditorPanel.querySelector(`[data-bulk-toggle="${CSS.escape(field)}"]`);
    if (toggle && String(event.target.value || "").trim()) toggle.checked = true;
  } else if (!event.target?.dataset?.bulkToggle) {
    return;
  }
  const { productPatch, monitorPatch } = getBulkEditorPatch();
  const changedFields = [...Object.keys(productPatch), ...Object.keys(monitorPatch)];
  bulkEditorStatus.textContent = changedFields.length
    ? `Ready: ${changedFields.map((item) => `${item}: ${productPatch[item] ?? monitorPatch[item]}`).join(", ")}`
    : `${checkedIds.size} selected monitor${checkedIds.size !== 1 ? "s" : ""}. Check each field you want to overwrite.`;
});

selectAllBtn.addEventListener("click", () => {
  clearSelectedCheckBatches();
  getFilteredMonitors().forEach((m) => checkedIds.add(m.id));
  updateBulkBtn();
  syncCheckedStates();
});

deselectBtn.addEventListener("click", () => {
  clearSelectedCheckBatches();
  checkedIds.clear();
  updateBulkBtn();
  syncCheckedStates();
});

checkAllBtn.addEventListener("click", () => {
  clearSelectedCheckBatches();
  runBulkCheck(allMonitors.map((m) => m.id));
});

checkSavedBtn?.addEventListener("click", () => {
  clearSelectedCheckBatches();
  const ids = allMonitors.filter(isSavedPendingMonitor).map((monitor) => monitor.id);
  runBulkCheck(ids);
});

checkBatchGrid?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-check-batch-index]");
  if (!button || button.disabled || importInProgress || activeBatchRun) return;
  toggleCheckBatch(Number(button.dataset.checkBatchIndex || 0) || 0);
});

selectAllBatchesBtn?.addEventListener("click", () => {
  if (importInProgress || activeBatchRun) return;
  const batches = getCheckBatches();
  selectedCheckBatchIndexes.clear();
  batches.forEach((batch) => selectedCheckBatchIndexes.add(batch.index));
  syncSelectedCheckBatchesToCards(`Selected all ${batches.length} batch${batches.length !== 1 ? "es" : ""}.`);
});

clearBatchesBtn?.addEventListener("click", () => {
  if (importInProgress || activeBatchRun) return;
  clearSelectedCheckBatches();
  checkedIds.clear();
  updateBulkBtn();
  syncCheckedStates();
  renderBatchCheckStatus("Cleared selected batches.");
});

batchCheckStatus?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-select-batch-errors]");
  if (!button) return;
  reconcileBatchCheckResultsWithCurrentState();
  const index = Number(button.dataset.selectBatchErrors);
  const result = batchCheckResults.get(index);
  if (!result?.errors?.length) return;
  clearSelectedCheckBatches();
  checkedIds.clear();
  result.errors.forEach((id) => checkedIds.add(id));
  updateBulkBtn();
  syncCheckedStates();
  renderBatchCheckStatus(`Selected ${result.errors.length} error${result.errors.length !== 1 ? "s" : ""} from Batch ${index * CHECK_BATCH_SIZE + 1}-${index * CHECK_BATCH_SIZE + result.ids.length}.`);
});

openErrorSectionBtn?.addEventListener("click", async () => {
  const response = await chrome.runtime.sendMessage({ type: "get-monitors" }).catch(() => null);
  if (response?.ok) {
    applyMonitorsUpdate(response.monitors || []);
  }
  collapsedGroups.delete("__errors__");
  groupPages.set("__errors__", 0);
  renderGrid(getFilteredMonitors());
  const group = monitorGrid.querySelector('.site-group[data-group-key="__errors__"]');
  if (group) {
    group.scrollIntoView({ behavior: "smooth", block: "start" });
  } else {
    usSizeAuditPanel.style.display = "";
    shopifyUnmonitoredPanel.style.display = "none";
    monitorNotShopifyPanel.style.display = "none";
    skuDuplicatesPanel.style.display = "none";
    await loadBadSizeAudit(true);
    usSizeAuditPanel.scrollIntoView({ behavior: "smooth", block: "start" });
    renderBatchCheckStatus("No monitor errors found. Opened Bad size audit for Store Data errors.");
  }
});

document.getElementById("select-range").addEventListener("click", () => {
  clearSelectedCheckBatches();
  const from = parseInt(document.getElementById("range-from").value);
  const to = parseInt(document.getElementById("range-to").value);
  if (isNaN(from) || isNaN(to) || from < 1 || to < from) return;
  getMonitorsFirstCreated().forEach((m, i) => {
    const num = i + 1;
    if (num >= from && num <= to) checkedIds.add(m.id);
  });
  updateBulkBtn();
  syncCheckedStates();
});

function syncBrandSelectDisplay() {
  filterBrand.querySelector("option[data-multi]")?.remove();
  if (selectedBrands.size === 0) {
    filterBrand.value = "";
  } else if (selectedBrands.size === 1) {
    filterBrand.value = [...selectedBrands][0];
  } else {
    const arr = [...selectedBrands];
    const label = arr.length === 2 ? arr.join(", ") : `${arr[0]} +${arr.length - 1}`;
    const opt = new Option(label, "_multi_brand_");
    opt.dataset.multi = "1";
    filterBrand.insertBefore(opt, filterBrand.options[0]);
    filterBrand.value = "_multi_brand_";
  }
  filterBrand.classList.toggle("active", selectedBrands.size > 0);
}

function syncTypeSelectDisplay() {
  filterType.querySelector("option[data-multi]")?.remove();
  if (selectedTypes.size === 0) {
    filterType.value = "";
  } else if (selectedTypes.size === 1) {
    filterType.value = [...selectedTypes][0];
  } else {
    const arr = [...selectedTypes];
    const label = arr.length === 2 ? arr.join(", ") : `${arr[0]} +${arr.length - 1}`;
    const opt = new Option(label, "_multi_type_");
    opt.dataset.multi = "1";
    filterType.insertBefore(opt, filterType.options[0]);
    filterType.value = "_multi_type_";
  }
  filterType.classList.toggle("active", selectedTypes.size > 0);
}

function applyFilters() {
  groupPages.clear(); // reset to page 1 on top-level filter change
  const filtered = getFilteredMonitors();
  renderStats(filtered);
  renderGrid(filtered);
  syncBrandSelectDisplay();
  syncTypeSelectDisplay();
}

function clearAllDashboardFilters() {
  searchInput.value = "";
  changedOnlyInput.checked = false;
  selectedBrands.clear();
  selectedTypes.clear();
  filterSort.value = "created-desc";
  activeStoreMetaFilters = [];
  clearMetaFilterBtn.style.display = "none";
  if (clearMonitorMetaFilterBtn) clearMonitorMetaFilterBtn.style.display = "none";
  groupFilters.clear();
  groupPages.clear();
  scheduleSaveDashboardUiState();
  renderStats(allMonitors);
  populateFilterOptions();
  renderGrid(allMonitors);
  updateMetaChipActiveStates();
}

function resetDashboardFiltersWithoutRendering() {
  searchInput.value = "";
  changedOnlyInput.checked = false;
  selectedBrands.clear();
  selectedTypes.clear();
  filterSort.value = "created-desc";
  activeStoreMetaFilters = [];
  if (clearMetaFilterBtn) clearMetaFilterBtn.style.display = "none";
  if (clearMonitorMetaFilterBtn) clearMonitorMetaFilterBtn.style.display = "none";
  groupFilters.clear();
  groupPages.clear();
  syncBrandSelectDisplay();
  syncTypeSelectDisplay();
}

window.addEventListener("keydown", e => { if (e.ctrlKey || e.metaKey || e.shiftKey) _multiFilterKeyHeld = true; });
window.addEventListener("keyup", e => { if (!e.ctrlKey && !e.metaKey && !e.shiftKey) _multiFilterKeyHeld = false; });
window.addEventListener("blur", () => { _multiFilterKeyHeld = false; });

searchInput.addEventListener("input", () => { applyFilters(); scheduleSaveDashboardUiState(); });
changedOnlyInput.addEventListener("change", () => { applyFilters(); scheduleSaveDashboardUiState(); });
filterBrand.addEventListener("change", () => {
  const val = filterBrand.value;
  if (!val || val === "_multi_brand_") {
    if (!_multiFilterKeyHeld) { selectedBrands.clear(); applyFilters(); scheduleSaveDashboardUiState(); }
    return;
  }
  if (_multiFilterKeyHeld) {
    if (selectedBrands.has(val)) selectedBrands.delete(val);
    else selectedBrands.add(val);
  } else {
    selectedBrands.clear();
    selectedBrands.add(val);
  }
  applyFilters();
  scheduleSaveDashboardUiState();
});
filterType.addEventListener("change", () => {
  const val = filterType.value;
  if (!val || val === "_multi_type_") {
    if (!_multiFilterKeyHeld) { selectedTypes.clear(); applyFilters(); scheduleSaveDashboardUiState(); }
    return;
  }
  if (_multiFilterKeyHeld) {
    if (selectedTypes.has(val)) selectedTypes.delete(val);
    else selectedTypes.add(val);
  } else {
    selectedTypes.clear();
    selectedTypes.add(val);
  }
  applyFilters();
  scheduleSaveDashboardUiState();
});
filterSort.addEventListener("change", () => { applyFilters(); scheduleSaveDashboardUiState(); });
clearAllFiltersBtn.addEventListener("click", clearAllDashboardFilters);
document.getElementById("refresh-dashboard").addEventListener("click", async () => {
  clearShopifyProductsSnapshotCache();
  await loadDashboard(true);
  refreshUndoCount().catch(() => {});
  refreshLogCount().catch(() => {});
});


// Queues a monitor update and flushes all queued updates in one RAF pass.
// Coalesces 6 concurrent completions into a single DOM update instead of 6 separate ones.
function patchMonitorInPlace(updated) {
  if (!updated?.id) return;
  _invalidateMetaSig();
  _invalidateFilterOpts();
  const idx = _monitorIndex.get(updated.id);
  if (idx === undefined) {
    // New monitor not yet in index — debounce a refresh so many new monitors
    // arriving together collapse into one full reload, not one per monitor.
    clearTimeout(dashboardStorageRefreshTimer);
    dashboardStorageRefreshTimer = setTimeout(() => silentRefresh().catch(() => {}), 200);
    return;
  }
  _pendingPatches.set(updated.id, updated);
  if (_patchRafId) return; // already scheduled — this update joins the batch
  _patchRafId = requestAnimationFrame(_flushPatches);
}

function _flushPatches() {
  _patchRafId = null;
  if (!_pendingPatches.size) return;
  const patches = [..._pendingPatches.values()];
  _pendingPatches.clear();

  let detailUpdated = null;
  const tpl = document.createElement("template");

  for (const updated of patches) {
    const idx = _monitorIndex.get(updated.id);
    if (idx === undefined) continue;
    const prev = allMonitors[idx];
    if (prev.lastChangedAt !== updated.lastChangedAt) {
      invalidateDiffCache(updated.id);
    }
    _updateStatCaches(prev, updated);
    allMonitors[idx] = updated;
    _monitorIndex.set(updated.id, idx);

    // Swap the card in the DOM only if its visible sig changed.
    const newSig = getMonitorCardSig(updated);
    if (_renderedMonitorSigs.get(updated.id) !== newSig) {
      _gridDataVersion++;
      _dirtyMonitorIds.add(updated.id);
      const cardEl = _cardIndex.get(updated.id); // O(1) — no DOM scan
      if (cardEl) {
        tpl.innerHTML = renderSquare(updated).trim();
        const newEl = tpl.content.firstElementChild;
        if (newEl) {
          cardEl.replaceWith(newEl);
          _cardIndex.set(updated.id, newEl);
          _renderedMonitorSigs.set(updated.id, newSig);
          initThumbLazyLoading(newEl);
        }
      }
    }

    if (updated.id === selectedMonitorId) detailUpdated = updated;
  }

  monitorImageCandidateCache.clear();
  cachedMonitorMetaRenderData = null;

  // One stats render for the whole batch, not one per monitor.
  renderStats(allMonitors);
  updateBulkBtn();

  if (detailUpdated) {
    const sig = [detailUpdated.lastCheckedAt, detailUpdated.lastChangedAt, detailUpdated.status, detailUpdated.lastError, detailUpdated.shopifyProductId, detailUpdated.shopifySyncStatus, detailUpdated.changeCount].join("|");
    if (sig !== _lastDetailSig) {
      _lastDetailSig = sig;
      renderDetail(detailUpdated);
    }
  }
}

chrome.storage.onChanged.addListener((changes, areaName) => {
  if (areaName !== "local") return;
  let togglesChanged = false;
  if (changes[SHOES_TYPE_METAFIELD_ENABLED_KEY]) {
    const names = Array.isArray(changes[SHOES_TYPE_METAFIELD_ENABLED_KEY].newValue)
      ? changes[SHOES_TYPE_METAFIELD_ENABLED_KEY].newValue : [];
    shoesTypeMetafieldEnabledNames = new Set(names.map(getShoesTypeToggleKey).filter(Boolean));
    togglesChanged = true;
  }
  if (changes[SHOES_TYPE_METAFIELD_DISABLED_KEY]) {
    const names = Array.isArray(changes[SHOES_TYPE_METAFIELD_DISABLED_KEY].newValue)
      ? changes[SHOES_TYPE_METAFIELD_DISABLED_KEY].newValue : [];
    shoesTypeMetafieldDisabledNames = new Set(names.map(getShoesTypeToggleKey).filter(Boolean));
    togglesChanged = true;
  }
  if (togglesChanged) {
    _renderedMonitorSigs.clear();
    renderAll();
    refreshMonitorMetaContent();
  }
});

chrome.runtime.onMessage.addListener((message) => {
  if (message.type === "selector-picked") {
    const { selector, outerHTML } = message;
    if (!selector || !selectedMonitorId) return;
    const list = monitorDetail.querySelector(".selectors-list");
    if (!list) return;
    const pickBtn = monitorDetail.querySelector(".pick-selector-btn");
    if (pickBtn) { pickBtn.textContent = "Pick from tab"; pickBtn.disabled = false; }
    addSelectorChip(list, selector);
    (() => {
      const chips = monitorDetail.querySelectorAll(".selectors-list .selector-chip");
      const selectors = Array.from(chips)
        .map(c => c.dataset.selector || c.querySelector(".chip-text")?.textContent?.trim() || "")
        .filter(Boolean);
      const monitor = _getMonitor(selectedMonitorId);
      const existing = monitor?.lastSelectedOuterHtmlSnapshot || "";
      const newSnap = existing ? existing + "\n" + (outerHTML || "") : (outerHTML || "");
      const autoCheck = monitorDetail.querySelector(".autocheck-toggle")?.checked ?? false;
      const intervalValue = Number(monitorDetail.querySelector(".interval-value")?.value) || 1;
      const intervalUnit = Number(monitorDetail.querySelector(".interval-unit")?.value) || 1440;

      // Update in-memory first so the toggle handler reads it immediately
      if (monitor && newSnap) {
        monitor.lastSelectedOuterHtmlSnapshot = newSnap;
        monitor.hasLastSelectedOuterHtmlSnapshot = true;
      }

      // Fire-and-forget storage save — don't await
      chrome.runtime.sendMessage({
        type: "update-monitor",
        payload: { id: selectedMonitorId, selectors, autoCheck, intervalMinutes: Math.max(1, intervalValue * intervalUnit), lastSelectedOuterHtmlSnapshot: newSnap }
      }).catch(() => {});

      // Replace section content unconditionally — bypasses placeholder/toggle race entirely
      if (newSnap) {
        let section = monitorDetail.querySelector(".selected-html-section");
        if (!section) {
          // Section wasn't rendered (monitor had no selectors before this pick) — inject it
          const liveSection = monitorDetail.querySelector(".live-extracted-section, .html-section");
          if (liveSection) {
            const div = document.createElement("details");
            div.className = "html-section selected-html-section";
            div.dataset.monitorId = selectedMonitorId;
            liveSection.after(div);
            section = div;
          }
        }
        if (section) {
          section.innerHTML = `
            <summary>Selected parts outerHTML</summary>
            <p class="subtle" style="margin:12px 12px 8px">Raw HTML of the element you just picked.</p>
            <div style="display:flex;justify-content:flex-end;margin:0 12px 8px">
              <button class="inline-button copy-selected-html-btn" type="button">Copy outerHTML</button>
            </div>
            <details class="raw-html-panel" open style="margin:0 12px 12px">
              <summary>Current selected outerHTML</summary>
              <pre>${truncateHtml(newSnap)}</pre>
            </details>`;
          section.open = true;
        }
      }
    })();
    return;
  }
  if (message.type === "monitors-batch-saved") {
    if (importInProgress || dashboardRenderPaused) return;
    clearTimeout(dashboardStorageRefreshTimer);
    dashboardStorageRefreshTimer = setTimeout(() => silentRefresh().catch(() => {}), 1500);
  }
  if (message.type === "monitors-updated") {
    if (importInProgress || dashboardRenderPaused) return;
    // If the updated monitor is included, patch in-place — no storage roundtrip needed.
    if (message.monitor && message.monitor.id) {
      patchMonitorInPlace(message.monitor);
      return;
    }
    // Fallback: full refresh with leading+trailing throttle.
    const sinceLastRender = Date.now() - dashboardStorageLastRenderTime;
    clearTimeout(dashboardStorageRefreshTimer);
    if (sinceLastRender >= 200) {
      dashboardStorageLastRenderTime = Date.now();
      silentRefresh().catch(() => {});
    } else {
      dashboardStorageRefreshTimer = setTimeout(() => {
        dashboardStorageLastRenderTime = Date.now();
        silentRefresh().catch(() => {});
      }, 200 - sinceLastRender);
    }
  }
  if (message.type === "dsg-pause-status") {
    const labels = {
      paused:     "Dick's checks paused — waiting for open tabs to finish...",
      clearing:   "Clearing Dick's cookies...",
      done:       "Cookies cleared",
      continuing: "Resuming Dick's checks..."
    };
    if (!message.status) {
      if (shopifySyncStatus) { shopifySyncStatus.className = "dashboard-sync-status"; shopifySyncStatus.textContent = ""; }
    } else {
      showShopifySyncStatus(labels[message.status] || message.status, "working", { autoHide: false });
    }
  }
});


await restoreDashboardUiState().catch(() => {});
await loadShoesTypeMetafieldToggles().catch(() => {});

// Detect when the extension is reloaded while this tab is still open and show a reload banner.
{
  const _ctxCheck = setInterval(() => {
    if (!chrome.runtime?.id) {
      clearInterval(_ctxCheck);
      const banner = document.createElement("div");
      banner.style.cssText = "position:fixed;top:0;left:0;right:0;z-index:9999;background:#b41f1f;color:#fff;padding:12px 16px;font-size:14px;text-align:center";
      banner.innerHTML = 'The extension was reloaded. <strong>Close this tab and reopen the dashboard</strong> from the extension icon for checks to work.';
      document.body.prepend(banner);
    }
  }, 3000);
}

// Monitor checks only need monitor URLs. Shopify is required for import/update,
// but check buttons should stay available even if Shopify auth is stale.
async function updateCheckState() {
  canCheck = true;
  updateBulkBtn();
  // Reset any stale stop-checks flag in the background (e.g. page was closed
  // mid-batch and resume-checks was never sent).
  chrome.runtime.sendMessage({ type: "resume-checks" }).catch(() => {});
}


updateCheckState();

// ── Auto interval toggle ───────────────────────────────────────────────────
const autoToggle = document.getElementById("auto-interval-toggle");
const autoLabel = document.getElementById("auto-interval-label");

async function initAutoToggle() {
  const { autoIntervalEnabled } = await chrome.storage.local.get("autoIntervalEnabled");
  autoEnabled = autoIntervalEnabled === true; // default OFF
  autoToggle.checked = autoEnabled;
  autoLabel.style.color = autoEnabled ? "#44aa00" : "";
}

autoToggle.addEventListener("change", async () => {
  autoEnabled = autoToggle.checked;
  _gridDataVersion++;
  await chrome.storage.local.set({ autoIntervalEnabled: autoEnabled });
  autoLabel.style.color = autoEnabled ? "#44aa00" : "";
  renderGrid(getFilteredMonitors());
});

initAutoToggle();

// ── Shopify connection ─────────────────────────────────────────────────────
const shopifyStatus = document.getElementById("shopify-status");
const shopifyConnectBtn = document.getElementById("shopify-connect-btn");
const shopifyDisconnectBtn = document.getElementById("shopify-disconnect-btn");
const shopifyError = document.getElementById("shopify-error");

async function refreshShopifyUI(runVerify = false) {
  const connected = await isConnected();
  if (connected) {
    shopifyStatus.textContent = "Connected ✓";
    shopifyStatus.style.color = "#16a34a";
    shopifyConnectBtn.style.display = "none";
    shopifyDisconnectBtn.style.display = "inline-flex";
    if (runVerify) {
      shopifyStatus.textContent = "Verifying…";
      shopifyStatus.style.color = "#9ca3af";
      const { ok, shopName, error } = await verifyConnection();
      if (ok) {
        shopifyStatus.textContent = `✓ Connected to ${shopName}`;
        shopifyStatus.style.color = "#16a34a";
      } else {
        shopifyStatus.textContent = `Token saved but API call failed: ${error}`;
        shopifyStatus.style.color = "#b41f1f";
      }
    }
  } else {
    shopifyStatus.textContent = "Not connected";
    shopifyStatus.style.color = "#9ca3af";
    shopifyConnectBtn.style.display = "inline-flex";
    shopifyDisconnectBtn.style.display = "none";
  }
  shopifyError.style.display = "none";
  updateCheckState().catch(() => {});
  scheduleDashboardAutoRefresh({ forceShopifyRefresh: false, delayMs: 3000 });
}

shopifyConnectBtn.addEventListener("click", async () => {
  shopifyConnectBtn.disabled = true;
  shopifyConnectBtn.textContent = "Connecting…";
  shopifyError.style.display = "none";
  try {
    await connectShopify();
    await refreshShopifyUI(true);
    await updateCheckState();
  } catch (e) {
    shopifyError.textContent = e.message || "Connection failed";
    shopifyError.style.display = "block";
    shopifyConnectBtn.disabled = false;
    shopifyConnectBtn.textContent = "Connect to Shopify";
  }
});

shopifyDisconnectBtn.addEventListener("click", async () => {
  await disconnectShopify();
  await refreshShopifyUI();
  await updateCheckState();
});

refreshShopifyUI();

// ── Shopify store metadata viewer ──────────────────────────────────────────
function getMetaKindForLabel(label) {
  if (/vendor/i.test(label)) return "vendor";
  if (/categor/i.test(label)) return "category";
  if (/product name/i.test(label)) return "name";
  if (/product type/i.test(label)) return "type";
  if (/variant|size/i.test(label)) return "variant";
  return "tag";
}

function getMetafieldDisplayName(key) {
  const labels = {
    gender: "Gender",
    color: "Color",
    product_type: "Product type",
    shoes_type: "Shoes type",
    cleats: "Cleats",
    notes: "Notes"
  };
  return labels[key] || String(key || "Metafield")
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function getMetafieldColor(key) {
  const colors = {
    gender: "#2563eb",
    color: "#c2410c",
    product_type: "#047857",
    shoes_type: "#7c3aed",
    cleats: "#b45309"
  };
  return colors[key] || "#475569";
}

function renderMetaGroup(label, items, color) {
  if (!items.length) return `<div><p style="font-size:12px;font-weight:700;color:#5f6c7b;margin:0 0 6px">${label} <span style="font-weight:400">(0)</span></p><p style="font-size:12px;color:#9ca3af">None found</p></div>`;
  const kind = getMetaKindForLabel(label);
  const expanded = metaExpandedGroups.has(kind);
  const visibleItems = expanded ? items : items.slice(0, META_CHIP_LIMIT);
  const hiddenCount = Math.max(0, items.length - visibleItems.length);
  const chips = visibleItems.map(v => {
    const active = activeStoreMetaFilters.some(f => f.kind === kind && normalizeMetaValue(f.value) === normalizeMetaValue(v));
    const isUsSize = kind === "variant" && isLikelyRawUsSize(v);
    const chipColor = isUsSize ? "#ff5a36" : color;
    const chipTitle = isUsSize
      ? `Public Shopify size "${v}" looks like a US size. Click to show products displaying it.`
      : `Click to show products for "${v}"`;
    const suffix = isUsSize ? `<span class="meta-chip-flag">US</span>` : "";
    return `<button type="button" class="meta-select-chip${active ? " active" : ""}${isUsSize ? " warning" : ""}" data-meta-kind="${escapeHtml(kind)}" data-meta-value="${escapeHtml(v)}" title="${escapeHtml(chipTitle)}" style="--meta-color:${chipColor};background:${chipColor}15;color:${chipColor};border-color:${chipColor}40"><span class="mf-sq"></span>${escapeHtml(v)}${suffix}</button>`;
  }).join("");
  const showMore = hiddenCount
    ? `<button type="button" class="meta-show-more" data-meta-kind="${escapeHtml(kind)}">Show ${hiddenCount} more</button>`
    : (expanded && items.length > META_CHIP_LIMIT ? `<button type="button" class="meta-show-more" data-meta-kind="${escapeHtml(kind)}" data-collapse="1">Show less</button>` : "");
  return `<div><p style="font-size:12px;font-weight:700;color:#5f6c7b;margin:0 0 6px">${label} <span style="font-weight:400">(${items.length})</span></p><div class="meta-chip-wrap">${chips}${showMore}</div></div>`;
}

function updateMetaChipActiveStates() {
  document.querySelectorAll(".meta-select-chip").forEach((chip) => {
    const active = activeStoreMetaFilters.some(f =>
      f.kind === chip.dataset.metaKind && normalizeMetaValue(f.value) === normalizeMetaValue(chip.dataset.metaValue)
    );
    chip.classList.toggle("active", active);
  });
}

function renderStoreMetaContentFromCache() {
  if (!cachedStoreMetaRenderData) return false;
  const { vendors, types, tags, variants, metafieldDefs } = cachedStoreMetaRenderData;
  metaContent.innerHTML = [
    renderMetaGroup("Vendors", vendors, "#5c6ac4"),
    renderMetaGroup("Product Types", types, "#007a5a"),
    renderMetaGroup("Variants", variants, "#6d5dfc"),
    renderMetaGroup("Tags", tags, "#c05717"),
    renderMetafieldDefinitionsPanel(metafieldDefs)
  ].join("");
  return true;
}

function addCount(map, value, monitorId = "") {
  const clean = String(value || "").replace(/\s+/g, " ").trim();
  if (!clean) return null;
  const key = clean.toLowerCase();
  if (!map.has(key)) map.set(key, { value: clean, monitorIds: new Set() });
  if (monitorId) map.get(key).monitorIds.add(String(monitorId));
  return map.get(key);
}

function sortedCountRows(map) {
  return [...map.values()]
    .sort((a, b) => b.monitorIds.size - a.monitorIds.size || a.value.localeCompare(b.value, undefined, { numeric: true, sensitivity: "base" }));
}

function sortedSizeCountRows(map) {
  return [...map.values()]
    .sort((a, b) => {
      const an = getSizeSortNumber(a.value);
      const bn = getSizeSortNumber(b.value);
      if (an !== bn) return an - bn;
      return a.value.localeCompare(b.value, undefined, { numeric: true, sensitivity: "base" });
    });
}

function getExpectedMonitorMetaData() {
  const vendors = new Map();
  const types = new Map();
  const categories = new Map();
  const names = new Map();
  const tags = new Map();
  const variants = new Map();
  const metafieldRows = new Map();
  const monitors = allMonitors.filter((monitor) => monitor?.productData && !isSavedPendingMonitor(monitor));

  for (const monitor of monitors) {
    const monitorId = String(monitor.id || "");
    const values = getMonitorStoreMetaValues(monitor);
    values.vendors.forEach((value) => addCount(vendors, value, monitorId));
    values.types.forEach((value) => addCount(types, value, monitorId));
    const pd = monitor.productData || {};
    if (pd.name) addCount(names, pd.name, monitorId);
    const shoesInfo = getMonitorShoesTypeInfo(monitor);
    if (shoesInfo.category) addCount(categories, shoesInfo.category, monitorId);
    values.tags.forEach((value) => addCount(tags, value, monitorId));
    values.variants.forEach((value) => addCount(variants, value, monitorId));
    getMonitorExpectedMetafields(monitor).forEach((field) => {
      const label = String(field.value || "").replace(/\s+/g, " ").trim();
      const row = addCount(metafieldRows, `${field.key}::${label}`, monitorId);
      if (row) {
        row.value = label;
        row.namespace = field.namespace || "custom";
        row.fieldKey = field.key || "";
        row.fieldLabel = getMetafieldDisplayName(field.key);
        row.filterKind = `metafield:${field.key}`;
        row.filterValue = field.value;
      }
    });
  }

  return {
    totalMonitors: monitors.length,
    vendors: sortedCountRows(vendors),
    types: sortedCountRows(types),
    categories: sortedCountRows(categories),
    names: sortedCountRows(names),
    tags: sortedCountRows(tags),
    variants: sortedSizeCountRows(variants),
    metafields: sortedCountRows(metafieldRows)
  };
}

function renderMonitorMetaGroup(label, rows, color) {
  const kind = getMetaKindForLabel(label);
  if (!rows.length) return `<div><p style="font-size:12px;font-weight:700;color:#5f6c7b;margin:0 0 6px">${label} <span style="font-weight:400">(0)</span></p><p style="font-size:12px;color:#9ca3af">None found</p></div>`;
  const expanded = monitorMetaExpandedGroups.has(kind);
  const isHeavyNameList = kind === "name";
  const visibleRows = isHeavyNameList ? rows.slice(0, META_CHIP_LIMIT) : (expanded ? rows : rows.slice(0, META_CHIP_LIMIT));
  const hiddenCount = Math.max(0, rows.length - visibleRows.length);
  const chips = visibleRows.map(({ value, monitorIds }) => {
    const active = activeStoreMetaFilters.some(f => f.kind === kind && normalizeMetaValue(f.value) === normalizeMetaValue(value));
    const count = monitorIds?.size || 0;
    return `<button type="button" class="meta-select-chip${active ? " active" : ""}" data-meta-kind="${escapeHtml(kind)}" data-meta-value="${escapeHtml(value)}" title="${escapeHtml(`Show ${count} monitor${count !== 1 ? "s" : ""} for "${value}"`)}" style="--meta-color:${color};background:${color}15;color:${color};border-color:${color}40"><span class="mf-sq"></span>${escapeHtml(value)}<span class="meta-chip-count">${count}</span></button>`;
  }).join("");
  const showMore = isHeavyNameList && hiddenCount
    ? `<span class="meta-show-more" style="cursor:default">Showing first ${META_CHIP_LIMIT}; use search for the rest</span>`
    : hiddenCount
    ? `<button type="button" class="monitor-meta-show-more meta-show-more" data-meta-kind="${escapeHtml(kind)}">Show ${hiddenCount} more</button>`
    : (expanded && rows.length > META_CHIP_LIMIT ? `<button type="button" class="monitor-meta-show-more meta-show-more" data-meta-kind="${escapeHtml(kind)}" data-collapse="1">Show less</button>` : "");
  return `<div><p style="font-size:12px;font-weight:700;color:#5f6c7b;margin:0 0 6px">${label} <span style="font-weight:400">(${rows.length})</span></p><div class="meta-chip-wrap">${chips}${showMore}</div></div>`;
}

function renderMonitorMetafieldRows(rows) {
  if (!rows.length) return `<section class="metafield-panel"><div class="metafield-panel-head"><h3>Metafields Written</h3><span>0 values</span></div><p class="metafield-empty">None found</p></section>`;
  const expanded = monitorMetaExpandedGroups.has("metafield");
  const grouped = new Map();
  rows.forEach((row) => {
    const key = row.fieldKey || "metafield";
    if (!grouped.has(key)) {
      grouped.set(key, {
        key,
        label: row.fieldLabel || getMetafieldDisplayName(key),
        namespace: row.namespace || "custom",
        rows: []
      });
    }
    grouped.get(key).rows.push(row);
  });

  const orderedGroups = [...grouped.values()].sort((a, b) => {
    const order = ["gender", "color", "product_type", "shoes_type", "cleats", "notes"];
    const ai = order.indexOf(a.key);
    const bi = order.indexOf(b.key);
    if (ai !== -1 || bi !== -1) return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
    return a.label.localeCompare(b.label, undefined, { sensitivity: "base" });
  });

  let renderedCount = 0;
  const maxVisible = expanded ? Number.POSITIVE_INFINITY : META_CHIP_LIMIT;
  const cards = orderedGroups.map((group) => {
    const color = getMetafieldColor(group.key);
    const visibleRows = [];
    for (const row of group.rows) {
      if (renderedCount >= maxVisible) break;
      visibleRows.push(row);
      renderedCount += 1;
    }
    if (!visibleRows.length) return "";
    const values = visibleRows.map(({ value, monitorIds, filterKind, filterValue }) => {
      const count = monitorIds?.size || 0;
      const active = activeStoreMetaFilters.some(f => f.kind === filterKind && normalizeMetaValue(f.value) === normalizeMetaValue(filterValue));
      return `<button type="button" class="metafield-value-chip meta-select-chip${active ? " active" : ""}" data-meta-kind="${escapeHtml(filterKind || "tag")}" data-meta-value="${escapeHtml(filterValue || value)}" title="${escapeHtml(`Show ${count} monitor${count !== 1 ? "s" : ""} for "${filterValue || value}"`)}" style="--meta-color:${color}"><span class="mf-sq"></span><span>${escapeHtml(value)}</span><em>${count}</em></button>`;
    }).join("");
    return `<article class="metafield-card" style="--meta-color:${color}">
      <div class="metafield-card-head">
        <strong>${escapeHtml(group.label)}</strong>
        <span>${escapeHtml(group.namespace)}.${escapeHtml(group.key)}</span>
      </div>
      <div class="metafield-value-list">${values}</div>
    </article>`;
  }).join("");

  const hiddenCount = Math.max(0, rows.length - renderedCount);
  const showMore = hiddenCount
    ? `<button type="button" class="monitor-meta-show-more meta-show-more" data-meta-kind="metafield">Show ${hiddenCount} more</button>`
    : (expanded && rows.length > META_CHIP_LIMIT ? `<button type="button" class="monitor-meta-show-more meta-show-more" data-meta-kind="metafield" data-collapse="1">Show less</button>` : "");
  return `<section class="metafield-panel">
    <div class="metafield-panel-head">
      <h3>Metafields Written</h3>
      <span>${rows.length} value${rows.length !== 1 ? "s" : ""}</span>
    </div>
    <div class="metafield-card-grid">${cards}</div>
    ${showMore ? `<div class="metafield-actions">${showMore}</div>` : ""}
  </section>`;
}

function renderMonitorMetaContentFromCache() {
  if (!cachedMonitorMetaRenderData || !monitorMetaContent) return false;
  const { totalMonitors, vendors, types, categories, names, tags, variants, metafields } = cachedMonitorMetaRenderData;
  monitorMetaContent.innerHTML = [
    `<p style="font-size:12px;color:var(--muted);margin:0">Expected Shopify filters from ${totalMonitors} monitor${totalMonitors !== 1 ? "s" : ""}. Variant chips show the public size labels that will appear in Shopify.</p>`,
    renderMonitorMetaGroup("Vendors", vendors, "#5c6ac4"),
    renderMonitorMetaGroup("Product Types", types, "#007a5a"),
    renderMonitorMetaGroup("Categories", categories || [], "#0f766e"),
    renderMonitorMetaGroup("Product Names", names || [], "#475569"),
    renderMonitorMetaGroup("Variants", variants, "#6d5dfc"),
    renderMonitorMetaGroup("Tags", tags, "#c05717"),
    renderMonitorMetafieldRows(metafields),
    renderProductNameGroups()
  ].join("");
  // Lazy-fill model item lists when their <details> is opened
  monitorMetaContent.querySelectorAll(".model-items-lazy[data-lazy-id]").forEach(ul => {
    const fill = () => {
      if (ul.dataset.filled) return;
      ul.dataset.filled = "1";
      ul.innerHTML = _lazyModelItemsCache.get(ul.dataset.lazyId) || "";
    };
    const details = ul.closest("details");
    if (!details || details.open) { fill(); return; }
    details.addEventListener("toggle", () => { if (details.open) fill(); });
  });
  // Prevent toggle-label clicks from collapsing the parent <details>
  monitorMetaContent.querySelectorAll("summary").forEach(summary => {
    if (summary.querySelector(".shoes-type-toggle")) {
      summary.addEventListener("click", (e) => {
        const toggleLabel = e.target.closest(".shoes-type-toggle");
        if (!toggleLabel) return;
        e.preventDefault();
        const input = toggleLabel.querySelector(".shoes-type-toggle-input");
        if (!input) return;
        const key = getShoesTypeToggleKey(input.dataset.shoesType || "");
        const allInputs = [...monitorMetaContent.querySelectorAll(".shoes-type-toggle-input")];
        const idx = allInputs.indexOf(input);

        if (e.ctrlKey || e.metaKey) {
          if (key) {
            if (selectedShoesTypeKeys.has(key)) selectedShoesTypeKeys.delete(key);
            else selectedShoesTypeKeys.add(key);
          }
          _lastShoesTypeClickIdx = idx;
          _applyShoesTypeSelectionState();
        } else if (e.shiftKey && _lastShoesTypeClickIdx >= 0 && idx >= 0) {
          const start = Math.min(_lastShoesTypeClickIdx, idx);
          const end = Math.max(_lastShoesTypeClickIdx, idx);
          for (let i = start; i <= end; i++) {
            const k = getShoesTypeToggleKey(allInputs[i]?.dataset.shoesType || "");
            if (k) selectedShoesTypeKeys.add(k);
          }
          _applyShoesTypeSelectionState();
        } else if (key && selectedShoesTypeKeys.has(key) && selectedShoesTypeKeys.size > 1) {
          _bulkSetShoesTypeToggles(!input.checked);
        } else {
          selectedShoesTypeKeys.clear();
          input.checked = !input.checked;
          input.dispatchEvent(new Event("change", { bubbles: true }));
          _lastShoesTypeClickIdx = idx;
          _applyShoesTypeSelectionState();
        }
      });
    }
  });
  _applyShoesTypeSelectionState();
  return true;
}

let _metaSigCache = null;
let _metaSigDirty = true;
function _invalidateMetaSig() { _metaSigDirty = true; }

function _computeMonitorMetaSig() {
  if (!_metaSigDirty && _metaSigCache !== null) return _metaSigCache;
  const toggles = [...shoesTypeMetafieldEnabledNames].sort().join(",");
  const mons = allMonitors.map(m => {
    const pd = m.productData;
    if (!pd?.name) return m.id + ":_";
    return `${m.id}:${pd.name}:${pd.brand || ""}:${pd.sku || ""}:${pd.type || ""}:${m.shopifyProductId || ""}:${m.status || ""}:${m.savedBatchId || ""}`;
  }).join("|");
  _metaSigCache = toggles + "|||" + mons;
  _metaSigDirty = false;
  return _metaSigCache;
}

function refreshMonitorMetaContent(force = false) {
  if (!force && !monitorMetaLoaded) return;
  monitorMetaLoaded = true;
  const sig = _computeMonitorMetaSig();
  if (!force && sig === _monitorMetaLastSig) {
    // Nothing changed — restore cached data if applyMonitorsUpdate nulled it
    if (!cachedMonitorMetaRenderData && _monitorMetaLastData) {
      cachedMonitorMetaRenderData = _monitorMetaLastData;
    }
    return;
  }
  _monitorMetaLastSig = sig;
  cachedMonitorMetaRenderData = getExpectedMonitorMetaData();
  _monitorMetaLastData = cachedMonitorMetaRenderData;
  renderMonitorMetaContentFromCache();
  if (monitorMetaStatus) monitorMetaStatus.textContent = `Updated ${new Date().toLocaleTimeString()}`;
}

function sortVariantValues(values) {
  return [...values].sort((a, b) => {
    const an = Number(String(a).replace(/[^\d.]/g, ""));
    const bn = Number(String(b).replace(/[^\d.]/g, ""));
    if (Number.isFinite(an) && Number.isFinite(bn) && an !== bn) return an - bn;
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" });
  });
}

function getMonitorsForShopifyProduct(product) {
  const productId = Number(product?.id || 0);
  const productSkus = getProductSkuSet(product);
  return allMonitors.filter((monitor) => {
    if (productId && Number(monitor.shopifyProductId || 0) === productId) return true;
    const monitorSku = normalizeSku(monitor.productData?.sku);
    return monitorSku && productSkus.has(monitorSku);
  });
}

async function getStoreVariantValues() {
  const values = new Set();
  const variantMonitorMap = new Map();
  const products = await getShopifyProductsSnapshot();
  const monitorsByProductId = new Map();
  const monitorsBySku = new Map();
  for (const monitor of allMonitors) {
    const productId = Number(monitor.shopifyProductId || 0);
    if (productId) {
      if (!monitorsByProductId.has(productId)) monitorsByProductId.set(productId, []);
      monitorsByProductId.get(productId).push(monitor);
    }
    const sku = normalizeSku(monitor.productData?.sku);
    if (sku) {
      if (!monitorsBySku.has(sku)) monitorsBySku.set(sku, []);
      monitorsBySku.get(sku).push(monitor);
    }
  }
  // Process in chunks of 300 to avoid long synchronous blocks on large catalogs
  for (let i = 0; i < products.length; i++) {
    if (i > 0 && i % 300 === 0) await new Promise(r => setTimeout(r, 0));
    const product = products[i];
    if (String(product.status || "").toLowerCase() !== "active") continue;
    const monitorIds = new Set();
    const productId = Number(product?.id || 0);
    (monitorsByProductId.get(productId) || []).forEach((monitor) => monitorIds.add(String(monitor.id)));
    for (const sku of getProductSkuSet(product)) {
      (monitorsBySku.get(sku) || []).forEach((monitor) => monitorIds.add(String(monitor.id)));
    }
    (product.variantOptions || []).forEach((option) => {
      const value = cleanSizeValue(option);
      if (!value || /^default title$/i.test(value)) return;
      values.add(value);
      const key = normalizeMetaValue(value);
      if (!variantMonitorMap.has(key)) variantMonitorMap.set(key, new Set());
      monitorIds.forEach((id) => variantMonitorMap.get(key).add(id));
    });
  }
  storeVariantMonitorIdsByValue = variantMonitorMap;
  shopifyBadSizeVariantCount = countBadSizeVariants(products);
  updateBulkBtn();
  return sortVariantValues(values);
}

function selectMonitorsByStoreMeta(kind, value) {
  const matches = allMonitors.filter((monitor) => monitorMatchesStoreMeta(monitor, kind, value));
  if (!matches.length) {
    if (metaStatus) metaStatus.textContent = kind === "variant"
      ? `Shopify has public size "${value}", but no dashboard monitor is linked to that Shopify product.`
      : `No monitors matched "${value}"`;
    return;
  }
  const existingIdx = activeStoreMetaFilters.findIndex(f =>
    f.kind === kind && normalizeMetaValue(f.value) === normalizeMetaValue(value)
  );
  const wasActive = existingIdx >= 0;
  if (wasActive) activeStoreMetaFilters.splice(existingIdx, 1);
  else activeStoreMetaFilters.push({ kind, value });

  const hasFilters = activeStoreMetaFilters.length > 0;
  if (clearMetaFilterBtn) clearMetaFilterBtn.style.display = hasFilters ? "" : "none";
  if (clearMonitorMetaFilterBtn) clearMonitorMetaFilterBtn.style.display = hasFilters ? "" : "none";

  const allSelected = matches.every((monitor) => checkedIds.has(monitor.id));
  matches.forEach((monitor) => {
    if (allSelected) checkedIds.delete(monitor.id);
    else checkedIds.add(monitor.id);
  });
  updateBulkBtn();
  const visible = getFilteredMonitors();
  renderStats(visible);
  renderGrid(visible);
  updateMetaChipActiveStates();
  const filterCount = activeStoreMetaFilters.length;
  const filterLabel = filterCount > 1 ? `${filterCount} filters active` : wasActive ? `Cleared "${value}"` : `"${value}"`;
  if (metaStatus) metaStatus.textContent = hasFilters
    ? `Showing ${visible.length} monitor${visible.length !== 1 ? "s" : ""} — ${filterLabel}. ${allSelected ? "Deselected" : "Selected"} ${matches.length}.`
    : `Cleared all store filters. ${allSelected ? "Deselected" : "Selected"} ${matches.length}.`;
  if (monitorMetaStatus) monitorMetaStatus.textContent = hasFilters
    ? `Showing ${visible.length} monitor${visible.length !== 1 ? "s" : ""} — ${filterLabel}.`
    : `Cleared monitor filter.`;
}

function renderMetafieldDefinitionsPanel(defs) {
  if (!defs.length) {
    return `<section class="metafield-panel"><div class="metafield-panel-head"><h3>Metafield Definitions</h3><span>0 fields</span></div><p class="metafield-empty">None found</p></section>`;
  }
  const byNs = new Map();
  defs.forEach((d) => {
    const ns = d.namespace || "custom";
    if (!byNs.has(ns)) byNs.set(ns, []);
    byNs.get(ns).push(d);
  });
  const rows = [...byNs.entries()].map(([ns, items]) => {
    const chips = items.map((d) => {
      const color = getMetafieldColor(d.key);
      const label = d.name || getMetafieldDisplayName(d.key);
      return `<span class="metafield-definition-chip" style="--meta-color:${color}">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(ns)}.${escapeHtml(d.key)}</span>
        <em>${escapeHtml(d.type?.name || "")}</em>
      </span>`;
    }).join("");
    return `<div class="metafield-definition-group"><p>${escapeHtml(ns)}</p><div>${chips}</div></div>`;
  }).join("");
  return `<section class="metafield-panel"><div class="metafield-panel-head"><h3>Metafield Definitions</h3><span>${defs.length} field${defs.length !== 1 ? "s" : ""}</span></div>${rows}</section>`;
}

function renderMetafieldDefinitions(defs) {
  if (!defs.length) return `<section class="metafield-panel"><div class="metafield-panel-head"><h3>Metafield Definitions</h3><span>0 fields</span></div><p class="metafield-empty">None found</p></section>`;
  const byNs = new Map();
  defs.forEach(d => {
    const ns = d.namespace || "custom";
    if (!byNs.has(ns)) byNs.set(ns, []);
    byNs.get(ns).push(d);
  });
  const rows = [...byNs.entries()].map(([ns, items]) => {
    const chips = items.map(d =>
      `<span style="display:inline-flex;flex-direction:column;align-items:flex-start;background:#f0f4ff;border:1px solid #c7d2fe;border-radius:8px;padding:5px 10px;font-size:11px;gap:2px">
        <span style="font-weight:700;color:#3730a3">${escapeHtml(d.key)}</span>
        <span style="color:#6b7280">${escapeHtml(d.type?.name || "")}${d.name && d.name !== d.key ? ` · ${escapeHtml(d.name)}` : ""}</span>
      </span>`
    ).join("");
    return `<div style="margin-bottom:8px"><p style="font-size:11px;font-weight:700;color:#6b7280;margin:0 0 4px;text-transform:uppercase;letter-spacing:.05em">${escapeHtml(ns)}</p><div style="display:flex;flex-wrap:wrap;gap:6px">${chips}</div></div>`;
  }).join("");
  return `<div><p style="font-size:12px;font-weight:700;color:#5f6c7b;margin:0 0 8px">Metafield Definitions <span style="font-weight:400">(${defs.length})</span></p>${rows}</div>`;
}

const _lazyModelItemsCache = new Map(); // lazyId → HTML string, cleared each render

function renderProductNameGroups() {
  _lazyModelItemsCache.clear();
  const monitors = allMonitors.filter(m => m.productData?.name);
  if (!monitors.length) return "";

  const site = m => {
    try { const h = new URL(m.url || "").hostname; if (/dickssportinggoods/i.test(h)) return "DSG"; if (/footlocker/i.test(h)) return "FL"; } catch (_) {}
    return "Other";
  };
  const nameSortKey = (m) => {
    const pd = m.productData || {};
    const shoesType = getMonitorShoesTypeInfo(m);
    const brand = canonicalizeBrand(pd.brand || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
    let text = String(pd.name || m.name || "")
      .toLowerCase()
      .replace(/\baf[\s-]*1\b/g, "air force 1")
      .replace(/&/g, " and ")
      .replace(/\b(?:men'?s?|women'?s?|kids?|youth|boys?|girls?|unisex)\b/g, " ")
      .replace(/\b(?:shoe|shoes|sneaker|sneakers|basketball|running|lifestyle|football|soccer)\b/g, " ")
      .replace(/[^a-z0-9.]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (brand) {
      const brandPattern = new RegExp(`\\b${brand.replace(/\s+/g, "\\s+")}\\b`, "g");
      text = text.replace(brandPattern, " ");
    }
    text = text
      .replace(/\bair\s+(?=retro\b|\d+\b)/g, " ")
      .replace(/\bhigh\b/g, "hi")
      .replace(/\blow\b/g, "lo")
      .replace(/\s+/g, " ")
      .trim();
    return `${normalizeLookupText(shoesType.model || shoesType.category) || text || String(pd.name || m.name || "").toLowerCase()} ${text}`;
  };

  const groups = new Map();
  monitors.forEach(m => {
    const pd = m.productData;
    const brand = canonicalizeBrand(pd.brand || "") || "Unknown";
    const rawType = (pd.type || "").trim();
    const type = rawType ? rawType.charAt(0).toUpperCase() + rawType.slice(1).toLowerCase() : "Uncategorized";
    const key = `${type}||${brand}`;
    if (!groups.has(key)) groups.set(key, { type, brand, monitors: [] });
    groups.get(key).monitors.push(m);
  });

  const typeCounts = new Map();
  for (const group of groups.values()) {
    typeCounts.set(group.type, (typeCounts.get(group.type) || 0) + group.monitors.length);
  }

  const sorted = [...groups.values()].sort((a, b) =>
    (typeCounts.get(b.type) || 0) - (typeCounts.get(a.type) || 0) ||
    a.type.localeCompare(b.type) ||
    b.monitors.length - a.monitors.length ||
    a.brand.localeCompare(b.brand)
  );

  const rows = sorted.map(g => {
    const sortedMonitors = [...g.monitors].sort((a, b) =>
      nameSortKey(a).localeCompare(nameSortKey(b), undefined, { numeric: true, sensitivity: "base" }) ||
      String(a.productData?.name || "").localeCompare(String(b.productData?.name || ""), undefined, { numeric: true, sensitivity: "base" }) ||
      String(a.productData?.color || "").localeCompare(String(b.productData?.color || ""), undefined, { numeric: true, sensitivity: "base" }) ||
      String(a.productData?.sku || "").localeCompare(String(b.productData?.sku || ""), undefined, { numeric: true, sensitivity: "base" })
    );
    // Group monitors by shoes_type model
    const byModel = new Map();
    for (const m of sortedMonitors) {
      const model = getMonitorShoesTypeInfo(m).model || "";
      if (!byModel.has(model)) byModel.set(model, []);
      byModel.get(model).push(m);
    }
    const modelEntries = [...byModel.entries()].sort(([a], [b]) => {
      if (!a && b) return 1;
      if (a && !b) return -1;
      return a.localeCompare(b, undefined, { sensitivity: "base", numeric: true });
    });

    const renderMonitorItem = (m) => {
      const pd = m.productData || {};
      return `<li style="font-size:11px;color:#374151;padding:2px 0;border-bottom:1px solid #f3f4f6;display:flex;align-items:center;gap:6px">
        <span style="flex:1">${escapeHtml(pd.name)}${pd.color ? ` <span style="color:#9ca3af">· ${escapeHtml(pd.color)}</span>` : ""}</span>
        <button type="button" class="product-name-filter-chip meta-select-chip" data-meta-kind="name" data-meta-value="${escapeHtml(pd.name || "")}" style="--meta-color:#475569;background:#f8fafc;border:1px solid #e5e7eb;color:#475569;border-radius:999px;padding:1px 6px;font-size:9px;font-weight:800;flex-shrink:0">Filter</button>
        <span style="color:#d1d5db;font-size:10px;flex-shrink:0">${escapeHtml(site(m))}</span>
      </li>`;
    };

    // Auto-enable models with 5+ monitors only if not explicitly disabled
    let _autoChanged = false;
    for (const [model, mons] of modelEntries) {
      if (model && mons.length >= 5) {
        const key = getShoesTypeToggleKey(model);
        if (key && !shoesTypeMetafieldEnabledNames.has(key) && !shoesTypeMetafieldDisabledNames.has(key)) {
          shoesTypeMetafieldEnabledNames.add(key);
          _autoChanged = true;
        }
      }
    }
    if (_autoChanged) saveShoesTypeMetafieldToggles().catch(() => {});

    const innerContent = modelEntries.map(([model, monitors]) => {
      const lazyId = `${g.type}||${g.brand}||${model || "__nomodel__"}`;
      _lazyModelItemsCache.set(lazyId, monitors.map(renderMonitorItem).join(""));
      if (model) {
        const enabled = isShoesTypeMetafieldEnabled(model);
        return `<details style="border-top:1px solid var(--border)">
          <summary style="font-size:11px;font-weight:600;cursor:pointer;color:var(--ink);list-style:none;display:flex;justify-content:space-between;align-items:center;padding:5px 12px;user-select:none;background:var(--void)">
            <span style="display:flex;align-items:center;gap:8px;min-width:0">
              <label class="shoes-type-toggle" title="${escapeHtml(enabled ? "This shoes type metafield will be written to Shopify" : "This shoes type metafield is off and will not be written")}">
                <input type="checkbox" class="shoes-type-toggle-input" data-shoes-type="${escapeHtml(model)}"${enabled ? " checked" : ""}>
                <span class="shoes-type-toggle-track"></span>
                <em>${enabled ? "On" : "Off"}</em>
              </label>
              <button type="button" class="product-name-filter-chip meta-select-chip${activeStoreMetaFilters.some(f => f.kind === "shoes_type" && normalizeMetaValue(f.value) === normalizeMetaValue(model)) ? " active" : ""}" data-meta-kind="shoes_type" data-meta-value="${escapeHtml(model)}" style="--meta-color:#7c3aed;background:#f5f3ff;border:1px solid #ddd6fe;color:#6d28d9;border-radius:999px;padding:2px 8px;font-size:10px;font-weight:800;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><span class="mf-sq"></span>${escapeHtml(model)}</button>
            </span>
            <strong style="font-family:'Jost',sans-serif;font-size:12px;color:var(--muted);font-weight:700;margin-left:8px">${monitors.length}</strong>
          </summary>
          <ul class="model-items-lazy" data-lazy-id="${escapeHtml(lazyId)}" style="margin:0;padding:0 12px 6px 20px;list-style:none"></ul>
        </details>`;
      }
      return `<ul class="model-items-lazy" data-lazy-id="${escapeHtml(lazyId)}" style="margin:0;padding:0 12px 6px;list-style:none"></ul>`;
    }).join("");

    return `<details style="border:1px solid var(--border);margin-bottom:3px;background:var(--void)">
      <summary style="font-size:11px;font-weight:700;cursor:pointer;color:var(--ink);list-style:none;display:flex;justify-content:space-between;align-items:center;padding:7px 12px;user-select:none">
        <span style="text-transform:uppercase;letter-spacing:0.04em">${escapeHtml(g.type)} <span style="color:var(--muted);font-weight:400">—</span> ${escapeHtml(g.brand)}</span>
        <strong style="font-family:'Jost',sans-serif;font-size:14px;color:var(--ink);font-weight:800;margin-left:12px">${g.monitors.length}</strong>
      </summary>
      ${innerContent}
    </details>`;
  }).join("");

  return `<div>
    <p style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:0.1em;margin:0 0 8px">Dashboard Products — Grouped by Type &amp; Brand <span style="font-weight:400">(${monitors.length})</span></p>
    ${rows}
  </div>`;
}

let _storeMetaLoadInProgress = false;

async function fetchAndShowMeta(forceRefresh = false) {
  if (_storeMetaLoadInProgress) return;
  _storeMetaLoadInProgress = true;
  metaStatus.textContent = "Loading…";
  fetchMetaBtn.disabled = true;
  try {
    if (forceRefresh) clearShopifyProductsSnapshotCache();
    const [{ vendors, types, tags }, variants, metafieldDefs] = await Promise.all([
      getShopifyMetadata(),
      getStoreVariantValues(),
      getAllProductMetafieldDefinitions().catch(() => [])
    ]);
    cachedStoreMetaRenderData = { vendors, types, tags, variants, metafieldDefs };
    // Yield one frame so the browser can update loading state before the DOM write
    await new Promise(r => requestAnimationFrame(r));
    renderStoreMetaContentFromCache();
    metaStatus.textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (e) {
    metaStatus.textContent = `Error: ${e.message}`;
  } finally {
    _storeMetaLoadInProgress = false;
    fetchMetaBtn.disabled = false;
  }
}

async function initMetaSection() {
  const connected = await isConnected();
  if (connected) {
    metaSection.style.display = "";
    metaStatus.textContent = "Loading store data…";
    // Silently pre-load store meta after the main dashboard settles.
    setTimeout(() => fetchAndShowMeta(false).catch(() => {}), 1800);
    // Keep it current: re-fetch every 5 minutes in the background.
    setInterval(() => fetchAndShowMeta(false).catch(() => {}), 5 * 60 * 1000);
  }
  if (monitorMetaSection) {
    monitorMetaSection.style.display = "";
    monitorMetaStatus.textContent = "Loading monitor data…";
  }
}

fetchMetaBtn.addEventListener("click", () => fetchAndShowMeta(true));
clearMetaFilterBtn.addEventListener("click", () => {
  activeStoreMetaFilters = [];
  clearMetaFilterBtn.style.display = "none";
  if (clearMonitorMetaFilterBtn) clearMonitorMetaFilterBtn.style.display = "none";
  const visible = getFilteredMonitors();
  renderStats(visible);
  renderGrid(visible);
  updateMetaChipActiveStates();
  metaStatus.textContent = "Store filter cleared";
  if (monitorMetaStatus) monitorMetaStatus.textContent = "Monitor filter cleared";
});
metaContent.addEventListener("click", (event) => {
  const showMore = event.target.closest(".meta-show-more");
  if (showMore) {
    const kind = showMore.dataset.metaKind;
    if (showMore.dataset.collapse) metaExpandedGroups.delete(kind);
    else metaExpandedGroups.add(kind);
    renderStoreMetaContentFromCache();
    return;
  }
  const chip = event.target.closest(".meta-select-chip");
  if (!chip) return;
  event.preventDefault();
  event.stopPropagation();
  selectMonitorsByStoreMeta(chip.dataset.metaKind, chip.dataset.metaValue);
});
refreshMonitorMetaBtn?.addEventListener("click", () => refreshMonitorMetaContent(true));
clearMonitorMetaFilterBtn?.addEventListener("click", () => {
  activeStoreMetaFilters = [];
  clearMonitorMetaFilterBtn.style.display = "none";
  if (clearMetaFilterBtn) clearMetaFilterBtn.style.display = "none";
  const visible = getFilteredMonitors();
  renderStats(visible);
  renderGrid(visible);
  updateMetaChipActiveStates();
  monitorMetaStatus.textContent = "Monitor filter cleared";
  if (metaStatus) metaStatus.textContent = "Store filter cleared";
});
monitorMetaContent?.addEventListener("click", (event) => {
  const toggleLabel = event.target.closest(".shoes-type-toggle");
  if (toggleLabel) {
    event.stopPropagation();
    return;
  }
  const showMore = event.target.closest(".monitor-meta-show-more");
  if (showMore) {
    const kind = showMore.dataset.metaKind;
    if (showMore.dataset.collapse) monitorMetaExpandedGroups.delete(kind);
    else monitorMetaExpandedGroups.add(kind);
    renderMonitorMetaContentFromCache();
    return;
  }
  const chip = event.target.closest(".meta-select-chip");
  if (!chip) return;
  event.preventDefault();
  event.stopPropagation();
  selectMonitorsByStoreMeta(chip.dataset.metaKind, chip.dataset.metaValue);
});
monitorMetaContent?.addEventListener("change", async (event) => {
  const toggle = event.target.closest(".shoes-type-toggle-input");
  if (!toggle) return;
  event.preventDefault();
  event.stopPropagation();
  const key = getShoesTypeToggleKey(toggle.dataset.shoesType || "");
  if (!key) return;
  if (toggle.checked) {
    shoesTypeMetafieldEnabledNames.add(key);
    shoesTypeMetafieldDisabledNames.delete(key);
  } else {
    shoesTypeMetafieldEnabledNames.delete(key);
    shoesTypeMetafieldDisabledNames.add(key);
  }
  await saveShoesTypeMetafieldToggles();
  clearShopifyProductsSnapshotCache();
  refreshMonitorMetaContent();
  monitorMetaStatus.textContent = `${toggle.dataset.shoesType || "Shoes type"} metafield ${toggle.checked ? "enabled" : "disabled"}. Run Update metadata to apply it in Shopify.`;
});
function _applyShoesTypeSelectionState() {
  if (!monitorMetaContent) return;
  monitorMetaContent.querySelectorAll(".shoes-type-toggle-input").forEach(input => {
    const key = getShoesTypeToggleKey(input.dataset.shoesType || "");
    const details = input.closest("details");
    if (details) details.classList.toggle("st-selected", !!(key && selectedShoesTypeKeys.has(key)));
  });
  const bar = document.getElementById("shoes-type-selection-bar");
  if (!bar) return;
  const n = selectedShoesTypeKeys.size;
  bar.style.display = n > 0 ? "flex" : "none";
  const countEl = document.getElementById("st-sel-count");
  if (countEl) countEl.textContent = `${n} selected —`;
}

async function _bulkSetShoesTypeToggles(enable) {
  let changed = false;
  for (const key of selectedShoesTypeKeys) {
    if (enable) {
      if (!shoesTypeMetafieldEnabledNames.has(key)) { shoesTypeMetafieldEnabledNames.add(key); shoesTypeMetafieldDisabledNames.delete(key); changed = true; }
    } else {
      if (shoesTypeMetafieldEnabledNames.has(key)) { shoesTypeMetafieldEnabledNames.delete(key); shoesTypeMetafieldDisabledNames.add(key); changed = true; }
    }
  }
  const n = selectedShoesTypeKeys.size;
  selectedShoesTypeKeys.clear();
  if (changed) {
    await saveShoesTypeMetafieldToggles();
    clearShopifyProductsSnapshotCache();
    refreshMonitorMetaContent();
    if (monitorMetaStatus) monitorMetaStatus.textContent = `${n} metafield${n !== 1 ? "s" : ""} ${enable ? "enabled" : "disabled"}. Run Update metadata to apply.`;
  } else {
    _applyShoesTypeSelectionState();
  }
}

document.getElementById("st-sel-enable-btn")?.addEventListener("click", () => _bulkSetShoesTypeToggles(true));
document.getElementById("st-sel-disable-btn")?.addEventListener("click", () => _bulkSetShoesTypeToggles(false));
document.getElementById("st-sel-clear-btn")?.addEventListener("click", () => {
  selectedShoesTypeKeys.clear();
  _applyShoesTypeSelectionState();
});

initMetaSection();

function getMonitorSkuSet() {
  return new Set(
    allMonitors
      .map((monitor) => normalizeSku(monitor.productData?.sku))
      .filter(Boolean)
  );
}

function getMonitorShopifyProductIdSet() {
  return new Set(
    allMonitors
      .map((monitor) => Number(monitor.shopifyProductId))
      .filter(Boolean)
  );
}

function getShopifySkuSet(products) {
  const skus = new Set();
  for (const product of products || []) {
    for (const sku of getProductSkuSet(product)) {
      skus.add(sku);
    }
  }
  return skus;
}

function getShopifyUnmonitoredStatusText() {
  const dupeCount = (shopifyProductCount != null && shopifyUniqueSkuCount != null)
    ? Math.max(0, shopifyProductCount - shopifyUniqueSkuCount - (shopifyMissingSkuCount || 0))
    : 0;
  const missingSkuCount = shopifyMissingSkuCount || 0;
  const base = `${shopifyUnmonitoredProducts.length} Shopify product${shopifyUnmonitoredProducts.length !== 1 ? "s" : ""} available on Shopify but not on monitor.`;
  if (shopifyUnmonitoredProducts.length === 0 && dupeCount > 0) {
    return `${base} All unique Shopify SKUs are monitored. The ${dupeCount}-product gap in the dashboard is from ${dupeCount} duplicate-SKU product${dupeCount !== 1 ? "s" : ""} in Shopify.`;
  }
  if (shopifyUnmonitoredProducts.length === 0 && missingSkuCount > 0) {
    return `${base} The ${missingSkuCount}-product gap in the dashboard is from ${missingSkuCount} Shopify product${missingSkuCount !== 1 ? "s" : ""} without SKU.`;
  }
  return base;
}

function getMonitorDuplicateSkuCount(monitors = allMonitors) {
  const monitorSkus = (Array.isArray(monitors) ? monitors : [])
    .map((monitor) => normalizeSku(monitor?.productData?.sku))
    .filter(Boolean);
  return monitorSkus.length - new Set(monitorSkus).size;
}

function computeMonitorDuplicateSkuGroups(monitors = allMonitors) {
  const bySku = new Map();
  for (const monitor of (Array.isArray(monitors) ? monitors : [])) {
    const sku = normalizeSku(monitor?.productData?.sku);
    if (!sku) continue;
    if (!bySku.has(sku)) bySku.set(sku, []);
    bySku.get(sku).push(monitor);
  }
  return [...bySku.entries()]
    .filter(([, group]) => group.length > 1)
      .map(([sku, monitorsWithSku]) => ({ sku, monitors: monitorsWithSku }))
      .sort((a, b) => a.sku.localeCompare(b.sku));
  }

function getMonitorDuplicateKeepScore(monitor) {
  let score = 0;
  if (Number(monitor?.shopifyProductId || 0)) score += 100;
  if (monitor?.productData?.name) score += 20;
  if (monitor?.productData?.sku) score += 20;
  if (getMonitorThumb(monitor)) score += 10;
  if (monitor?.lastExtractedData && Object.keys(monitor.lastExtractedData).length) score += 10;
  if (monitor?.status === "changed") score += 4;
  if (monitor?.status === "ok") score += 3;
  if (!monitor?.lastError) score += 2;
  score += Number(monitor?.lastCheckedAt || monitor?.createdAt || 0) / 10000000000000;
  return score;
}

function pickMonitorToKeepFromDuplicateGroup(monitors = []) {
  return [...monitors].sort((a, b) => {
    const scoreDiff = getMonitorDuplicateKeepScore(b) - getMonitorDuplicateKeepScore(a);
    if (scoreDiff) return scoreDiff;
    return String(a?.id || "").localeCompare(String(b?.id || ""));
  })[0] || null;
}

function autoSelectDuplicateMonitorExtras(groups = monitorDuplicateSkuGroups) {
  selectedDuplicateMonitorIds.clear();
  for (const group of groups) {
    const keepId = String(pickMonitorToKeepFromDuplicateGroup(group.monitors)?.id || "");
    for (const monitor of group.monitors || []) {
      const id = String(monitor?.id || "");
      if (id && id !== keepId) selectedDuplicateMonitorIds.add(id);
    }
  }
  return selectedDuplicateMonitorIds.size;
}

function findSkuMismatchedLinkedMonitors(products, monitors) {
  const productById = new Map((products || []).map((p) => [Number(p?.id || 0), p]).filter(([id]) => id));
  return (monitors || []).filter((monitor) => {
    const monitorSku = normalizeSku(monitor?.productData?.sku);
    if (!monitorSku) return false;
    const linkedProductId = Number(monitor?.shopifyProductId || 0);
    if (!linkedProductId) return false;
    const linked = productById.get(linkedProductId);
    if (!linked) return false;
    const productSku = getProductPrimarySku(linked);
    return productSku && productSku !== monitorSku;
  }).map((monitor) => {
    const linkedProductId = Number(monitor.shopifyProductId);
    const linked = productById.get(linkedProductId);
    return { monitor, monitorSku: normalizeSku(monitor.productData?.sku), productSku: getProductPrimarySku(linked) };
  });
}

function renderMonitorDuplicateGroupsHtml(groups = monitorDuplicateSkuGroups) {
  const missingSkuMonitors = getMonitorsMissingSku(allMonitors);
  const duplicateHtml = groups.length ? groups.map((group) => `
    <div style="padding:12px 16px;border-bottom:1px solid #f3f4f6">
      <p style="margin:0;font-size:12px;font-weight:700;color:#111">${escapeHtml(group.sku)}</p>
      <div style="margin-top:6px;display:flex;flex-direction:column;gap:6px">
        ${group.monitors.map((monitor) => {
          const thumb = getMonitorThumb(monitor);
          return `
            <label style="display:flex;align-items:flex-start;gap:10px;padding:8px 10px;border-radius:10px;background:#fff;border:1px solid #eef2f7;cursor:pointer">
              <input type="checkbox" class="monitor-duplicate-check" data-id="${escapeHtml(String(monitor.id))}" ${selectedDuplicateMonitorIds.has(String(monitor.id)) ? "checked" : ""}>
              ${thumb
                ? `<img src="${escapeHtml(thumb)}" alt="${escapeHtml(monitor.productData?.name || monitor.name || "Product image")}" style="width:48px;height:48px;object-fit:cover;border-radius:8px;flex-shrink:0;border:1px solid #e5e7eb">`
                : `<div style="width:48px;height:48px;border-radius:8px;flex-shrink:0;background:#f3f4f6;border:1px solid #e5e7eb;display:flex;align-items:center;justify-content:center;font-size:18px;color:#d1d5db">â–¡</div>`}
              <div style="min-width:0;flex:1">
                <p style="margin:0;font-size:12px;color:#111">${escapeHtml(monitor.productData?.name || monitor.name || "Untitled monitor")}</p>
                <p style="margin:2px 0 0;font-size:11px;color:#6b7280">${escapeHtml(monitor.id)}</p>
              </div>
            </label>
          `;
        }).join("")}
      </div>
    </div>
  `).join("") : `<p style="padding:16px;color:#9ca3af;font-size:13px">No duplicate monitor SKUs found.</p>`;

  const missingSkuHtml = missingSkuMonitors.length ? `
    <div style="border-top:1px solid #e5e7eb;background:#fcfcfd">
      <div style="padding:12px 16px;border-bottom:1px solid #f3f4f6">
        <p style="margin:0;font-size:12px;font-weight:700;color:#111">Monitors without SKU</p>
        <p style="margin:4px 0 0;font-size:12px;color:#6b7280">These monitors reduce the unique SKU count without being duplicates.</p>
      </div>
      ${missingSkuMonitors.map((monitor) => {
        const thumb = getMonitorThumb(monitor);
        return `
          <label style="display:flex;align-items:flex-start;gap:10px;padding:12px 16px;border-bottom:1px solid #f3f4f6;background:#fff;cursor:pointer">
            <input type="checkbox" class="monitor-duplicate-check" data-id="${escapeHtml(String(monitor.id))}" ${selectedDuplicateMonitorIds.has(String(monitor.id)) ? "checked" : ""}>
            ${thumb
              ? `<img src="${escapeHtml(thumb)}" alt="${escapeHtml(monitor.productData?.name || monitor.name || "Product image")}" style="width:48px;height:48px;object-fit:cover;border-radius:8px;flex-shrink:0;border:1px solid #e5e7eb">`
              : `<div style="width:48px;height:48px;border-radius:8px;flex-shrink:0;background:#f3f4f6;border:1px solid #e5e7eb;display:flex;align-items:center;justify-content:center;font-size:18px;color:#d1d5db">â–¡</div>`}
            <div style="min-width:0;flex:1">
              <p style="margin:0;font-size:12px;color:#111">${escapeHtml(monitor.productData?.name || monitor.name || "Untitled monitor")}</p>
              <p style="margin:2px 0 0;font-size:11px;color:#6b7280">${escapeHtml(monitor.id)}</p>
            </div>
          </label>
        `;
      }).join("")}
    </div>
  ` : `<p style="padding:16px;color:#9ca3af;font-size:13px">No monitors without SKU found.</p>`;

  const mismatchHtml = skuMismatchedLinkedMonitors.length ? `
    <div style="border-top:2px solid #fbbf24;background:#fffbeb">
      <div style="padding:12px 16px;border-bottom:1px solid #fde68a">
        <p style="margin:0;font-size:12px;font-weight:700;color:#92400e">SKU mismatch — monitor SKU â‰  linked Shopify product SKU (${skuMismatchedLinkedMonitors.length})</p>
        <p style="margin:4px 0 0;font-size:12px;color:#b45309">The monitor's recorded SKU differs from what's actually on the linked Shopify product. Pick a fix for each row below.</p>
      </div>
      ${skuMismatchedLinkedMonitors.map(({ monitor, monitorSku, productSku }) => {
        const thumb = getMonitorThumb(monitor);
        return `
          <div style="display:flex;align-items:flex-start;gap:10px;padding:10px 16px;border-bottom:1px solid #fde68a;background:#fffdf0">
            ${thumb
              ? `<img src="${escapeHtml(thumb)}" alt="" style="width:40px;height:40px;object-fit:cover;flex-shrink:0;border:1px solid #fde68a">`
              : `<div style="width:40px;height:40px;flex-shrink:0;background:#fef3c7;border:1px solid #fde68a;display:flex;align-items:center;justify-content:center;font-size:16px;color:#d1d5db">â–¡</div>`}
            <div style="min-width:0;flex:1">
              <p style="margin:0;font-size:12px;font-weight:600;color:#111">${escapeHtml(monitor.productData?.name || monitor.name || "Untitled monitor")}</p>
              <div style="margin:4px 0;display:flex;align-items:center;gap:12px;flex-wrap:wrap">
                <span style="font-size:11px;color:#b45309">Monitor SKU: <strong>${escapeHtml(monitorSku)}</strong></span>
                <span style="font-size:11px;color:#6b7280">→</span>
                <span style="font-size:11px;color:#6b7280">Shopify SKU: <strong>${escapeHtml(productSku)}</strong></span>
                <span style="font-size:10px;color:#9ca3af">Product ID: ${escapeHtml(String(monitor.shopifyProductId || ""))}</span>
              </div>
              <div style="display:flex;gap:6px;margin-top:6px;flex-wrap:wrap">
                <button class="ghost sku-mismatch-fix-monitor" data-monitor-id="${escapeHtml(String(monitor.id))}" data-sku="${escapeHtml(productSku)}" style="font-size:10px;padding:4px 10px;border-color:#fbbf24;color:#92400e">
                  Use Shopify SKU on monitor → <strong>${escapeHtml(productSku)}</strong>
                </button>
                <button class="ghost sku-mismatch-fix-shopify" data-monitor-id="${escapeHtml(String(monitor.id))}" style="font-size:10px;padding:4px 10px;border-color:#fbbf24;color:#92400e">
                  Push monitor SKU to Shopify → <strong>${escapeHtml(monitorSku)}</strong>
                </button>
              </div>
            </div>
          </div>
        `;
      }).join("")}
    </div>
  ` : "";

  return `
    ${mismatchHtml}
    <div style="border-top:1px solid #e5e7eb;background:#fcfcfd">
      <div style="padding:12px 16px;border-bottom:1px solid #f3f4f6">
        <p style="margin:0;font-size:12px;font-weight:700;color:#111">Duplicate monitor SKUs</p>
        <p style="margin:4px 0 0;font-size:12px;color:#6b7280">These are actual repeated monitor SKUs.</p>
      </div>
      ${duplicateHtml}
    </div>
    ${missingSkuHtml}
  `;
}

function renderMonitorDuplicatesPanel() {
  monitorDuplicatesList.innerHTML = renderMonitorDuplicateGroupsHtml(monitorDuplicateSkuGroups);
  monitorDuplicatesDeleteBtn.disabled = selectedDuplicateMonitorIds.size === 0;
}

async function loadDuplicateMonitorGroups() {
  monitorDuplicateSkuGroups = computeMonitorDuplicateSkuGroups(allMonitors);
  const missingSkuMonitors = getMonitorsMissingSku(allMonitors);
  const autoSelectedCount = autoSelectDuplicateMonitorExtras(monitorDuplicateSkuGroups);
  const duplicateCount = monitorDuplicateSkuGroups.reduce((count, group) => count + group.monitors.length, 0);
  const parts = [];
  if (monitorDuplicateSkuGroups.length) parts.push(`${monitorDuplicateSkuGroups.length} duplicate SKU group${monitorDuplicateSkuGroups.length !== 1 ? "s" : ""}`);
  if (missingSkuMonitors.length) parts.push(`${missingSkuMonitors.length} monitor${missingSkuMonitors.length !== 1 ? "s" : ""} without SKU`);
  if (skuMismatchedLinkedMonitors.length) parts.push(`${skuMismatchedLinkedMonitors.length} SKU mismatch${skuMismatchedLinkedMonitors.length !== 1 ? "es" : ""} (monitor SKU â‰  Shopify product SKU)`);
  monitorDuplicatesStatus.textContent = parts.length
    ? `Found: ${parts.join(", ")}.${autoSelectedCount ? ` Auto-selected ${autoSelectedCount} duplicate monitor${autoSelectedCount !== 1 ? "s" : ""} to delete.` : ""}`
    : "No monitor SKU issues found.";
  renderMonitorDuplicatesPanel();
}

function getMonitorNotShopifyStatusText() {
  const shopifyDupeCount = (shopifyProductCount != null && shopifyUniqueSkuCount != null)
    ? Math.max(0, shopifyProductCount - shopifyUniqueSkuCount - (shopifyMissingSkuCount || 0))
    : 0;
  const shopifyMissingCount = shopifyMissingSkuCount || 0;
  const monitorDupeCount = getMonitorDuplicateSkuCount(allMonitors);
  const monitorUniqueSkuCount = getMonitorUniqueSkuCount(allMonitors);
  const shopifySkuGap = shopifyUniqueSkuCount == null ? 0 : Math.max(0, monitorUniqueSkuCount - shopifyUniqueSkuCount);
  const skuGapExampleText = shopifySkuGapDetails.length
    ? `, e.g. ${shopifySkuGapDetails.slice(0, 5).map((detail) => detail.sku).join(", ")}`
    : "";
  const base = `${monitorNotShopifyMonitors.length} monitor${monitorNotShopifyMonitors.length !== 1 ? "s" : ""} missing its own Shopify product.`;
  if (monitorNotShopifyMonitors.length === 0) {
    const notes = [];
    if (shopifySkuGap > 0) {
      notes.push(`${shopifySkuGap} monitor SKU${shopifySkuGap !== 1 ? "s" : ""} still not matched by Shopify SKU coverage${skuGapExampleText}`);
    }
    if (monitorDupeCount > 0) {
      notes.push(`${monitorDupeCount} duplicate-SKU monitor${monitorDupeCount !== 1 ? "s" : ""}`);
    }
    if (shopifyDupeCount > 0) {
      notes.push(`${shopifyDupeCount} duplicate-SKU Shopify product${shopifyDupeCount !== 1 ? "s" : ""}`);
    }
    if (shopifyMissingCount > 0) {
      notes.push(`${shopifyMissingCount} Shopify product${shopifyMissingCount !== 1 ? "s" : ""} without SKU`);
    }
    if (notes.length) {
      return `${base} All unique monitor SKUs have a matching Shopify product. The count gap is explained by ${notes.join(" and ")}.`;
    }
  }
  return base;
}

function computeShopifyMonitorDiffs(products, monitors) {
  const safeMonitors = Array.isArray(monitors) ? monitors : [];
  const primarySkuToProduct = new Map();
  const productIds = new Set();
  for (const product of (products || [])) {
    const productId = Number(product?.id || 0);
    if (productId) productIds.add(productId);
    const primarySku = getProductPrimarySku(product);
    if (primarySku && !primarySkuToProduct.has(primarySku)) primarySkuToProduct.set(primarySku, product);
  }

  const assignedProductIds = new Set();
  const assignedPrimarySkus = new Set();
  const monitorOnlyMonitors = [];
  for (const monitor of safeMonitors) {
    const monitorSku = normalizeSku(monitor?.productData?.sku);
    const linkedProductId = Number(monitor?.shopifyProductId || 0);
    if (linkedProductId && productIds.has(linkedProductId) && !assignedProductIds.has(linkedProductId)) {
      assignedProductIds.add(linkedProductId);
      continue;
    }

    const product = monitorSku ? primarySkuToProduct.get(monitorSku) : null;
    const productId = Number(product?.id || 0);
    if (product && !assignedPrimarySkus.has(monitorSku) && (!productId || !assignedProductIds.has(productId))) {
      assignedPrimarySkus.add(monitorSku);
      if (productId) assignedProductIds.add(productId);
      continue;
    }

    monitorOnlyMonitors.push(monitor);
  }

  const shopifyOnlyProducts = [];
  for (const product of (products || [])) {
    const productId = Number(product?.id || 0);
    if (productId && assignedProductIds.has(productId)) continue;
    const productSku = getProductPrimarySku(product);
    if (!productSku) continue;
    shopifyOnlyProducts.push(product);
  }

  return { shopifyOnlyProducts, monitorOnlyMonitors };
}

function findMonitorShopifySkuGapDetails(products, monitors) {
  const monitorBySku = new Map();
  for (const monitor of (Array.isArray(monitors) ? monitors : [])) {
    const sku = normalizeSku(monitor?.productData?.sku);
    if (sku && !monitorBySku.has(sku)) monitorBySku.set(sku, monitor);
  }
  const comparableSkuSet = getShopifyComparableSkuSet(products, monitors);
  const allShopifySkuSet = new Set();
  for (const product of (products || [])) {
    getProductSkuSet(product).forEach((sku) => allShopifySkuSet.add(sku));
  }
  return [...monitorBySku.keys()]
    .filter((sku) => !comparableSkuSet.has(sku))
    .sort()
    .map((sku) => ({
      sku,
      monitor: monitorBySku.get(sku),
      hasAnyShopifySkuMatch: allShopifySkuSet.has(sku)
    }));
}

function findShopifySkuGapProducts(products, monitors) {
  const details = findMonitorShopifySkuGapDetails(products, monitors);
  if (!details.length) return [];
  const detailSkus = new Set(details.map((detail) => detail.sku));
  const detailBySku = new Map(details.map((detail) => [detail.sku, detail]));
  const productById = new Map((products || []).map((product) => [Number(product?.id || 0), product]).filter(([id]) => id));
  const results = [];
  const seen = new Set();
  const addResult = (product, detail, matchType = "") => {
    const productId = Number(product?.id || 0);
    const key = `${productId || "missing"}:${detail.sku}`;
    if (seen.has(key)) return;
    seen.add(key);
    const monitor = detail.monitor;
    results.push({
      id: productId || "",
      sku: detail.sku,
      primarySku: getProductPrimarySku(product),
      title: product?.title || "",
      monitorId: monitor?.id || "",
      monitorName: monitor?.productData?.name || monitor?.name || "",
      matchType
    });
  };

  for (const detail of details) {
    const monitor = detail.monitor;
    const linkedProductId = Number(monitor?.shopifyProductId || 0);
    const linkedProduct = linkedProductId ? productById.get(linkedProductId) : null;
    if (linkedProduct) {
      addResult(linkedProduct, detail, "linked");
      continue;
    }
    const titleMatchedProduct = findUniqueProductByMonitorIdentity(products, monitor);
    if (titleMatchedProduct) addResult(titleMatchedProduct, detail, "title");
  }

  for (const product of (products || [])) {
    const productId = Number(product?.id || 0);
    const matchingSku = [...getProductSkuSet(product)].find((sku) => detailSkus.has(sku));
    if (!matchingSku) continue;
    addResult(product, detailBySku.get(matchingSku), "sku");
  }
  for (const detail of details) {
    if (results.some((item) => item.sku === detail.sku)) continue;
    const monitor = detail.monitor;
    results.push({
      id: "",
      sku: detail.sku,
      primarySku: "",
      title: detail.hasAnyShopifySkuMatch ? "Matched in Shopify SKUs, but not as a primary SKU" : "No Shopify product found in cached SKU list",
      monitorId: monitor?.id || "",
      monitorName: monitor?.productData?.name || monitor?.name || ""
    });
  }
  return results.sort((a, b) => a.sku.localeCompare(b.sku));
}

function computeMonitorMissingFromShopifyIds(products, monitors) {
  const safeMonitors = Array.isArray(monitors) ? monitors : [];
  const productIds = new Set();
  const primarySkuToProduct = new Map();
  for (const product of (products || [])) {
    const productId = Number(product?.id || 0);
    if (productId) productIds.add(productId);
    const primarySku = getProductPrimarySku(product);
    if (primarySku && !primarySkuToProduct.has(primarySku)) primarySkuToProduct.set(primarySku, product);
  }

  const missingIds = new Set();
  const assignedProductIds = new Set();
  const assignedPrimarySkus = new Set();
  for (const monitor of safeMonitors) {
    const linkedProductId = Number(monitor?.shopifyProductId || 0);
    const monitorSku = normalizeSku(monitor?.productData?.sku);
    if (linkedProductId && productIds.has(linkedProductId) && !assignedProductIds.has(linkedProductId)) {
      assignedProductIds.add(linkedProductId);
      continue;
    }

    const product = monitorSku ? primarySkuToProduct.get(monitorSku) : null;
    const productId = Number(product?.id || 0);
    if (product && !assignedPrimarySkus.has(monitorSku) && (!productId || !assignedProductIds.has(productId))) {
      assignedPrimarySkus.add(monitorSku);
      if (productId) assignedProductIds.add(productId);
      continue;
    }

    missingIds.add(String(monitor?.id || ""));
  }

  return missingIds;
}

function renderShopifyUnmonitoredList() {
  if (!shopifyUnmonitoredProducts.length) {
    shopifyUnmonitoredList.innerHTML = `<p style="padding:24px;text-align:center;color:#9ca3af;font-size:13px">No Shopify-only products found.</p>`;
    shopifyUnmonitoredClearSelectedBtn.disabled = true;
    shopifyUnmonitoredDeleteSelectedBtn.disabled = true;
    shopifyUnmonitoredDeleteAllBtn.disabled = true;
    return;
  }
  shopifyUnmonitoredList.innerHTML = shopifyUnmonitoredProducts.map((item) => `
    <label style="display:flex;align-items:flex-start;gap:10px;padding:12px 16px;border-bottom:1px solid #f3f4f6;cursor:pointer">
      <input type="checkbox" class="shopify-unmonitored-check" data-id="${escapeHtml(String(item.id))}" ${selectedShopifyUnmonitoredIds.has(String(item.id)) ? "checked" : ""}>
      <div style="min-width:0;flex:1">
        <p style="margin:0;font-size:12px;font-weight:700;color:#111">${escapeHtml(item.sku)}</p>
        ${item.title ? `<p style="margin:2px 0 0;font-size:12px;color:#5f6c7b">${escapeHtml(item.title)}</p>` : ""}
      </div>
    </label>
  `).join("");
  shopifyUnmonitoredClearSelectedBtn.disabled = selectedShopifyUnmonitoredIds.size === 0;
  shopifyUnmonitoredDeleteSelectedBtn.disabled = selectedShopifyUnmonitoredIds.size === 0;
  shopifyUnmonitoredDeleteAllBtn.disabled = shopifyUnmonitoredProducts.length === 0;
}

async function loadShopifyUnmonitoredProducts(forceRefresh = false) {
  if (!await isConnected()) {
    shopifyUnmonitoredStatus.textContent = "Connect Shopify first.";
    shopifyUnmonitoredProducts = [];
    selectedShopifyUnmonitoredIds.clear();
    renderShopifyUnmonitoredList();
    return;
  }
  shopifyUnmonitoredStatus.textContent = "Loading Shopify products…";
  shopifyUnmonitoredRefreshBtn.disabled = true;
  try {
    if (forceRefresh) clearShopifyProductsSnapshotCache();
    const products = await getShopifyProductsSnapshot();
    shopifyComparableSkuSet = getShopifyComparableSkuSet(products, allMonitors);
    shopifyProductCount = products.length;
    shopifyUniqueSkuCount = shopifyComparableSkuSet.size;
    shopifyMissingSkuCount = getShopifyMissingSkuCount(products);
    shopifySkuGapDetails = findMonitorShopifySkuGapDetails(products, allMonitors);
    shopifySkuGapProducts = findShopifySkuGapProducts(products, allMonitors);
    const diffs = computeShopifyMonitorDiffs(products, allMonitors);
    shopifyUnmonitoredProducts = diffs.shopifyOnlyProducts;
    monitorOnlyMonitorIds = new Set((diffs.monitorOnlyMonitors || []).map((monitor) => String(monitor.id)));
    selectedShopifyUnmonitoredIds.clear();
    shopifyUnmonitoredStatus.textContent = getShopifyUnmonitoredStatusText();
    renderShopifyUnmonitoredList();
  } catch (error) {
    shopifyUnmonitoredStatus.textContent = `Error: ${error.message || error}`;
    shopifyUnmonitoredProducts = [];
    selectedShopifyUnmonitoredIds.clear();
    renderShopifyUnmonitoredList();
  }
  renderStats(getFilteredMonitors());
  updateBulkBtn();
  shopifyUnmonitoredRefreshBtn.disabled = false;
}

function renderMonitorNotShopifyList() {
  const duplicateDeleteBtn = ensureMonitorDuplicateDeleteBtn();
  const hasMonitorOnly = monitorNotShopifyMonitors.length > 0;
  const hasDuplicates = monitorDuplicateSkuGroups.length > 0;
  const hasSkuGapProducts = shopifySkuGapProducts.length > 0;
  if (!hasMonitorOnly && !hasDuplicates && !hasSkuGapProducts) {
    monitorNotShopifyList.innerHTML = `<p style="padding:24px;text-align:center;color:#9ca3af;font-size:13px">No monitor-only items found.</p>`;
    monitorNotShopifyClearSelectedBtn.disabled = true;
    if (duplicateDeleteBtn) duplicateDeleteBtn.disabled = true;
    return;
  }
  const monitorOnlyHtml = hasMonitorOnly ? monitorNotShopifyMonitors.map((monitor) => {
    const sku = monitor.productData?.sku || "No SKU";
    const title = monitor.productData?.name || monitor.name || "";
    const linkedProductId = monitor.shopifyProductId ? `Linked Shopify product ID: ${monitor.shopifyProductId}` : "";
    return `
      <label style="display:flex;align-items:flex-start;gap:10px;padding:12px 16px;border-bottom:1px solid #f3f4f6;cursor:pointer">
        <input type="checkbox" class="monitor-not-shopify-check" data-id="${escapeHtml(String(monitor.id))}" ${selectedMonitorNotShopifyIds.has(String(monitor.id)) ? "checked" : ""}>
        <div style="min-width:0;flex:1">
          <p style="margin:0;font-size:12px;font-weight:700;color:#111">${escapeHtml(sku)}</p>
          ${title ? `<p style="margin:2px 0 0;font-size:12px;color:#5f6c7b">${escapeHtml(title)}</p>` : ""}
          ${linkedProductId ? `<p style="margin:2px 0 0;font-size:11px;color:#9ca3af">${escapeHtml(linkedProductId)} is already used by another monitor.</p>` : ""}
        </div>
      </label>
    `;
  }).join("") : `<p style="padding:16px;color:#9ca3af;font-size:13px">No monitor-only items found.</p>`;

  const duplicatesHtml = hasDuplicates ? `
    <div style="border-top:1px solid #e5e7eb;background:#fcfcfd">
      <div style="padding:12px 16px;border-bottom:1px solid #f3f4f6">
        <p style="margin:0;font-size:12px;font-weight:700;color:#111">Duplicate monitor SKUs</p>
        <p style="margin:4px 0 0;font-size:12px;color:#6b7280">These duplicates explain the count gap.</p>
      </div>
      ${monitorDuplicateSkuGroups.map((group) => `
        <div style="padding:12px 16px;border-bottom:1px solid #f3f4f6">
          <p style="margin:0;font-size:12px;font-weight:700;color:#111">${escapeHtml(group.sku)}</p>
          <div style="margin-top:6px;display:flex;flex-direction:column;gap:6px">
            ${group.monitors.map((monitor) => `
              ${(() => {
                const thumb = getMonitorThumb(monitor);
                return `
              <label style="display:flex;align-items:flex-start;gap:10px;padding:8px 10px;border-radius:10px;background:#fff;border:1px solid #eef2f7;cursor:pointer">
                <input type="checkbox" class="monitor-duplicate-check" data-id="${escapeHtml(String(monitor.id))}" ${selectedDuplicateMonitorIds.has(String(monitor.id)) ? "checked" : ""}>
                ${thumb
                  ? `<img src="${escapeHtml(thumb)}" alt="${escapeHtml(monitor.productData?.name || monitor.name || "Product image")}" style="width:48px;height:48px;object-fit:cover;border-radius:8px;flex-shrink:0;border:1px solid #e5e7eb">`
                  : `<div style="width:48px;height:48px;border-radius:8px;flex-shrink:0;background:#f3f4f6;border:1px solid #e5e7eb;display:flex;align-items:center;justify-content:center;font-size:18px;color:#d1d5db">â–¡</div>`}
                <div style="min-width:0;flex:1">
                  <p style="margin:0;font-size:12px;color:#111">${escapeHtml(monitor.productData?.name || monitor.name || "Untitled monitor")}</p>
                  <p style="margin:2px 0 0;font-size:11px;color:#6b7280">${escapeHtml(monitor.id)}</p>
                </div>
              </label>
                `;
              })()}
            `).join("")}
          </div>
        </div>
      `).join("")}
    </div>
  ` : "";

  const skuGapHtml = hasSkuGapProducts ? `
    <div style="border-top:1px solid #e5e7eb;background:#fcfcfd">
      <div style="padding:12px 16px;border-bottom:1px solid #f3f4f6">
        <p style="margin:0;font-size:12px;font-weight:700;color:#111">SKU count gap details</p>
        <p style="margin:4px 0 0;font-size:12px;color:#6b7280">These monitor SKUs still do not match Shopify by SKU coverage or linked product ID.</p>
      </div>
      ${shopifySkuGapProducts.map((item) => `
        <label style="display:flex;align-items:flex-start;gap:10px;padding:12px 16px;border-bottom:1px solid #f3f4f6;background:#fff;cursor:pointer">
          <input type="checkbox" class="monitor-not-shopify-check" data-id="${escapeHtml(String(item.monitorId || ""))}" ${item.monitorId && selectedMonitorNotShopifyIds.has(String(item.monitorId)) ? "checked" : ""} ${item.monitorId ? "" : "disabled"}>
          <div style="min-width:0;flex:1">
            <p style="margin:0;font-size:12px;font-weight:700;color:#111">${escapeHtml(item.sku)}</p>
            <p style="margin:3px 0 0;font-size:12px;color:#5f6c7b">${escapeHtml(item.title || "Untitled Shopify product")}</p>
            <p style="margin:3px 0 0;font-size:11px;color:#9ca3af">Shopify primary SKU: ${escapeHtml(item.primarySku || "none")} · Product ID: ${escapeHtml(String(item.id || ""))}${item.matchType ? ` · Match: ${escapeHtml(item.matchType)}` : ""}</p>
            ${item.monitorName ? `<p style="margin:3px 0 0;font-size:11px;color:#9ca3af">Monitor: ${escapeHtml(item.monitorName)}${item.monitorId ? ` · ${escapeHtml(item.monitorId)}` : ""}</p>` : ""}
          </div>
        </label>
      `).join("")}
    </div>
  ` : "";

  monitorNotShopifyList.innerHTML = `${hasMonitorOnly ? `<div>${monitorOnlyHtml}</div>` : ""}${duplicatesHtml}${skuGapHtml}`;
  monitorNotShopifyClearSelectedBtn.disabled = selectedMonitorNotShopifyIds.size === 0;
  if (duplicateDeleteBtn) duplicateDeleteBtn.disabled = selectedDuplicateMonitorIds.size === 0;
}

async function loadMonitorNotShopifyMonitors(forceRefresh = false) {
  if (!await isConnected()) {
    monitorNotShopifyStatus.textContent = "Connect Shopify first.";
    monitorNotShopifyMonitors = [];
    monitorDuplicateSkuGroups = [];
    shopifySkuGapProducts = [];
    selectedMonitorNotShopifyIds.forEach((id) => checkedIds.delete(id));
    selectedMonitorNotShopifyIds.clear();
    updateBulkBtn();
    selectedDuplicateMonitorIds.clear();
    renderMonitorNotShopifyList();
    return;
  }
  monitorNotShopifyStatus.textContent = "Loading monitor comparison…";
  monitorNotShopifyRefreshBtn.disabled = true;
  try {
    if (forceRefresh) clearShopifyProductsSnapshotCache();
    const products = await getShopifyProductsSnapshot();
    shopifyComparableSkuSet = getShopifyComparableSkuSet(products, allMonitors);
    shopifyProductCount = products.length;
    shopifyUniqueSkuCount = shopifyComparableSkuSet.size;
    shopifyMissingSkuCount = getShopifyMissingSkuCount(products);
    shopifySkuGapDetails = findMonitorShopifySkuGapDetails(products, allMonitors);
    shopifySkuGapProducts = findShopifySkuGapProducts(products, allMonitors);
    const diffs = computeShopifyMonitorDiffs(products, allMonitors);
    monitorNotShopifyMonitors = diffs.monitorOnlyMonitors;
    monitorOnlyMonitorIds = new Set(monitorNotShopifyMonitors.map((monitor) => String(monitor.id)));
    monitorDuplicateSkuGroups = computeMonitorDuplicateSkuGroups(allMonitors);
    skuMismatchedLinkedMonitors = findSkuMismatchedLinkedMonitors(products, allMonitors);
    selectedMonitorNotShopifyIds.forEach((id) => checkedIds.delete(id));
    selectedMonitorNotShopifyIds.clear();
    updateBulkBtn();
    autoSelectDuplicateMonitorExtras(monitorDuplicateSkuGroups);
    monitorNotShopifyStatus.textContent = getMonitorNotShopifyStatusText();
    renderMonitorNotShopifyList();
  } catch (error) {
    monitorNotShopifyStatus.textContent = `Error: ${error.message || error}`;
    monitorNotShopifyMonitors = [];
    monitorDuplicateSkuGroups = [];
    shopifySkuGapProducts = [];
    selectedMonitorNotShopifyIds.forEach((id) => checkedIds.delete(id));
    selectedMonitorNotShopifyIds.clear();
    updateBulkBtn();
    selectedDuplicateMonitorIds.clear();
    renderMonitorNotShopifyList();
  }
  renderStats(getFilteredMonitors());
  updateBulkBtn();
  monitorNotShopifyRefreshBtn.disabled = false;
}

async function loadShopifyOutOfStockMonitors(forceRefresh = false) {
  if (!await isConnected()) {
    shopifyOutOfStockMonitorIds = null;
    updateOutOfStockButtons();
    renderGrid(getFilteredMonitors());
    window.alert("Connect Shopify first.");
    return;
  }

  const linkedMonitors = allMonitors.filter((monitor) => monitor.shopifyProductId);
  if (!linkedMonitors.length) {
    shopifyOutOfStockMonitorIds = new Set();
    updateOutOfStockButtons();
    renderGrid(getFilteredMonitors());
    return;
  }

  const originalText = checkShopifyOutOfStockBtn.textContent;
  checkShopifyOutOfStockBtn.disabled = true;
  try {
    const productIds = [...new Set(linkedMonitors.map((monitor) => Number(monitor.shopifyProductId)).filter(Boolean))];
    if (forceRefresh) clearShopifyProductsSnapshotCache();
    setProgressLabel(checkShopifyOutOfStockBtn, "Checking", 0, productIds.length);
    const outOfStockProductIds = await getFullyOutOfStockProductIds(productIds, (current, total) => {
      setProgressLabel(checkShopifyOutOfStockBtn, "Checking", current, total);
    });
    const outOfStockIdSet = new Set(outOfStockProductIds.map((id) => Number(id)));
    shopifyOutOfStockMonitorIds = new Set(
      linkedMonitors
        .filter((monitor) => outOfStockIdSet.has(Number(monitor.shopifyProductId)))
        .map((monitor) => monitor.id)
    );
    updateOutOfStockButtons();
    renderGrid(getFilteredMonitors());
  } catch (error) {
    shopifyOutOfStockMonitorIds = null;
    updateOutOfStockButtons();
    renderGrid(getFilteredMonitors());
    window.alert(`Failed to load Shopify out-of-stock monitors: ${error.message || error}`);
  } finally {
    checkShopifyOutOfStockBtn.textContent = originalText;
    checkShopifyOutOfStockBtn.disabled = false;
    updateOutOfStockButtons();
  }
}

async function clearShopifyUnmonitoredList() {
  const ids = shopifyUnmonitoredProducts.map((item) => String(item.id));
  if (!ids.length) return;
  const originalText = shopifyUnmonitoredClearBtn.textContent;
  try {
    await runButtonProgress(shopifyUnmonitoredClearBtn, ids, "Clearing", async (id) => {
      shopifyUnmonitoredProducts = shopifyUnmonitoredProducts.filter((item) => String(item.id) !== id);
      selectedShopifyUnmonitoredIds.delete(id);
      renderShopifyUnmonitoredList();
    });
    shopifyUnmonitoredStatus.textContent = "List cleared.";
  } finally {
    shopifyUnmonitoredClearBtn.textContent = originalText;
  }
}

async function clearSelectedShopifyUnmonitoredList() {
  const ids = new Set([...selectedShopifyUnmonitoredIds]);
  if (!ids.size) return;
  const originalText = shopifyUnmonitoredClearSelectedBtn.textContent;
  try {
    await runButtonProgress(shopifyUnmonitoredClearSelectedBtn, [...ids], "Clearing", async (id) => {
      shopifyUnmonitoredProducts = shopifyUnmonitoredProducts.filter((item) => String(item.id) !== id);
      selectedShopifyUnmonitoredIds.delete(id);
      renderShopifyUnmonitoredList();
    });
    shopifyUnmonitoredStatus.textContent = getShopifyUnmonitoredStatusText();
  } finally {
    shopifyUnmonitoredClearSelectedBtn.textContent = originalText;
  }
}

async function deleteShopifyUnmonitored(ids, onProgress = null) {
  const productIds = [...new Set(ids.map((id) => Number(id)).filter(Boolean))];
  if (!productIds.length) return;
  for (let index = 0; index < productIds.length; index++) {
    if (onProgress) onProgress(index + 1, productIds.length);
    await deleteShopifyProducts([productIds[index]]);
  }
  shopifyUnmonitoredProducts = shopifyUnmonitoredProducts.filter((item) => !productIds.includes(Number(item.id)));
  productIds.forEach((id) => selectedShopifyUnmonitoredIds.delete(String(id)));
  shopifyUnmonitoredStatus.textContent = getShopifyUnmonitoredStatusText();
  renderShopifyUnmonitoredList();
  await refreshShopifyDashboardState(true);
}

async function clearMonitorNotShopifyList() {
  const ids = monitorNotShopifyMonitors.map((monitor) => String(monitor.id));
  if (!ids.length) return;
  const originalText = monitorNotShopifyClearBtn.textContent;
  try {
    await runButtonProgress(monitorNotShopifyClearBtn, ids, "Clearing", async (id) => {
      monitorNotShopifyMonitors = monitorNotShopifyMonitors.filter((monitor) => String(monitor.id) !== id);
      selectedMonitorNotShopifyIds.delete(id);
      checkedIds.delete(id);
      updateBulkBtn();
      renderMonitorNotShopifyList();
    });
    monitorNotShopifyStatus.textContent = "List cleared.";
  } finally {
    monitorNotShopifyClearBtn.textContent = originalText;
  }
}

async function clearSelectedMonitorNotShopifyList() {
  const ids = new Set([...selectedMonitorNotShopifyIds]);
  if (!ids.size) return;
  const originalText = monitorNotShopifyClearSelectedBtn.textContent;
  try {
    await runButtonProgress(monitorNotShopifyClearSelectedBtn, [...ids], "Clearing", async (id) => {
      monitorNotShopifyMonitors = monitorNotShopifyMonitors.filter((monitor) => String(monitor.id) !== id);
      selectedMonitorNotShopifyIds.delete(id);
      checkedIds.delete(id);
      updateBulkBtn();
      renderMonitorNotShopifyList();
    });
    monitorNotShopifyStatus.textContent = getMonitorNotShopifyStatusText();
  } finally {
    monitorNotShopifyClearSelectedBtn.textContent = originalText;
  }
}

async function deleteSelectedDuplicateMonitors() {
  const ids = [...selectedDuplicateMonitorIds];
  if (!ids.length) return;
  if (!window.confirm(`Delete ${ids.length} selected duplicate monitor${ids.length > 1 ? "s" : ""} from monitors? This cannot be undone.`)) return;

  const button = monitorDuplicatesPanel.style.display !== "none"
    ? monitorDuplicatesDeleteBtn
    : ensureMonitorDuplicateDeleteBtn();
  const originalText = button?.textContent || "Delete selected monitors";
  const failures = [];

  try {
    await runButtonProgress(button, ids, "Deleting", async (id) => {
      const monitor = _getMonitor(String(id));
      if (!monitor) {
        selectedDuplicateMonitorIds.delete(String(id));
        renderMonitorNotShopifyList();
        renderMonitorDuplicatesPanel();
        return;
      }

      try {
        await chrome.runtime.sendMessage({ type: "delete-monitor", monitorId: monitor.id });
        if (monitor.id === selectedMonitorId) {
          selectedMonitorId = null;
          monitorDetail.style.display = "none";
        }
        selectedDuplicateMonitorIds.delete(String(id));
      } catch (error) {
        failures.push({
          name: monitor.productData?.sku || monitor.name || monitor.id,
          reason: error?.message || String(error) || "Unknown error"
        });
      }

      renderMonitorNotShopifyList();
      renderMonitorDuplicatesPanel();
    });

    await loadDashboard(true);
    await loadDuplicateMonitorGroups();
    if (monitorNotShopifyPanel.style.display !== "none") {
      await loadMonitorNotShopifyMonitors(true);
    }
    refreshUndoCount();

    if (failures.length) {
      const details = failures.map((failure) => `${failure.name}: ${failure.reason}`).join("\n");
      monitorNotShopifyStatus.textContent = `Deleted ${ids.length - failures.length}/${ids.length} selected monitor${ids.length !== 1 ? "s" : ""}.`;
      monitorDuplicatesStatus.textContent = `Deleted ${ids.length - failures.length}/${ids.length} selected monitor${ids.length !== 1 ? "s" : ""}.`;
      alert(`Some duplicate deletions failed:\n\n${details}`);
    } else {
      monitorNotShopifyStatus.textContent = `Deleted ${ids.length} selected monitor${ids.length !== 1 ? "s" : ""} from monitors.`;
      monitorDuplicatesStatus.textContent = `Deleted ${ids.length} selected monitor${ids.length !== 1 ? "s" : ""} from monitors.`;
    }
  } finally {
    const safeButton = ensureMonitorDuplicateDeleteBtn();
    if (safeButton) safeButton.textContent = originalText;
    monitorDuplicatesDeleteBtn.textContent = "Delete selected monitors";
  }
}

// ── Duplicate SKU cleanup panel ────────────────────────────────────────────

function computeDuplicateSkuGroups(products, skuMapping, monitors) {
  // Source of truth priority (mirrors shopify.js updateShopifyForMonitor line 647):
  //   1. monitor.shopifyProductId  — the product a monitor directly pushes updates to
  //   2. skuMapping[baseSku]       — fallback used when no monitor ID is set
  //   3. lowest product ID         — oldest product, last resort
  // A product is only safe to delete when it appears in NONE of the above for its SKU.

  const monitorReferencedIds = new Set(
    (monitors || []).map((m) => Number(m.shopifyProductId)).filter(Boolean)
  );
  const mappedIds = new Set(Object.values(skuMapping).map(Number));

  // Build a per-SKU map: sku → Set of product IDs that are protected (monitor or mapping)
  const skuProtectedIds = new Map();
  for (const m of (monitors || [])) {
    const sku = normalizeSku(m.productData?.sku);
    const id = Number(m.shopifyProductId);
    if (sku && id) {
      if (!skuProtectedIds.has(sku)) skuProtectedIds.set(sku, new Set());
      skuProtectedIds.get(sku).add(id);
    }
  }
  for (const [sku, id] of Object.entries(skuMapping)) {
    const nid = Number(id);
    if (sku && nid) {
      if (!skuProtectedIds.has(sku)) skuProtectedIds.set(sku, new Set());
      skuProtectedIds.get(sku).add(nid);
    }
  }

  const bySkuMap = new Map();
  for (const product of products) {
    const sku = getProductPrimarySku(product);
    if (!sku) continue;
    if (!bySkuMap.has(sku)) bySkuMap.set(sku, []);
    bySkuMap.get(sku).push(product);
  }

  const groups = [];
  for (const [sku, group] of bySkuMap) {
    if (group.length < 2) continue;

    const protected_ = skuProtectedIds.get(sku) || new Set();

    // Priority 1: monitor-referenced product
    const monitorReferenced = group.find((p) => monitorReferencedIds.has(Number(p.id)));
    // Priority 2: mapping product (only if no monitor reference exists)
    const mapped = !monitorReferenced && group.find((p) => mappedIds.has(Number(p.id)));
    // Priority 3: oldest (lowest Shopify ID)
    const oldest = group.reduce((a, b) => Number(a.id) < Number(b.id) ? a : b);

    const keepProduct = monitorReferenced || mapped || oldest;
    const keepReason = monitorReferenced ? "monitor" : mapped ? "mapping" : "oldest";

    const safeExtras = [];
    const protectedProducts = [];
    for (const product of group) {
      if (Number(product.id) === Number(keepProduct.id)) continue;
      if (protected_.has(Number(product.id))) protectedProducts.push(product);
      else safeExtras.push(product);
    }

    const blockedReasons = [];
    if (group.some((p) => monitorReferencedIds.has(Number(p.id)) && Number(p.id) !== Number(keepProduct.id))) {
      blockedReasons.push("used by monitor");
    }
    if (group.some((p) => mappedIds.has(Number(p.id)) && Number(p.id) !== Number(keepProduct.id))) {
      blockedReasons.push("in SKU mapping");
    }

    groups.push({
      sku,
      keepProduct,
      keepReason,
      extraProducts: safeExtras,
      protectedProducts,
      blockedReasons
    });
  }
  groups.sort((a, b) => a.sku.localeCompare(b.sku));
  return groups;
}

function renderDuplicateSkuPanel() {
  const totalExtras = skuDuplicateGroups.reduce((n, g) => n + g.extraProducts.length, 0);
  skuDuplicatesDeleteBtn.disabled = selectedDuplicateIds.size === 0;
  skuDuplicatesSelectAllBtn.disabled = totalExtras === 0;
  skuDuplicatesDeselectBtn.disabled = selectedDuplicateIds.size === 0;

  if (!skuDuplicateGroups.length && !shopifyMissingSkuProducts.length) {
    skuDuplicatesList.innerHTML = `<p style="padding:24px;text-align:center;color:#9ca3af;font-size:13px">No duplicate products are currently eligible for automatic cleanup.</p>`;
      return;
  }

  const keepReasonLabel = { monitor: "used by monitor", mapping: "in SKU mapping", oldest: "oldest product" };

  function productInfoHtml(p) {
    const thumb = p.image
      ? `<img src="${escapeHtml(p.image)}" alt="" style="width:48px;height:48px;object-fit:cover;border-radius:4px;flex-shrink:0;border:1px solid #e5e7eb">`
      : `<div style="width:48px;height:48px;border-radius:4px;flex-shrink:0;background:#f3f4f6;border:1px solid #e5e7eb;display:flex;align-items:center;justify-content:center;font-size:18px;color:#d1d5db">â–¡</div>`;
    const adminUrl = skuDuplicatesAdminBase ? `${skuDuplicatesAdminBase}/${p.id}` : "";
    return `
      <div style="display:flex;align-items:center;gap:10px;min-width:0;flex:1">
        ${thumb}
        <div style="min-width:0;flex:1">
          <p style="margin:0;font-size:12px;font-weight:700;color:#111;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(p.title || "(no title)")}</p>
          <p style="margin:3px 0 0;font-size:11px;color:#5f6c7b">Supplier SKU: <strong>${escapeHtml(p.sku || "—")}</strong></p>
          <p style="margin:2px 0 0;font-size:11px;color:#9ca3af">
            Shopify ID: ${p.id}${adminUrl ? `&ensp;<a href="${adminUrl}" target="_blank" style="color:#5c6ac4;text-decoration:none">View â†—</a>` : ""}
          </p>
        </div>
      </div>`;
  }

  const duplicateGroupsHtml = skuDuplicateGroups.map(({ sku, keepProduct, keepReason, extraProducts, protectedProducts = [], blockedReasons = [] }) => {
    const keepHtml = `
      <div style="display:flex;align-items:center;gap:10px;padding:12px 16px;background:#f0fdf4;border-bottom:1px solid #bbf7d0">
        <span style="flex-shrink:0;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:700;background:#16a34a;color:#fff;white-space:nowrap">KEEP · ${escapeHtml(keepReasonLabel[keepReason] || keepReason)}</span>
        ${productInfoHtml(keepProduct)}
      </div>`;
    const extrasHtml = extraProducts.map((p) => {
      const idStr = String(p.id);
      const checked = selectedDuplicateIds.has(idStr) ? "checked" : "";
      return `
        <label style="display:flex;align-items:center;gap:10px;padding:12px 16px;background:#fff5f5;border-bottom:1px solid #fecaca;cursor:pointer">
          <input type="checkbox" class="sku-dup-check" data-id="${idStr}" ${checked} style="flex-shrink:0">
          <span style="flex-shrink:0;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:700;background:#dc2626;color:#fff;white-space:nowrap">DELETE</span>
          ${productInfoHtml(p)}
        </label>`;
    }).join("");
    const protectedHtml = protectedProducts.map((p) => `
      <div style="display:flex;align-items:center;gap:10px;padding:12px 16px;background:#fff7ed;border-bottom:1px solid #fed7aa">
        <span style="flex-shrink:0;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:700;background:#ea580c;color:#fff;white-space:nowrap">PROTECTED</span>
        ${productInfoHtml(p)}
      </div>
    `).join("");
    const blockedReasonText = blockedReasons.length
      ? ` · blocked by ${escapeHtml(blockedReasons.join(" + "))}`
      : "";
    const summaryParts = [];
    summaryParts.push(`${extraProducts.length + protectedProducts.length + 1} product${extraProducts.length + protectedProducts.length + 1 !== 1 ? "s" : ""}`);
    if (extraProducts.length) summaryParts.push(`${extraProducts.length} safe to delete`);
    if (protectedProducts.length) summaryParts.push(`${protectedProducts.length} protected`);
    return `
      <div style="border-bottom:2px solid var(--border)">
        <div style="padding:8px 16px;background:#f8fafc;border-bottom:1px solid var(--border)">
          <span style="font-size:11px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:0.05em">SKU: ${escapeHtml(sku)}</span>
          <span style="margin-left:8px;font-size:11px;color:#9ca3af">${summaryParts.join(" — ")}${blockedReasonText}</span>
        </div>
        ${keepHtml}${protectedHtml}${extrasHtml}
      </div>`;
  }).join("");

  const missingSkuHtml = shopifyMissingSkuProducts.length
    ? `
      <div style="border-bottom:2px solid var(--border)">
        <div style="padding:8px 16px;background:#f8fafc;border-bottom:1px solid var(--border)">
          <span style="font-size:11px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:0.05em">Products without SKU</span>
          <span style="margin-left:8px;font-size:11px;color:#9ca3af">${shopifyMissingSkuProducts.length} product${shopifyMissingSkuProducts.length !== 1 ? "s" : ""}</span>
        </div>
        ${shopifyMissingSkuProducts.map((product) => `
          <div style="display:flex;align-items:center;gap:10px;padding:12px 16px;background:#fff7ed;border-bottom:1px solid #fed7aa">
            <span style="flex-shrink:0;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:700;background:#b45309;color:#fff;white-space:nowrap">NO SKU</span>
            ${productInfoHtml(product)}
          </div>
        `).join("")}
      </div>
    `
    : "";

  skuDuplicatesList.innerHTML = `${duplicateGroupsHtml}${missingSkuHtml}`;

  skuDuplicatesList.querySelectorAll(".sku-dup-check").forEach((cb) => {
    cb.addEventListener("change", () => {
      if (cb.checked) selectedDuplicateIds.add(cb.dataset.id);
      else selectedDuplicateIds.delete(cb.dataset.id);
      skuDuplicatesDeleteBtn.disabled = selectedDuplicateIds.size === 0;
      skuDuplicatesDeselectBtn.disabled = selectedDuplicateIds.size === 0;
    });
  });
}

async function loadDuplicateSkuProducts(forceRefresh = false) {
  if (!await isConnected()) {
    skuDuplicatesStatus.textContent = "Connect Shopify first.";
    skuDuplicateGroups = [];
    shopifyMissingSkuProducts = [];
    selectedDuplicateIds.clear();
    renderDuplicateSkuPanel();
    return;
  }
  skuDuplicatesStatus.textContent = "Loading Shopify products…";
  skuDuplicatesRefreshBtn.disabled = true;
  try {
    if (forceRefresh) clearShopifyProductsSnapshotCache();
    const products = await getShopifyProductsSnapshot();
    shopifyComparableSkuSet = getShopifyComparableSkuSet(products, allMonitors);
    shopifyProductCount = products.length;
    shopifyUniqueSkuCount = shopifyComparableSkuSet.size;
    shopifyMissingSkuCount = getShopifyMissingSkuCount(products);
    shopifyMissingSkuProducts = products.filter((product) => !getProductPrimarySku(product));
    const rawDuplicateProductCount = getShopifyDuplicateProductCount(products);
    const { shopifySkuMapping, SHOPIFY_SHOP } = await chrome.storage.local.get(["shopifySkuMapping", "SHOPIFY_SHOP"]);
    const shopDomain = SHOPIFY_SHOP || "orjn.myshopify.com";
    const storeName = shopDomain.replace(".myshopify.com", "");
    skuDuplicatesAdminBase = `https://admin.shopify.com/store/${storeName}/products`;
    skuDuplicateGroups = computeDuplicateSkuGroups(products, shopifySkuMapping || {}, allMonitors);
    selectedDuplicateIds.clear();
    // Auto-select all extras by default
    for (const { extraProducts } of skuDuplicateGroups) {
      for (const p of extraProducts) selectedDuplicateIds.add(String(p.id));
    }
    const totalExtras = skuDuplicateGroups.reduce((n, g) => n + g.extraProducts.length, 0);
    const totalProtected = skuDuplicateGroups.reduce((n, g) => n + (g.protectedProducts?.length || 0), 0);
    const actionableGroups = skuDuplicateGroups.filter((g) => g.extraProducts.length > 0).length;
    if (skuDuplicateGroups.length === 0 && shopifyMissingSkuProducts.length === 0) {
      skuDuplicatesStatus.textContent = rawDuplicateProductCount > 0
        ? `${rawDuplicateProductCount} duplicate-SKU Shopify product${rawDuplicateProductCount !== 1 ? "s exist" : " exists"}, but none ${rawDuplicateProductCount !== 1 ? "are" : "is"} safe to delete automatically because ${rawDuplicateProductCount !== 1 ? "they are" : "it is"} still referenced by a monitor or SKU mapping.`
        : "No duplicate SKUs found. Your Shopify store is clean.";
    } else {
      const statusParts = [];
      if (skuDuplicateGroups.length > 0) {
        const protectedText = totalProtected ? ` ${totalProtected} protected product${totalProtected !== 1 ? "s" : ""} still block full cleanup.` : "";
        statusParts.push(`Found ${skuDuplicateGroups.length} SKU${skuDuplicateGroups.length !== 1 ? "s" : ""} with duplicates — ${totalExtras} extra product${totalExtras !== 1 ? "s" : ""} can be deleted across ${actionableGroups} actionable group${actionableGroups !== 1 ? "s" : ""}.${protectedText}`);
      }
      if (shopifyMissingSkuProducts.length > 0) {
        statusParts.push(`${shopifyMissingSkuProducts.length} Shopify product${shopifyMissingSkuProducts.length !== 1 ? "s" : ""} without SKU ${shopifyMissingSkuProducts.length !== 1 ? "are" : "is"} listed below.`);
      }
      skuDuplicatesStatus.textContent = statusParts.join(" ");
    }
    renderDuplicateSkuPanel();
  } catch (error) {
    skuDuplicatesStatus.textContent = `Error: ${error.message || error}`;
    skuDuplicateGroups = [];
    shopifyMissingSkuProducts = [];
    selectedDuplicateIds.clear();
    renderDuplicateSkuPanel();
  }
  renderStats(getFilteredMonitors());
  updateBulkBtn();
  skuDuplicatesRefreshBtn.disabled = false;
}

async function deleteSelectedDuplicates() {
  const ids = [...selectedDuplicateIds].map(Number).filter(Boolean);
  if (!ids.length) return;
  if (!window.confirm(`Delete ${ids.length} duplicate product${ids.length !== 1 ? "s" : ""} from Shopify? This cannot be undone.`)) return;
  importInProgress = true;
  updateBulkBtn();
  skuDuplicatesDeleteBtn.disabled = true;
  skuDuplicatesDeleteBtn.textContent = `Deleting 0/${ids.length}…`;

  // Build a map: deleted product id → keep product id, for mapping repair below
  const deletedIdToKeepId = new Map();
  const deletedIdToSku = new Map();
  for (const { sku, keepProduct, extraProducts } of skuDuplicateGroups) {
    for (const p of extraProducts) {
      deletedIdToKeepId.set(Number(p.id), Number(keepProduct.id));
      deletedIdToSku.set(Number(p.id), sku);
    }
  }

  let done = 0;
  const errors = [];
  const { shopifySkuMapping } = await chrome.storage.local.get("shopifySkuMapping");
  const mapping = shopifySkuMapping || {};

  for (const id of ids) {
    try {
      await deleteShopifyProducts([id]);
      selectedDuplicateIds.delete(String(id));
      skuDuplicateGroups = skuDuplicateGroups.map((g) => ({
        ...g,
        extraProducts: g.extraProducts.filter((p) => Number(p.id) !== id)
      })).filter((g) => g.extraProducts.length > 0);

      // If the deleted product was the SKU mapping target, point mapping to the keep product now.
      // This prevents the stale-mapping window where updateShopifyForMonitor would hit a 404
      // and fall back to findProductBySkuPrefix before self-healing.
      const sku = deletedIdToSku.get(id);
      if (sku && Number(mapping[sku]) === id) {
        const keepId = deletedIdToKeepId.get(id);
        if (keepId) mapping[sku] = keepId;
        else delete mapping[sku];
      }
    } catch (err) {
      errors.push(`ID ${id}: ${err.message || err}`);
    }
    done++;
    skuDuplicatesDeleteBtn.textContent = `Deleting ${done}/${ids.length}…`;
    renderDuplicateSkuPanel();
  }
  try {
    // Persist the repaired mapping so future updates don't hit a stale/deleted product
    await chrome.storage.local.set({ shopifySkuMapping: mapping });
  } catch (_) {}

  importInProgress = false;
  updateBulkBtn();
  skuDuplicatesDeleteBtn.textContent = "Delete selected";
  skuDuplicatesDeleteBtn.disabled = selectedDuplicateIds.size === 0;
  const totalExtras = skuDuplicateGroups.reduce((n, g) => n + g.extraProducts.length, 0);
  if (errors.length) {
    skuDuplicatesStatus.textContent = `Done with ${errors.length} error${errors.length !== 1 ? "s" : ""}: ${errors.join("; ")}`;
  } else if (totalExtras === 0) {
    skuDuplicatesStatus.textContent = "All duplicates deleted. Your Shopify store is clean.";
    shopifyProductCount = (shopifyProductCount || 0) - done;
  } else {
    skuDuplicatesStatus.textContent = `Deleted ${done}. ${totalExtras} duplicate${totalExtras !== 1 ? "s" : ""} remaining.`;
    shopifyProductCount = (shopifyProductCount || 0) - done;
  }
  renderStats(allMonitors);
  scheduleDashboardAutoRefresh({ forceShopifyRefresh: true });
}

openSkuDuplicatesBtn.addEventListener("click", async () => {
  const isOpen = skuDuplicatesPanel.style.display !== "none";
  if (isOpen) {
    skuDuplicatesPanel.style.display = "none";
  } else {
    skuDuplicatesPanel.style.display = "";
    shopifyUnmonitoredPanel.style.display = "none";
    monitorNotShopifyPanel.style.display = "none";
    await loadDuplicateSkuProducts(true);
    skuDuplicatesPanel.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

closeSkuDuplicatesBtn.addEventListener("click", () => { skuDuplicatesPanel.style.display = "none"; });
skuDuplicatesRefreshBtn.addEventListener("click", () => loadDuplicateSkuProducts(true));
skuDuplicatesDeleteBtn.addEventListener("click", deleteSelectedDuplicates);
skuDuplicatesSelectAllBtn.addEventListener("click", () => {
  for (const { extraProducts } of skuDuplicateGroups) {
    for (const p of extraProducts) selectedDuplicateIds.add(String(p.id));
  }
  renderDuplicateSkuPanel();
});
skuDuplicatesDeselectBtn.addEventListener("click", () => {
  selectedDuplicateIds.clear();
  renderDuplicateSkuPanel();
});

// Bad Size Audit panel
function renderUsSizeAuditPanel() {
  usSizeAuditDeleteBtn.disabled = selectedUsSizeVariantIds.size === 0;
  usSizeAuditSelectAllBtn.disabled = usSizeAuditRows.length === 0;
  usSizeAuditDeselectBtn.disabled = selectedUsSizeVariantIds.size === 0;

  if (!usSizeAuditRows.length) {
    renderAuditHtmlInChunks([], `<p style="padding:24px;text-align:center;color:#9ca3af;font-size:13px">No bad size variants found in active products.</p>`);
    return;
  }

  const byProduct = new Map();
  for (const row of usSizeAuditRows) {
    const key = String(row.productId);
    if (!byProduct.has(key)) byProduct.set(key, { ...row, variants: [] });
    byProduct.get(key).variants.push(row);
  }

  const htmlParts = [...byProduct.values()].map(({ productId, productTitle, vendor, productSku, image, variants }) => {
    const thumb = image
      ? `<img src="${escapeHtml(image)}" alt="" style="width:36px;height:36px;object-fit:cover;border-radius:4px;flex-shrink:0;border:1px solid #e5e7eb">`
      : `<div style="width:36px;height:36px;border-radius:4px;flex-shrink:0;background:#f3f4f6;border:1px solid #e5e7eb;display:flex;align-items:center;justify-content:center;font-size:16px;color:#d1d5db">â–¡</div>`;
    const variantsHtml = variants.map((v) => {
      const idStr = String(v.variantId);
      const checked = selectedUsSizeVariantIds.has(idStr) ? "checked" : "";
      return `<label style="display:flex;align-items:center;gap:8px;padding:6px 16px 6px 52px;border-bottom:1px solid #f3f4f6;cursor:pointer;background:#fff">
        <input type="checkbox" class="us-size-check" data-id="${idStr}" ${checked} style="flex-shrink:0">
        <span style="font-size:12px;font-weight:700;color:#374151;min-width:50px">${escapeHtml(v.size)}</span>
        <span style="font-size:11px;color:#9ca3af">SKU: ${escapeHtml(v.variantSku || "—")}</span>
      </label>`;
    }).join("");
    return `<div style="border-bottom:2px solid var(--border)">
      <div style="display:flex;align-items:center;gap:10px;padding:10px 16px;background:#f8fafc;border-bottom:1px solid var(--border)">
        ${thumb}
        <div style="min-width:0;flex:1">
          <p style="margin:0;font-size:12px;font-weight:700;color:#111;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(productTitle)}</p>
          <p style="margin:2px 0 0;font-size:11px;color:#5f6c7b">${escapeHtml(vendor || "")}${productSku ? ` · SKU: ${escapeHtml(productSku)}` : ""} · ${variants.length} bad size variant${variants.length !== 1 ? "s" : ""}</p>
        </div>
      </div>
      ${variantsHtml}
    </div>`;
  });
  renderAuditHtmlInChunks(htmlParts);
}

function renderBadSizeAuditPanel() {
  updateBadSizeAuditButtons();

  if (!usSizeAuditRows.length) {
    renderAuditHtmlInChunks([], `<p style="padding:24px;text-align:center;color:#9ca3af;font-size:13px">No bad sizes found in Store Data or Monitor Data.</p>`);
    return;
  }

  const byProduct = new Map();
  for (const row of usSizeAuditRows) {
    const key = `${row.source}:${row.productId}`;
    if (!byProduct.has(key)) byProduct.set(key, { ...row, variants: [] });
    byProduct.get(key).variants.push(row);
  }

  const htmlParts = [...byProduct.values()].map(({ source, monitorId, productTitle, vendor, productSku, image, variants }) => {
    const thumb = image
      ? `<img src="${escapeHtml(image)}" alt="" style="width:36px;height:36px;object-fit:cover;border-radius:4px;flex-shrink:0;border:1px solid #e5e7eb">`
      : `<div style="width:36px;height:36px;border-radius:4px;flex-shrink:0;background:#f3f4f6;border:1px solid #e5e7eb;display:flex;align-items:center;justify-content:center;font-size:16px;color:#d1d5db"></div>`;
    const sourceLabel = source === "monitor" ? "Monitor Data" : "Store Data";
    const sourceColor = source === "monitor" ? "#2563eb" : "#16a34a";
    const badSizeCount = variants.reduce((n, v) => n + (v.source === "monitor" ? (v.badSizes?.length || 0) : 1), 0);
    const variantsHtml = variants.map((v) => {
      if (v.source === "monitor") {
        const badList = Array.isArray(v.badSizes) ? v.badSizes : [v.size].filter(Boolean);
        return badList.map((size) => {
          const key = getMonitorBadSizeKey(v.monitorId, size);
          const checked = selectedMonitorBadSizeKeys.has(key) ? "checked" : "";
          return `<div style="display:flex;align-items:center;gap:8px;padding:8px 16px 8px 52px;border-bottom:1px solid #f3f4f6;background:#fff;flex-wrap:wrap">
          <input type="checkbox" class="monitor-bad-size-check" data-monitor-id="${escapeHtml(v.monitorId)}" data-size="${escapeHtml(size)}" ${checked} style="flex-shrink:0">
          <span style="font-size:12px;font-weight:700;color:#374151;min-width:50px">${escapeHtml(size)}</span>
          <span style="font-size:11px;color:#9ca3af">No EU conversion in monitor data</span>
        </div>`;
        }).join("");
      }
      const idStr = String(v.variantId);
      const checked = selectedUsSizeVariantIds.has(idStr) ? "checked" : "";
      const displaySize = v.expectedSize ? `${v.size} -> ${v.expectedSize}` : v.size;
      return `<label style="display:flex;align-items:center;gap:8px;padding:6px 16px 6px 52px;border-bottom:1px solid #f3f4f6;cursor:pointer;background:#fff">
        <input type="checkbox" class="us-size-check" data-id="${idStr}" ${checked} style="flex-shrink:0">
        <span style="font-size:12px;font-weight:700;color:#374151;min-width:50px">${escapeHtml(displaySize)}</span>
        <span style="font-size:11px;color:#9ca3af">${escapeHtml(v.reason || "Shopify variant")} · SKU: ${escapeHtml(v.variantSku || "-")}</span>
      </label>`;
    }).join("");
    return `<div style="border-bottom:2px solid var(--border)">
      <div style="display:flex;align-items:center;gap:10px;padding:10px 16px;background:#f8fafc;border-bottom:1px solid var(--border)">
        ${thumb}
        <div style="min-width:0;flex:1">
          <p style="margin:0;font-size:12px;font-weight:700;color:#111;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(productTitle)}</p>
          <p style="margin:2px 0 0;font-size:11px;color:#5f6c7b"><span style="font-weight:800;color:${sourceColor}">${sourceLabel}</span>${vendor ? ` · ${escapeHtml(vendor)}` : ""}${productSku ? ` · SKU: ${escapeHtml(productSku)}` : ""} · ${badSizeCount} bad size${badSizeCount !== 1 ? "s" : ""}</p>
        </div>
        ${source === "monitor" ? `<button type="button" class="inline-button audit-monitor-open-btn" data-id="${escapeHtml(monitorId)}" style="font-size:11px;padding:3px 8px;flex-shrink:0">Open monitor</button>` : ""}
      </div>
      ${variantsHtml}
    </div>`;
  });
  renderAuditHtmlInChunks(htmlParts);
  updateBadSizeAuditButtons();
}

usSizeAuditList?.addEventListener("change", (event) => {
  const usInput = event.target.closest(".us-size-check");
  if (usInput) {
    if (usInput.checked) selectedUsSizeVariantIds.add(usInput.dataset.id);
    else selectedUsSizeVariantIds.delete(usInput.dataset.id);
    updateBadSizeAuditButtons();
    usSizeAuditDeleteBtn.disabled = selectedUsSizeVariantIds.size === 0 && selectedMonitorBadSizeKeys.size === 0;
    usSizeAuditDeselectBtn.disabled = selectedUsSizeVariantIds.size === 0 && selectedMonitorBadSizeKeys.size === 0;
    return;
  }
  const monitorInput = event.target.closest(".monitor-bad-size-check");
  if (monitorInput) {
    const key = getMonitorBadSizeKey(monitorInput.dataset.monitorId, monitorInput.dataset.size);
    if (monitorInput.checked) selectedMonitorBadSizeKeys.add(key);
    else selectedMonitorBadSizeKeys.delete(key);
    updateBadSizeAuditButtons();
  }
});

usSizeAuditList?.addEventListener("click", (event) => {
  const btn = event.target.closest(".audit-monitor-open-btn");
  if (!btn) return;
  event.preventDefault();
  event.stopPropagation();
  const monitor = _getMonitor(btn.dataset.id);
  if (!monitor) return;
  selectedMonitorId = monitor.id;
  _lastDetailSig = "";
  renderAll();
  renderDetail(monitor);
  monitorDetail.scrollIntoView({ behavior: "smooth", block: "start" });
});

async function loadBadSizeAudit(forceRefresh = false) {
  const connected = await isConnected();
  usSizeAuditStatus.textContent = connected ? "Loading Store Data and Monitor Data..." : "Loading Monitor Data...";
  usSizeAuditRefreshBtn.disabled = true;
  usSizeAuditSelectAllBtn.disabled = true;
  usSizeAuditDeselectBtn.disabled = true;
  usSizeAuditDeleteBtn.disabled = true;
  usSizeAuditUpdateMetadataBtn.disabled = true;
  usSizeAuditDeleteBtn.textContent = "Remove selected bad sizes";
  try {
    const monitorResponse = await chrome.runtime.sendMessage({ type: "get-monitors" }).catch(() => null);
    if (monitorResponse?.ok) {
      applyMonitorsUpdate(monitorResponse.monitors || []);
      renderAll();
      refreshMonitorMetaContent();
    }
    let shopifyRows = [];
    if (connected) {
      if (forceRefresh) clearShopifyProductsSnapshotCache();
      const products = await getShopifyProductsSnapshot();
      shopifyRows = getBadSizeVariantRows(products);
    }
    const monitorRows = getMonitorBadSizeRows(allMonitors);
    usSizeAuditRows = [...shopifyRows, ...monitorRows];
    shopifyBadSizeVariantCount = shopifyRows.length;
    selectedUsSizeVariantIds.clear();
    selectedMonitorBadSizeKeys.clear();
    const storeProductCount = new Set(shopifyRows.map((r) => r.productId)).size;
    const monitorCount = new Set(monitorRows.map((r) => r.monitorId)).size;
    const monitorSizeCount = monitorRows.reduce((n, row) => n + (row.badSizes?.length || 0), 0);
    usSizeAuditStatus.textContent = usSizeAuditRows.length === 0
      ? `No bad sizes found in ${connected ? "Store Data or " : ""}Monitor Data.`
      : `Found ${shopifyRows.length} Store Data bad size variant${shopifyRows.length !== 1 ? "s" : ""} across ${storeProductCount} Shopify product${storeProductCount !== 1 ? "s" : ""}, and ${monitorSizeCount} Monitor Data bad size${monitorSizeCount !== 1 ? "s" : ""} across ${monitorCount} monitor${monitorCount !== 1 ? "s" : ""}.`;
    renderBadSizeAuditPanel();
  } catch (error) {
    usSizeAuditStatus.textContent = `Error: ${error.message || error}`;
    usSizeAuditRows = [];
    selectedUsSizeVariantIds.clear();
    selectedMonitorBadSizeKeys.clear();
    renderBadSizeAuditPanel();
  }
  renderStats(getFilteredMonitors());
  updateBulkBtn();
  usSizeAuditRefreshBtn.disabled = false;
  updateBadSizeAuditButtons();
}

async function loadUsSizeAudit(forceRefresh = false) {
  if (!await isConnected()) {
    usSizeAuditStatus.textContent = "Connect Shopify first.";
    usSizeAuditRows = [];
    selectedUsSizeVariantIds.clear();
    renderUsSizeAuditPanel();
    return;
  }
  usSizeAuditStatus.textContent = "Loading Shopify products…";
  usSizeAuditRefreshBtn.disabled = true;
  try {
    if (forceRefresh) clearShopifyProductsSnapshotCache();
    const products = await getShopifyProductsSnapshot();
    usSizeAuditRows = getBadSizeVariantRows(products);
    shopifyBadSizeVariantCount = usSizeAuditRows.length;
    selectedUsSizeVariantIds.clear();
    for (const row of usSizeAuditRows) selectedUsSizeVariantIds.add(String(row.variantId));
    const productCount = new Set(usSizeAuditRows.map((r) => r.productId)).size;
    usSizeAuditStatus.textContent = usSizeAuditRows.length === 0
      ? "No bad size variants found. All Shopify public sizes look EU-only."
      : `Found ${usSizeAuditRows.length} bad size variant${usSizeAuditRows.length !== 1 ? "s" : ""} across ${productCount} product${productCount !== 1 ? "s" : ""}.`;
    renderUsSizeAuditPanel();
  } catch (error) {
    usSizeAuditStatus.textContent = `Error: ${error.message || error}`;
    usSizeAuditRows = [];
    selectedUsSizeVariantIds.clear();
    renderUsSizeAuditPanel();
  }
  renderStats(getFilteredMonitors());
  updateBulkBtn();
  usSizeAuditRefreshBtn.disabled = false;
}

async function deleteSelectedUsSizeVariants() {
  const ids = [...selectedUsSizeVariantIds].map(Number).filter(Boolean);
  if (!ids.length) return;
  if (!window.confirm(`Delete ${ids.length} bad size variant${ids.length !== 1 ? "s" : ""} from Shopify? This cannot be undone.`)) return;
  importInProgress = true;
  updateBulkBtn();
  usSizeAuditDeleteBtn.disabled = true;
  usSizeAuditDeleteBtn.textContent = `Deleting 0/${ids.length}…`;
  const errors = [];
  let done = 0;
  try {
    for (const id of ids) {
      try {
        await deleteShopifyVariantsByIds([id]);
        selectedUsSizeVariantIds.delete(String(id));
        usSizeAuditRows = usSizeAuditRows.filter((r) => r.source !== "shopify" || r.variantId !== id);
        done++;
      } catch (err) {
        errors.push(`ID ${id}: ${err.message || err}`);
      }
      usSizeAuditDeleteBtn.textContent = `Deleting ${done}/${ids.length}…`;
      renderBadSizeAuditPanel();
    }
  } finally {
    importInProgress = false;
    updateBulkBtn();
  }
  usSizeAuditDeleteBtn.textContent = "Delete selected variants";
  usSizeAuditDeleteBtn.disabled = selectedUsSizeVariantIds.size === 0;
  const remainingShopifyRows = usSizeAuditRows.filter((row) => row.source === "shopify");
  if (errors.length) {
    usSizeAuditStatus.textContent = `Done with ${errors.length} error${errors.length !== 1 ? "s" : ""}: ${errors.join("; ")}`;
  } else if (remainingShopifyRows.length === 0) {
    usSizeAuditStatus.textContent = usSizeAuditRows.length === 0
      ? "All bad sizes deleted."
      : "All Store Data bad size variants deleted. Monitor Data bad sizes still need monitor cleanup.";
    shopifyBadSizeVariantCount = 0;
  } else {
    usSizeAuditStatus.textContent = `Deleted ${done}. ${remainingShopifyRows.length} Store Data bad size variant${remainingShopifyRows.length !== 1 ? "s" : ""} remaining.`;
    shopifyBadSizeVariantCount = remainingShopifyRows.length;
  }
  renderStats(allMonitors);
  clearShopifyProductsSnapshotCache();
  scheduleDashboardAutoRefresh({ forceShopifyRefresh: true });
}

async function removeSelectedBadSizeItems() {
  const { shopifyIds: ids, monitorSelections, selectedMonitorTargets, selectedShopifyRows } = getVisibleBadSizeAuditSelection();
  const monitorSizeCount = [...monitorSelections.values()].reduce((n, sizes) => n + sizes.length, 0);
  if (!ids.length && !monitorSizeCount) {
    usSizeAuditStatus.textContent = "Select at least one bad size first.";
    updateBadSizeAuditButtons();
    return;
  }
  const parts = [
    ids.length ? `${ids.length} Store Data Shopify variant${ids.length !== 1 ? "s" : ""}` : "",
    monitorSizeCount ? `${monitorSizeCount} Monitor Data size${monitorSizeCount !== 1 ? "s" : ""}` : ""
  ].filter(Boolean).join(" and ");
  if (!window.confirm(`Remove ${parts}? Shopify variant deletes cannot be undone.`)) return;

  importInProgress = true;
  updateBulkBtn();
  usSizeAuditDeleteBtn.disabled = true;
  const total = ids.length + monitorSizeCount;
  usSizeAuditDeleteBtn.textContent = `Removing 0/${total}`;
  const errors = [];
  let done = 0;

  try {
    if (ids.length) {
      try {
        await deleteShopifyVariantsByIds(ids);
        const idSet = new Set(ids.map(String));
        ids.forEach((id) => selectedUsSizeVariantIds.delete(String(id)));
        usSizeAuditRows = usSizeAuditRows.filter((r) => r.source !== "shopify" || !idSet.has(String(r.variantId)));
        done += ids.length;
      } catch (err) {
        errors.push(`Shopify variants: ${err.message || err}`);
      }
      usSizeAuditDeleteBtn.textContent = `Removing ${done}/${total}`;
    }

    for (const [monitorId, sizes] of monitorSelections.entries()) {
      try {
        const resp = await chrome.runtime.sendMessage({ type: "remove-us-sizes", monitorId, sizes });
        if (!resp?.ok) throw new Error(resp?.error || "unknown");
        sizes.forEach((size) => selectedMonitorBadSizeKeys.delete(getMonitorBadSizeKey(monitorId, size)));
        done += sizes.length;
      } catch (err) {
        errors.push(`Monitor ${monitorId}: ${err.message || err}`);
      }
      usSizeAuditDeleteBtn.textContent = `Removing ${done}/${total}`;
    }
  } finally {
    importInProgress = false;
    updateBulkBtn();
  }
  usSizeAuditDeleteBtn.textContent = "Remove selected";
  if (errors.length) {
    usSizeAuditStatus.textContent = `Done with ${errors.length} error${errors.length !== 1 ? "s" : ""}: ${errors.join("; ")}`;
  } else {
    usSizeAuditStatus.textContent = `Removed ${done} selected bad size item${done !== 1 ? "s" : ""}.`;
  }
  clearShopifyProductsSnapshotCache();
  const response = await chrome.runtime.sendMessage({ type: "get-monitors" }).catch(() => null);
  if (response?.ok) {
    applyMonitorsUpdate(response.monitors || []);
    renderAll();
    refreshMonitorMetaContent();
  }
  await loadBadSizeAudit(true);
  const remainingShopifyKeys = new Set(usSizeAuditRows
    .filter((row) => row.source === "shopify")
    .map((row) => getBadSizeTargetKey("shopify", row.productId, row.size)));
  const remainingMonitorKeys = new Set(usSizeAuditRows
    .filter((row) => row.source === "monitor")
    .flatMap((row) => (row.badSizes || []).map((size) => getBadSizeTargetKey("monitor", row.monitorId, size))));
  const selectedShopifyKeys = selectedShopifyRows.map((row) => getBadSizeTargetKey("shopify", row.productId, row.size));
  const selectedMonitorKeys = selectedMonitorTargets.map((row) => getBadSizeTargetKey("monitor", row.monitorId, row.size));
  let stillInShopify = selectedShopifyKeys.filter((key) => remainingShopifyKeys.has(key)).length;
  let stillInMonitor = selectedMonitorKeys.filter((key) => remainingMonitorKeys.has(key)).length;
  if (stillInShopify || stillInMonitor) {
    const retryErrors = [];
    const retryShopifyRows = usSizeAuditRows.filter((row) =>
      row.source === "shopify" && selectedShopifyKeys.includes(getBadSizeTargetKey("shopify", row.productId, row.size))
    );
    for (const row of retryShopifyRows) {
      try {
        await deleteShopifyVariantsByIds([row.variantId]);
      } catch (err) {
        retryErrors.push(`ID ${row.variantId}: ${err.message || err}`);
      }
    }
    const retryMonitorRows = usSizeAuditRows.filter((row) =>
      row.source === "monitor" && (row.badSizes || []).some((size) => selectedMonitorKeys.includes(getBadSizeTargetKey("monitor", row.monitorId, size)))
    );
    for (const row of retryMonitorRows) {
      const sizes = (row.badSizes || []).filter((size) => selectedMonitorKeys.includes(getBadSizeTargetKey("monitor", row.monitorId, size)));
      if (!sizes.length) continue;
      try {
        const resp = await chrome.runtime.sendMessage({ type: "remove-us-sizes", monitorId: row.monitorId, sizes });
        if (!resp?.ok) throw new Error(resp?.error || "unknown");
      } catch (err) {
        retryErrors.push(`Monitor ${row.monitorId}: ${err.message || err}`);
      }
    }
    if (retryShopifyRows.length || retryMonitorRows.length) {
      clearShopifyProductsSnapshotCache();
      await silentRefresh(true);
      await loadBadSizeAudit(true);
      const finalShopifyKeys = new Set(usSizeAuditRows
        .filter((row) => row.source === "shopify")
        .map((row) => getBadSizeTargetKey("shopify", row.productId, row.size)));
      const finalMonitorKeys = new Set(usSizeAuditRows
        .filter((row) => row.source === "monitor")
        .flatMap((row) => (row.badSizes || []).map((size) => getBadSizeTargetKey("monitor", row.monitorId, size))));
      stillInShopify = selectedShopifyKeys.filter((key) => finalShopifyKeys.has(key)).length;
      stillInMonitor = selectedMonitorKeys.filter((key) => finalMonitorKeys.has(key)).length;
      if (retryErrors.length) errors.push(...retryErrors);
    }
  }
  if (stillInShopify || stillInMonitor) {
    usSizeAuditStatus.textContent = `Removed ${done}, but ${stillInShopify} Store Data and ${stillInMonitor} Monitor Data selected bad size${stillInShopify + stillInMonitor !== 1 ? "s" : ""} still remain. Refresh completed so you can select the remaining rows.`;
  } else if (errors.length) {
    usSizeAuditStatus.textContent = `Selected bad sizes are gone after retry, with ${errors.length} warning${errors.length !== 1 ? "s" : ""}: ${errors.join("; ")}`;
  } else if (!errors.length) {
    usSizeAuditStatus.textContent = `Removed ${done} selected bad size item${done !== 1 ? "s" : ""}. Verified clean for the selected rows.`;
  }
  scheduleDashboardAutoRefresh({ forceShopifyRefresh: true });
}

openUsSizeAuditBtn.addEventListener("click", async () => {
  const isOpen = usSizeAuditPanel.style.display !== "none";
  if (isOpen) {
    usSizeAuditPanel.style.display = "none";
  } else {
    usSizeAuditPanel.style.display = "";
    shopifyUnmonitoredPanel.style.display = "none";
    monitorNotShopifyPanel.style.display = "none";
    skuDuplicatesPanel.style.display = "none";
    await loadBadSizeAudit(true);
    usSizeAuditPanel.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

closeUsSizeAuditBtn.addEventListener("click", () => { usSizeAuditPanel.style.display = "none"; });
usSizeAuditRefreshBtn.addEventListener("click", () => loadBadSizeAudit(true));
usSizeAuditDeleteBtn.addEventListener("click", removeSelectedBadSizeItems);
usSizeAuditList.addEventListener("change", (event) => {
  const target = event.target;
  if (target.classList?.contains("us-size-check")) {
    if (target.checked) selectedUsSizeVariantIds.add(target.dataset.id);
    else selectedUsSizeVariantIds.delete(target.dataset.id);
    updateBadSizeAuditButtons();
  }
  if (target.classList?.contains("monitor-bad-size-check")) {
    const key = getMonitorBadSizeKey(target.dataset.monitorId, target.dataset.size);
    if (target.checked) selectedMonitorBadSizeKeys.add(key);
    else selectedMonitorBadSizeKeys.delete(key);
    updateBadSizeAuditButtons();
  }
});
usSizeAuditSelectAllBtn.addEventListener("click", () => {
  for (const row of usSizeAuditRows) {
    if (row.source === "shopify" && row.variantId) selectedUsSizeVariantIds.add(String(row.variantId));
    if (row.source === "monitor") {
      for (const size of (row.badSizes || [])) selectedMonitorBadSizeKeys.add(getMonitorBadSizeKey(row.monitorId, size));
    }
  }
  renderBadSizeAuditPanel();
});
usSizeAuditDeselectBtn.addEventListener("click", () => {
  selectedUsSizeVariantIds.clear();
  selectedMonitorBadSizeKeys.clear();
  renderBadSizeAuditPanel();
});
usSizeAuditUpdateMetadataBtn.addEventListener("click", async () => {
  const uniqueSkus = [...new Set(usSizeAuditRows.map((r) => r.productSku).filter(Boolean))];
  const uniqueProductIds = [...new Set(usSizeAuditRows.map((r) => r.productId).filter(Boolean))];
  const targets = allMonitors.filter((monitor) => {
    const sku = monitor.productData?.sku;
    return (sku && uniqueSkus.includes(sku)) ||
           (monitor.shopifyProductId && uniqueProductIds.includes(Number(monitor.shopifyProductId)));
  });
  const n = targets.length;
  if (!n) {
    alert("No dashboard monitors match the products in this audit. Make sure the monitors have matching SKUs or Shopify product IDs.");
    return;
  }
  if (!await isConnected()) { alert("Connect to Shopify first (see top of page)."); return; }
  if (!window.confirm(`Update Shopify filter tags and metafields for ${n} monitor${n !== 1 ? "s" : ""}? This will not create new Shopify products.`)) return;

  usSizeAuditUpdateMetadataBtn.disabled = true;
  showShopifySyncStatus(`Updating Shopify metadata for ${n} audited product${n !== 1 ? "s" : ""}...`, "working", { autoHide: false });
  usSizeAuditUpdateMetadataBtn.textContent = `Updating 0/${n}…`;

  const succeeded = [];
  const failed = [];
  let processed = 0;
  let repairedSkus = 0;

  try { await primeImportCaches(); } catch (_) {}

  const metadataContext = {};
  for (const monitor of targets) {
    try {
      const result = await updateMonitorShopifyMetadata(monitor, metadataContext);
      repairedSkus += Number(result?.repairedVariantSkus || 0);
      await Promise.all([
        chrome.runtime.sendMessage({
          type: "update-monitor",
          payload: {
            id: monitor.id,
            shopifyProductId: result?.id || monitor.shopifyProductId || null,
            shopifyLastSyncAt: new Date().toISOString(),
            shopifySyncStatus: "ok"
          }
        }),
        addLog({
          type: "shopify-sync",
          title: [monitor.productData?.brand, monitor.productData?.sku].filter(Boolean).join(" ") || monitor.name,
          productName: monitor.productData?.name || monitor.name || "",
          brand: monitor.productData?.brand || "",
          sku: monitor.productData?.sku || "",
          details: ["Updated Shopify filter tags and metafields"],
          monitorId: monitor.id,
          url: monitor.url
        }).catch(() => {})
      ]);
      succeeded.push(monitor.id);
    } catch (error) {
      const pd = monitor.productData || {};
      const reason = error?.message || String(error) || "Unknown error";
      await addLog({
        type: "error",
        title: [pd.brand, pd.sku].filter(Boolean).join(" ") || monitor.name,
        productName: pd.name || monitor.name || "",
        brand: pd.brand || "",
        sku: pd.sku || "",
        details: [reason],
        monitorId: monitor.id,
        url: monitor.url
      }).catch(() => {});
      failed.push({ name: pd.sku || monitor.name || monitor.id, reason });
    }
    processed++;
    usSizeAuditUpdateMetadataBtn.textContent = `Updating ${processed}/${n}…`;
  }

  usSizeAuditUpdateMetadataBtn.textContent = "Update metadata";
  usSizeAuditUpdateMetadataBtn.disabled = false;
  await refreshShopifyDashboardState(true);

  if (failed.length) {
    showShopifySyncStatus(`Updated ${succeeded.length}/${n} audited products; ${failed.length} failed.`, "error", { autoHide: false });
    alert(`Updated ${succeeded.length}/${n} products.\nRepaired variant SKUs: ${repairedSkus}\n\nFailed (${failed.length}):\n\n${failed.map((f) => `- ${f.name}\n  ${f.reason}`).join("\n\n")}`);
  } else {
    showShopifySyncStatus(`Updated Shopify metadata for ${succeeded.length} audited product${succeeded.length !== 1 ? "s" : ""}. Repaired variant SKUs: ${repairedSkus}.`, "success");
    alert(`Updated Shopify filter metadata for ${succeeded.length} product${succeeded.length !== 1 ? "s" : ""}.\nRepaired variant SKUs: ${repairedSkus}`);
  }
});

// ── Logs panel ─────────────────────────────────────────────────────────────
const logsPanel = document.getElementById("logs-panel");
const logsList = document.getElementById("logs-list");
const openLogsBtn = document.getElementById("open-logs-btn");
const closeLogsBtn = document.getElementById("close-logs-btn");
const logsRefreshBtn = document.getElementById("logs-refresh-btn");
const logsClearBtn = document.getElementById("logs-clear-btn");

const TYPE_CONFIG = {
  change:  { label: "Change",  color: "#ff5a36", bg: "#fff3f0" },
  import:  { label: "Import",  color: "#5c6ac4", bg: "#f4f5ff" },
  error:   { label: "Error",   color: "#b41f1f", bg: "#fff0f0" }
};

function renderLogs(logs) {
  if (!logs.length) {
    logsList.innerHTML = `<p style="padding:24px;text-align:center;color:#9ca3af;font-size:13px">No activity yet.</p>`;
    return;
  }
  logsList.innerHTML = logs.map(entry => {
    const cfg = TYPE_CONFIG[entry.type] || TYPE_CONFIG.change;
    const details = (entry.details || []).map(d => `<span style="font-size:11px;color:#5f6c7b;display:block;margin-top:2px">${escapeHtml(d)}</span>`).join("");
    return `
      <div style="display:flex;gap:12px;padding:12px 18px;border-bottom:1px solid #f3f4f6;align-items:flex-start">
        <span style="flex-shrink:0;margin-top:2px;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:700;background:${cfg.bg};color:${cfg.color}">${cfg.label}</span>
        <div style="min-width:0;flex:1">
          <p style="margin:0;font-size:13px;font-weight:700;color:#111">${escapeHtml(entry.title || "—")}</p>
          ${entry.productName ? `<p style="margin:1px 0 3px;font-size:12px;color:#374151">${escapeHtml(entry.productName)}</p>` : ""}
          ${details}
        </div>
        <span style="flex-shrink:0;font-size:11px;color:#9ca3af;white-space:nowrap">${formatTimestamp(entry.timestamp, "")}</span>
      </div>`;
  }).join("");
}

async function loadLogs() {
  logsList.innerHTML = `<p style="padding:16px 18px;color:#9ca3af;font-size:12px">Loading…</p>`;
  const logs = await getLogs();
  renderLogs(logs);
  openLogsBtn.textContent = `Logs${logs.length ? ` (${logs.length})` : ""}`;
}

openLogsBtn.addEventListener("click", async () => {
  const isOpen = logsPanel.style.display !== "none";
  if (isOpen) {
    logsPanel.style.display = "none";
    openLogsBtn.textContent = openLogsBtn.textContent.replace(/\s*\(.*\)/, "") || "Logs";
  } else {
    logsPanel.style.display = "";
    await loadLogs();
    logsPanel.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

closeLogsBtn.addEventListener("click", () => { logsPanel.style.display = "none"; });
logsRefreshBtn.addEventListener("click", loadLogs);
logsClearBtn.addEventListener("click", async () => {
  if (!window.confirm("Clear all logs?")) return;
  await clearLogs();
  await loadLogs();
});

openShopifyUnmonitoredBtn.addEventListener("click", async () => {
  const isOpen = shopifyUnmonitoredPanel.style.display !== "none";
  if (isOpen) {
    shopifyUnmonitoredPanel.style.display = "none";
  } else {
    shopifyUnmonitoredPanel.style.display = "";
    monitorNotShopifyPanel.style.display = "none";
    monitorDuplicatesPanel.style.display = "none";
    skuDuplicatesPanel.style.display = "none";
    await loadShopifyUnmonitoredProducts(true);
    shopifyUnmonitoredPanel.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

openMonitorNotShopifyBtn.addEventListener("click", async () => {
  const isOpen = monitorNotShopifyPanel.style.display !== "none";
  if (isOpen) {
    monitorNotShopifyPanel.style.display = "none";
  } else {
    monitorNotShopifyPanel.style.display = "";
    shopifyUnmonitoredPanel.style.display = "none";
    monitorDuplicatesPanel.style.display = "none";
    skuDuplicatesPanel.style.display = "none";
    await loadMonitorNotShopifyMonitors(true);
    monitorNotShopifyPanel.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

openMonitorDuplicatesBtn.addEventListener("click", async () => {
  const isOpen = monitorDuplicatesPanel.style.display !== "none";
  if (isOpen) {
    monitorDuplicatesPanel.style.display = "none";
  } else {
    monitorDuplicatesPanel.style.display = "";
    shopifyUnmonitoredPanel.style.display = "none";
    monitorNotShopifyPanel.style.display = "none";
    skuDuplicatesPanel.style.display = "none";
    await loadDuplicateMonitorGroups();
    monitorDuplicatesPanel.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

openShopifyOutOfStockBtn.addEventListener("click", () => {
  outOfStockSectionHidden = false;
  collapsedGroups.delete(SHOPIFY_OOS_GROUP_KEY);
  updateOutOfStockButtons();
  renderGrid(getFilteredMonitors());
  if (getVisibleOutOfStockMonitorIds(allMonitors).size) {
    monitorGrid.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

checkShopifyOutOfStockBtn.addEventListener("click", async () => {
  dismissedOutOfStockMonitorIds.clear();
  outOfStockSectionHidden = false;
  collapsedGroups.delete(SHOPIFY_OOS_GROUP_KEY);
  await loadShopifyOutOfStockMonitors(true);
  if (getVisibleOutOfStockMonitorIds(allMonitors).size) {
    monitorGrid.scrollIntoView({ behavior: "smooth", block: "start" });
  }
});

closeShopifyUnmonitoredBtn.addEventListener("click", () => {
  shopifyUnmonitoredPanel.style.display = "none";
});
closeMonitorNotShopifyBtn.addEventListener("click", () => {
  monitorNotShopifyPanel.style.display = "none";
});
closeMonitorDuplicatesBtn.addEventListener("click", () => {
  monitorDuplicatesPanel.style.display = "none";
});

ensureMonitorDuplicateDeleteBtn();

shopifyUnmonitoredRefreshBtn.addEventListener("click", () => loadShopifyUnmonitoredProducts(true));
shopifyUnmonitoredClearSelectedBtn.addEventListener("click", clearSelectedShopifyUnmonitoredList);
shopifyUnmonitoredClearBtn.addEventListener("click", clearShopifyUnmonitoredList);
monitorNotShopifyRefreshBtn.addEventListener("click", () => loadMonitorNotShopifyMonitors(true));
monitorDuplicatesRefreshBtn.addEventListener("click", () => loadDuplicateMonitorGroups());
monitorNotShopifyClearSelectedBtn.addEventListener("click", clearSelectedMonitorNotShopifyList);
monitorNotShopifyClearBtn.addEventListener("click", clearMonitorNotShopifyList);
monitorNotShopifyDeleteDuplicatesBtn?.addEventListener("click", deleteSelectedDuplicateMonitors);
monitorDuplicatesDeleteBtn.addEventListener("click", deleteSelectedDuplicateMonitors);

shopifyUnmonitoredList.addEventListener("change", (event) => {
  const target = event.target;
  if (!target.classList.contains("shopify-unmonitored-check")) return;
  const id = String(target.dataset.id || "");
  if (!id) return;
  if (target.checked) selectedShopifyUnmonitoredIds.add(id);
  else selectedShopifyUnmonitoredIds.delete(id);
  renderShopifyUnmonitoredList();
});

monitorNotShopifyList.addEventListener("change", (event) => {
  const target = event.target;
  if (target.classList.contains("monitor-not-shopify-check")) {
    const id = String(target.dataset.id || "");
    if (!id) return;
    if (target.checked) {
      selectedMonitorNotShopifyIds.add(id);
      checkedIds.add(id);
    } else {
      selectedMonitorNotShopifyIds.delete(id);
      checkedIds.delete(id);
    }
    updateBulkBtn();
    renderMonitorNotShopifyList();
    return;
  }
  if (!target.classList.contains("monitor-duplicate-check")) return;
  const id = String(target.dataset.id || "");
  if (!id) return;
  if (target.checked) selectedDuplicateMonitorIds.add(id);
  else selectedDuplicateMonitorIds.delete(id);
  renderMonitorNotShopifyList();
  if (monitorDuplicatesPanel.style.display !== "none") {
    renderMonitorDuplicatesPanel();
  }
});

monitorDuplicatesList.addEventListener("change", (event) => {
  const target = event.target;
  if (!target.classList.contains("monitor-duplicate-check")) return;
  const id = String(target.dataset.id || "");
  if (!id) return;
  if (target.checked) selectedDuplicateMonitorIds.add(id);
  else selectedDuplicateMonitorIds.delete(id);
  renderMonitorDuplicatesPanel();
  if (monitorNotShopifyPanel.style.display !== "none") {
    renderMonitorNotShopifyList();
  }
});

monitorDuplicatesList.addEventListener("click", async (event) => {
  const target = event.target.closest("button");
  if (!target) return;
  const monitorId = target.dataset.monitorId;
  if (!monitorId) return;
  const monitor = _getMonitor(monitorId);
  if (!monitor) return;

  if (target.classList.contains("sku-mismatch-fix-monitor")) {
    const newSku = target.dataset.sku;
    if (!newSku) return;
    target.textContent = "Saving…";
    target.disabled = true;
    await chrome.runtime.sendMessage({
      type: "update-monitor",
      payload: { id: monitorId, productData: { ...(monitor.productData || {}), sku: newSku } }
    });
    await silentRefresh();
    skuMismatchedLinkedMonitors = skuMismatchedLinkedMonitors.filter((m) => m.monitor.id !== monitorId);
    renderMonitorDuplicatesPanel();
    monitorDuplicatesStatus.textContent = skuMismatchedLinkedMonitors.length
      ? `Fixed. ${skuMismatchedLinkedMonitors.length} SKU mismatch${skuMismatchedLinkedMonitors.length !== 1 ? "es" : ""} remaining.`
      : "All SKU mismatches resolved.";
    return;
  }

  if (target.classList.contains("sku-mismatch-fix-shopify")) {
    if (!await isConnected()) { alert("Connect to Shopify first."); return; }
    target.textContent = "Pushing…";
    target.disabled = true;
    try {
      await reapplyMonitorDataToShopify(monitor, monitor.lastExtractedData || {});
      await silentRefresh(true);
      skuMismatchedLinkedMonitors = skuMismatchedLinkedMonitors.filter((m) => m.monitor.id !== monitorId);
      renderMonitorDuplicatesPanel();
      monitorDuplicatesStatus.textContent = skuMismatchedLinkedMonitors.length
        ? `Fixed. ${skuMismatchedLinkedMonitors.length} SKU mismatch${skuMismatchedLinkedMonitors.length !== 1 ? "es" : ""} remaining.`
        : "All SKU mismatches resolved.";
    } catch (e) {
      target.textContent = "Failed — retry";
      target.disabled = false;
      alert(`Failed to update Shopify: ${e.message || e}`);
    }
  }
});

shopifyUnmonitoredDeleteSelectedBtn.addEventListener("click", async () => {
  const ids = [...selectedShopifyUnmonitoredIds];
  if (!ids.length) return;
  if (!window.confirm(`Delete ${ids.length} selected Shopify product${ids.length > 1 ? "s" : ""}?`)) return;
  const originalText = shopifyUnmonitoredDeleteSelectedBtn.textContent;
  shopifyUnmonitoredDeleteSelectedBtn.disabled = true;
  try {
    await deleteShopifyUnmonitored(ids, (current, total) => {
      setProgressLabel(shopifyUnmonitoredDeleteSelectedBtn, "Deleting", current, total);
    });
  } finally {
    shopifyUnmonitoredDeleteSelectedBtn.textContent = originalText;
    shopifyUnmonitoredDeleteSelectedBtn.disabled = selectedShopifyUnmonitoredIds.size === 0;
  }
});

shopifyUnmonitoredDeleteAllBtn.addEventListener("click", async () => {
  const ids = shopifyUnmonitoredProducts.map((item) => String(item.id));
  if (!ids.length) return;
  if (!window.confirm(`Delete all ${ids.length} Shopify-only product${ids.length > 1 ? "s" : ""}?`)) return;
  const originalText = shopifyUnmonitoredDeleteAllBtn.textContent;
  shopifyUnmonitoredDeleteAllBtn.disabled = true;
  try {
    await deleteShopifyUnmonitored(ids, (current, total) => {
      setProgressLabel(shopifyUnmonitoredDeleteAllBtn, "Deleting", current, total);
    });
  } finally {
    shopifyUnmonitoredDeleteAllBtn.textContent = originalText;
    shopifyUnmonitoredDeleteAllBtn.disabled = shopifyUnmonitoredProducts.length === 0;
  }
});

// Keep log count in button updated when dashboard refreshes
const _origLoadDashboard = loadDashboard;
async function refreshLogCount() {
  const logs = await getLogs();
  openLogsBtn.textContent = `Logs${logs.length ? ` (${logs.length})` : ""}`;
}

document.getElementById("export-monitors-btn").addEventListener("click", async () => {
  const resp = await chrome.runtime.sendMessage({ type: "get-monitors" }).catch(() => null);
  if (!resp?.ok) return;
  const json = JSON.stringify(resp.monitors, null, 2);
  const blob = new Blob([json], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `monitors-backup-${new Date().toISOString().slice(0, 10)}.json`;
  a.click();
  URL.revokeObjectURL(url);
});


document.getElementById("import-monitors-input").addEventListener("change", async (e) => {
  const file = e.target.files[0];
  if (!file) return;
  e.target.value = "";
  let monitors;
  try {
    monitors = JSON.parse(await file.text());
  } catch {
    alert("Invalid JSON file.");
    return;
  }
  if (!Array.isArray(monitors)) { alert("File must contain a JSON array of monitors."); return; }
  const replace = window.confirm(
    `Import ${monitors.length} monitor${monitors.length !== 1 ? "s" : ""}?\n\nOK = replace all existing monitors\nCancel = merge (skip duplicates)`
  );
  const resp = await chrome.runtime.sendMessage({
    type: "import-monitors",
    payload: { monitors, replace }
  });
  if (resp?.ok) {
    alert(`Imported ${resp.count} monitor${resp.count !== 1 ? "s" : ""} successfully.`);
    loadDashboard();
  } else {
    alert("Import failed: " + (resp?.error || "unknown error"));
  }
});



// --- Google Drive backup/restore ---

const driveBackupBtn = document.getElementById("drive-backup-btn");
const driveRestoreBtn = document.getElementById("drive-restore-btn");
const driveBackupInfo = document.getElementById("drive-backup-info");

async function refreshDriveInfo() {
  const resp = await sendRuntimeMessageWithTimeout({ type: "drive-backup-info" }, 15000).catch(() => null);
  if (resp?.ok && resp.info) {
    const d = new Date(resp.info.modifiedTime);
    driveBackupInfo.textContent = `Last backup: ${d.toLocaleDateString()} ${d.toLocaleTimeString()}`;
  } else {
    driveBackupInfo.textContent = "No backup found";
  }
}

driveBackupBtn.addEventListener("click", async () => {
  const original = driveBackupBtn.textContent;
  driveBackupBtn.disabled = true;
  driveBackupBtn.textContent = "Backing up…";
  try {
    const resp = await sendRuntimeMessageWithTimeout({ type: "drive-backup" }, 30000).catch((error) => ({ ok: false, error: error.message || String(error) }));
    if (resp?.ok) {
      driveBackupInfo.textContent = `Backed up ${resp.count} monitors just now`;
    } else {
      alert("Backup failed: " + (resp?.error || "unknown error"));
    }
  } finally {
    driveBackupBtn.textContent = original;
    driveBackupBtn.disabled = false;
  }
});

driveRestoreBtn.addEventListener("click", async () => {
  const replace = window.confirm(
    "Restore monitors from Google Drive?\n\nOK = replace all existing monitors\nCancel = merge (skip duplicates)"
  );
  const original = driveRestoreBtn.textContent;
  driveRestoreBtn.disabled = true;
  driveRestoreBtn.textContent = "Restoring…";
  try {
    const resp = await sendRuntimeMessageWithTimeout({ type: "drive-restore", payload: { replace } }, 30000).catch((error) => ({ ok: false, error: error.message || String(error) }));
    if (resp?.ok) {
      const d = new Date(resp.modifiedTime);
      alert(`Restored ${resp.count} monitors from backup saved on ${d.toLocaleDateString()} ${d.toLocaleTimeString()}.`);
      loadDashboard();
      await refreshDriveInfo();
    } else {
      alert("Restore failed: " + (resp?.error || "unknown error"));
    }
  } finally {
    driveRestoreBtn.textContent = original;
    driveRestoreBtn.disabled = false;
  }
});

refreshDriveInfo();

// --- Local folder backup ---

const localSetBtn = document.getElementById("local-backup-set-btn");
const localReauthBtn = document.getElementById("local-backup-reauth-btn");
const localClearBtn = document.getElementById("local-backup-clear-btn");
const localStatus = document.getElementById("local-backup-status");
const localBackupInfo = document.getElementById("local-backup-info");
const localBackupFolder = document.getElementById("local-backup-folder");
const localBackupLastWrite = document.getElementById("local-backup-last-write");

function _formatBackupTime(ts) {
  if (!ts) return "Never";
  const d = new Date(ts);
  const now = new Date();
  const diffMs = now - d;
  const diffMin = Math.floor(diffMs / 60000);
  const diffH = Math.floor(diffMs / 3600000);
  const diffD = Math.floor(diffMs / 86400000);
  const remH = diffH % 24;
  const remMin = diffMin % 60;
  const timeStr = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  const dateStr = d.toLocaleDateString([], { weekday: "long", day: "2-digit", month: "long", year: "numeric" });
  if (diffMin < 1) return `Just now — ${timeStr}`;
  if (diffMin < 60) return `${diffMin} minute${diffMin !== 1 ? "s" : ""} ago — ${timeStr}`;
  if (diffH < 24) return `${diffH} hour${diffH !== 1 ? "s" : ""} ${remMin} minute${remMin !== 1 ? "s" : ""} ago — ${timeStr}`;
  return `${diffD} day${diffD !== 1 ? "s" : ""} ${remH} hour${remH !== 1 ? "s" : ""} ${remMin} minute${remMin !== 1 ? "s" : ""} ago — ${dateStr} at ${timeStr}`;
}

async function refreshLocalBackupStatus() {
  const resp = await sendRuntimeMessageWithTimeout({ type: "local-backup-status" }, 5000).catch(() => null);
  const perm = resp?.perm ?? null;
  const folderName = resp?.folderName ?? null;
  const lastWriteAt = resp?.lastWriteAt ?? null;

  if (perm === null) {
    localStatus.textContent = "No backup folder set — click Set folder to enable";
    localStatus.style.color = "var(--infrared, #b91c1c)";
    localSetBtn.style.display = "";
    localReauthBtn.style.display = "none";
    localClearBtn.style.display = "none";
    if (localBackupInfo) localBackupInfo.style.display = "none";
  } else if (perm === "granted") {
    localStatus.textContent = "● Connected";
    localStatus.style.color = "var(--green, #4caf50)";
    localSetBtn.style.display = "none";
    localReauthBtn.style.display = "none";
    localClearBtn.style.display = "";
    if (localBackupInfo) {
      localBackupInfo.style.display = "flex";
      if (localBackupFolder) localBackupFolder.textContent = `Folder: ${folderName ?? "unknown"}`;
      if (localBackupLastWrite) localBackupLastWrite.textContent = `Last saved: ${_formatBackupTime(lastWriteAt)}`;
    }
  } else {
    localStatus.textContent = "● Disconnected — permission lost after restart";
    localStatus.style.color = "var(--infrared, #b91c1c)";
    localSetBtn.style.display = "none";
    localReauthBtn.style.display = "";
    localClearBtn.style.display = "";
    if (localBackupInfo) {
      localBackupInfo.style.display = "flex";
      if (localBackupFolder) localBackupFolder.textContent = `Folder: ${folderName ?? "unknown"}`;
      if (localBackupLastWrite) localBackupLastWrite.textContent = `Last saved: ${_formatBackupTime(lastWriteAt)}`;
    }
  }
}

localSetBtn.addEventListener("click", async () => {
  try {
    const handle = await window.showDirectoryPicker({ mode: "readwrite" });
    await storeDirectoryHandle(handle);
    const resp = await sendRuntimeMessageWithTimeout({ type: "local-backup-write-now" }, 15000).catch((error) => ({ ok: false, error: error.message || String(error) }));
    if (resp?.ok) {
      localStatus.textContent = `Folder set â€" saved ${resp.count} monitors`;
      localStatus.style.color = "var(--green, #4caf50)";
    }
    await refreshLocalBackupStatus();
  } catch (e) {
    if (e.name !== "AbortError") alert("Could not set folder: " + e.message);
  }
});

localReauthBtn.addEventListener("click", async () => {
  try {
    const perm = await requestLocalBackupPermission();
    if (perm === "granted") {
      await sendRuntimeMessageWithTimeout({ type: "local-backup-write-now" }, 15000).catch(() => null);
    }
    await refreshLocalBackupStatus();
  } catch (e) {
    alert("Could not re-authorize: " + e.message);
  }
});

localClearBtn.addEventListener("click", async () => {
  if (!window.confirm("Stop auto-saving to local folder?")) return;
  await clearDirectoryHandle();
  await refreshLocalBackupStatus();
});

refreshLocalBackupStatus();

// --- Backup permission warning banner ---
// Silently restore local backup folder permission on first click after restart.
// No banner, no dialog in most cases — completely invisible to the user.
(async () => {
  const resp = await sendRuntimeMessageWithTimeout({ type: 'local-backup-status' }, 5000).catch(() => null);
  if (!resp?.perm || resp.perm === 'granted') return;
  let _done = false;
  async function _silentRestorePerm() {
    if (_done) return;
    _done = true;
    document.removeEventListener('click', _silentRestorePerm, true);
    try {
      const perm = await requestLocalBackupPermission();
      if (perm === 'granted') {
        await sendRuntimeMessageWithTimeout({ type: 'local-backup-write-now' }, 15000).catch(() => null);
        await refreshLocalBackupStatus();
      }
    } catch (_) {}
  }
  document.addEventListener('click', _silentRestorePerm, true);
})();


// â"€â"€ SW connection watchdog â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
// When the extension background service worker crashes or is restarted, the
// options page context becomes stale. This watchdog pings the SW every 5 s
// and auto-reloads the dashboard when the connection recovers. If the extension
// context itself is invalidated (extension was reloaded while page was open),
// it prompts the user to reload the page.
let _swLastOk = true;
let _swWatchdogRetrying = false;
let _swContextInvalidated = false;
function _swPing() {
  if (_swContextInvalidated) return;
  let resp;
  try {
    resp = chrome.runtime.sendMessage({ type: "ping" });
  } catch (e) {
    // context invalidated â€" chrome.runtime itself is gone
    if (!_swContextInvalidated) {
      _swContextInvalidated = true;
      const bar = document.createElement("div");
      bar.style.cssText = "position:fixed;top:0;left:0;right:0;z-index:99999;background:#b41f1f;color:#fff;padding:10px 16px;font:14px/1.4 system-ui,sans-serif;text-align:center";
      bar.textContent = "Extension was reloaded — ";
      const btn = document.createElement("button");
      btn.textContent = "click here to refresh this page";
      btn.style.cssText = "margin-left:8px;padding:2px 12px;cursor:pointer;border-radius:4px;border:none;background:#fff;color:#b41f1f;font-weight:600";
      btn.onclick = () => location.reload();
      bar.appendChild(btn);
      document.body?.prepend(bar);
    }
    return;
  }
  resp.then(() => {
    if (!_swLastOk && !_swWatchdogRetrying) {
      _swLastOk = true;
      _swWatchdogRetrying = true;
      // SW restarted — reset version so the next silentRefresh does a full fetch.
      _lastKnownSwVersion = -1;
      loadDashboard(false)
        .then(() => updateCheckState().catch(() => {}))
        .catch(() => {})
        .finally(() => { _swWatchdogRetrying = false; });
    } else {
      _swLastOk = true;
    }
  }).catch(() => {
    _swLastOk = false;
  });
}
setInterval(_swPing, 15000);

chrome.runtime.sendMessage({ type: "normalize-monitor-brands" }).catch(() => {});
await loadDashboard(false).catch(() => {});
await updateCheckState().catch(() => {});
refreshUndoCount().catch(() => {});
refreshLogCount().catch(() => {});

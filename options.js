import { buildDiffRows, summarizeDiff, getLogs, clearLogs, addLog, canonicalizeBrand } from "./shared.js";
import { isConnected, connectShopify, disconnectShopify, verifyConnection, importMonitorProduct, undoLastImport, getRecentImports, getShopifyMetadata, getShopifyProductsSnapshot, deleteShopifyProducts, deleteProduct, clearShopifyProductsSnapshotCache, getFullyOutOfStockProductIds, primeImportCaches } from "./shopify.js";

const monitorGrid = document.getElementById("monitor-grid");
const monitorDetail = document.getElementById("monitor-detail");
const dashboardStats = document.getElementById("dashboard-stats");
const searchInput = document.getElementById("dashboard-search");
const changedOnlyInput = document.getElementById("changed-only");
const bulkCheckBtn = document.getElementById("bulk-check");
const bulkImportBtn = document.getElementById("bulk-import");
const stopImportBtn = document.getElementById("stop-import");
const bulkDeleteBtn = document.getElementById("bulk-delete");
const bulkDeleteShopifyBtn = document.getElementById("bulk-delete-shopify");
const selectAllBtn = document.getElementById("select-all");
const deselectBtn = document.getElementById("deselect-all");
const checkAllBtn = document.getElementById("check-all");
const stopChecksBtn = document.getElementById("stop-checks");
const undoLastBtn = document.getElementById("undo-last");
const filterBrand = document.getElementById("filter-brand");
const filterType = document.getElementById("filter-type");
const filterSort = document.getElementById("filter-sort");
const openShopifyUnmonitoredBtn = document.getElementById("open-shopify-unmonitored-btn");
const openMonitorNotShopifyBtn = document.getElementById("open-monitor-not-shopify-btn");
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
const skuDuplicatesPanel = document.getElementById("sku-duplicates-panel");
const skuDuplicatesStatus = document.getElementById("sku-duplicates-status");
const skuDuplicatesList = document.getElementById("sku-duplicates-list");
const skuDuplicatesRefreshBtn = document.getElementById("sku-duplicates-refresh-btn");
const skuDuplicatesSelectAllBtn = document.getElementById("sku-duplicates-select-all-btn");
const skuDuplicatesDeselectBtn = document.getElementById("sku-duplicates-deselect-btn");
const skuDuplicatesDeleteBtn = document.getElementById("sku-duplicates-delete-btn");
const closeSkuDuplicatesBtn = document.getElementById("close-sku-duplicates-btn");
const openSkuDuplicatesBtn = document.getElementById("open-sku-duplicates-btn");

let allMonitors = [];
let selectedMonitorId = null;
const checkedIds = new Set();
const collapsedGroups = new Set();
let lastCheckedId = null;
let lastCheckedGroupKey = null;
let canCheck = false; // requires Shopify connection OR test mode
let autoEnabled = true; // mirrors chrome.storage autoIntervalEnabled
let shopifyUnmonitoredProducts = [];
let monitorNotShopifyMonitors = [];
let shopifyOutOfStockMonitorIds = null;
let monitorDuplicateSkuGroups = [];
let shopifyProductCount = null;
let shopifyUniqueSkuCount = null;
let outOfStockSectionHidden = false;
let lastOutOfStockSignature = "";
const dismissedOutOfStockMonitorIds = new Set();
const selectedShopifyUnmonitoredIds = new Set();
const selectedMonitorNotShopifyIds = new Set();
const selectedDuplicateMonitorIds = new Set();
const selectedDuplicateIds = new Set();
let skuDuplicateGroups = []; // [{ sku, keepProduct, keepReason, extraProducts }]
let skuDuplicatesAdminBase = ""; // https://admin.shopify.com/store/{name}/products
let dashboardStorageRefreshTimer = null;
let dashboardUiStateSaveTimer = null;
let stopImportRequested = false;
let importInProgress = false;

const PAGE_SIZE = 50; // 10 cols × 5 rows
const SHOPIFY_OOS_GROUP_KEY = "__shopify_oos__";
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
  try { return new URL(url).hostname; } catch { return url; }
}

function normalizeSku(value) {
  return String(value || "").trim().toLowerCase();
}

function getProductSkuSet(product) {
  const skus = new Set();
  const baseSku = normalizeSku(product?.sku);
  if (baseSku) skus.add(baseSku);
  for (const sku of (product?.variantSkus || [])) {
    const normalized = normalizeSku(sku);
    if (normalized) skus.add(normalized);
  }
  return skus;
}

function isMonitorFullyOutOfStock(monitor) {
  const live = monitor?.lastExtractedData || {};
  const inStock = Array.isArray(live.inStock) ? live.inStock.filter(Boolean) : [];
  const outOfStock = Array.isArray(live.outOfStock) ? live.outOfStock.filter(Boolean) : [];
  return inStock.length === 0 && outOfStock.length > 0;
}

function getCombinedOutOfStockMonitorIds(monitors = allMonitors) {
  const ids = new Set((monitors || []).filter(isMonitorFullyOutOfStock).map((monitor) => monitor.id));
  if (shopifyOutOfStockMonitorIds) {
    shopifyOutOfStockMonitorIds.forEach((id) => ids.add(id));
  }
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
    filterBrand: filterBrand.value || "",
    filterType: filterType.value || "",
    filterSort: filterSort.value || "created-desc",
    collapsedGroups: [...collapsedGroups],
    groupPages: [...groupPages.entries()],
    groupFilters: [...groupFilters.entries()],
    outOfStockSectionHidden: !!outOfStockSectionHidden,
    dismissedOutOfStockMonitorIds: [...dismissedOutOfStockMonitorIds]
  };
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

  searchInput.value = state.search || "";
  changedOnlyInput.checked = !!state.changedOnly;
  filterBrand.value = state.filterBrand || "";
  filterType.value = state.filterType || "";
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

  groupFilters.clear();
  for (const entry of (state.groupFilters || [])) {
    if (Array.isArray(entry) && entry.length === 2) {
      groupFilters.set(entry[0], entry[1] || {});
    }
  }

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

function applyMonitorsUpdate(nextMonitors = []) {
  const previousMonitors = allMonitors;
  reconcileOutOfStockMonitorUpdates(previousMonitors, nextMonitors);
  allMonitors = nextMonitors;
}

function createStatCard(label, value, tone = "") {
  return `<article class="stat-card ${tone}"><span>${escapeHtml(label)}</span><strong>${escapeHtml(String(value))}</strong></article>`;
}

function getProductPrimarySku(product) {
  return normalizeSku(product?.sku) || [...getProductSkuSet(product)][0] || "";
}

function renderStats(monitors) {
  const changed = monitors.filter((m) => m.changeCount > 0).length;
  const errors = monitors.filter((m) => m.status === "error").length;
  const dupeCount = (shopifyProductCount != null && shopifyUniqueSkuCount != null)
    ? shopifyProductCount - shopifyUniqueSkuCount
    : 0;
  const shopifyCountLabel = shopifyProductCount == null ? "..." : shopifyProductCount;
  const shopifySkuLabel = shopifyUniqueSkuCount == null ? "..." : shopifyUniqueSkuCount;
  const shopifyProductsCard = shopifyProductCount == null
    ? createStatCard("Shopify Products", "...")
    : dupeCount > 0
      ? `<article class="stat-card" title="${dupeCount} product${dupeCount !== 1 ? "s" : ""} share a SKU with another Shopify product (duplicates). The diff buttons compare by unique SKU."><span>Shopify Products</span><strong>${shopifyCountLabel}</strong><div style="font-size:11px;color:#ef4444;margin-top:2px">+${dupeCount} duplicate${dupeCount !== 1 ? "s" : ""}</div></article>`
      : createStatCard("Shopify Products", shopifyCountLabel);
  const changedActionsHtml = changed > 0 ? `
    <div class="stat-card-actions">
      <button class="stat-action-btn" data-stat-action="filter-changed">Filter</button>
      <button class="stat-action-btn" data-stat-action="reset-changed">Reset all</button>
    </div>` : "";
  dashboardStats.innerHTML = [
    createStatCard("Monitors", monitors.length),
    shopifyProductsCard,
    createStatCard("Shopify Unique SKUs", shopifySkuLabel),
    `<article class="stat-card warm"><span>Changed</span><strong>${changed}</strong>${changedActionsHtml}</article>`,
    createStatCard("Errors", errors, errors ? "danger" : "")
  ].join("");
}

dashboardStats.addEventListener("click", async (e) => {
  const action = e.target.dataset.statAction;
  if (!action) return;
  if (action === "filter-changed") {
    changedOnlyInput.checked = !changedOnlyInput.checked;
    renderGrid(getFilteredMonitors());
  }
  if (action === "reset-changed") {
    e.target.textContent = "…";
    e.target.disabled = true;
    await chrome.runtime.sendMessage({ type: "reset-all-change-counts" });
    await silentRefresh();
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

function buildHistoryEntry(entry, index) {
  const rows = buildDiffRows(entry.previousHtml || "", entry.currentHtml || "");
  const s = summarizeDiff(rows);
  const liveChangeHtml = (entry.liveChanges && entry.liveChanges.length)
    ? `<div class="live-change-pills">${entry.liveChanges.map((c) => `<span class="live-change-pill">${escapeHtml(c)}</span>`).join("")}</div>`
    : "";

  return `
    <details class="history-entry" ${index === 0 ? "open" : ""}>
      <summary>
        <div>
          <strong>${escapeHtml(formatTimestamp(entry.changedAt, "Change recorded"))}</strong>
          <p class="subtle">${escapeHtml(entry.reason || "scheduled")}</p>
        </div>
        <div class="pill-row">
          <span class="pill add">+${s.added}</span>
          <span class="pill remove">-${s.removed}</span>
          <span class="pill neutral">~${s.changed}</span>
        </div>
      </summary>
      ${liveChangeHtml}
      ${renderDiffViewer(rows)}
    </details>`;
}

function truncateHtml(str, maxLen = 150000) {
  if (!str || str.length <= maxLen) return escapeHtml(str || "");
  const kb = Math.round(str.length / 1024);
  return escapeHtml(str.slice(0, maxLen)) + `\n\n… [truncated — full size: ${kb} KB]`;
}

function renderProductData(p, monitorId, priceAdjustment = 80) {
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

  const imagesHtml = (p.images && p.images.length) ? `
    <div class="product-images-row">
      ${p.images.map((src) => `<a href="${escapeHtml(src)}" target="_blank" rel="noopener" class="product-thumb-link"><img class="product-thumb" src="${escapeHtml(src)}" alt="" loading="lazy"></a>`).join("")}
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
        ${field("Type", p.type)}
        ${field("Gender", p.gender)}
        ${field("Gender display", p.genderDisplay)}
        ${field("Color raw", p.colorRaw)}
        ${field("Color", p.color)}
        ${field("Color final", p.colorFinal)}
        ${field("Added amount", adjustmentStr)}
        ${field("Price", priceStr)}
        ${field("SKU / Code", p.sku)}
      </div>
      ${p.description ? `<div class="product-description-block"><p class="product-field-label">Description</p><p class="product-description">${escapeHtml(p.description)}</p></div>` : ""}
      ${sourceTags ? `<div class="source-row">${sourceTags}</div>` : ""}
    </div>`;
}

function renderProductDataEdit(p, monitorId) {
  const val = (v) => escapeHtml(v || "");
  return `
    <div class="product-data-section">
      <p class="product-data-title">Edit product data</p>
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

function getIntervalDisplay(minutes) {
  const m = minutes || 1440;
  if (m >= 1440 && m % 1440 === 0) return { value: m / 1440, unit: 1440 };
  if (m >= 60 && m % 60 === 0) return { value: m / 60, unit: 60 };
  return { value: m, unit: 1 };
}

function getMonitorThumb(monitor) {
  const images = monitor.productData?.images;
  return Array.isArray(images) && images.length ? images[0] : "";
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

function renderSquare(monitor) {
  const isActive = selectedMonitorId === monitor.id;
  const isChecked = checkedIds.has(monitor.id);
  const num = allMonitors.findIndex((m) => m.id === monitor.id) + 1;
  const thumb = getMonitorThumb(monitor);
  const importedBadge = monitor.shopifyProductId ? `<span class="square-imported">Shopify</span>` : "";
  const outOfStockBadge = isMonitorFullyOutOfStock(monitor) ? `<span class="square-oos">Out of stock</span>` : "";
  const adjustment = Number.isFinite(Number(monitor.priceAdjustment)) ? Number(monitor.priceAdjustment) : 80;

  return `
    <article class="monitor-square${isActive ? " active" : ""}" data-id="${monitor.id}">
      <span class="monitor-num">#${num}</span>
      <input type="checkbox" class="square-check" data-id="${monitor.id}"${isChecked ? " checked" : ""} title="Select for bulk check">
      <div class="status-dot ${escapeHtml(monitor.status || "idle")}"></div>
      ${importedBadge}
      ${outOfStockBadge}
      ${thumb ? `<div class="square-thumb-wrap"><img class="square-thumb" src="${escapeHtml(thumb)}" alt="${escapeHtml(monitor.productData?.name || monitor.name || "Product image")}"></div>` : ""}
      ${(() => {
        const pd = monitor.productData;
        const brand = pd?.brand || "";
        const sku = pd?.sku || "";
        const cardTitle = [brand, sku].filter(Boolean).join(" ") || monitor.name;
        const productName = pd?.name || "";
        return `<p class="square-name" title="${escapeHtml(cardTitle)}">${escapeHtml(cardTitle)}</p>
      ${productName ? `<p class="square-domain" title="${escapeHtml(productName)}" style="white-space:normal;line-height:1.3;font-size:10px">${escapeHtml(productName)}</p>` : `<p class="square-domain" title="${escapeHtml(getDomain(monitor.url))}">${escapeHtml(getDomain(monitor.url))}</p>`}
      <p class="square-domain" title="Added amount: ${escapeHtml(String(adjustment))}" style="font-size:10px">Added: ${escapeHtml(String(adjustment))}</p>`;
      })()}
      ${renderSquareLiveData(monitor)}
      <div class="square-meta">
        ${monitor.changeCount > 0 ? `<span class="square-changes">${monitor.changeCount}</span>` : ""}
        <span class="square-time">${escapeHtml(timeAgo(monitor.lastCheckedAt))}</span>
      </div>
      <div class="auto-badge ${autoEnabled ? "auto-on" : "auto-off"}">${autoEnabled ? "AUTO ON" : "AUTO OFF"}</div>
    </article>`;
}

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
  }
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
  const chartGender = /women|girl|female/i.test(String(gender || "")) ? "Women" : "Men";
  const chart = SIZE_CHARTS[chartBrand]?.[chartGender];
  if (!chart) return null;
  const n = parseFloat(String(usSize).replace(/[^\d.]/g, ""));
  if (isNaN(n) || n <= 0 || n > 30) return null;
  return chart[String(n)] || null;
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
    const eu = getEuSize(s, brand, gender);
    const label = eu ? `${s} US · ${eu} EU` : s;
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
  const liveRows = buildDiffRows(monitor.previousHtmlSnapshot || "", monitor.lastHtmlSnapshot || "");

  const selectorsHtml = selectors.map((sel) => `
    <span class="selector-chip" data-selector="${escapeHtml(sel)}">
      <span class="chip-text">${escapeHtml(sel)}</span>
      <button class="chip-remove" title="Remove">×</button>
    </span>`).join("");

  const historyHtml = (monitor.changeHistory || []).length
    ? monitor.changeHistory.map((e, i) => buildHistoryEntry(e, i)).join("")
    : `<div class="empty-panel">No changes recorded yet.</div>`;

  const hasSnapshot = !!monitor.lastHtmlSnapshot;

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
      ${monitor.lastError ? `<div><p class="field-label">Error</p><p class="danger-text">${escapeHtml(monitor.lastError)}</p></div>` : ""}
    </div>

    ${monitor.initialFullPageText ? `
      <details class="html-section first-capture" open>
        <summary>First-visit capture · ${escapeHtml(formatTimestamp(monitor.initialCapturedAt))}</summary>
        <p class="first-capture-hint">Everything extracted on the first visit — product name, description, code, brand, price.</p>
        ${renderProductData(monitor.productData, monitor.id, monitor.priceAdjustment)}
        <details class="raw-html-panel" style="margin:0 12px 12px">
          <summary>Full page text (readable)</summary>
          <pre>${truncateHtml(monitor.initialFullPageText)}</pre>
        </details>
      </details>
    ` : ""}

    ${renderLiveExtracted(monitor)}

    ${hasSnapshot ? `
      <details class="html-section">
        <summary>Changes — before vs after</summary>
        ${renderDiffViewer(liveRows)}
      </details>
    ` : `<p class="subtle">No snapshot yet — click "Check now" to capture one.</p>`}

    <details class="history-panel"${(monitor.changeHistory || []).length ? " open" : ""}>
      <summary>Change history (${(monitor.changeHistory || []).length})</summary>
      <div class="history-list">${historyHtml}</div>
    </details>

    <div class="card-actions">
      <button data-save="${monitor.id}" class="primary">Save</button>
      <button data-refresh="${monitor.id}" class="secondary"${canCheck ? "" : " disabled"}>Check now</button>
      <button class="inline-button back-to-monitor-btn" title="Scroll to monitor card">&#8593; Back to card</button>
      <button data-delete="${monitor.id}" class="inline-button danger">Delete</button>
    </div>
  `;

  monitorDetail.style.display = "";
}

function getFilteredMonitors() {
  const query = searchInput.value.trim().toLowerCase();
  const changedOnly = changedOnlyInput.checked;
  const brandFilter = filterBrand.value;
  const typeFilter = filterType.value;
  const sort = filterSort.value;

  let result = allMonitors.filter((m) => {
    if (changedOnly && !m.changeCount) return false;
    if (brandFilter && (getBrandFromMonitor(m) || "").trim().toLowerCase() !== brandFilter.trim().toLowerCase()) return false;
    if (typeFilter && (m.productData?.type || "").trim().toLowerCase() !== typeFilter.trim().toLowerCase()) return false;
    if (!query) return true;
    const sels = (m.selectors || (m.selector ? [m.selector] : [])).join(" ");
    const pd = m.productData;
    const pdText = pd ? [pd.name, pd.sku, pd.description, pd.brand, pd.type].filter(Boolean).join(" ") : "";
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

function populateFilterOptions() {
  const brands = [...new Set(allMonitors.map(getBrandFromMonitor).filter(Boolean))].sort();
  const types = [...new Set(allMonitors.map(m => m.productData?.type).filter(Boolean))].sort();

  const prevBrand = filterBrand.value;
  const prevType = filterType.value;

  filterBrand.innerHTML = `<option value="">All brands</option>` + brands.map(b => `<option value="${escapeHtml(b)}"${b === prevBrand ? " selected" : ""}>${escapeHtml(b)}</option>`).join("");
  filterType.innerHTML = `<option value="">All types</option>` + types.map(t => `<option value="${escapeHtml(t)}"${t === prevType ? " selected" : ""}>${escapeHtml(t)}</option>`).join("");

  // restore active class after repopulating
  filterBrand.classList.toggle("active", !!filterBrand.value);
  filterType.classList.toggle("active", !!filterType.value);
}

function renderGrid(monitors) {
  if (!monitors.length) {
    monitorGrid.innerHTML = `<section class="empty-state large"><p>No monitors match this view.</p><p class="subtle">Try clearing the search or filters.</p></section>`;
    return;
  }

  const NOW = Date.now();
  const HOURS_48 = 48 * 60 * 60 * 1000;
  const NEW_GROUP_KEY = "__new48h__";
  const ERROR_GROUP_KEY = "__errors__";
  // Apply group-level brand/type filter then paginate
  function applyGroupFilter(key, mons) {
    const f = groupFilters.get(key) || {};
    let result = mons;
    if (f.brand) result = result.filter(m => (getBrandFromMonitor(m) || "").toLowerCase() === f.brand.toLowerCase());
    if (f.type) result = result.filter(m => (m.productData?.type || "").toLowerCase() === f.type.toLowerCase());
    return result;
  }

  function buildGroupFilterPills(key, allMons) {
    const brands = [...new Set(allMons.map(getBrandFromMonitor).filter(Boolean))].sort();
    const types  = [...new Set(allMons.map(m => m.productData?.type).filter(Boolean))].sort();
    const f = groupFilters.get(key) || {};
    if (!brands.length && !types.length) return "";
    const bPills = brands.map(b => {
      const active = f.brand === b;
      const count = allMons.filter(m => (getBrandFromMonitor(m)||"").toLowerCase()===b.toLowerCase()).length;
      return `<button class="group-filter-pill${active?" active":""}" data-group-key="${escapeHtml(key)}" data-filter-brand="${escapeHtml(b)}">${escapeHtml(b)} <span>${count}</span></button>`;
    }).join("");
    const tPills = types.map(t => {
      const active = f.type === t;
      const count = allMons.filter(m => (m.productData?.type||"").toLowerCase()===t.toLowerCase()).length;
      return `<button class="group-filter-pill type-pill${active?" active":""}" data-group-key="${escapeHtml(key)}" data-filter-type="${escapeHtml(t)}">${escapeHtml(t)} <span>${count}</span></button>`;
    }).join("");
    const hasActive = f.brand || f.type;
    return `<div class="group-filter-bar">
      ${bPills}${types.length ? `<span class="group-filter-sep"></span>${tPills}` : ""}
      ${hasActive ? `<button class="group-filter-clear" data-group-key="${escapeHtml(key)}">✕ Clear</button>` : ""}
    </div>`;
  }

  function buildGroup(key, titleHtml, allMons, extraActions = "") {
    const collapsed = collapsedGroups.has(key);
    const arrow = collapsed ? "&#9654;" : "&#9660;";
    const filterPills = buildGroupFilterPills(key, allMons);
    const filtered = applyGroupFilter(key, allMons);
    const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
    const currentPage = Math.min(groupPages.get(key) || 0, totalPages - 1);
    groupPages.set(key, currentPage);
    const pageSlice = filtered.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE);

    const pagination = totalPages > 1 ? `
      <div class="group-pagination">
        <button class="inline-button group-page-btn" data-group-key="${escapeHtml(key)}" data-page="${currentPage - 1}" ${currentPage === 0 ? "disabled" : ""}>&#8592; Prev</button>
        <span class="group-page-label">Page ${currentPage + 1} of ${totalPages} &nbsp;·&nbsp; ${filtered.length} monitors</span>
        <button class="inline-button group-page-btn" data-group-key="${escapeHtml(key)}" data-page="${currentPage + 1}" ${currentPage >= totalPages - 1 ? "disabled" : ""}>Next &#8594;</button>
      </div>` : "";

    return `
    <div class="site-group">
      <div class="site-group-header">
        <button class="group-toggle-btn" data-group-key="${escapeHtml(key)}" title="${collapsed ? "Expand" : "Collapse"}">${arrow}</button>
        ${titleHtml}
        <span class="site-group-count">${allMons.length} monitor${allMons.length !== 1 ? "s" : ""}</span>
        ${extraActions}
      </div>
      ${filterPills}
      ${collapsed ? "" : `<div class="site-group-grid">${pageSlice.map(renderSquare).join("")}</div>${pagination}`}
    </div>`;
  }

  const newMons = monitors.filter((m) => m.createdAt && !m.shopifyProductId && !m.hiddenFromNew48h && (NOW - new Date(m.createdAt).getTime()) < HOURS_48);
  const errorMons = monitors.filter((m) => m.status === "error");
  const outOfStockIdSet = getVisibleOutOfStockMonitorIds(monitors);
  const shopifyOutOfStockMons = [...outOfStockIdSet].map((id) => monitors.find((m) => m.id === id)).filter(Boolean);
  const selectedNewMons = newMons.filter((m) => checkedIds.has(m.id));
  const selectedShopifyOutOfStockMons = shopifyOutOfStockMons.filter((m) => checkedIds.has(m.id));

  const newSection = newMons.length
    ? buildGroup(
        NEW_GROUP_KEY,
        `<span class="site-group-title new-group-title">New · last 48h</span>`,
        newMons,
        `<div class="site-group-actions">
          <button class="site-select-btn inline-button" data-ids="${escapeHtml(newMons.map((m) => m.id).join(","))}">${selectedNewMons.length === newMons.length && newMons.length ? "Deselect shown" : "Select shown"}</button>
          <button class="site-clear-selected-btn inline-button" data-ids="${escapeHtml(selectedNewMons.map((m) => m.id).join(","))}" ${selectedNewMons.length ? "" : "disabled"}>Clear selected</button>
          <button class="site-clear-btn inline-button danger" data-ids="${escapeHtml(newMons.map((m) => m.id).join(","))}">Clear</button>
        </div>`
      )
    : "";

  const errorSection = errorMons.length
    ? buildGroup(ERROR_GROUP_KEY, `<span class="site-group-title error-group-title">Errors</span>`, errorMons)
    : "";

  const shopifyOutOfStockSection = shopifyOutOfStockMons.length && !outOfStockSectionHidden
    ? buildGroup(
        SHOPIFY_OOS_GROUP_KEY,
        `<span class="site-group-title error-group-title">Out of stock</span>`,
        shopifyOutOfStockMons,
        `<div class="site-group-actions">
          <button class="site-select-btn inline-button" data-ids="${escapeHtml(shopifyOutOfStockMons.map((m) => m.id).join(","))}">${selectedShopifyOutOfStockMons.length === shopifyOutOfStockMons.length && shopifyOutOfStockMons.length ? "Deselect all out of stock" : "Select all out of stock"}</button>
          <button class="shopify-oos-clear-btn inline-button">Clear</button>
        </div>`
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

  monitorGrid.innerHTML = errorSection + shopifyOutOfStockSection + newSection + domainSections;
}

function updateBulkBtn() {
  const n = checkedIds.size;
  bulkCheckBtn.disabled = n === 0 || !canCheck || importInProgress;
  bulkDeleteBtn.disabled = n === 0 || importInProgress;
  bulkDeleteShopifyBtn.disabled = n === 0 || importInProgress;
  bulkImportBtn.disabled = n === 0 || importInProgress;
  bulkCheckBtn.textContent = n > 0 ? `Check selected (${n})` : "Check selected";
  bulkDeleteBtn.textContent = n > 0 ? `Delete selected (${n})` : "Delete selected";
  bulkDeleteShopifyBtn.textContent = n > 0 ? `Delete from Shopify + Monitors (${n})` : "Delete from Shopify + Monitors";
  if (!importInProgress) {
    bulkImportBtn.textContent = n > 0 ? `Import to Shopify (${n})` : "Import to Shopify";
    stopImportBtn.textContent = "Stop Import";
  }
  stopImportBtn.style.display = importInProgress ? "" : "none";
  stopImportBtn.disabled = !importInProgress;
  const bulkIntervalRow = document.getElementById("bulk-interval-row");
  if (bulkIntervalRow) bulkIntervalRow.style.display = n > 0 ? "" : "none";
  selectAllBtn.style.display = "";
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
  const currentOutOfStockSignature = getOutOfStockSignature(allMonitors);
  const currentOutOfStockIds = getCombinedOutOfStockMonitorIds(allMonitors);
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
  renderStats(allMonitors);
  populateFilterOptions();
  renderGrid(getFilteredMonitors());
  updateOutOfStockButtons();

  if (selectedMonitorId) {
    const m = allMonitors.find((x) => x.id === selectedMonitorId);
    if (m) renderDetail(m);
    else { selectedMonitorId = null; monitorDetail.style.display = "none"; }
  }

  updateBulkBtn();
  scheduleSaveDashboardUiState();
}

async function loadDashboard() {
  monitorGrid.innerHTML = `<p style="grid-column:1/-1;padding:20px;color:var(--muted)">Loading monitors…</p>`;
  const response = await chrome.runtime.sendMessage({ type: "get-monitors" });

  if (!response?.ok) {
    monitorGrid.innerHTML = `<p style="grid-column:1/-1;padding:20px;color:#b41f1f">${escapeHtml(response?.error || "Unable to load monitors.")}</p>`;
    return;
  }

  applyMonitorsUpdate(response.monitors || []);

  if (!allMonitors.length) {
    renderStats([]);
    monitorGrid.innerHTML = `<section class="empty-state large" style="grid-column:1/-1"><p>No monitors yet.</p><p class="subtle">Use the extension popup to create one.</p></section>`;
    monitorDetail.style.display = "none";
    refreshShopifyDashboardState().catch(() => {});
    return;
  }

  renderAll();
  refreshShopifyDashboardState().catch(() => {});
}

// Refresh data and re-render without the loading spinner flash.
async function silentRefresh() {
  const response = await chrome.runtime.sendMessage({ type: "get-monitors" });
  if (!response?.ok) return;
  applyMonitorsUpdate(response.monitors || []);
  renderAll();
  refreshShopifyDashboardState().catch(() => {});
}

async function refreshShopifyProductCount(forceRefresh = false) {
  if (!await isConnected()) {
    shopifyProductCount = null;
    shopifyUniqueSkuCount = null;
    renderStats(allMonitors);
    return;
  }
  try {
    if (forceRefresh) clearShopifyProductsSnapshotCache();
    const products = await getShopifyProductsSnapshot();
    shopifyProductCount = products.length;
    const uniqueSkus = new Set(products.map((product) => getProductPrimarySku(product)).filter(Boolean));
    shopifyUniqueSkuCount = uniqueSkus.size;
  } catch (_) {
    shopifyProductCount = null;
    shopifyUniqueSkuCount = null;
  }
  renderStats(allMonitors);
}

async function refreshOpenShopifyPanels(forceRefresh = false) {
  if (shopifyUnmonitoredPanel.style.display !== "none") {
    await loadShopifyUnmonitoredProducts(forceRefresh);
  }
  if (monitorNotShopifyPanel.style.display !== "none") {
    await loadMonitorNotShopifyMonitors(forceRefresh);
  }
  if (skuDuplicatesPanel.style.display !== "none") {
    await loadDuplicateSkuProducts(forceRefresh);
  }
  if (metaSection.style.display !== "none" && (forceRefresh || !metaContent.innerHTML.trim())) {
    await fetchAndShowMeta();
  }
}

async function refreshShopifyDashboardState(forceRefresh = false) {
  await refreshShopifyProductCount(forceRefresh);
  await refreshOpenShopifyPanels(forceRefresh);
}

function addSelectorChip(list, value) {
  const chip = document.createElement("span");
  chip.className = "selector-chip";
  chip.dataset.selector = value;
  chip.innerHTML = `<span class="chip-text">${escapeHtml(value)}</span><button class="chip-remove" title="Remove">×</button>`;
  list.appendChild(chip);
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
    const id = target.dataset.id;
    const groupKey = target.closest(".site-group")?.querySelector(".group-toggle-btn")?.dataset.groupKey || null;
    if (event.shiftKey && lastCheckedId && lastCheckedId !== id && groupKey && groupKey === lastCheckedGroupKey) {
      const visible = getFilteredMonitors();
      const NOW = Date.now();
      const HOURS_48 = 48 * 60 * 60 * 1000;
      const groupMonitors = groupKey === "__new48h__"
        ? visible.filter((m) => m.createdAt && !m.shopifyProductId && !m.hiddenFromNew48h && (NOW - new Date(m.createdAt).getTime()) < HOURS_48)
        : groupKey === "__shopify_oos__"
          ? visible.filter((m) => isMonitorFullyOutOfStock(m) || shopifyOutOfStockMonitorIds?.has(m.id))
        : groupKey === "__errors__"
          ? visible.filter((m) => m.status === "error")
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
    renderGrid(getFilteredMonitors());
    return;
  }

  if (target.classList.contains("site-select-btn")) {
    const ids = target.dataset.ids.split(",").filter(Boolean);
    const allSelected = ids.every((id) => checkedIds.has(id));
    ids.forEach((id) => allSelected ? checkedIds.delete(id) : checkedIds.add(id));
    updateBulkBtn();
    renderGrid(getFilteredMonitors());
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
    if (!window.confirm(`Clear ${ids.length} selected monitor${ids.length > 1 ? "s" : ""} from New · last 48h?`)) return;
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
    if (!window.confirm(`Clear ${ids.length} monitor${ids.length > 1 ? "s" : ""} from New · last 48h?`)) return;
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
      renderGrid(getFilteredMonitors());
    }
    return;
  }

  // Group filter pill — brand
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
    renderGrid(getFilteredMonitors());
    return;
  }

  // Clear group filter
  if (target.classList.contains("group-filter-clear")) {
    const key = target.dataset.groupKey;
    groupFilters.delete(key);
    groupPages.set(key, 0);
    renderGrid(getFilteredMonitors());
    return;
  }

  const square = target.closest(".monitor-square");
  if (!square) return;
  const id = square.dataset.id;
  const monitor = allMonitors.find((m) => m.id === id);
  if (!monitor) return;

  if (selectedMonitorId === id) {
    selectedMonitorId = null;
    monitorDetail.style.display = "none";
    square.classList.remove("active");
    return;
  }

  monitorGrid.querySelector(".monitor-square.active")?.classList.remove("active");
  selectedMonitorId = id;
  square.classList.add("active");
  renderDetail(monitor);
  monitorDetail.scrollIntoView({ behavior: "smooth", block: "nearest" });
});

// Detail panel: chip-remove, add-selector, save, check, delete
monitorDetail.addEventListener("click", async (event) => {
  const target = event.target;

  if (target.classList.contains("back-to-monitor-btn")) {
    const card = monitorGrid.querySelector(".monitor-square.active");
    if (card) card.scrollIntoView({ behavior: "smooth", block: "center" });
    return;
  }

  if (target.classList.contains("reset-count-btn")) {
    target.textContent = "…";
    target.disabled = true;
    await chrome.runtime.sendMessage({ type: "reset-change-count", monitorId: target.dataset.id });
    await silentRefresh();
    return;
  }

  if (target.classList.contains("chip-remove")) {
    target.closest(".selector-chip")?.remove();
    return;
  }

  if (target.classList.contains("pdata-edit-btn")) {
    const monitorId = target.dataset.monitorId;
    const monitor = allMonitors.find((m) => m.id === monitorId);
    if (!monitor?.productData) return;
    target.closest(".product-data-section").outerHTML = renderProductDataEdit(monitor.productData, monitorId);
    return;
  }

  if (target.classList.contains("pdata-cancel-btn")) {
    const monitor = allMonitors.find((m) => m.id === selectedMonitorId);
    if (monitor) renderDetail(monitor);
    return;
  }

  if (target.classList.contains("pdata-save-btn")) {
    const monitorId = target.dataset.monitorId;
    const monitor = allMonitors.find((m) => m.id === monitorId);
    if (!monitor) return;
    const section = target.closest(".product-data-section");
    const updated = { ...(monitor.productData || {}), source: monitor.productData?.source || [] };
    section.querySelectorAll(".pdata-input").forEach((inp) => {
      const f = inp.dataset.field;
      if (f) updated[f] = inp.value.trim() || null;
    });
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
    await loadDashboard();
    return;
  }

  if (target.dataset.refresh) {
    target.textContent = "Checking…";
    target.disabled = true;
    await chrome.runtime.sendMessage({ type: "refresh-monitor", monitorId: target.dataset.refresh });
    await loadDashboard();
    return;
  }

  if (target.dataset.delete) {
    await chrome.runtime.sendMessage({ type: "delete-monitor", monitorId: target.dataset.delete });
    selectedMonitorId = null;
    monitorDetail.style.display = "none";
    await loadDashboard();
    refreshUndoCount();
  }
});

// Enter to add selector
monitorDetail.addEventListener("keydown", (event) => {
  if (event.key !== "Enter" || !event.target.classList.contains("selector-input")) return;
  const value = event.target.value.trim();
  if (!value) return;
  addSelectorChip(monitorDetail.querySelector(".selectors-list"), value);
  event.target.value = "";
});

function runBulkCheck(ids) {
  if (!ids.length) return;
  const total = ids.length;
  const CONCURRENCY = 8;
  let done = 0;
  let started = 0;
  let stopped = false;

  const freeze = () => {
    checkAllBtn.disabled = true;
    bulkCheckBtn.disabled = true;
    bulkDeleteBtn.disabled = true;
    selectAllBtn.disabled = true;
    deselectBtn.disabled = true;
    stopChecksBtn.style.display = "";
    const row = document.getElementById("bulk-interval-row");
    if (row) row.style.display = "none";
    bulkCheckBtn.textContent = `Checking ${done}/${total}…`;
    checkAllBtn.textContent = `Checking ${done}/${total}…`;
  };

  const unfreeze = () => {
    checkAllBtn.disabled = false;
    checkAllBtn.textContent = "Check all";
    selectAllBtn.disabled = false;
    deselectBtn.disabled = false;
    stopChecksBtn.style.display = "none";
    checkedIds.clear();
    updateBulkBtn();
  };

  freeze();

  function runNext() {
    if (stopped || started >= total) return;
    const id = ids[started++];
    chrome.runtime.sendMessage({ type: "refresh-monitor", monitorId: id })
      .then(async () => {
        done++;
        await silentRefresh();
        refreshUndoCount();
        if (done >= total || stopped) {
          unfreeze();
        } else {
          freeze();
          runNext();
        }
      });
  }

  const stopOnce = () => { stopped = true; };
  stopChecksBtn.addEventListener("click", stopOnce, { once: true });

  for (let i = 0; i < Math.min(CONCURRENCY, total); i++) {
    runNext();
  }
}

async function refreshUndoCount() {
  const resp = await chrome.runtime.sendMessage({ type: "get-undo-count" });
  const count = resp?.count ?? 0;
  undoLastBtn.textContent = `Undo (${count})`;
  undoLastBtn.disabled = count === 0;
}

stopChecksBtn.addEventListener("click", async () => {
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

// Bulk delete selected monitors
bulkImportBtn.addEventListener("click", async () => {
  const n = checkedIds.size;
  if (!n) return;
  if (!await isConnected()) { alert("Connect to Shopify first (see top of page)."); return; }

  const toImport = allMonitors.filter(m => checkedIds.has(m.id));
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

  const importContext = {};
  for (let i = 0; i < toImport.length; i++) {
    if (stopImportRequested) break;
    const monitor = toImport[i];
    try {
      if (monitor.shopifyProductId) {
        throw new Error("Already imported to Shopify; this monitor is kept for ongoing sync.");
      }
      const created = await importMonitorProduct(monitor, importContext);
      await Promise.all([
        chrome.runtime.sendMessage({
          type: "update-monitor",
          payload: {
            id: monitor.id,
            shopifyProductId: created.id,
            shopifyImportedAt: new Date().toISOString(),
            shopifyLastSyncAt: new Date().toISOString(),
            shopifySyncStatus: "ok"
          }
        }),
        addLog({
          type: "import",
          title: [monitor.productData?.brand, monitor.productData?.sku].filter(Boolean).join(" ") || monitor.name,
          productName: monitor.productData?.name || monitor.name || "",
          brand: monitor.productData?.brand || "",
          sku: monitor.productData?.sku || "",
          details: ["Imported to Shopify", "Monitor kept for ongoing price/size sync"],
          monitorId: monitor.id, url: monitor.url
        }).catch(() => {})
      ]);
      succeeded.push(monitor.id);
    } catch (e) {
      // Parse Shopify error body if JSON
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

  // Only delete successfully imported monitors — failed ones stay
  succeeded.forEach(id => checkedIds.delete(id));

  importInProgress = false;
  stopImportRequested = false;
  await loadDashboard();
  await refreshUndoImportBtn();
  updateBulkBtn();

  if (processed < n) {
    alert(`Stopped after ${processed}/${n} imports.\n\nImported: ${succeeded.length}\nFailed: ${failed.length}`);
  } else if (failed.length && succeeded.length) {
    alert(`✓ Imported ${succeeded.length} / ${n}.\n\nImported monitors were kept for ongoing Shopify sync.\n\n❌ Failed (${failed.length}):\n\n${failed.map(f => `• ${f.name}\n  ${f.reason}`).join("\n\n")}`);
  } else if (failed.length) {
    alert(`❌ All ${n} imports failed:\n\n${failed.map(f => `• ${f.name}\n  ${f.reason}`).join("\n\n")}`);
  } else {
    alert(`✓ Imported ${succeeded.length} product${succeeded.length !== 1 ? "s" : ""} to Shopify as active products. Monitors were kept for ongoing sync.`);
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
  bulkDeleteBtn.textContent = `Deleting 0/${n}...`;
  bulkDeleteBtn.disabled = true;
  bulkCheckBtn.disabled = true;
  for (let i = 0; i < ids.length; i++) {
    setProgressLabel(bulkDeleteBtn, "Deleting", i + 1, n);
    await chrome.runtime.sendMessage({ type: "delete-monitor", monitorId: ids[i] });
    if (ids[i] === selectedMonitorId) {
      selectedMonitorId = null;
      monitorDetail.style.display = "none";
    }
    checkedIds.delete(ids[i]);
  }

  await loadDashboard();
  refreshUndoCount();
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
  bulkCheckBtn.disabled = true;

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
        monitorDetail.style.display = "none";
      }
      checkedIds.delete(monitor.id);
    } catch (error) {
      failures.push({
        name: monitor.productData?.sku || monitor.name || monitor.id,
        reason: error?.message || String(error) || "Unknown error"
      });
    }
  }

  await loadDashboard();
  refreshUndoCount();
  if (failures.length) {
    alert(`Deleted ${targets.length - failures.length}/${targets.length} selected item${targets.length - failures.length !== 1 ? "s" : ""}.\n\nThese stayed because Shopify delete failed:\n\n${failures.map((item) => `• ${item.name}\n  ${item.reason}`).join("\n\n")}`);
  }
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
  await loadDashboard();
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
  await loadDashboard();
});

selectAllBtn.addEventListener("click", () => {
  getFilteredMonitors().forEach((m) => checkedIds.add(m.id));
  updateBulkBtn();
  renderGrid(getFilteredMonitors());
});

deselectBtn.addEventListener("click", () => {
  checkedIds.clear();
  updateBulkBtn();
  renderGrid(getFilteredMonitors());
});

checkAllBtn.addEventListener("click", () => {
  runBulkCheck(allMonitors.map((m) => m.id));
});

document.getElementById("select-range").addEventListener("click", () => {
  const from = parseInt(document.getElementById("range-from").value);
  const to = parseInt(document.getElementById("range-to").value);
  if (isNaN(from) || isNaN(to) || from < 1 || to < from) return;
  allMonitors.forEach((m, i) => {
    const num = i + 1;
    if (num >= from && num <= to) checkedIds.add(m.id);
  });
  updateBulkBtn();
  renderGrid(getFilteredMonitors());
});

function applyFilters() {
  groupPages.clear(); // reset to page 1 on top-level filter change
  const filtered = getFilteredMonitors();
  renderStats(filtered);
  renderGrid(filtered);
  // visual active state on filter dropdowns
  filterBrand.classList.toggle("active", !!filterBrand.value);
  filterType.classList.toggle("active", !!filterType.value);
}

searchInput.addEventListener("input", () => { applyFilters(); scheduleSaveDashboardUiState(); });
changedOnlyInput.addEventListener("change", () => { applyFilters(); scheduleSaveDashboardUiState(); });
filterBrand.addEventListener("change", () => { applyFilters(); scheduleSaveDashboardUiState(); });
filterType.addEventListener("change", () => { applyFilters(); scheduleSaveDashboardUiState(); });
filterSort.addEventListener("change", () => { applyFilters(); scheduleSaveDashboardUiState(); });
document.getElementById("refresh-dashboard").addEventListener("click", loadDashboard);

chrome.storage.onChanged.addListener((changes, areaName) => {
  if (areaName !== "local" || !changes.pageMonitors) return;
  clearTimeout(dashboardStorageRefreshTimer);
  dashboardStorageRefreshTimer = setTimeout(() => {
    const nextMonitors = changes.pageMonitors?.newValue || [];
    applyMonitorsUpdate(nextMonitors);
    if (!allMonitors.length) {
      renderStats([]);
      monitorGrid.innerHTML = `<section class="empty-state large" style="grid-column:1/-1"><p>No monitors yet.</p><p class="subtle">Use the extension popup to create one.</p></section>`;
      monitorDetail.style.display = "none";
      updateBulkBtn();
      refreshShopifyDashboardState().catch(() => {});
      return;
    }
    renderAll();
    refreshOpenShopifyPanels().catch(() => {});
  }, 100);
});

await restoreDashboardUiState();
chrome.runtime.sendMessage({ type: "normalize-monitor-brands" }).catch(() => {});
loadDashboard();
refreshUndoCount();

// ── Check button gating (Shopify connection required, or test mode) ─────────
async function updateCheckState() {
  const connected = await isConnected();
  const { shopifyTestMode } = await chrome.storage.local.get("shopifyTestMode");
  canCheck = connected || !!shopifyTestMode;

  checkAllBtn.disabled = !canCheck;
  bulkCheckBtn.disabled = !canCheck || checkedIds.size === 0;

  const label = document.getElementById("test-mode-label");
  const toggle = document.getElementById("test-mode-toggle");
  if (toggle) {
    toggle.checked = !!shopifyTestMode;
    if (label) label.style.color = shopifyTestMode ? "#ff5a36" : "";
  }
}

const testModeToggle = document.getElementById("test-mode-toggle");
testModeToggle.addEventListener("change", async () => {
  if (testModeToggle.checked) {
    await chrome.runtime.sendMessage({ type: "test-mode-start" });
  } else {
    testModeToggle.disabled = true;
    testModeToggle.closest("label").querySelector("span").textContent = "Reverting…";
    await chrome.runtime.sendMessage({ type: "test-mode-end" });
    await loadDashboard();
    testModeToggle.disabled = false;
    testModeToggle.closest("label").querySelector("span").textContent = "Test mode";
  }
  await updateCheckState();
});

updateCheckState();

// ── Auto interval toggle ───────────────────────────────────────────────────
const autoToggle = document.getElementById("auto-interval-toggle");
const autoLabel = document.getElementById("auto-interval-label");

async function initAutoToggle() {
  const { autoIntervalEnabled } = await chrome.storage.local.get("autoIntervalEnabled");
  autoEnabled = autoIntervalEnabled !== false; // default ON
  autoToggle.checked = autoEnabled;
  autoLabel.style.color = autoEnabled ? "#44aa00" : "";
}

autoToggle.addEventListener("change", async () => {
  autoEnabled = autoToggle.checked;
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
  refreshShopifyDashboardState().catch(() => {});
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

// ── Undo Import ────────────────────────────────────────────────────────────
const undoImportBtn = document.getElementById("undo-import");

async function refreshUndoImportBtn() {
  const recent = await getRecentImports();
  if (recent.length > 0) {
    undoImportBtn.style.display = "inline-flex";
    undoImportBtn.textContent = `Undo Import (${recent.length})`;
    undoImportBtn.disabled = false;
  } else {
    undoImportBtn.style.display = "none";
  }
}

undoImportBtn.addEventListener("click", async () => {
  undoImportBtn.disabled = true;
  undoImportBtn.textContent = "Undoing…";
  try {
    const entry = await undoLastImport();
    await loadDashboard();
    await refreshUndoImportBtn();
    alert(`✓ Undone: "${entry.monitorName}" removed from Shopify and restored to dashboard.`);
  } catch (e) {
    alert(`Failed to undo: ${e.message}`);
    undoImportBtn.disabled = false;
    await refreshUndoImportBtn();
  }
});

refreshUndoImportBtn();

// ── Shopify store metadata viewer ──────────────────────────────────────────
const metaSection = document.getElementById("shopify-meta-section");
const metaContent = document.getElementById("shopify-meta-content");
const metaStatus = document.getElementById("fetch-meta-status");
const fetchMetaBtn = document.getElementById("fetch-meta-btn");

function renderMetaGroup(label, items, color) {
  if (!items.length) return `<div><p style="font-size:12px;font-weight:700;color:#5f6c7b;margin:0 0 6px">${label} <span style="font-weight:400">(0)</span></p><p style="font-size:12px;color:#9ca3af">None found</p></div>`;
  const chips = items.map(v => `<span style="display:inline-block;padding:3px 10px;border-radius:999px;font-size:12px;background:${color}15;color:${color};margin:2px 3px 2px 0;border:1px solid ${color}30">${escapeHtml(v)}</span>`).join("");
  return `<div><p style="font-size:12px;font-weight:700;color:#5f6c7b;margin:0 0 6px">${label} <span style="font-weight:400">(${items.length})</span></p><div>${chips}</div></div>`;
}

async function fetchAndShowMeta() {
  metaStatus.textContent = "Loading…";
  fetchMetaBtn.disabled = true;
  try {
    const { vendors, types, tags } = await getShopifyMetadata();
    metaContent.innerHTML = [
      renderMetaGroup("Vendors", vendors, "#5c6ac4"),
      renderMetaGroup("Product Types", types, "#007a5a"),
      renderMetaGroup("Tags", tags, "#c05717")
    ].join("");
    metaStatus.textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (e) {
    metaStatus.textContent = `Error: ${e.message}`;
  }
  fetchMetaBtn.disabled = false;
}

async function initMetaSection() {
  const connected = await isConnected();
  if (connected) {
    metaSection.style.display = "";
    fetchAndShowMeta();
  }
}

fetchMetaBtn.addEventListener("click", fetchAndShowMeta);
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
    ? shopifyProductCount - shopifyUniqueSkuCount
    : 0;
  const base = `${shopifyUnmonitoredProducts.length} Shopify product${shopifyUnmonitoredProducts.length !== 1 ? "s" : ""} available on Shopify but not on monitor.`;
  if (shopifyUnmonitoredProducts.length === 0 && dupeCount > 0) {
    return `${base} All unique Shopify SKUs are monitored. The ${dupeCount}-product gap in the dashboard is from ${dupeCount} duplicate-SKU product${dupeCount !== 1 ? "s" : ""} in Shopify.`;
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

function getMonitorNotShopifyStatusText() {
  const shopifyDupeCount = (shopifyProductCount != null && shopifyUniqueSkuCount != null)
    ? shopifyProductCount - shopifyUniqueSkuCount
    : 0;
  const monitorDupeCount = getMonitorDuplicateSkuCount(allMonitors);
  const base = `${monitorNotShopifyMonitors.length} monitor${monitorNotShopifyMonitors.length !== 1 ? "s" : ""} available on monitor but not on Shopify.`;
  if (monitorNotShopifyMonitors.length === 0) {
    const notes = [];
    if (monitorDupeCount > 0) {
      notes.push(`${monitorDupeCount} duplicate-SKU monitor${monitorDupeCount !== 1 ? "s" : ""}`);
    }
    if (shopifyDupeCount > 0) {
      notes.push(`${shopifyDupeCount} duplicate-SKU Shopify product${shopifyDupeCount !== 1 ? "s" : ""}`);
    }
    if (notes.length) {
      return `${base} All unique monitor SKUs have a matching Shopify product. The count gap is explained by ${notes.join(" and ")}.`;
    }
  }
  return base;
}

function computeShopifyMonitorDiffs(products, monitors) {
  const safeMonitors = Array.isArray(monitors) ? monitors : [];
  const monitorSkuSet = new Set(
    safeMonitors
      .map((monitor) => normalizeSku(monitor?.productData?.sku))
      .filter(Boolean)
  );
  const monitorShopifyIdSet = new Set(
    safeMonitors
      .map((monitor) => Number(monitor?.shopifyProductId))
      .filter(Boolean)
  );

  const shopifySkuSet = new Set();
  const shopifyProductIdSet = new Set();
  const shopifyOnlyProducts = [];
  for (const product of (products || [])) {
    const productId = Number(product?.id);
    if (productId) shopifyProductIdSet.add(productId);
    const skuSet = getProductSkuSet(product);
    const matchingSku = [...skuSet].find((sku) => monitorSkuSet.has(sku));
    const linkedMonitor = productId && monitorShopifyIdSet.has(productId);
    if (matchingSku || linkedMonitor) {
      skuSet.forEach((sku) => shopifySkuSet.add(sku));
      continue;
    }

    const productSku = normalizeSku(product?.sku) || [...skuSet][0] || "";
    if (!productSku) continue;
    if (shopifySkuSet.has(productSku)) continue;
    shopifySkuSet.add(productSku);
    shopifyOnlyProducts.push(product);
  }

  const monitorOnlyMonitors = [];
  const seenMonitorKeys = new Set();
  for (const monitor of safeMonitors) {
    const sku = normalizeSku(monitor?.productData?.sku);
    const linkedProductId = Number(monitor?.shopifyProductId);
    const hasLinkedShopifyProduct = linkedProductId && shopifyProductIdSet.has(linkedProductId);
    const isSkuMatched = sku && shopifySkuSet.has(sku);
    if (hasLinkedShopifyProduct || isSkuMatched) continue;
    const uniqueKey = sku || `monitor:${monitor?.id || "unknown"}`;
    if (seenMonitorKeys.has(uniqueKey)) continue;
    seenMonitorKeys.add(uniqueKey);
    monitorOnlyMonitors.push(monitor);
  }

  return { shopifyOnlyProducts, monitorOnlyMonitors };
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
    shopifyProductCount = products.length;
    shopifyUniqueSkuCount = new Set(products.map(getProductPrimarySku).filter(Boolean)).size;
    const diffs = computeShopifyMonitorDiffs(products, allMonitors);
    shopifyUnmonitoredProducts = diffs.shopifyOnlyProducts;
    selectedShopifyUnmonitoredIds.clear();
    shopifyUnmonitoredStatus.textContent = getShopifyUnmonitoredStatusText();
    renderShopifyUnmonitoredList();
  } catch (error) {
    shopifyUnmonitoredStatus.textContent = `Error: ${error.message || error}`;
    shopifyUnmonitoredProducts = [];
    selectedShopifyUnmonitoredIds.clear();
    renderShopifyUnmonitoredList();
  }
  shopifyUnmonitoredRefreshBtn.disabled = false;
}

function renderMonitorNotShopifyList() {
  const duplicateDeleteBtn = ensureMonitorDuplicateDeleteBtn();
  const hasMonitorOnly = monitorNotShopifyMonitors.length > 0;
  const hasDuplicates = monitorDuplicateSkuGroups.length > 0;
  if (!hasMonitorOnly && !hasDuplicates) {
    monitorNotShopifyList.innerHTML = `<p style="padding:24px;text-align:center;color:#9ca3af;font-size:13px">No monitor-only items found.</p>`;
    monitorNotShopifyClearSelectedBtn.disabled = true;
    if (duplicateDeleteBtn) duplicateDeleteBtn.disabled = true;
    return;
  }
  const monitorOnlyHtml = hasMonitorOnly ? monitorNotShopifyMonitors.map((monitor) => {
    const sku = monitor.productData?.sku || "No SKU";
    const title = monitor.productData?.name || monitor.name || "";
    return `
      <label style="display:flex;align-items:flex-start;gap:10px;padding:12px 16px;border-bottom:1px solid #f3f4f6;cursor:pointer">
        <input type="checkbox" class="monitor-not-shopify-check" data-id="${escapeHtml(String(monitor.id))}" ${selectedMonitorNotShopifyIds.has(String(monitor.id)) ? "checked" : ""}>
        <div style="min-width:0;flex:1">
          <p style="margin:0;font-size:12px;font-weight:700;color:#111">${escapeHtml(sku)}</p>
          ${title ? `<p style="margin:2px 0 0;font-size:12px;color:#5f6c7b">${escapeHtml(title)}</p>` : ""}
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
                  : `<div style="width:48px;height:48px;border-radius:8px;flex-shrink:0;background:#f3f4f6;border:1px solid #e5e7eb;display:flex;align-items:center;justify-content:center;font-size:18px;color:#d1d5db">□</div>`}
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

  monitorNotShopifyList.innerHTML = `${hasMonitorOnly ? `<div>${monitorOnlyHtml}</div>` : ""}${duplicatesHtml}`;
  monitorNotShopifyClearSelectedBtn.disabled = selectedMonitorNotShopifyIds.size === 0;
  if (duplicateDeleteBtn) duplicateDeleteBtn.disabled = selectedDuplicateMonitorIds.size === 0;
}

async function loadMonitorNotShopifyMonitors(forceRefresh = false) {
  if (!await isConnected()) {
    monitorNotShopifyStatus.textContent = "Connect Shopify first.";
    monitorNotShopifyMonitors = [];
    monitorDuplicateSkuGroups = [];
    selectedMonitorNotShopifyIds.clear();
    selectedDuplicateMonitorIds.clear();
    renderMonitorNotShopifyList();
    return;
  }
  monitorNotShopifyStatus.textContent = "Loading monitor comparison…";
  monitorNotShopifyRefreshBtn.disabled = true;
  try {
    if (forceRefresh) clearShopifyProductsSnapshotCache();
    const products = await getShopifyProductsSnapshot();
    shopifyProductCount = products.length;
    shopifyUniqueSkuCount = new Set(products.map(getProductPrimarySku).filter(Boolean)).size;
    const diffs = computeShopifyMonitorDiffs(products, allMonitors);
    monitorNotShopifyMonitors = diffs.monitorOnlyMonitors;
    monitorDuplicateSkuGroups = computeMonitorDuplicateSkuGroups(allMonitors);
    selectedMonitorNotShopifyIds.clear();
    selectedDuplicateMonitorIds.clear();
    monitorNotShopifyStatus.textContent = getMonitorNotShopifyStatusText();
    renderMonitorNotShopifyList();
  } catch (error) {
    monitorNotShopifyStatus.textContent = `Error: ${error.message || error}`;
    monitorNotShopifyMonitors = [];
    monitorDuplicateSkuGroups = [];
    selectedMonitorNotShopifyIds.clear();
    selectedDuplicateMonitorIds.clear();
    renderMonitorNotShopifyList();
  }
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

  const button = ensureMonitorDuplicateDeleteBtn();
  const originalText = button?.textContent || "Delete selected duplicate monitors";
  const failures = [];

  try {
    await runButtonProgress(button, ids, "Deleting", async (id) => {
      const monitor = allMonitors.find((item) => String(item.id) === String(id));
      if (!monitor) {
        selectedDuplicateMonitorIds.delete(String(id));
        renderMonitorNotShopifyList();
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
    });

    await loadDashboard();
    await loadMonitorNotShopifyMonitors(true);
    refreshUndoCount();

    if (failures.length) {
      const details = failures.map((failure) => `${failure.name}: ${failure.reason}`).join("\n");
      monitorNotShopifyStatus.textContent = `Deleted ${ids.length - failures.length}/${ids.length} selected duplicate monitor${ids.length !== 1 ? "s" : ""}.`;
      alert(`Some duplicate deletions failed:\n\n${details}`);
    } else {
      monitorNotShopifyStatus.textContent = `Deleted ${ids.length} selected duplicate monitor${ids.length !== 1 ? "s" : ""} from monitors.`;
    }
  } finally {
    const safeButton = ensureMonitorDuplicateDeleteBtn();
    if (safeButton) safeButton.textContent = originalText;
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

    // A product is only safe to delete when it is not protected by ANY reference for this SKU.
    // This is stricter than just checking monitorReferencedIds — it also covers the mapping.
    const safeExtras = group.filter(
      (p) => p.id !== keepProduct.id && !protected_.has(Number(p.id))
    );

    // Skip groups where nothing can be safely deleted
    if (safeExtras.length === 0) continue;

    groups.push({ sku, keepProduct, keepReason, extraProducts: safeExtras });
  }
  groups.sort((a, b) => a.sku.localeCompare(b.sku));
  return groups;
}

function renderDuplicateSkuPanel() {
  const totalExtras = skuDuplicateGroups.reduce((n, g) => n + g.extraProducts.length, 0);
  skuDuplicatesDeleteBtn.disabled = selectedDuplicateIds.size === 0;
  skuDuplicatesSelectAllBtn.disabled = totalExtras === 0;
  skuDuplicatesDeselectBtn.disabled = selectedDuplicateIds.size === 0;

  if (!skuDuplicateGroups.length) {
    skuDuplicatesList.innerHTML = `<p style="padding:24px;text-align:center;color:#9ca3af;font-size:13px">No duplicate SKUs found.</p>`;
    return;
  }

  const keepReasonLabel = { monitor: "used by monitor", mapping: "in SKU mapping", oldest: "oldest product" };

  function productInfoHtml(p) {
    const thumb = p.image
      ? `<img src="${escapeHtml(p.image)}" alt="" style="width:48px;height:48px;object-fit:cover;border-radius:4px;flex-shrink:0;border:1px solid #e5e7eb">`
      : `<div style="width:48px;height:48px;border-radius:4px;flex-shrink:0;background:#f3f4f6;border:1px solid #e5e7eb;display:flex;align-items:center;justify-content:center;font-size:18px;color:#d1d5db">□</div>`;
    const adminUrl = skuDuplicatesAdminBase ? `${skuDuplicatesAdminBase}/${p.id}` : "";
    return `
      <div style="display:flex;align-items:center;gap:10px;min-width:0;flex:1">
        ${thumb}
        <div style="min-width:0;flex:1">
          <p style="margin:0;font-size:12px;font-weight:700;color:#111;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(p.title || "(no title)")}</p>
          <p style="margin:3px 0 0;font-size:11px;color:#5f6c7b">Supplier SKU: <strong>${escapeHtml(p.sku || "—")}</strong></p>
          <p style="margin:2px 0 0;font-size:11px;color:#9ca3af">
            Shopify ID: ${p.id}${adminUrl ? `&ensp;<a href="${adminUrl}" target="_blank" style="color:#5c6ac4;text-decoration:none">View ↗</a>` : ""}
          </p>
        </div>
      </div>`;
  }

  skuDuplicatesList.innerHTML = skuDuplicateGroups.map(({ sku, keepProduct, keepReason, extraProducts }) => {
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
    return `
      <div style="border-bottom:2px solid var(--border)">
        <div style="padding:8px 16px;background:#f8fafc;border-bottom:1px solid var(--border)">
          <span style="font-size:11px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:0.05em">SKU: ${escapeHtml(sku)}</span>
          <span style="margin-left:8px;font-size:11px;color:#9ca3af">${extraProducts.length + 1} product${extraProducts.length + 1 !== 1 ? "s" : ""} — ${extraProducts.length} safe to delete</span>
        </div>
        ${keepHtml}${extrasHtml}
      </div>`;
  }).join("");

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
    selectedDuplicateIds.clear();
    renderDuplicateSkuPanel();
    return;
  }
  skuDuplicatesStatus.textContent = "Loading Shopify products…";
  skuDuplicatesRefreshBtn.disabled = true;
  try {
    if (forceRefresh) clearShopifyProductsSnapshotCache();
    const products = await getShopifyProductsSnapshot();
    shopifyProductCount = products.length;
    shopifyUniqueSkuCount = new Set(products.map(getProductPrimarySku).filter(Boolean)).size;
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
    if (skuDuplicateGroups.length === 0) {
      skuDuplicatesStatus.textContent = "No duplicate SKUs found. Your Shopify store is clean.";
    } else {
      skuDuplicatesStatus.textContent = `Found ${skuDuplicateGroups.length} SKU${skuDuplicateGroups.length !== 1 ? "s" : ""} with duplicates — ${totalExtras} extra product${totalExtras !== 1 ? "s" : ""} can be deleted. Review below, then click Delete selected.`;
    }
    renderDuplicateSkuPanel();
  } catch (error) {
    skuDuplicatesStatus.textContent = `Error: ${error.message || error}`;
    skuDuplicateGroups = [];
    selectedDuplicateIds.clear();
    renderDuplicateSkuPanel();
  }
  skuDuplicatesRefreshBtn.disabled = false;
}

async function deleteSelectedDuplicates() {
  const ids = [...selectedDuplicateIds].map(Number).filter(Boolean);
  if (!ids.length) return;
  if (!window.confirm(`Delete ${ids.length} duplicate product${ids.length !== 1 ? "s" : ""} from Shopify? This cannot be undone.`)) return;
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
  // Persist the repaired mapping so future updates don't hit a stale/deleted product
  await chrome.storage.local.set({ shopifySkuMapping: mapping });

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
    skuDuplicatesPanel.style.display = "none";
    await loadMonitorNotShopifyMonitors(true);
    monitorNotShopifyPanel.scrollIntoView({ behavior: "smooth", block: "start" });
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

ensureMonitorDuplicateDeleteBtn();

shopifyUnmonitoredRefreshBtn.addEventListener("click", () => loadShopifyUnmonitoredProducts(true));
shopifyUnmonitoredClearSelectedBtn.addEventListener("click", clearSelectedShopifyUnmonitoredList);
shopifyUnmonitoredClearBtn.addEventListener("click", clearShopifyUnmonitoredList);
monitorNotShopifyRefreshBtn.addEventListener("click", () => loadMonitorNotShopifyMonitors(true));
monitorNotShopifyClearSelectedBtn.addEventListener("click", clearSelectedMonitorNotShopifyList);
monitorNotShopifyClearBtn.addEventListener("click", clearMonitorNotShopifyList);
monitorNotShopifyDeleteDuplicatesBtn?.addEventListener("click", deleteSelectedDuplicateMonitors);

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
    if (target.checked) selectedMonitorNotShopifyIds.add(id);
    else selectedMonitorNotShopifyIds.delete(id);
    renderMonitorNotShopifyList();
    return;
  }
  if (!target.classList.contains("monitor-duplicate-check")) return;
  const id = String(target.dataset.id || "");
  if (!id) return;
  if (target.checked) selectedDuplicateMonitorIds.add(id);
  else selectedDuplicateMonitorIds.delete(id);
  renderMonitorNotShopifyList();
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
refreshLogCount();

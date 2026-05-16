import {
  DEFAULT_INTERVAL_MINUTES,
  buildHtmlDiff,
  getMonitors,
  getMonitorById,
  saveMonitors as _saveMonitors,
  saveMonitorById as _saveMonitorById,
  deleteMonitorsByIds,
  migrateToPerMonitorStorage,
  saveStorageBackup,
  restoreFromStorageBackup,
  uid,
  addLog,
  canonicalizeBrand,
  normalizeDsgSoccerCleatsMonitor,
  getMissingProductDataFields,
  withIncompleteDataError
} from "./shared.js";
import { backupToDrive, restoreFromDrive, getDriveBackupInfo } from "./drive.js";
import { writeLocalBackup, getLocalBackupPermission, getLocalBackupInfo } from "./local-backup.js";
import {
  configureCloud,
  getCloudStatus,
  signInCloud,
  signUpCloud,
  signOutCloud,
  pushCloudMonitors,
  pullCloudMonitors
} from "./cloud.js";
import { updateShopifyForMonitor, syncMonitorBrandsToShopify, normalizeAllShopifyVendors, syncMonitorShopifyStatus, getEuSize } from "./shopify.js";

function _onBackupResult(_result) {}

// Debounced storage backup — fires once 20 s after the last save burst, not per save.
// Keeps chrome.storage.local writes rare so they never compete with active checks.
let _storageBackupTimer = null;
function _scheduleStorageBackup() {
  clearTimeout(_storageBackupTimer);
  _storageBackupTimer = setTimeout(() => {
    getMonitors().then((all) => saveStorageBackup(all).catch(() => {})).catch(() => {});
  }, 20000);
}

async function saveMonitors(monitors) {
  const errorCheckedMonitors = (Array.isArray(monitors) ? monitors : []).map(withIncompleteDataError);
  await _saveMonitors(errorCheckedMonitors);
  _bumpMonitorsVersion();
  _scheduleStorageBackup();
  writeLocalBackup(errorCheckedMonitors).then(_onBackupResult).catch(() => {});
  scheduleCloudAutoSync("monitors saved");
}

async function saveMonitorFast(monitor) {
  const checked = withIncompleteDataError(monitor);
  await _saveMonitorById(checked);
  _bumpMonitorsVersion();
  _scheduleLocalBackup();
  scheduleCloudAutoSync("monitor saved");
}

// Debounced so rapid concurrent check completions coalesce into one read.
let _localBackupTimer = null;
function _scheduleLocalBackup() {
  clearTimeout(_localBackupTimer);
  _localBackupTimer = setTimeout(() => {
    getMonitors().then((all) => {
      _scheduleStorageBackup();
      writeLocalBackup(all).then(_onBackupResult).catch(() => {});
    }).catch(() => {});
  }, 2000);
}

const CLOUD_AUTO_SYNC_DEBOUNCE_MS = 30000;
const DEFAULT_CLOUD_EMAIL = "orjnstore@gmail.com";
const DEFAULT_CLOUD_PASSWORD = "Oussamariad2004!";
const DEFAULT_SUPABASE_URL = "https://ywadupymrjbnzmijuein.supabase.co";
const DEFAULT_SUPABASE_PUBLISHABLE_KEY = "sb_publishable_z2-r4PNs7R5tO8VVbINofw_VcQxM2tn";
let _cloudAutoSyncTimer = null;
let _cloudAutoSyncRunning = false;
let _cloudAutoSyncPending = false;
let _cloudAutoSyncReason = "";

function scheduleCloudAutoSync(_reason = "monitor change", _delayMs = CLOUD_AUTO_SYNC_DEBOUNCE_MS) {
  // Cloud sync disabled — Supabase removed.
}

const ORJN_EMAIL = "orjnstore@gmail.com";
const ORJN_PASSWORD = "Oussamariad2004!";

async function ensureOrjnSignedIn() {
  const status = await getCloudStatus().catch(() => null);
  if (status?.signedIn) return true;
  try {
    await signInCloud(ORJN_EMAIL, ORJN_PASSWORD);
    return true;
  } catch (_) {
    return false;
  }
}

async function runCloudAutoSync(reason = "monitor change") {
  if (_cloudAutoSyncRunning) {
    _cloudAutoSyncPending = true;
    return;
  }
  _cloudAutoSyncRunning = true;
  _cloudAutoSyncPending = false;
  try {
    const signedIn = await ensureOrjnSignedIn();
    if (!signedIn) return;
    notifyDashboard("cloud-sync-status", {
      ok: true,
      message: "Auto-syncing monitors to Supabase..."
    }).catch(() => {});
    const monitors = await getMonitorsMarkingIncompleteAsErrors();
    // Never push an empty list — if IDB is empty it means storage was wiped,
    // not that the user deleted everything. Pushing empty would destroy the
    // cloud backup which is the only recovery path.
    if (!monitors.length) return;
    const result = await pushCloudMonitors(monitors);
    if (result?.skipped) return;
    await chrome.storage.local.set({ _lastSyncedMonitorCount: monitors.length, _lastSyncedAt: Date.now() });
    notifyDashboard("cloud-sync-status", {
      ok: true,
      message: `Auto-synced ${result.count || monitors.length} monitors to Supabase`
    }).catch(() => {});
  } catch (error) {
    const message = error?.message || String(error);
    addLog({
      type: "cloud-sync",
      title: "Auto-sync to Supabase failed",
      details: [reason, message]
    }).catch(() => {});
    notifyDashboard("cloud-sync-status", {
      ok: false,
      message: `Auto-sync failed: ${message}`
    }).catch(() => {});
  } finally {
    _cloudAutoSyncRunning = false;
    if (_cloudAutoSyncPending) {
      scheduleCloudAutoSync("pending changes");
    }
  }
}

async function ensureDefaultCloudSignIn() {
  const status = await getCloudStatus().catch(() => null);
  if (status?.signedIn) return true;
  await configureCloud({
    url: DEFAULT_SUPABASE_URL,
    anonKey: DEFAULT_SUPABASE_PUBLISHABLE_KEY
  });
  await signInCloud(DEFAULT_CLOUD_EMAIL, DEFAULT_CLOUD_PASSWORD);
  scheduleCloudAutoSync("default sign-in", 5000);
  return true;
}

// â"€â"€ Monitor version counter ────────────────────────────────────────────────
// Incremented whenever monitors are saved/changed. Lets options.js skip
// the full IDB read + chrome message transfer when nothing has changed.
let _monitorsVersion = 0;
function _bumpMonitorsVersion() { _monitorsVersion++; }

// â"€â"€ Service-worker keep-alive â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
// Chrome MV3 can terminate the SW while checks are running. A periodic no-op
// API call prevents that while there are active checks.
let _keepAliveTimer = null;
let _activeCheckCount = 0;

function _startKeepAlive() {
  _activeCheckCount++;
  if (_keepAliveTimer) return;
  _keepAliveTimer = setInterval(() => chrome.runtime.getPlatformInfo(() => {}), 20000);
}

function _stopKeepAlive() {
  _activeCheckCount = Math.max(0, _activeCheckCount - 1);
  if (_activeCheckCount === 0 && _keepAliveTimer) {
    clearInterval(_keepAliveTimer);
    _keepAliveTimer = null;
  }
}

const ALARM_PREFIX = "monitor:";
const MAX_HISTORY_ENTRIES = 12;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Strip large HTML fields before sending to options page to prevent OOM.
// HTML snapshots are fetched on-demand via "get-monitor-snapshots".
function slimMonitor(m) {
  const { lastHtmlSnapshot, previousHtmlSnapshot, lastHtmlDiff, lastSelectedOuterHtmlSnapshot, previousSelectedOuterHtmlSnapshot, changeHistory, ...rest } = m;
  return {
    ...rest,
    hasLastHtmlSnapshot: !!(lastHtmlSnapshot),
    hasPreviousHtmlSnapshot: !!(previousHtmlSnapshot),
    hasLastSelectedOuterHtmlSnapshot: !!(lastSelectedOuterHtmlSnapshot),
    hasPreviousSelectedOuterHtmlSnapshot: !!(previousSelectedOuterHtmlSnapshot),
    changeHistory: (changeHistory || []).map(({ previousHtml, currentHtml, htmlDiff, previousText, currentText, ...entry }) => ({
      ...entry,
      hasPreviousHtml: !!(previousHtml),
      hasCurrentHtml: !!(currentHtml),
    })),
  };
}

async function notifyDashboard(type, payload = {}) {
  const url = chrome.runtime.getURL("options.html");
  const tabs = await chrome.tabs.query({ url: `${url}*` }).catch(() => []);
  for (const tab of tabs) {
    chrome.tabs.sendMessage(tab.id, { type, ...payload }).catch(() => {});
  }
}

function getUnconvertedSizes(sizes, monitor = {}) {
  const pd = monitor.productData || {};
  const brand = canonicalizeBrand(pd.brand || "");
  const gender = pd.extractedGender || pd.genderDisplay || pd.gender || "";
  const ignoredSizes = monitor.ignoredSizes || [];
  const seen = new Set();
  return (sizes || [])
    .map((size) => String(size || "").replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .filter((size) => !ignoredSizes.some((ignored) => isIgnoredSizeMatch(size, ignored)))
    .filter((size) => {
      const key = size.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      const numeric = Number((size.match(/\d+(?:[.,]\d+)?/) || [""])[0].replace(",", "."));
      if (!/way\s*of\s*wade|li[\s-]*ning|lining/i.test(brand) && Number.isFinite(numeric) && numeric > 30) return false;
      return !/\bEU\b/i.test(size) && !getEuSize(size, brand, gender);
    });
}

function getFractionalSizeBrand(monitor = {}) {
  const pd = monitor.productData || {};
  const overrides = monitor.productDataOverrides || {};
  const brand = canonicalizeBrand(pd.brand || overrides.brand || monitor.brand || "");
  const haystack = [
    brand,
    pd.brand,
    overrides.brand,
    pd.name,
    monitor.name,
    monitor.url
  ].join(" ");
  return /adidas|yeezy|hoka|way\s*of\s*wade|li[\s-]*ning|lining/i.test(haystack) ? brand || haystack : "";
}

function getStaleFractionalEuSizes(sizes, monitor = {}) {
  const brand = getFractionalSizeBrand(monitor);
  if (!brand) return [];
  const ignoredSizes = monitor.ignoredSizes || [];
  const seen = new Set();
  return (sizes || [])
    .map((size) => String(size || "").replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .filter((size) => !ignoredSizes.some((ignored) => isIgnoredSizeMatch(size, ignored)))
    .filter((size) => {
      const normalized = size.replace(",", ".");
      const key = normalized.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      if (/\//.test(size)) return false;
      const match = normalized.match(/\d+(?:\.\d+)?/);
      if (!match || !/\d+\.\d+/.test(match[0])) return false;
      const numeric = Number(match[0]);
      if (!Number.isFinite(numeric) || numeric < 30 || numeric > 60) return false;
      const decimal = match[0].split(".")[1] || "";
      return /^(3|33|5|6|66|67)$/.test(decimal);
    });
}

function getStaleFractionalEuError(sizes) {
  return `Fraction EU sizes showing as decimals: ${sizes.join(", ")} - recheck to refresh 1/3 and 2/3 labels`;
}

function getMonitorSavedSizes(monitor = {}) {
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

function normalizeIgnoredSize(value) {
  return String(value || "").replace(/,/g, ".").replace(/\s+/g, " ").trim().toLowerCase();
}

function getIgnoredSizeNumber(value) {
  const text = normalizeIgnoredSize(value);
  const match = text.match(/\d+(?:\.\d+)?/);
  if (!match) return "";
  const n = Number(match[0]);
  return Number.isFinite(n) ? String(n) : "";
}

function isIgnoredSizeMatch(size, ignored) {
  const normalizedSize = normalizeIgnoredSize(size);
  const normalizedIgnored = normalizeIgnoredSize(ignored);
  if (!normalizedSize || !normalizedIgnored) return false;
  if (normalizedSize === normalizedIgnored) return true;
  const sizeNumber = getIgnoredSizeNumber(normalizedSize);
  const ignoredNumber = getIgnoredSizeNumber(normalizedIgnored);
  if (!sizeNumber || !ignoredNumber || sizeNumber !== ignoredNumber) return false;
  return /\b(?:us|size|m\s*\d|w\s*\d)|^\d+(?:\.\d+)?$/i.test(normalizedSize) ||
    /\b(?:us|size|m\s*\d|w\s*\d)|^\d+(?:\.\d+)?$/i.test(normalizedIgnored);
}

function filterIgnoredSizesFromList(values = [], ignoredSizes = []) {
  const ignored = (ignoredSizes || []).map(normalizeIgnoredSize).filter(Boolean);
  if (!ignored.length) return Array.isArray(values) ? values : [];
  return (Array.isArray(values) ? values : []).filter((size) => !ignored.some((ignoredSize) => isIgnoredSizeMatch(size, ignoredSize)));
}

function applyIgnoredSizesToMonitorData(data, ignoredSizes = []) {
  if (!data || typeof data !== "object") return data;
  const next = { ...data };
  if (Array.isArray(next.inStock)) next.inStock = filterIgnoredSizesFromList(next.inStock, ignoredSizes);
  if (Array.isArray(next.outOfStock)) next.outOfStock = filterIgnoredSizesFromList(next.outOfStock, ignoredSizes);
  if (Array.isArray(next.sizes)) next.sizes = filterIgnoredSizesFromList(next.sizes, ignoredSizes);
  return next;
}

function removeIgnoredSizesFromMonitor(monitor = {}, ignoredSizes = []) {
  const ignored = (ignoredSizes || []).filter(Boolean);
  if (!ignored.length) return monitor;
  const next = { ...monitor };
  if (next.lastExtractedData) next.lastExtractedData = applyIgnoredSizesToMonitorData(next.lastExtractedData, ignored);
  if (next.previousExtractedData) next.previousExtractedData = applyIgnoredSizesToMonitorData(next.previousExtractedData, ignored);
  if (next.productData) next.productData = applyIgnoredSizesToMonitorData(next.productData, ignored);
  if (next.productData?.outOfStock) {
    next.productData = {
      ...next.productData,
      outOfStock: filterIgnoredSizesFromList(next.productData.outOfStock || [], ignored)
    };
  }
  return next;
}

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
const MAX_CONCURRENT_RETAIL_CAPTURES = 8;  // Foot Locker
const MAX_CONCURRENT_DSG_CAPTURES = 6;     // Dick's: up to 6 tabs, staggered 5 s apart
const MAX_CONCURRENT_WOW_CAPTURES = 8;     // Way of Wade: up to 8 tabs, staggered 2 s apart
let activeCaptureCount = 0;
let activeBatchCount = 0;
let activeRetailCaptureCount = 0;
let activeDsgCaptureCount = 0;
let activeWowCaptureCount = 0;
const captureQueue = [];
const batchQueue = [];
const retailCaptureQueue = [];
const dsgCaptureQueue = [];
const wowCaptureQueue = [];

// DSG tab-open queue: 5 s metronome between tab creations (cookie wipe runs
// during the wait so it doesn't inflate the gap), up to 6 concurrent tabs.
// After every 8 tabs the pump pauses, waits for all open tabs to close, does
// a full cookie clear, shows a status bar, then continues.
const DSG_TAB_GAP_MS = 5000;
const DSG_PAUSE_AFTER = 8;
const DSG_PAUSE_WAIT_FOR_TABS_MS = 130000;
const _dsgTabQueue = [];
let _dsgTabPumping = false;
let _lastDsgTabOpenTime = 0;
let _dsgTabOpenedSinceClear = 0;
const _openDsgTabIds = new Set();
let _dsgAllTabsClosedResolve = null;
const WOW_TAB_GAP_MS = 2000;
const _wowTabQueue = [];
let _wowTabPumping = false;
let _lastWowTabOpenTime = 0;
const NON_DSG_COOKIE_CLEAR_EVERY = 8;
let _nonDsgCaptureCountSinceCookieClear = 0;
let _nonDsgCookieClearQueue = Promise.resolve();

function getCookieDomainForUrl(url = "") {
  try {
    return new URL(url || "").hostname.replace(/^www\./i, "");
  } catch (_) {
    return "";
  }
}

function queueNonDsgCookieClear(domain) {
  if (!domain || /(^|\.)dickssportinggoods\.com\b/i.test(domain)) return;
  _nonDsgCaptureCountSinceCookieClear++;
  if (_nonDsgCaptureCountSinceCookieClear < NON_DSG_COOKIE_CLEAR_EVERY) return;
  _nonDsgCaptureCountSinceCookieClear = 0;
  _nonDsgCookieClearQueue = _nonDsgCookieClearQueue
    .catch(() => {})
    .then(async () => {
      const result = await clearCookiesForDomain(domain).catch(() => ({ cleared: 0, attempted: 0, failed: 0 }));
      await addLog({
        type: "cookie-clear",
        title: "Cookies cleared after 8 checks",
        details: [`${domain}: cleared ${result.cleared || 0}/${result.attempted || 0}`]
      }).catch(() => {});
    });
}

function _untrackDsgTab(tabId) {
  if (!_openDsgTabIds.has(tabId)) return;
  _openDsgTabIds.delete(tabId);
  if (_openDsgTabIds.size === 0 && _dsgAllTabsClosedResolve) {
    const r = _dsgAllTabsClosedResolve;
    _dsgAllTabsClosedResolve = null;
    r();
  }
}

function openDsgTab(url, active = false) {
  return new Promise((resolve, reject) => {
    _dsgTabQueue.push({ url, active, resolve, reject });
    _pumpDsgTabQueue();
  });
}

async function closeOpenDsgTabs(reason = "cleanup") {
  const tabIds = [..._openDsgTabIds];
  if (!tabIds.length) return 0;
  let closed = 0;
  for (const tabId of tabIds) {
    _openDsgTabIds.delete(tabId);
    captureTabIds.delete(tabId);
    await chrome.tabs.remove(tabId).then(() => { closed++; }).catch(() => {});
  }
  if (_dsgAllTabsClosedResolve) {
    const r = _dsgAllTabsClosedResolve;
    _dsgAllTabsClosedResolve = null;
    r();
  }
  await addLog({
    type: "dsg-tab-cleanup",
    title: "Closed stuck Dick's tabs",
    details: [`${closed}/${tabIds.length} tab(s) closed during ${reason}`]
  }).catch(() => {});
  return closed;
}

async function _dsgPauseAndClear() {
  _startKeepAlive();
  try {
  notifyDashboard("dsg-pause-status", { status: "paused" }).catch(() => {});
  if (_openDsgTabIds.size > 0) {
    const closedNaturally = await Promise.race([
      new Promise(r => { _dsgAllTabsClosedResolve = () => r(true); }),
      sleep(DSG_PAUSE_WAIT_FOR_TABS_MS).then(() => false)
    ]);
    if (!closedNaturally && _openDsgTabIds.size > 0) {
      notifyDashboard("dsg-pause-status", { status: "closing-stuck-tabs" }).catch(() => {});
      await closeOpenDsgTabs("DSG cookie clear pause");
    } else if (!closedNaturally) {
      _dsgAllTabsClosedResolve = null;
    }
  }
  if (stopChecksFlag) return; // checks were stopped while we waited â€" bail out
  notifyDashboard("dsg-pause-status", { status: "clearing" }).catch(() => {});
  await clearCookiesForDomain("dickssportinggoods.com").catch(() => {});
  const rem = await getCookiesForDomain("dickssportinggoods.com").catch(() => []);
  if (rem.length) await clearCookiesForDomain("dickssportinggoods.com").catch(() => {});
  notifyDashboard("dsg-pause-status", { status: "done" }).catch(() => {});
  await sleep(1000);
  notifyDashboard("dsg-pause-status", { status: "continuing" }).catch(() => {});
  await sleep(800);
  notifyDashboard("dsg-pause-status", { status: null }).catch(() => {});
  _lastDsgTabOpenTime = 0; // reset so next tab opens without extra delay
  } finally {
    _stopKeepAlive();
  }
}

async function _pumpDsgTabQueue() {
  if (_dsgTabPumping) return;
  _dsgTabPumping = true;
  while (_dsgTabQueue.length > 0) {
    const entry = _dsgTabQueue.shift();
    try {
      // Run the 5 s wait and cookie wipe in parallel so the wipe doesn't
      // push the interval beyond 5 s.
      const waitMs = Math.max(0, DSG_TAB_GAP_MS - (Date.now() - _lastDsgTabOpenTime));
      const cookieWipe = clearCookiesForDomain("dickssportinggoods.com")
        .catch(() => {})
        .then(async () => {
          const rem = await getCookiesForDomain("dickssportinggoods.com").catch(() => []);
          if (rem.length) await clearCookiesForDomain("dickssportinggoods.com").catch(() => {});
        });
      await Promise.all([waitMs > 0 ? sleep(waitMs) : Promise.resolve(), cookieWipe]);
      if (stopChecksFlag) throw new Error("stopped");
      _lastDsgTabOpenTime = Date.now();
      const tab = await chrome.tabs.create({ url: entry.url, active: !!entry.active });
      _openDsgTabIds.add(tab.id);
      entry.resolve(tab);

      _dsgTabOpenedSinceClear++;
      if (_dsgTabOpenedSinceClear >= DSG_PAUSE_AFTER) {
        _dsgTabOpenedSinceClear = 0;
        await _dsgPauseAndClear();
      }
    } catch (err) {
      entry.reject(err);
    }
  }
  _dsgTabPumping = false;
}

function openWowTab(url, active = false) {
  return new Promise((resolve, reject) => {
    _wowTabQueue.push({ url, active, resolve, reject });
    _pumpWowTabQueue();
  });
}

async function _pumpWowTabQueue() {
  if (_wowTabPumping) return;
  _wowTabPumping = true;
  while (_wowTabQueue.length > 0) {
    const entry = _wowTabQueue.shift();
    try {
      const waitMs = Math.max(0, WOW_TAB_GAP_MS - (Date.now() - _lastWowTabOpenTime));
      if (waitMs > 0) await sleep(waitMs);
      if (stopChecksFlag) throw new Error("stopped");
      _lastWowTabOpenTime = Date.now();
      const tab = await chrome.tabs.create({ url: entry.url, active: !!entry.active });
      entry.resolve(tab);
    } catch (err) {
      entry.reject(err);
    }
  }
  _wowTabPumping = false;
}

function getCookieRemovalUrl(cookie, fallbackDomain) {
  const domain = String(cookie.domain || fallbackDomain).replace(/^\./, "") || fallbackDomain;
  const path = String(cookie.path || "/").startsWith("/") ? cookie.path : `/${cookie.path || ""}`;
  return `${cookie.secure ? "https" : "http"}://${domain}${path}`;
}

async function getCookiesForDomain(domain) {
  if (!chrome.cookies?.getAll) return [];
  const stores = chrome.cookies.getAllCookieStores
    ? await chrome.cookies.getAllCookieStores().catch(() => [])
    : [];
  const storeIds = stores.length ? stores.map((s) => s.id).filter(Boolean) : [undefined];
  const seen = new Set();
  const all = [];
  for (const storeId of storeIds) {
    const query = storeId ? { domain, storeId } : { domain };
    const cookies = await chrome.cookies.getAll(query).catch(() => []);
    for (const cookie of cookies) {
      const key = [cookie.storeId || "", cookie.domain || "", cookie.path || "", cookie.name || ""].join("\t");
      if (seen.has(key)) continue;
      seen.add(key);
      all.push(cookie);
    }
  }
  return all;
}

async function removeCookieSafe(cookie, domain) {
  const url = getCookieRemovalUrl(cookie, domain);
  const base = { url, name: cookie.name, storeId: cookie.storeId };
  if (cookie.partitionKey) {
    const removed = await chrome.cookies.remove({ ...base, partitionKey: cookie.partitionKey }).catch(() => null);
    if (removed) return removed;
  }
  return chrome.cookies.remove(base).catch(() => null);
}

async function clearCookiesForDomain(domain) {
  if (!chrome.cookies?.getAll || !chrome.cookies?.remove) return { cleared: 0, failed: 0 };
  const cookies = await getCookiesForDomain(domain);
  const results = await Promise.all(cookies.map((cookie) => removeCookieSafe(cookie, domain)));
  const cleared = results.filter(Boolean).length;
  return { cleared, failed: results.length - cleared, attempted: cookies.length };
}

async function clearSiteCookies() {
  const withTimeout = (promise, domain) => Promise.race([
    promise,
    sleep(12000).then(() => ({ cleared: 0, failed: 0, attempted: 0, timedOut: true, domain }))
  ]);
  const [dsg, fl] = await Promise.all([
    withTimeout(clearCookiesForDomain("dickssportinggoods.com"), "dickssportinggoods.com"),
    withTimeout(clearCookiesForDomain("footlocker.com"), "footlocker.com")
  ]);
  return { dsg, fl };
}

function isDsgUrl(url = "") {
  try {
    return /(^|\.)dickssportinggoods\.com\b/i.test(new URL(url || "").hostname || "");
  } catch (_) {
    return /(^|\.)dickssportinggoods\.com\b/i.test(String(url || ""));
  }
}


function drainWaitingCaptureQueues() {
  while (activeDsgCaptureCount < MAX_CONCURRENT_DSG_CAPTURES && dsgCaptureQueue.length) {
    activeDsgCaptureCount++;
    dsgCaptureQueue.shift()(activeDsgCaptureCount);
  }
  while (activeWowCaptureCount < MAX_CONCURRENT_WOW_CAPTURES && wowCaptureQueue.length) {
    activeWowCaptureCount++;
    wowCaptureQueue.shift()();
  }
  while (activeRetailCaptureCount < MAX_CONCURRENT_RETAIL_CAPTURES && retailCaptureQueue.length) {
    activeRetailCaptureCount++;
    retailCaptureQueue.shift()();
  }
  while (activeBatchCount < MAX_CONCURRENT_BATCH && batchQueue.length) {
    activeBatchCount++;
    batchQueue.shift()();
  }
  while (activeCaptureCount < MAX_CONCURRENT_CAPTURES && captureQueue.length) {
    activeCaptureCount++;
    captureQueue.shift()();
  }
}


async function acquireSlot(isBatch = false, isRetailLimited = false, isDsgLimited = false, isWowLimited = false) {
  if (isDsgLimited) {
    if (activeDsgCaptureCount < MAX_CONCURRENT_DSG_CAPTURES) { activeDsgCaptureCount++; return; }
    return new Promise((resolve) => dsgCaptureQueue.push(resolve));
  }
  if (isWowLimited) {
    if (activeWowCaptureCount < MAX_CONCURRENT_WOW_CAPTURES) { activeWowCaptureCount++; return; }
    return new Promise((resolve) => wowCaptureQueue.push(resolve));
  }
  if (isRetailLimited) {
    if (activeRetailCaptureCount < MAX_CONCURRENT_RETAIL_CAPTURES) { activeRetailCaptureCount++; return; }
    return new Promise((resolve) => retailCaptureQueue.push(resolve));
  }
  if (isBatch) {
    if (activeBatchCount < MAX_CONCURRENT_BATCH) { activeBatchCount++; return; }
    return new Promise((resolve) => batchQueue.push(resolve));
  }
  if (activeCaptureCount < MAX_CONCURRENT_CAPTURES) { activeCaptureCount++; return; }
  return new Promise((resolve) => captureQueue.push(resolve));
}

function releaseSlot(isBatch = false, isRetailLimited = false, isDsgLimited = false, isWowLimited = false) {
  if (isDsgLimited) {
    if (dsgCaptureQueue.length > 0) { dsgCaptureQueue.shift()(1); } else { activeDsgCaptureCount = Math.max(0, activeDsgCaptureCount - 1); }
    return;
  }
  if (isWowLimited) {
    if (wowCaptureQueue.length > 0) { wowCaptureQueue.shift()(); } else { activeWowCaptureCount = Math.max(0, activeWowCaptureCount - 1); }
    return;
  }
  if (isRetailLimited) {
    if (retailCaptureQueue.length > 0) { retailCaptureQueue.shift()(); } else { activeRetailCaptureCount = Math.max(0, activeRetailCaptureCount - 1); }
    return;
  }
  if (isBatch) {
    if (batchQueue.length > 0) { batchQueue.shift()(); } else { activeBatchCount = Math.max(0, activeBatchCount - 1); }
    return;
  }
  if (captureQueue.length > 0) { captureQueue.shift()(); } else { activeCaptureCount = Math.max(0, activeCaptureCount - 1); }
}

function resetCaptureQueues() {
  activeCaptureCount = 0;
  activeBatchCount = 0;
  activeRetailCaptureCount = 0;
  activeDsgCaptureCount = 0;
  activeWowCaptureCount = 0;
  captureQueue.splice(0);
  batchQueue.splice(0);
  retailCaptureQueue.splice(0);
  dsgCaptureQueue.splice(0);
  wowCaptureQueue.splice(0);
}

async function getMonitorsMarkingIncompleteAsErrors() {
  let result = [];
  await withStorageLock(async () => {
    const monitors = await getMonitors();
    let changed = false;
    result = monitors.map((monitor) => {
      let next = withIncompleteDataError(monitor);
      if (getMonitorSavedSizes(next).length) {
        const ignoredSizes = next.ignoredSizes || [];
        const filteredLive = applyIgnoredSizesToMonitorData(next.lastExtractedData, ignoredSizes);
        const filteredProductData = applyIgnoredSizesToMonitorData(next.productData, ignoredSizes);
        if (filteredLive !== next.lastExtractedData || filteredProductData !== next.productData) {
          next = { ...next, lastExtractedData: filteredLive, productData: filteredProductData };
        }
        const allSizes = getMonitorSavedSizes(next);
        const unconverted = getUnconvertedSizes(allSizes, next);
        const staleFractional = getStaleFractionalEuSizes(allSizes, next);
        if (unconverted.length) {
          next = {
            ...next,
            hasUsOnlySizes: true,
            usOnlySizesList: unconverted,
            status: "error",
            lastError: `Sizes with no EU conversion: ${unconverted.join(", ")} â€" remove them before importing`
          };
        } else if (next.hasUsOnlySizes || next.usOnlySizesList?.length || /Sizes with no EU conversion/i.test(String(next.lastError || ""))) {
          const hadBadSizeError = /Sizes with no EU conversion/i.test(String(next.lastError || ""));
          next = {
            ...next,
            hasUsOnlySizes: false,
            usOnlySizesList: [],
            status: hadBadSizeError && next.status === "error" ? "ok" : next.status,
            lastError: hadBadSizeError ? "" : next.lastError
          };
        }
        if (!unconverted.length && staleFractional.length) {
          next = {
            ...next,
            hasStaleFractionalEuSizes: true,
            staleFractionalEuSizesList: staleFractional,
            status: "error",
            lastError: getStaleFractionalEuError(staleFractional)
          };
        } else if (!staleFractional.length && (next.hasStaleFractionalEuSizes || next.staleFractionalEuSizesList?.length || /Fraction EU sizes showing as decimals/i.test(String(next.lastError || "")))) {
          const hadStaleFractionError = /Fraction EU sizes showing as decimals/i.test(String(next.lastError || ""));
          next = {
            ...next,
            hasStaleFractionalEuSizes: false,
            staleFractionalEuSizesList: [],
            status: hadStaleFractionError && next.status === "error" ? "ok" : next.status,
            lastError: hadStaleFractionError ? "" : next.lastError
          };
        }
      }
      if (next !== monitor) changed = true;
      return next;
    });
    if (changed) await saveMonitors(result);
  });
  return result;
}

// â"€â"€ Stop checks â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
let stopChecksFlag = false;

function stopAllChecks() {
  stopChecksFlag = true;
  for (const tabId of [...captureTabIds]) {
    captureTabIds.delete(tabId);
    chrome.tabs.remove(tabId).catch(() => {});
  }
  while (captureQueue.length) captureQueue.shift()();
  while (batchQueue.length) batchQueue.shift()();
  while (retailCaptureQueue.length) retailCaptureQueue.shift()();
  while (dsgCaptureQueue.length) dsgCaptureQueue.shift()();
  while (wowCaptureQueue.length) wowCaptureQueue.shift()();
  activeCaptureCount = 0;
  activeBatchCount = 0;
  activeRetailCaptureCount = 0;
  activeDsgCaptureCount = 0;
  activeWowCaptureCount = 0;
  // Drain the DSG tab queue so pending openDsgTab callers don't block forever.
  while (_dsgTabQueue.length) _dsgTabQueue.shift().reject(new Error("stopped"));
  while (_wowTabQueue.length) _wowTabQueue.shift().reject(new Error("stopped"));
  _dsgTabPumping = false;
  _wowTabPumping = false;
  _lastDsgTabOpenTime = 0;
  _lastWowTabOpenTime = 0;
  _dsgTabOpenedSinceClear = 0;
  closeOpenDsgTabs("stop checks").catch(() => { _openDsgTabIds.clear(); });
  if (_dsgAllTabsClosedResolve) { const r = _dsgAllTabsClosedResolve; _dsgAllTabsClosedResolve = null; r(); }
  notifyDashboard("dsg-pause-status", { status: null }).catch(() => {});
  setTimeout(() => { stopChecksFlag = false; }, 120000);
}

function resumeChecks() {
  stopChecksFlag = false;
}

// â"€â"€ Undo stack â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
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

// â"€â"€ Shared extraction logic injected into both capture paths â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
// Returns { buildFullText, extractProduct } as a string block that can be
// eval'd inside any executeScript func (same-origin, isolated world is fine).
// We define it once here and splice it into both funcs below to avoid drift.

function waitWhileTabOpen(tabId, delayMs) {
  if (!captureTabIds.has(tabId)) return Promise.resolve(false);
  return new Promise((resolve) => {
    let done = false;
    const finish = (stayedOpen) => {
      if (done) return;
      done = true;
      clearTimeout(timeout);
      chrome.tabs.onRemoved.removeListener(removedListener);
      resolve(stayedOpen);
    };
    const timeout = setTimeout(() => finish(captureTabIds.has(tabId)), delayMs);
    function removedListener(removedTabId) {
      if (removedTabId === tabId) finish(false);
    }
    chrome.tabs.onRemoved.addListener(removedListener);
  });
}

function pageExtractionCode() {
  // NOTE: this function's BODY is extracted as a string and injected inline.
  // It must be self-contained â€" no references to outer scope.

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

// â"€â"€ Capture from the tab the user already has open â€" no new tab needed â"€â"€â"€â"€â"€â"€â"€
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
        const r = { name: null, brand: null, type: null, color: null, colorRaw: null, colorFinal: null, gender: null, player: null, price: null, currency: null, sku: null, description: null, images: [], sizes: [], outOfStock: [], source: [] };
        const BRANDS = ["Nike","Nike Sportswear","Nike SB","Air Jordan","Jordan","Jordan Brand","Adidas","Adidas Originals","Puma","Reebok","Reebok Classic","New Balance","NewBalance","NB","Converse","Converse All Star","Converse CONS","Vans","Van's","Under Armour","Under Armor","UA","Asics","Asics Tiger","Saucony","Brooks","Brooks Running","Hoka","Hoka One One","ON Cloud","On Running","On Cloud Running","Salomon","Salomon Sportstyle","Timberland","Timberland Pro","UGG","UGG Australia","Dr. Martens","Dr Martens","Doc Martens","Birkenstock","Clarks","Clarks Originals","The North Face","North Face","TNF","Columbia","Columbia Sportswear","Patagonia","Supreme","Off-White","Off White","Offwhite","Balenciaga","Gucci","Louis Vuitton","LV","Yeezy","Adidas Yeezy","Fila","Tommy Hilfiger","Tommy","Ralph Lauren","Polo Ralph Lauren","Polo","Lacoste","Champion","Kappa","Umbro","Ellesse","Diadora","Le Coq Sportif","Lecoqsportif","Mizuno","Karhu","Crocs","Skechers","Skecher","Steve Madden","Ecco","Geox","Camper","Stussy","StÃ¼ssy","Palace","Palace Skateboards","Kith","Carhartt","Carhartt WIP","Dickies","Stone Island","Moncler","Arc'teryx","Arcteryx","Merrell","Keen","Teva","Calvin Klein","CK","Hugo Boss","Boss","Boss by Hugo Boss","Way of Wade","Li-Ning","LiNing"];
        const TYPE_KEYWORDS = {"basketball":"Basketball","casual":"Lifestyle","lifestyle":"Lifestyle","running":"Running","football":"Football","soccer":"Football","cleat":"Football","cleats":"Football","training":"Training","hiking":"Hiking","trail":"Trail","tennis":"Tennis","golf":"Golf","skate":"Skate","skateboarding":"Skate","crossfit":"Training","cross-training":"Training","walking":"Walking","sneaker":"Lifestyle","slip-on":"Lifestyle","sandal":"Sandal","boot":"Boot","loafer":"Lifestyle"};
        const COLORS = {"Black":["black","onyx","jet","ebony","obsidian","raven","coal","ink","shadow","noir","licorice","pitch","tripleblack","triple-black","coreblack","core-black","phantom","anthracite","soot","carbon"],"White":["white","ivory","snow","pearl","sail","cream","bone","eggshell","linen","frost","alabaster","porcelain","chalk","milk","cotton","ghost","offwhite","off-white","whisper","paper","shell","antique"],"Red":["red","crimson","scarlet","ruby","burgundy","maroon","wine","cherry","carmine","cardinal","tomato","garnet","vermillion","vermilion","brick","blood","firebrick","cranberry","raspberry","strawberry","rose","claret","mahogany","terra","cotta","sienna","auburn","rubyred","oxblood","merlot","poppy","coralred","sunsetred","chili","rubywine"],"Blue":["blue","navy","cobalt","royal","indigo","denim","sky","powder","midnight","steel","slate","sapphire","azure","thunder","ice","cornflower","periwinkle","iris","ultramarine","prussian","admiral","marine","federal","storm","glacier","arctic","aegean","obsidian-blue","turbo","polar","mistblue","oceanblue","deepblue","lightblue","darkblue","hyperblue","universityblue","carolinablue"],"Green":["green","olive","sage","forest","army","jade","emerald","mint","fern","moss","pine","volt","lime","hunter","bottle","kelly","shamrock","chartreuse","avocado","pistachio","pear","leaf","basil","seaweed","jungle","cucumber","matcha","celadon","viridian","malachite","voltgreen","neongreen","electricgreen","loden","spruce","evergreen","clover","pea","grassy","seaglass","seafoamgreen"],"Yellow":["yellow","gold","golden","mustard","lemon","canary","butter","banana","honey","sunflower","flaxen","straw","blonde","champagne","vanilla","daffodil","citrine","topaz","citrus","maize","corn","ambergold","sandgold","sulphur","mustardseed","dijon","neonyellow","electricyellow"],"Orange":["orange","amber","tangerine","apricot","rust","copper","pumpkin","saffron","coral","burnt","cinnamon","papaya","mango","melon","clay","ginger","tiger","marigold","bronze","peach","persimmon","nectarine","cantaloupe","sunset","burntorange","terracotta","carrot","kumquat"],"Violet":["purple","violet","lavender","lilac","plum","grape","mauve","amethyst","orchid","wisteria","heather","thistle","periwinkle","mulberry","eggplant","byzantium","aubergine","boysenberry","violetdust","deeppurple","royalpurple"],"Pink":["pink","blush","fuchsia","magenta","salmon","rose","bubblegum","flamingo","watermelon","peony","carnation","petal","flush","rouge","blossom","pastel","candy","lollipop","neon","cerise","hot","dusty","millennial","rosepink","powderpink","softpink","brightpink","shockpink"],"Brown":["brown","tan","beige","camel","mocha","chocolate","coffee","sand","taupe","nude","natural","khaki","wheat","stone","walnut","hazel","toffee","espresso","sepia","umber","fawn","oatmeal","biscuit","latte","ecru","buff","driftwood","chestnut","cacao","bark","leather","suede","caramel","pecan","almond","acorn","cocoa","mink","tobacco","saddle","oak","hickory","truffle","earth","mud","dune","bran"],"Gray":["gray","grey","silver","charcoal","ash","smoke","graphite","pewter","cement","concrete","cloud","wolf","pebble","flint","iron","lead","fossil","heather","marengo","dove","cool","smokey","stonegrey","stone-gray","coolgrey","cool-grey","neutralgray","neutralgrey","platinum","gunmetal"],"Turquoise":["turquoise","teal","aqua","cyan","seafoam","aquamarine","caribbean","lagoon","cerulean","peacock","ocean","pool","mintblue","tiffany","robinsegg","bluegreen","turq"],"Multicolor":["multi","multicolor","multi-color","assorted"]};
        const canonicalizeBrand = (value) => {
          const normalized = nt(value);
          if (!normalized) return "";
          const lower = normalized.toLowerCase();
          if (lower === "air jordan" || lower === "jordan") return "Jordan";
          if (lower === "on" || lower === "on running" || lower === "on cloud") return "ON Cloud";
          if (lower === "li-ning" || lower === "lining" || lower === "way of wade" || lower === "wade") return "Way of Wade";
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
          if (l.includes("way of wade") || l.includes("li-ning")) return "Way of Wade";
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
        const normalizeWadeUsMSize = (value) => { const n = Number(String(value || "").replace(",", ".")); return Number.isFinite(n) && n > 0 && n <= 30 ? String(n) : ""; };
        const extractWadeUsMSizeLabel = (label) => {
          const text = nt(label).replace(/,/g, ".");
          const usM = text.match(/\bUS-?M\s*([0-9]+(?:\.\d+)?)/i) || text.match(/^([0-9]+(?:\.\d+)?)$/);
          return usM ? normalizeWadeUsMSize(usM[1]) : "";
        };
        const normalizeDualGenderSizeLabel = (label, genderText = "") => {
          const text = nt(label);
          if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) return extractWadeUsMSizeLabel(text);
          const m = text.match(/\bM\s*(\d+(?:[.,]\d+)?)\s*\/\s*W\s*(\d+(?:[.,]\d+)?)/i);
          if (!m) return text;
          const raw = /women|girl|female/i.test(genderText) ? m[2] : m[1];
          const n = parseFloat(String(raw).replace(",", "."));
          return Number.isFinite(n) ? String(n) : raw;
        };
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
        if (!r.price) { for (const sel of [".price","[class*='price']","[data-price]","[class*='Price']",".product-price",".sale-price",".current-price"]) { const e = document.querySelector(sel); if (!e) continue; const t = e.textContent?.trim() || ''; if (/\b(installment|interest.?free|klarna|afterpay|sezzle|quadpay|pay in \d|4x |zip\s*pay|split\s*pay|paidy|laybuy)\b/i.test(t)) continue; const pm = t.match(/[\$â‚¬Â£Â¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/); if (pm) { r.price = pm[1].replace(',', '.'); r.source.push("dom"); break; } } }
        if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) {
          const parseWadeMoneyValue = (text) => {
            const m = String(text || '').match(/[\$â‚¬Â£Â¥]?\s*(\d{1,5}(?:[.,]\d{1,2})?)/);
            if (!m) return null;
            const n = parseFloat(m[1].replace(',', '.'));
            return Number.isFinite(n) ? n : null;
          };
          const priceBox = document.querySelector('.t4s-product-price[data-product-price],.t4s-product-price[data-pr-price],.t4s-product-price');
          if (priceBox) {
            const salePrice = parseWadeMoneyValue(priceBox.querySelector('ins .money, ins')?.textContent || '');
            const regularPrice = salePrice != null
              ? salePrice
              : parseWadeMoneyValue((Array.from(priceBox.querySelectorAll('.money')).find((el) => !el.closest('del,ins')) || priceBox.querySelector('.money'))?.textContent || priceBox.textContent || '');
            if (regularPrice != null) {
              r.price = String(regularPrice);
              r.source.push("wade-price");
            }
          }
        }
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
        let wadeRawColor = null;
        if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) {
          for (const row of document.querySelectorAll('.wade-spec-row')) {
            const label = row.querySelector('.wade-spec-label')?.textContent?.trim() || '';
            if (!/^colou?r\s*:?\s*$/i.test(label)) continue;
            const value = row.querySelector('.wade-spec-value')?.textContent?.replace(/\s+/g, ' ').trim() || '';
            const first = value.split('/')[0].trim();
            if (first) {
              wadeRawColor = first;
              r.colorRaw = first;
              r.color = first;
              r.source.push("wade-color");
            }
            break;
          }
          const wadeDescription = document.querySelector('span.metafield-multi_line_text_field')?.textContent?.replace(/\s+/g, ' ').trim() || '';
          if (wadeDescription.length > 20) {
            r.description = wadeDescription.slice(0, 3000);
            r.source.push("wade-description");
          }
          if (!r.sku) {
            const wadeSku = document.querySelector('[data-product__sku-number]')?.textContent?.trim() || '';
            if (wadeSku) { r.sku = wadeSku; r.source.push("wade-sku"); }
          }
          if (!r.brand) { r.brand = "Way of Wade"; r.source.push("wade-default"); }
          r.type = "Basketball";
          r.gender = "Men";
          r.genderDisplay = "Men, Women";
        }
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
        if (!r.gender) { const dsgBc = document.querySelector('nav[aria-label="Breadcrumbs"]'); if (dsgBc) { for (const a of dsgBc.querySelectorAll('a[itemprop="item"]')) { const href = (a.getAttribute('href') || '').toLowerCase(); const em = (a.getAttribute('data-em') || '').toLowerCase(); const combined = href + ' ' + em; if (/\/womens[-/]|womens/.test(combined)) { r.gender = 'Women'; r.source.push('dsg-breadcrumb'); break; } if (/\/mens[-/]|[^o]mens/.test(combined)) { r.gender = 'Men'; r.source.push('dsg-breadcrumb'); break; } if (/\/kids[-/]|\/youth[-/]|\/boys[-/]|\/girls[-/]/.test(combined)) { r.gender = 'Kids'; r.source.push('dsg-breadcrumb'); break; } } } }
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
          // Accordion heading "Details" â†' next sibling content
          const allHds = Array.from(document.querySelectorAll('h2,h3,h4,button,summary'));
          for (const hd of allHds) {
            if (!/^\s*details?\s*$/i.test(hd.textContent?.trim())) continue;
            const content = hd.nextElementSibling || hd.parentElement?.nextElementSibling;
            if (content) { const d = (content.innerText || content.textContent || '').trim(); if (d.length > 20 && d.length < 6000) { r.description = d.slice(0, 3000); break; } }
          }
        }
        if (!r.description) {
          const descSels = ['.product-description','.product__description','.product-single__description','[data-product-description]','.rte','.t4s-rte','.t4s-product-description','.product-info__description','[class*="product-description"]','[class*="product-body"]','[class*="product-text"]','[class*="description"]'];
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
        if (wadeRawColor) { r.colorRaw = wadeRawColor; r.color = wadeRawColor; r.colorFinal = (() => { const spaced = wadeRawColor.replace(/([a-z])([A-Z])/g, "$1 $2"); const words = spaced.toLowerCase().replace(/[^a-z0-9\s]/g, " ").split(/\s+/).filter(Boolean); for (const word of words) { const clean = word.replace(/[^a-z0-9]/g, ""); for (const [base, synonyms] of Object.entries(COLORS)) { if (base === "Multicolor") continue; if (synonyms.some((c) => String(c).toLowerCase().replace(/[^a-z0-9]/g, "") === clean)) return base; } } return "Multicolor"; })(); }
        if (!r.player && /\bd['’]?lo\s*1\b/i.test(r.name || "")) r.player = "D'Angelo Russell";
        { const dsgSkuM = location.pathname.match(/\/p\/[^/]+\/([A-Z0-9]{8,20})(?:\/|$|\?)/i); if (dsgSkuM && !r.sku) r.sku = dsgSkuM[1].toUpperCase(); const dsgColorParam = new URLSearchParams(location.search).get('color'); if (dsgColorParam && !r.colorRaw) { r.colorRaw = dsgColorParam.split('/')[0].trim(); r.color = pickColor(r.colorRaw); r.colorFinal = r.color; } }
        if (/(^|\.)dickssportinggoods\.com\b/i.test(location.hostname)) {
          if (!r.colorRaw) { const colorEl = document.querySelector('pdp-attributes-components-base-attribute-label span.hmf-body-m'); if (colorEl) { r.colorRaw = colorEl.textContent.trim(); r.color = pickColor(r.colorRaw); r.colorFinal = r.color; } }
          if (!r.sizes.length) { document.querySelectorAll('.hmf-selectable-container button[aria-label]').forEach(btn => { const raw = btn.getAttribute('aria-label') || ''; const mw = raw.match(/\bM\s*(\d+(?:[.,]\d+)?)\s*\/\s*W\s*\d+(?:[.,]\d+)?/i); if (!mw) return; const t = String(parseFloat(mw[1].replace(',', '.'))); if (!t || t === 'NaN' || r.sizes.includes(t)) return; r.sizes.push(t); if (btn.classList.contains('hmf-selectable-unavailable')) r.outOfStock.push(t); }); }
        }
        if (!r.sizes.length && /(^|\.)footlocker\.com\b/i.test(location.hostname)) { document.querySelectorAll('#tabPanel button[class*="SizeSelector"]').forEach(btn => { const t = btn.querySelector('span')?.textContent?.trim() || ''; if (!t || r.sizes.includes(t)) return; r.sizes.push(t); if (btn.classList.contains('SizeSelectorNewDesign-button--disabled') || /sold.?out/i.test(btn.getAttribute('aria-label') || '')) r.outOfStock.push(t); }); }
        if (!r.price && /(^|\.)footlocker\.com\b/i.test(location.hostname)) { const flPEl = document.querySelector('.ProductPrice span[class*="text-sale_red"][aria-hidden="true"]') || document.querySelector('.ProductPrice span[aria-hidden="true"]:not([class*="line-through"])'); if (flPEl) { const m = flPEl.textContent.match(/[\$€£¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/); if (m) { r.price = String(parseFloat(m[1].replace(',', '.'))); r.currency = 'USD'; r.source.push('fl-price-dom'); } } }
        if (!r.sizes.length && /(^|\.)wayofwade\.com\b/i.test(location.hostname)) { const root = document.querySelector('.t4s-size-selector__content,[data-size-content]'); if (root) { const seen = new Set(); root.querySelectorAll('[data-size-btn][data-size-value]').forEach(btn => { const usM = extractWadeUsMSizeLabel(btn.getAttribute('data-size-value') || btn.getAttribute('aria-label') || btn.textContent || ''); if (!usM) return; const key = btn.getAttribute('data-variant-id') || usM.toLowerCase(); if (seen.has(key)) return; seen.add(key); if (!r.sizes.includes(usM)) r.sizes.push(usM); if (btn.getAttribute('data-available') === 'false') { if (!r.outOfStock.includes(usM)) r.outOfStock.push(usM); } }); } }
        if (!r.sizes.length) { const sel = document.querySelector('select[name*="size" i],select[id*="size" i],select[class*="size" i],select[data-option-name*="size" i]'); if (sel) { Array.from(sel.options).forEach((o) => { const t = normalizeDualGenderSizeLabel(o.text.trim(), r.gender || metaText); if (!t || /^(select|choose|--)/i.test(t)) return; r.sizes.push(t); if (o.disabled || /out.?of.?stock|sold.?out|unavailable/i.test(o.text)) r.outOfStock.push(t); }); } }
        if (!r.sizes.length) { document.querySelectorAll('input[type="radio"][name*="size" i],input[type="radio"][data-option-value-id]').forEach((input) => { const t = normalizeDualGenderSizeLabel((input.value || "").trim(), r.gender || metaText); if (!t || /^(select|choose|--)/i.test(t) || t.length > 20) return; if (!r.sizes.includes(t)) r.sizes.push(t); if (input.classList.contains("disabled") || input.disabled) r.outOfStock.push(t); }); }
        if (!r.sizes.length) { for (const csel of ['[class*="size"] button,[class*="size"] label','[class*="swatch"] button,[class*="swatch"] label','[data-option-name*="size" i] button,[data-option-name*="size" i] label','[class*="size-selector"] button','[class*="sizebtn"]','[class*="size-btn"]','button[aria-label*="/W"],button[aria-label*="M"][aria-label*="/"]']) { const els = document.querySelectorAll(csel); if (els.length > 0 && els.length < 80) { els.forEach((el) => { const rawSize = el.getAttribute("aria-label") || el.textContent?.trim(); const t = normalizeDualGenderSizeLabel(rawSize, r.gender || metaText); if (!t || t.length > 20 || r.sizes.includes(t)) return; r.sizes.push(t); const cls = ((el.className || "") + " " + (el.getAttribute("aria-label") || "")).toLowerCase(); const oos = el.classList.contains("disabled") || el.disabled || el.getAttribute("aria-disabled") === "true" || /disabled|out.?of.?stock|sold.?out|unavailable|not-available/i.test(cls) || window.getComputedStyle(el).textDecoration.includes("line-through"); if (oos) r.outOfStock.push(t); }); if (r.sizes.length) break; } } }
        if (!r.sizes.length) { const pa = document.querySelector('main,[role="main"]') || document.body; pa.querySelectorAll('button').forEach((el) => { const t = (el.textContent || "").replace(",", ".").replace(/\s+/g, " ").trim(); const size = t.replace(/^(?:EU|EUR)\s*/i, "").trim(); if (!t || t.length > 12 || r.sizes.includes(size)) return; if (!/^(?:EU|EUR|US|UK|CM)?\s*\d+(?:\.\d+)?(?:\s+(?:1\/2|[12]\/3))?(?:\s*(?:US|EU|UK|CM|EUR|Â½))?$/i.test(t)) return; r.sizes.push(size); const oos = el.classList.contains("disabled") || el.disabled || el.getAttribute("aria-disabled") === "true" || window.getComputedStyle(el).textDecoration.includes("line-through"); if (oos) r.outOfStock.push(size); }); }
        if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) {
          const wadeRoot = document.querySelector('.t4s-product__media-wrapper,[data-product-single-media-group]');
          const addWadeImage = (raw) => {
            let src = String(raw || '').trim().replace(/&amp;/g, '&');
            if (!src || src.startsWith('data:')) return;
            if (src.startsWith('//')) src = 'https:' + src;
            if (!/^https?:\/\//i.test(src)) return;
            src = src.replace(/([?&])width=\d+(&?)/i, (m, p1, p2) => p2 ? p1 : '').replace(/[?&]$/, '');
            if (!/\/cdn\/shop\/files\//i.test(src)) return;
            if (!r.images.includes(src)) r.images.push(src);
          };
          if (wadeRoot) {
            wadeRoot.querySelectorAll('[data-main-slide] img[data-master], img[data-master]').forEach((img) => addWadeImage(img.getAttribute('data-master')));
            if (!r.images.length) {
              wadeRoot.querySelectorAll('[data-main-slide] img[data-srcset], [data-main-slide] img[srcset]').forEach((img) => {
                const ss = img.getAttribute('data-srcset') || img.getAttribute('srcset') || '';
                const best = ss.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 });
                addWadeImage(best.u);
              });
            }
          }
        }
        const skipGenericImages = (/(^|\.)wayofwade\.com\b/i.test(location.hostname) || /(^|\.)dickssportinggoods\.com\b/i.test(location.hostname)) && r.images.length > 0;
        const swatchEls = new Set();
        document.querySelectorAll('[class*="ColorChip"],[class*="color-chip"],[class*="colorway"],[class*="ColorSwatch"],[class*="color-swatch"],[class*="StyleChip"],[class*="style-chip"],[class*="VariantImage"],[class*="variant-color"],[class*="SwatchImage"],[class*="color-option"],[class*="product-variants"],[class*="ProductVariants"]').forEach(el => swatchEls.add(el));
        const isInSwatch = (img) => { let el = img.parentElement; while (el && el !== document.body) { if (swatchEls.has(el)) return true; const cls = el.className || ''; if (/color.?chip|color.?swatch|colorway|style.?chip|swatch.?img|variant.?img|color.?option/i.test(cls)) return true; el = el.parentElement; } return false; };
        const imgRoot = document.querySelector('.ProductGallery,[class*="ProductGallery"],[class*="product-gallery"],[class*="pdp-gallery"],[class*="Gallery--pdp"],[class*="GallerySlider"],[class*="gallery-slider"],.product-media,.pdp-product-images') || document.querySelector('main,[role="main"]') || document.body;
        const seenImgs = new Set();
        if (!skipGenericImages) imgRoot.querySelectorAll('img').forEach((img) => {
          if (isInSwatch(img)) return;
          let src = '';
          if (img.srcset) { const best = img.srcset.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 }); if (best.u) src = best.u; }
          if (!src) src = img.currentSrc || img.src || '';
          if (src && src.startsWith('//')) src = 'https:' + src;
          if (!src || !/^https?:\/\//.test(src)) { const lzRaw = img.getAttribute('data-src') || img.getAttribute('data-lazy-src') || img.getAttribute('data-original') || img.getAttribute('data-lazy') || img.getAttribute('data-master') || ''; const lz = lzRaw.startsWith('//') ? 'https:' + lzRaw : lzRaw; if (/^https?:\/\//.test(lz)) src = lz; else if (img.dataset && img.dataset.srcset) { const ds = img.dataset.srcset; const dsBest = ds.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 }); if (dsBest.u) src = dsBest.u.startsWith('//') ? 'https:' + dsBest.u : dsBest.u; } }
          if (!src || !/^https?:\/\//.test(src)) return;
          const w = img.naturalWidth || parseInt(img.getAttribute('width') || '0');
          const h = img.naturalHeight || parseInt(img.getAttribute('height') || '0');
          if ((w > 0 && w < 200) || (h > 0 && h < 200)) return;
          if (/logo|icon|badge|star|rating|avatar|social|sprite|pixel|tracking|placeholder|blank/i.test(src)) return;
          if (seenImgs.has(src)) return;
          seenImgs.add(src);
          r.images.push(src);
        });
        if (/(^|\.)dickssportinggoods\.com\b/i.test(location.hostname) && !r.images.length) {
          const seenDsgImgs = new Set();
          document.querySelectorAll('#pdp-upper-left img[itemprop="image"], pdp-carousel img[itemprop="image"]').forEach(img => {
            const ss = img.getAttribute('srcset') || '';
            let src = '';
            if (ss) { const best = ss.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 }); if (best.u) src = best.u; }
            if (!src) src = img.src || '';
            const baseMatch = src.match(/^(https:\/\/dks\.scene7\.com\/is\/image\/[^?]+)/);
            if (!baseMatch || seenDsgImgs.has(baseMatch[1])) return;
            seenDsgImgs.add(baseMatch[1]);
            r.images.push(baseMatch[1] + '?qlt=85&wid=1200&hei=1200&fmt=pjpeg&fit=constrain');
          });
        }
        if (/(^|\.)footlocker\.com\b/i.test(location.hostname) && !r.images.length) {
          document.querySelectorAll('.ProductGallery img[data-id="ProductImage"]').forEach(img => {
            const src = img.getAttribute('src') || img.currentSrc || '';
            if (!src || !/^https?:\/\//.test(src) || r.images.includes(src)) return;
            r.images.push(src);
          });
        }
        r.images = r.images.slice(0, /(^|\.)wayofwade\.com\b/i.test(location.hostname) ? 24 : 8);
        if (/(^|\.)dickssportinggoods\.com\b/i.test(location.hostname)) { r.gender = r.type === 'Football' ? 'Both' : null; }
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
        const pageGenderText = Array.from(document.querySelectorAll('nav[aria-label*="breadcrumb" i] a,nav[aria-label*="breadcrumb" i] span,[class*="breadcrumb"] a,[class*="breadcrumb"] span,h1')).map((el) => el.textContent || "").join(" ");
        const isWayOfWadePage = /(^|\.)wayofwade\.com\b/i.test(location.hostname);
        const normalizeWadeUsMSize = (value) => { const n = Number(String(value || "").replace(",", ".")); return Number.isFinite(n) && n > 0 && n <= 30 ? String(n) : ""; };
        const extractWadeUsMLiveSizeLabel = (label) => {
          const text = nt(label).replace(/,/g, ".");
          const usM = text.match(/\bUS-?M\s*([0-9]+(?:\.\d+)?)/i) || text.match(/^([0-9]+(?:\.\d+)?)$/);
          return usM ? normalizeWadeUsMSize(usM[1]) : "";
        };
        const collectWadeUsMSizeButtons = (available, unavailable) => {
          if (!isWayOfWadePage) return;
          const panel = document.querySelector('[data-size-panel="US-M"]');
          if (!panel) return;
          const seen = new Set();
          panel.querySelectorAll('[data-size-btn][data-size-value]').forEach((btn) => {
            const usM = normalizeWadeUsMSize(btn.getAttribute('data-size-value') || '');
            if (!usM) return;
            const key = btn.getAttribute('data-variant-id') || usM.toLowerCase();
            if (seen.has(key)) return;
            seen.add(key);
            const oos = btn.getAttribute('data-available') === 'false' || btn.disabled || btn.getAttribute('aria-disabled') === 'true' || /disabled|out.?of.?stock|sold.?out|unavailable|not-available/i.test(btn.className || '');
            const list = oos ? unavailable : available;
            if (!list.includes(usM)) list.push(usM);
          });
        };
        const normalizeLiveSizeLabel = (label) => {
          let text = nt(label);
          text = text.replace(/^Size:\s*/i, "").replace(/\s*[-â€"]\s*Sold\s+Out/i, "").trim();
          if (isWayOfWadePage) return extractWadeUsMLiveSizeLabel(text);
          const m = text.match(/\bM\s*(\d+(?:[.,]\d+)?)\s*\/\s*W\s*(\d+(?:[.,]\d+)?)/i);
          if (!m) return text;
          const raw = /women|girl|female/i.test(pageGenderText) && !/men'?s/i.test(pageGenderText) ? m[2] : m[1];
          const n = parseFloat(String(raw).replace(",", "."));
          return Number.isFinite(n) ? String(n) : raw;
        };
        const parseMoneyValue = (text) => {
          const m = String(text || '').match(/[\$â‚¬Â£Â¥]?\s*(\d{1,5}(?:[.,]\d{1,2})?)/);
          if (!m) return null;
          const n = parseFloat(m[1].replace(',', '.'));
          return Number.isFinite(n) ? n : null;
        };
        const isDsgPage = /(^|\.)dickssportinggoods\.com\b/i.test(location.hostname);
        let wadePrice = null;
        let wadeCompareAt = null;
        if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) {
          const priceBox = document.querySelector('.t4s-product-price[data-product-price],.t4s-product-price[data-pr-price],.t4s-product-price');
          if (priceBox) {
            const salePrice = parseMoneyValue(priceBox.querySelector('ins .money, ins')?.textContent || '');
            const comparePrice = parseMoneyValue(priceBox.querySelector('del .money, del')?.textContent || '');
            if (salePrice != null) {
              wadePrice = salePrice;
              if (comparePrice != null && comparePrice > salePrice) wadeCompareAt = comparePrice;
            } else {
              const regularEl = Array.from(priceBox.querySelectorAll('.money')).find((el) => !el.closest('del,ins')) || priceBox.querySelector('.money');
              const regularPrice = parseMoneyValue(regularEl?.textContent || priceBox.textContent || '');
              if (regularPrice != null) wadePrice = regularPrice;
            }
          }
        }
        let dsgPrice = null;
        if (isDsgPage) {
          dsgPrice = parseMoneyValue(
            document.querySelector('meta[itemprop="price"]')?.getAttribute('content') ||
            document.querySelector('#offer-price .product-price, hmf-price .product-price, pdp-price .product-price')?.textContent ||
            ''
          );
        }
        const rawNums = new Set();
        if (wadePrice == null) for (const root of roots) {
          const bnpl = /\b(installment|interest.?free|klarna|afterpay|sezzle|quadpay|pay in \d|4x |zip\s*pay|split\s*pay|paidy|laybuy)\b/i;
          const addPrices = (txt) => { if (bnpl.test(txt)) return; for (const m of txt.matchAll(/[\$â‚¬Â£Â¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/g)) { const n = parseFloat(m[1].replace(",", ".")); if (n >= 1 && n < 100000) rawNums.add(n); } };
          root.querySelectorAll("[class*='price'],[data-price],[class*='Price'],[data-test*='price'],[class*='final-price'],[class*='sale-price'],[class*='current-price'],[class*='product-price'],ins,del,.was-price,.compare-at,.original-price").forEach((el) => addPrices(el.textContent || ""));
          if (!rawNums.size) { root.querySelectorAll("span,b,strong,div,p").forEach((el) => { if (el.children.length > 3) return; const txt = (el.textContent || "").trim(); if (txt.length > 50) return; addPrices(txt); }); }
        }
        const sorted = [...rawNums].sort((a, b) => a - b);
        const r5 = Math.round;
        const mk = priceAdjustment;
        const price = wadePrice != null ? wadePrice : (dsgPrice != null ? dsgPrice : (sorted.length ? sorted[0] : null));
        const cmp = dsgPrice != null ? null : (wadePrice != null ? wadeCompareAt : (sorted.length >= 2 ? sorted[sorted.length - 1] : null));
        const inStock = [], outOfStock = [];
        collectWadeUsMSizeButtons(inStock, outOfStock);
        if (isDsgPage) {
          document.querySelectorAll('.hmf-selectable-container button[aria-label], .selector-attribute-outer button[aria-label], common-product-availability button[aria-label]').forEach((btn) => {
            const raw = btn.getAttribute('aria-label') || btn.textContent || '';
            const mw = raw.match(/\bM\s*(\d+(?:[.,]\d+)?)\s*\/\s*W\s*\d+(?:[.,]\d+)?/i);
            const t = mw ? String(parseFloat(mw[1].replace(',', '.'))) : normalizeLiveSizeLabel(raw);
            if (!t || t === 'NaN' || t.length > 15 || inStock.includes(t) || outOfStock.includes(t)) return;
            const oos = btn.disabled || btn.getAttribute('aria-disabled') === 'true' || btn.classList.contains('hmf-selectable-unavailable');
            (oos ? outOfStock : inStock).push(t);
          });
        }
        for (const root of roots) {
          root.querySelectorAll('select[name*="size" i],select[id*="size" i],select[class*="size" i],select[data-option-name*="size" i]').forEach((sel) => {
            Array.from(sel.options).forEach((o) => {
              const t = normalizeLiveSizeLabel(o.text.trim());
              if (!t || /^(select|choose|--)/i.test(t)) return;
              const list = (o.disabled || /out.?of.?stock|sold.?out|unavailable/i.test(o.text)) ? outOfStock : inStock;
              if (!list.includes(t)) list.push(t);
            });
          });
          root.querySelectorAll('input[type="radio"][name*="size" i],input[type="radio"][data-option-value-id]').forEach((input) => {
            const t = normalizeLiveSizeLabel((input.value || "").trim());
            if (!t || /^(select|choose|--)/i.test(t) || t.length > 20) return;
            const list = (input.classList.contains("disabled") || input.disabled) ? outOfStock : inStock;
            if (!list.includes(t)) list.push(t);
          });
          if (!inStock.length && !outOfStock.length) {
            for (const csel of ['[class*="size"] button,[class*="size"] label','[class*="swatch"] button,[class*="swatch"] label','[data-option-name*="size" i] button,[data-option-name*="size" i] label','button[aria-label*="/W"],button[aria-label*="M"][aria-label*="/"]']) {
              const els = root.querySelectorAll(csel);
              if (!els.length || els.length > 60) continue;
              els.forEach((el) => {
                const t = normalizeLiveSizeLabel(el.getAttribute("aria-label") || el.textContent?.trim());
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
        if (!inStock.length && !outOfStock.length && /(^|\.)wayofwade\.com\b/i.test(location.hostname)) { const fullPanel = document.querySelector('[data-size-panel]'); if (fullPanel) { fullPanel.querySelectorAll('[data-size-btn]').forEach(btn => { const usM = extractWadeUsMLiveSizeLabel(btn.getAttribute('data-size-value') || btn.getAttribute('aria-label') || btn.textContent || ''); if (!usM) return; const oos = btn.getAttribute('data-available') === 'false'; const list = oos ? outOfStock : inStock; if (!list.includes(usM)) list.push(usM); }); } }
        { const dsgPrice = document.querySelector('hmf-price, [class*="our-price"]'); if (dsgPrice && !rawNums.size) { const bnpl2 = /\b(klarna|afterpay|sezzle|quadpay|zip\s*pay|pay in \d)\b/i; const txt = dsgPrice.textContent || ''; if (!bnpl2.test(txt)) { for (const m of txt.matchAll(/[\$â‚¬Â£Â¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/g)) { const n = parseFloat(m[1].replace(',', '.')); if (n >= 1 && n < 100000) rawNums.add(n); } } } }
        { if (!inStock.length && !outOfStock.length) { const dsgBtns = document.querySelectorAll('common-product-availability button, hmf-size-selector button, [class*="size-selector"] button, [class*="sizeTile"] button, [class*="size-tile"] button'); if (dsgBtns.length && dsgBtns.length < 60) { dsgBtns.forEach(btn => { const t = normalizeLiveSizeLabel(btn.textContent?.trim() || ''); if (!t || t.length > 10) return; if (inStock.includes(t) || outOfStock.includes(t)) return; const oos = btn.disabled || btn.getAttribute('aria-disabled') === 'true' || /\b(disabled|oos|out-of-stock|unavailable)\b/i.test(btn.className || ''); (oos ? outOfStock : inStock).push(t); }); } } }
        { if (!inStock.length && !outOfStock.length) { const isDsgPage = /(^|\.)dickssportinggoods\.com\b/i.test(location.hostname); if (isDsgPage) { document.querySelectorAll('button').forEach(btn => { const t = (btn.textContent || '').replace(',', '.').replace(/\s+/g, ' ').trim(); if (!t || t.length > 12) return; if (!/^(?:EU|EUR|US|UK|CM)?\s*\d+(?:\.\d+)?(?:\s+(?:1\/2|[12]\/3))?(?:\s*(?:US|EU|UK|CM|Â½))?$/i.test(t)) return; const size = t.replace(/^(?:EU|EUR)\s*/i, '').trim(); if (inStock.includes(size) || outOfStock.includes(size)) return; const oos = btn.disabled || btn.getAttribute('aria-disabled') === 'true' || /\b(disabled|oos|out-of-stock)\b/i.test(btn.className || ''); (oos ? outOfStock : inStock).push(size); }); } } }
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
          errors.push("Invalid: " + selector + " - " + err.message);
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
        const r = { name: null, brand: null, type: null, color: null, colorRaw: null, colorFinal: null, gender: null, player: null, price: null, currency: null, sku: null, description: null, images: [], sizes: [], outOfStock: [], source: [] };
        const BRANDS = ["Nike","Nike Sportswear","Nike SB","Air Jordan","Jordan","Jordan Brand","Adidas","Adidas Originals","Puma","Reebok","Reebok Classic","New Balance","NewBalance","NB","Converse","Converse All Star","Converse CONS","Vans","Van's","Under Armour","Under Armor","UA","Asics","Asics Tiger","Saucony","Brooks","Brooks Running","Hoka","Hoka One One","ON Cloud","On Running","On Cloud Running","Salomon","Salomon Sportstyle","Timberland","Timberland Pro","UGG","UGG Australia","Dr. Martens","Dr Martens","Doc Martens","Birkenstock","Clarks","Clarks Originals","The North Face","North Face","TNF","Columbia","Columbia Sportswear","Patagonia","Supreme","Off-White","Off White","Offwhite","Balenciaga","Gucci","Louis Vuitton","LV","Yeezy","Adidas Yeezy","Fila","Tommy Hilfiger","Tommy","Ralph Lauren","Polo Ralph Lauren","Polo","Lacoste","Champion","Kappa","Umbro","Ellesse","Diadora","Le Coq Sportif","Lecoqsportif","Mizuno","Karhu","Crocs","Skechers","Skecher","Steve Madden","Ecco","Geox","Camper","Stussy","StÃ¼ssy","Palace","Palace Skateboards","Kith","Carhartt","Carhartt WIP","Dickies","Stone Island","Moncler","Arc'teryx","Arcteryx","Merrell","Keen","Teva","Calvin Klein","CK","Hugo Boss","Boss","Boss by Hugo Boss","Way of Wade","Li-Ning","LiNing"];
        const TYPE_KEYWORDS = {"basketball":"Basketball","casual":"Lifestyle","lifestyle":"Lifestyle","running":"Running","football":"Football","soccer":"Football","cleat":"Football","cleats":"Football","training":"Training","hiking":"Hiking","trail":"Trail","tennis":"Tennis","golf":"Golf","skate":"Skate","skateboarding":"Skate","crossfit":"Training","cross-training":"Training","walking":"Walking","sneaker":"Lifestyle","slip-on":"Lifestyle","sandal":"Sandal","boot":"Boot","loafer":"Lifestyle"};
        const COLORS = {"Black":["black","onyx","jet","ebony","obsidian","raven","coal","ink","shadow","noir","licorice","pitch","tripleblack","triple-black","coreblack","core-black","phantom","anthracite","soot","carbon"],"White":["white","ivory","snow","pearl","sail","cream","bone","eggshell","linen","frost","alabaster","porcelain","chalk","milk","cotton","ghost","offwhite","off-white","whisper","paper","shell","antique"],"Red":["red","crimson","scarlet","ruby","burgundy","maroon","wine","cherry","carmine","cardinal","tomato","garnet","vermillion","vermilion","brick","blood","firebrick","cranberry","raspberry","strawberry","rose","claret","mahogany","terra","cotta","sienna","auburn","rubyred","oxblood","merlot","poppy","coralred","sunsetred","chili","rubywine"],"Blue":["blue","navy","cobalt","royal","indigo","denim","sky","powder","midnight","steel","slate","sapphire","azure","thunder","ice","cornflower","periwinkle","iris","ultramarine","prussian","admiral","marine","federal","storm","glacier","arctic","aegean","obsidian-blue","turbo","polar","mistblue","oceanblue","deepblue","lightblue","darkblue","hyperblue","universityblue","carolinablue"],"Green":["green","olive","sage","forest","army","jade","emerald","mint","fern","moss","pine","volt","lime","hunter","bottle","kelly","shamrock","chartreuse","avocado","pistachio","pear","leaf","basil","seaweed","jungle","cucumber","matcha","celadon","viridian","malachite","voltgreen","neongreen","electricgreen","loden","spruce","evergreen","clover","pea","grassy","seaglass","seafoamgreen"],"Yellow":["yellow","gold","golden","mustard","lemon","canary","butter","banana","honey","sunflower","flaxen","straw","blonde","champagne","vanilla","daffodil","citrine","topaz","citrus","maize","corn","ambergold","sandgold","sulphur","mustardseed","dijon","neonyellow","electricyellow"],"Orange":["orange","amber","tangerine","apricot","rust","copper","pumpkin","saffron","coral","burnt","cinnamon","papaya","mango","melon","clay","ginger","tiger","marigold","bronze","peach","persimmon","nectarine","cantaloupe","sunset","burntorange","terracotta","carrot","kumquat"],"Violet":["purple","violet","lavender","lilac","plum","grape","mauve","amethyst","orchid","wisteria","heather","thistle","periwinkle","mulberry","eggplant","byzantium","aubergine","boysenberry","violetdust","deeppurple","royalpurple"],"Pink":["pink","blush","fuchsia","magenta","salmon","rose","bubblegum","flamingo","watermelon","peony","carnation","petal","flush","rouge","blossom","pastel","candy","lollipop","neon","cerise","hot","dusty","millennial","rosepink","powderpink","softpink","brightpink","shockpink"],"Brown":["brown","tan","beige","camel","mocha","chocolate","coffee","sand","taupe","nude","natural","khaki","wheat","stone","walnut","hazel","toffee","espresso","sepia","umber","fawn","oatmeal","biscuit","latte","ecru","buff","driftwood","chestnut","cacao","bark","leather","suede","caramel","pecan","almond","acorn","cocoa","mink","tobacco","saddle","oak","hickory","truffle","earth","mud","dune","bran"],"Gray":["gray","grey","silver","charcoal","ash","smoke","graphite","pewter","cement","concrete","cloud","wolf","pebble","flint","iron","lead","fossil","heather","marengo","dove","cool","smokey","stonegrey","stone-gray","coolgrey","cool-grey","neutralgray","neutralgrey","platinum","gunmetal"],"Turquoise":["turquoise","teal","aqua","cyan","seafoam","aquamarine","caribbean","lagoon","cerulean","peacock","ocean","pool","mintblue","tiffany","robinsegg","bluegreen","turq"],"Multicolor":["multi","multicolor","multi-color","assorted"]};
        const canonicalizeBrand = (value) => {
          const normalized = nt(value);
          if (!normalized) return "";
          const lower = normalized.toLowerCase();
          if (lower === "air jordan" || lower === "jordan") return "Jordan";
          if (lower === "on" || lower === "on running" || lower === "on cloud") return "ON Cloud";
          if (lower === "li-ning" || lower === "lining" || lower === "way of wade" || lower === "wade") return "Way of Wade";
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
          if (l.includes("way of wade") || l.includes("li-ning")) return "Way of Wade";
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
        const normalizeWadeUsMSize = (value) => { const n = Number(String(value || "").replace(",", ".")); return Number.isFinite(n) && n > 0 && n <= 30 ? String(n) : ""; };
        const extractWadeUsMSizeLabel = (label) => {
          const text = nt(label).replace(/,/g, ".");
          const usM = text.match(/\bUS-?M\s*([0-9]+(?:\.\d+)?)/i) || text.match(/^([0-9]+(?:\.\d+)?)$/);
          return usM ? normalizeWadeUsMSize(usM[1]) : "";
        };
        const collectWadeUsMSizeButtons = (available, unavailable) => {
          if (!/(^|\.)wayofwade\.com\b/i.test(location.hostname)) return;
          const panel = document.querySelector('[data-size-panel="US-M"]');
          if (!panel) return;
          const seen = new Set();
          panel.querySelectorAll('[data-size-btn][data-size-value]').forEach((btn) => {
            const usM = normalizeWadeUsMSize(btn.getAttribute('data-size-value') || '');
            if (!usM) return;
            const key = btn.getAttribute('data-variant-id') || usM.toLowerCase();
            if (seen.has(key)) return;
            seen.add(key);
            const oos = btn.getAttribute('data-available') === 'false' || btn.disabled || btn.getAttribute('aria-disabled') === 'true' || /disabled|out.?of.?stock|sold.?out|unavailable|not-available/i.test(btn.className || '');
            const list = oos ? unavailable : available;
            if (!list.includes(usM)) list.push(usM);
          });
        };
        const normalizeDualGenderSizeLabel = (label, genderText = "") => {
          const text = nt(label);
          if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) return extractWadeUsMSizeLabel(text);
          const m = text.match(/\bM\s*(\d+(?:[.,]\d+)?)\s*\/\s*W\s*(\d+(?:[.,]\d+)?)/i);
          if (!m) return text;
          const raw = /women|girl|female/i.test(genderText) ? m[2] : m[1];
          const n = parseFloat(String(raw).replace(",", "."));
          return Number.isFinite(n) ? String(n) : raw;
        };
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
        if (!r.price) { for (const sel of [".price","[class*='price']","[data-price]","[class*='Price']",".product-price",".sale-price",".current-price"]) { const e = document.querySelector(sel); if (!e) continue; const t = e.textContent?.trim() || ''; if (/\b(installment|interest.?free|klarna|afterpay|sezzle|quadpay|pay in \d|4x |zip\s*pay|split\s*pay|paidy|laybuy)\b/i.test(t)) continue; const pm = t.match(/[\$â‚¬Â£Â¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/); if (pm) { r.price = pm[1].replace(',', '.'); r.source.push("dom"); break; } } }
        if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) {
          const parseWadeMoneyValue = (text) => {
            const m = String(text || '').match(/[\$â‚¬Â£Â¥]?\s*(\d{1,5}(?:[.,]\d{1,2})?)/);
            if (!m) return null;
            const n = parseFloat(m[1].replace(',', '.'));
            return Number.isFinite(n) ? n : null;
          };
          const priceBox = document.querySelector('.t4s-product-price[data-product-price],.t4s-product-price[data-pr-price],.t4s-product-price');
          if (priceBox) {
            const salePrice = parseWadeMoneyValue(priceBox.querySelector('ins .money, ins')?.textContent || '');
            const regularPrice = salePrice != null
              ? salePrice
              : parseWadeMoneyValue((Array.from(priceBox.querySelectorAll('.money')).find((el) => !el.closest('del,ins')) || priceBox.querySelector('.money'))?.textContent || priceBox.textContent || '');
            if (regularPrice != null) {
              r.price = String(regularPrice);
              r.source.push("wade-price");
            }
          }
        }
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
        let wadeRawColor = null;
        if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) {
          for (const row of document.querySelectorAll('.wade-spec-row')) {
            const label = row.querySelector('.wade-spec-label')?.textContent?.trim() || '';
            if (!/^colou?r\s*:?\s*$/i.test(label)) continue;
            const value = row.querySelector('.wade-spec-value')?.textContent?.replace(/\s+/g, ' ').trim() || '';
            const first = value.split('/')[0].trim();
            if (first) {
              wadeRawColor = first;
              r.colorRaw = first;
              r.color = first;
              r.source.push("wade-color");
            }
            break;
          }
          const wadeDescription = document.querySelector('span.metafield-multi_line_text_field')?.textContent?.replace(/\s+/g, ' ').trim() || '';
          if (wadeDescription.length > 20) {
            r.description = wadeDescription.slice(0, 3000);
            r.source.push("wade-description");
          }
          if (!r.sku) {
            const wadeSku = document.querySelector('[data-product__sku-number]')?.textContent?.trim() || '';
            if (wadeSku) { r.sku = wadeSku; r.source.push("wade-sku"); }
          }
          if (!r.brand) { r.brand = "Way of Wade"; r.source.push("wade-default"); }
          r.type = "Basketball";
          r.gender = "Men";
          r.genderDisplay = "Men, Women";
        }
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
        if (!r.gender) { const dsgBc = document.querySelector('nav[aria-label="Breadcrumbs"]'); if (dsgBc) { for (const a of dsgBc.querySelectorAll('a[itemprop="item"]')) { const href = (a.getAttribute('href') || '').toLowerCase(); const em = (a.getAttribute('data-em') || '').toLowerCase(); const combined = href + ' ' + em; if (/\/womens[-/]|womens/.test(combined)) { r.gender = 'Women'; r.source.push('dsg-breadcrumb'); break; } if (/\/mens[-/]|[^o]mens/.test(combined)) { r.gender = 'Men'; r.source.push('dsg-breadcrumb'); break; } if (/\/kids[-/]|\/youth[-/]|\/boys[-/]|\/girls[-/]/.test(combined)) { r.gender = 'Kids'; r.source.push('dsg-breadcrumb'); break; } } } }
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
          // Accordion heading "Details" â†' next sibling content
          const allHds = Array.from(document.querySelectorAll('h2,h3,h4,button,summary'));
          for (const hd of allHds) {
            if (!/^\s*details?\s*$/i.test(hd.textContent?.trim())) continue;
            const content = hd.nextElementSibling || hd.parentElement?.nextElementSibling;
            if (content) { const d = (content.innerText || content.textContent || '').trim(); if (d.length > 20 && d.length < 6000) { r.description = d.slice(0, 3000); break; } }
          }
        }
        if (!r.description) {
          const descSels = ['.product-description','.product__description','.product-single__description','[data-product-description]','.rte','.t4s-rte','.t4s-product-description','.product-info__description','[class*="product-description"]','[class*="product-body"]','[class*="product-text"]','[class*="description"]'];
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
        if (wadeRawColor) { r.colorRaw = wadeRawColor; r.color = wadeRawColor; r.colorFinal = (() => { const spaced = wadeRawColor.replace(/([a-z])([A-Z])/g, "$1 $2"); const words = spaced.toLowerCase().replace(/[^a-z0-9\s]/g, " ").split(/\s+/).filter(Boolean); for (const word of words) { const clean = word.replace(/[^a-z0-9]/g, ""); for (const [base, synonyms] of Object.entries(COLORS)) { if (base === "Multicolor") continue; if (synonyms.some((c) => String(c).toLowerCase().replace(/[^a-z0-9]/g, "") === clean)) return base; } } return "Multicolor"; })(); }
        if (!r.player && /\bd['']?lo\s*1\b/i.test(r.name || "")) r.player = "D'Angelo Russell";
        { const dsgSkuM = location.pathname.match(/\/p\/[^/]+\/([A-Z0-9]{8,20})(?:\/|$|\?)/i); if (dsgSkuM && !r.sku) r.sku = dsgSkuM[1].toUpperCase(); const dsgColorParam = new URLSearchParams(location.search).get('color'); if (dsgColorParam && !r.colorRaw) { r.colorRaw = dsgColorParam.split('/')[0].trim(); r.color = pickColor(r.colorRaw); r.colorFinal = r.color; } }
        if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) { r.sizes = []; r.outOfStock = []; collectWadeUsMSizeButtons(r.sizes, r.outOfStock); } else if (!r.sizes.length) collectWadeUsMSizeButtons(r.sizes, r.outOfStock);
        if (!r.sizes.length && /(^|\.)footlocker\.com\b/i.test(location.hostname)) { document.querySelectorAll('#tabPanel button[class*="SizeSelector"]').forEach(btn => { const t = btn.querySelector('span')?.textContent?.trim() || ''; if (!t || r.sizes.includes(t)) return; r.sizes.push(t); if (btn.classList.contains('SizeSelectorNewDesign-button--disabled') || /sold.?out/i.test(btn.getAttribute('aria-label') || '')) r.outOfStock.push(t); }); }
        if (!r.price && /(^|\.)footlocker\.com\b/i.test(location.hostname)) { const flPEl = document.querySelector('.ProductPrice span[class*="text-sale_red"][aria-hidden="true"]') || document.querySelector('.ProductPrice span[aria-hidden="true"]:not([class*="line-through"])'); if (flPEl) { const m = flPEl.textContent.match(/[\$€£¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/); if (m) { r.price = String(parseFloat(m[1].replace(',', '.'))); r.currency = 'USD'; r.source.push('fl-price-dom'); } } }
        if (!r.sizes.length) { const sel = document.querySelector('select[name*="size" i],select[id*="size" i],select[class*="size" i],select[data-option-name*="size" i]'); if (sel) { Array.from(sel.options).forEach((o) => { const t = normalizeDualGenderSizeLabel(o.text.trim(), r.gender || metaText); if (!t || /^(select|choose|--)/i.test(t)) return; r.sizes.push(t); if (o.disabled || /out.?of.?stock|sold.?out|unavailable/i.test(o.text)) r.outOfStock.push(t); }); } }
        if (!r.sizes.length) { document.querySelectorAll('input[type="radio"][name*="size" i],input[type="radio"][data-option-value-id]').forEach((input) => { const t = normalizeDualGenderSizeLabel((input.value || "").trim(), r.gender || metaText); if (!t || /^(select|choose|--)/i.test(t) || t.length > 20) return; if (!r.sizes.includes(t)) r.sizes.push(t); if (input.classList.contains("disabled") || input.disabled) r.outOfStock.push(t); }); }
        if (!r.sizes.length) { for (const csel of ['[class*="size"] button,[class*="size"] label','[class*="swatch"] button,[class*="swatch"] label','[data-option-name*="size" i] button,[data-option-name*="size" i] label','[class*="size-selector"] button','[class*="sizebtn"]','[class*="size-btn"]','button[aria-label*="/W"],button[aria-label*="M"][aria-label*="/"]']) { const els = document.querySelectorAll(csel); if (els.length > 0 && els.length < 80) { els.forEach((el) => { const rawSize = el.getAttribute("aria-label") || el.textContent?.trim(); const t = normalizeDualGenderSizeLabel(rawSize, r.gender || metaText); if (!t || t.length > 20 || r.sizes.includes(t)) return; r.sizes.push(t); const cls = ((el.className || "") + " " + (el.getAttribute("aria-label") || "")).toLowerCase(); const oos = el.classList.contains("disabled") || el.disabled || el.getAttribute("aria-disabled") === "true" || /disabled|out.?of.?stock|sold.?out|unavailable|not-available/i.test(cls) || window.getComputedStyle(el).textDecoration.includes("line-through"); if (oos) r.outOfStock.push(t); }); if (r.sizes.length) break; } } }
        if (!r.sizes.length) { const pa = document.querySelector('main,[role="main"]') || document.body; pa.querySelectorAll('button').forEach((el) => { const t = (el.textContent || "").replace(",", ".").replace(/\s+/g, " ").trim(); const size = t.replace(/^(?:EU|EUR)\s*/i, "").trim(); if (!t || t.length > 12 || r.sizes.includes(size)) return; if (!/^(?:EU|EUR|US|UK|CM)?\s*\d+(?:\.\d+)?(?:\s+(?:1\/2|[12]\/3))?(?:\s*(?:US|EU|UK|CM|EUR|Â½))?$/i.test(t)) return; r.sizes.push(size); const oos = el.classList.contains("disabled") || el.disabled || el.getAttribute("aria-disabled") === "true" || window.getComputedStyle(el).textDecoration.includes("line-through"); if (oos) r.outOfStock.push(size); }); }
        if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) {
          const wadeRoot = document.querySelector('.t4s-product__media-wrapper,[data-product-single-media-group]');
          const addWadeImage = (raw) => {
            let src = String(raw || '').trim().replace(/&amp;/g, '&');
            if (!src || src.startsWith('data:')) return;
            if (src.startsWith('//')) src = 'https:' + src;
            if (!/^https?:\/\//i.test(src)) return;
            src = src.replace(/([?&])width=\d+(&?)/i, (m, p1, p2) => p2 ? p1 : '').replace(/[?&]$/, '');
            if (!/\/cdn\/shop\/files\//i.test(src)) return;
            if (!r.images.includes(src)) r.images.push(src);
          };
          if (wadeRoot) {
            wadeRoot.querySelectorAll('[data-main-slide] img[data-master], img[data-master]').forEach((img) => addWadeImage(img.getAttribute('data-master')));
            if (!r.images.length) {
              wadeRoot.querySelectorAll('[data-main-slide] img[data-srcset], [data-main-slide] img[srcset]').forEach((img) => {
                const ss = img.getAttribute('data-srcset') || img.getAttribute('srcset') || '';
                const best = ss.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 });
                addWadeImage(best.u);
              });
            }
          }
        }
        const skipGenericImages = (/(^|\.)wayofwade\.com\b/i.test(location.hostname) || /(^|\.)dickssportinggoods\.com\b/i.test(location.hostname)) && r.images.length > 0;
        const swatchEls = new Set();
        document.querySelectorAll('[class*="ColorChip"],[class*="color-chip"],[class*="colorway"],[class*="ColorSwatch"],[class*="color-swatch"],[class*="StyleChip"],[class*="style-chip"],[class*="VariantImage"],[class*="variant-color"],[class*="SwatchImage"],[class*="color-option"],[class*="product-variants"],[class*="ProductVariants"]').forEach(el => swatchEls.add(el));
        const isInSwatch = (img) => { let el = img.parentElement; while (el && el !== document.body) { if (swatchEls.has(el)) return true; const cls = el.className || ''; if (/color.?chip|color.?swatch|colorway|style.?chip|swatch.?img|variant.?img|color.?option/i.test(cls)) return true; el = el.parentElement; } return false; };
        const imgRoot = document.querySelector('.ProductGallery,[class*="ProductGallery"],[class*="product-gallery"],[class*="pdp-gallery"],[class*="Gallery--pdp"],[class*="GallerySlider"],[class*="gallery-slider"],.product-media,.pdp-product-images') || document.querySelector('main,[role="main"]') || document.body;
        const seenImgs = new Set();
        if (!skipGenericImages) imgRoot.querySelectorAll('img').forEach((img) => {
          if (isInSwatch(img)) return;
          let src = '';
          if (img.srcset) { const best = img.srcset.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 }); if (best.u) src = best.u; }
          if (!src) src = img.currentSrc || img.src || '';
          if (src && src.startsWith('//')) src = 'https:' + src;
          if (!src || !/^https?:\/\//.test(src)) { const lzRaw = img.getAttribute('data-src') || img.getAttribute('data-lazy-src') || img.getAttribute('data-original') || img.getAttribute('data-lazy') || img.getAttribute('data-master') || ''; const lz = lzRaw.startsWith('//') ? 'https:' + lzRaw : lzRaw; if (/^https?:\/\//.test(lz)) src = lz; else if (img.dataset && img.dataset.srcset) { const ds = img.dataset.srcset; const dsBest = ds.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 }); if (dsBest.u) src = dsBest.u.startsWith('//') ? 'https:' + dsBest.u : dsBest.u; } }
          if (!src || !/^https?:\/\//.test(src)) return;
          const w = img.naturalWidth || parseInt(img.getAttribute('width') || '0');
          const h = img.naturalHeight || parseInt(img.getAttribute('height') || '0');
          if ((w > 0 && w < 200) || (h > 0 && h < 200)) return;
          if (/logo|icon|badge|star|rating|avatar|social|sprite|pixel|tracking|placeholder|blank/i.test(src)) return;
          if (seenImgs.has(src)) return;
          seenImgs.add(src);
          r.images.push(src);
        });
        if (/(^|\.)dickssportinggoods\.com\b/i.test(location.hostname) && !r.images.length) {
          const seenDsgImgs = new Set();
          document.querySelectorAll('#pdp-upper-left img[itemprop="image"], pdp-carousel img[itemprop="image"]').forEach(img => {
            const ss = img.getAttribute('srcset') || '';
            let src = '';
            if (ss) { const best = ss.split(',').map(s => { const p = s.trim().split(/\s+/); return { u: p[0] || '', w: parseInt(p[1]) || 0 }; }).reduce((a, b) => b.w > a.w ? b : a, { u: '', w: 0 }); if (best.u) src = best.u; }
            if (!src) src = img.src || '';
            const baseMatch = src.match(/^(https:\/\/dks\.scene7\.com\/is\/image\/[^?]+)/);
            if (!baseMatch || seenDsgImgs.has(baseMatch[1])) return;
            seenDsgImgs.add(baseMatch[1]);
            r.images.push(baseMatch[1] + '?qlt=85&wid=1200&hei=1200&fmt=pjpeg&fit=constrain');
          });
        }
        r.images = r.images.slice(0, /(^|\.)wayofwade\.com\b/i.test(location.hostname) ? 24 : 8);
        if (/(^|\.)dickssportinggoods\.com\b/i.test(location.hostname)) { r.gender = r.type === 'Football' ? 'Both' : null; }
        if (r.price != null) { const raw = parseFloat(String(r.price).replace(',', '.')); if (!isNaN(raw)) r.price = String(Math.round(raw + priceAdjustment)); }
        return r;
      }

      function extractLiveData(sels) {
        const roots = [];
        if (sels && sels.length) { for (const s of sels) { try { const e = document.querySelector(s); if (e) roots.push(e); } catch (_) {} } }
        if (!roots.length) roots.push(document.body);
        const pageGenderText = Array.from(document.querySelectorAll('nav[aria-label*="breadcrumb" i] a,nav[aria-label*="breadcrumb" i] span,[class*="breadcrumb"] a,[class*="breadcrumb"] span,h1')).map((el) => el.textContent || "").join(" ");
        const isWayOfWadePage = /(^|\.)wayofwade\.com\b/i.test(location.hostname);
        const normalizeWadeUsMSize = (value) => { const n = Number(String(value || "").replace(",", ".")); return Number.isFinite(n) && n > 0 && n <= 30 ? String(n) : ""; };
        const extractWadeUsMLiveSizeLabel = (label) => {
          const text = nt(label).replace(/,/g, ".");
          const usM = text.match(/\bUS-?M\s*([0-9]+(?:\.\d+)?)/i) || text.match(/^([0-9]+(?:\.\d+)?)$/);
          return usM ? normalizeWadeUsMSize(usM[1]) : "";
        };
        const collectWadeUsMSizeButtons = (available, unavailable) => {
          if (!isWayOfWadePage) return;
          const panel = document.querySelector('[data-size-panel="US-M"]');
          if (!panel) return;
          const seen = new Set();
          panel.querySelectorAll('[data-size-btn][data-size-value]').forEach((btn) => {
            const usM = normalizeWadeUsMSize(btn.getAttribute('data-size-value') || '');
            if (!usM) return;
            const key = btn.getAttribute('data-variant-id') || usM.toLowerCase();
            if (seen.has(key)) return;
            seen.add(key);
            const oos = btn.getAttribute('data-available') === 'false' || btn.disabled || btn.getAttribute('aria-disabled') === 'true' || /disabled|out.?of.?stock|sold.?out|unavailable|not-available/i.test(btn.className || '');
            const list = oos ? unavailable : available;
            if (!list.includes(usM)) list.push(usM);
          });
        };
        const normalizeLiveSizeLabel = (label) => {
          let text = nt(label);
          text = text.replace(/^Size:\s*/i, "").replace(/\s*[-â€"]\s*Sold\s+Out/i, "").trim();
          if (isWayOfWadePage) return extractWadeUsMLiveSizeLabel(text);
          const m = text.match(/\bM\s*(\d+(?:[.,]\d+)?)\s*\/\s*W\s*(\d+(?:[.,]\d+)?)/i);
          if (!m) return text;
          const raw = /women|girl|female/i.test(pageGenderText) && !/men'?s/i.test(pageGenderText) ? m[2] : m[1];
          const n = parseFloat(String(raw).replace(",", "."));
          return Number.isFinite(n) ? String(n) : raw;
        };
        const parseMoneyValue = (text) => {
          const m = String(text || '').match(/[\$â‚¬Â£Â¥]?\s*(\d{1,5}(?:[.,]\d{1,2})?)/);
          if (!m) return null;
          const n = parseFloat(m[1].replace(',', '.'));
          return Number.isFinite(n) ? n : null;
        };
        const isDsgPage = /(^|\.)dickssportinggoods\.com\b/i.test(location.hostname);
        let wadePrice = null;
        let wadeCompareAt = null;
        if (/(^|\.)wayofwade\.com\b/i.test(location.hostname)) {
          const priceBox = document.querySelector('.t4s-product-price[data-product-price],.t4s-product-price[data-pr-price],.t4s-product-price');
          if (priceBox) {
            const salePrice = parseMoneyValue(priceBox.querySelector('ins .money, ins')?.textContent || '');
            const comparePrice = parseMoneyValue(priceBox.querySelector('del .money, del')?.textContent || '');
            if (salePrice != null) {
              wadePrice = salePrice;
              if (comparePrice != null && comparePrice > salePrice) wadeCompareAt = comparePrice;
            } else {
              const regularEl = Array.from(priceBox.querySelectorAll('.money')).find((el) => !el.closest('del,ins')) || priceBox.querySelector('.money');
              const regularPrice = parseMoneyValue(regularEl?.textContent || priceBox.textContent || '');
              if (regularPrice != null) wadePrice = regularPrice;
            }
          }
        }
        let dsgPrice = null;
        if (isDsgPage) {
          dsgPrice = parseMoneyValue(
            document.querySelector('meta[itemprop="price"]')?.getAttribute('content') ||
            document.querySelector('#offer-price .product-price, hmf-price .product-price, pdp-price .product-price')?.textContent ||
            ''
          );
        }
        const rawNums = new Set();
        if (wadePrice == null) for (const root of roots) {
          const bnpl = /\b(installment|interest.?free|klarna|afterpay|sezzle|quadpay|pay in \d|4x |zip\s*pay|split\s*pay|paidy|laybuy)\b/i;
          const addPrices = (txt) => { if (bnpl.test(txt)) return; for (const m of txt.matchAll(/[\$â‚¬Â£Â¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/g)) { const n = parseFloat(m[1].replace(",", ".")); if (n >= 1 && n < 100000) rawNums.add(n); } };
          root.querySelectorAll("[class*='price'],[data-price],[class*='Price'],[data-test*='price'],[class*='final-price'],[class*='sale-price'],[class*='current-price'],[class*='product-price'],ins,del,.was-price,.compare-at,.original-price").forEach((el) => addPrices(el.textContent || ""));
          if (!rawNums.size) { root.querySelectorAll("span,b,strong,div,p").forEach((el) => { if (el.children.length > 3) return; const txt = (el.textContent || "").trim(); if (txt.length > 50) return; addPrices(txt); }); }
        }
        const sorted = [...rawNums].sort((a, b) => a - b);
        const r5 = Math.round;
        const mk = priceAdjustment;
        const price = wadePrice != null ? wadePrice : (dsgPrice != null ? dsgPrice : (sorted.length ? sorted[0] : null));
        const cmp = dsgPrice != null ? null : (wadePrice != null ? wadeCompareAt : (sorted.length >= 2 ? sorted[sorted.length - 1] : null));
        const inStock = [], outOfStock = [];
        collectWadeUsMSizeButtons(inStock, outOfStock);
        if (isDsgPage) {
          document.querySelectorAll('.hmf-selectable-container button[aria-label], .selector-attribute-outer button[aria-label], common-product-availability button[aria-label]').forEach((btn) => {
            const raw = btn.getAttribute('aria-label') || btn.textContent || '';
            const mw = raw.match(/\bM\s*(\d+(?:[.,]\d+)?)\s*\/\s*W\s*\d+(?:[.,]\d+)?/i);
            const t = mw ? String(parseFloat(mw[1].replace(',', '.'))) : normalizeLiveSizeLabel(raw);
            if (!t || t === 'NaN' || t.length > 15 || inStock.includes(t) || outOfStock.includes(t)) return;
            const oos = btn.disabled || btn.getAttribute('aria-disabled') === 'true' || btn.classList.contains('hmf-selectable-unavailable');
            (oos ? outOfStock : inStock).push(t);
          });
        }
        for (const root of roots) {
          root.querySelectorAll('select[name*="size" i],select[id*="size" i],select[class*="size" i],select[data-option-name*="size" i]').forEach((sel) => {
            Array.from(sel.options).forEach((o) => {
              const t = normalizeLiveSizeLabel(o.text.trim());
              if (!t || /^(select|choose|--)/i.test(t)) return;
              const list = (o.disabled || /out.?of.?stock|sold.?out|unavailable/i.test(o.text)) ? outOfStock : inStock;
              if (!list.includes(t)) list.push(t);
            });
          });
          root.querySelectorAll('input[type="radio"][name*="size" i],input[type="radio"][data-option-value-id]').forEach((input) => {
            const t = normalizeLiveSizeLabel((input.value || "").trim());
            if (!t || /^(select|choose|--)/i.test(t) || t.length > 20) return;
            const list = (input.classList.contains("disabled") || input.disabled) ? outOfStock : inStock;
            if (!list.includes(t)) list.push(t);
          });
          if (!inStock.length && !outOfStock.length) {
            for (const csel of ['[class*="size"] button,[class*="size"] label','[class*="swatch"] button,[class*="swatch"] label','[data-option-name*="size" i] button,[data-option-name*="size" i] label','button[aria-label*="/W"],button[aria-label*="M"][aria-label*="/"]']) {
              const els = root.querySelectorAll(csel);
              if (!els.length || els.length > 60) continue;
              els.forEach((el) => {
                const t = normalizeLiveSizeLabel(el.getAttribute("aria-label") || el.textContent?.trim());
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
        if (!inStock.length && !outOfStock.length && /(^|\.)wayofwade\.com\b/i.test(location.hostname)) { const fullPanel = document.querySelector('[data-size-panel]'); if (fullPanel) { fullPanel.querySelectorAll('[data-size-btn]').forEach(btn => { const usM = extractWadeUsMLiveSizeLabel(btn.getAttribute('data-size-value') || btn.getAttribute('aria-label') || btn.textContent || ''); if (!usM) return; const oos = btn.getAttribute('data-available') === 'false'; const list = oos ? outOfStock : inStock; if (!list.includes(usM)) list.push(usM); }); } }
        { const dsgPrice = document.querySelector('hmf-price, [class*="our-price"]'); if (dsgPrice && !rawNums.size) { const bnpl2 = /\b(klarna|afterpay|sezzle|quadpay|zip\s*pay|pay in \d)\b/i; const txt = dsgPrice.textContent || ''; if (!bnpl2.test(txt)) { for (const m of txt.matchAll(/[\$â‚¬Â£Â¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/g)) { const n = parseFloat(m[1].replace(',', '.')); if (n >= 1 && n < 100000) rawNums.add(n); } } } }
        { if (!inStock.length && !outOfStock.length) { const dsgBtns = document.querySelectorAll('common-product-availability button, hmf-size-selector button, [class*="size-selector"] button, [class*="sizeTile"] button, [class*="size-tile"] button'); if (dsgBtns.length && dsgBtns.length < 60) { dsgBtns.forEach(btn => { const t = normalizeLiveSizeLabel(btn.textContent?.trim() || ''); if (!t || t.length > 10) return; if (inStock.includes(t) || outOfStock.includes(t)) return; const oos = btn.disabled || btn.getAttribute('aria-disabled') === 'true' || /\b(disabled|oos|out-of-stock|unavailable)\b/i.test(btn.className || ''); (oos ? outOfStock : inStock).push(t); }); } } }
        { if (!inStock.length && !outOfStock.length) { const isDsgPage = /(^|\.)dickssportinggoods\.com\b/i.test(location.hostname); if (isDsgPage) { document.querySelectorAll('button').forEach(btn => { const t = (btn.textContent || '').replace(',', '.').replace(/\s+/g, ' ').trim(); if (!t || t.length > 12) return; if (!/^(?:EU|EUR|US|UK|CM)?\s*\d+(?:\.\d+)?(?:\s+(?:1\/2|[12]\/3))?(?:\s*(?:US|EU|UK|CM|Â½))?$/i.test(t)) return; const size = t.replace(/^(?:EU|EUR)\s*/i, '').trim(); if (inStock.includes(size) || outOfStock.includes(size)) return; const oos = btn.disabled || btn.getAttribute('aria-disabled') === 'true' || /\b(disabled|oos|out-of-stock)\b/i.test(btn.className || ''); (oos ? outOfStock : inStock).push(size); }); } } }
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

        // 2. DSG/FL: wait for h1 â€" both Angular SSR and Next.js SSR include the
        // product title in the initial HTML, so a non-empty h1 means the page
        // shell is ready. Never wait for React/Angular client components here
        // because hidden tabs get throttled and they may never fully hydrate.
        const isDSG = /(^|\.)dickssportinggoods\.com\b/i.test(location.hostname);
        const isFL = /(^|\.)footlocker\.com\b/i.test(location.hostname);
        if (isDSG || isFL) {
          const h1 = document.querySelector('h1');
          if (!h1 || !h1.textContent?.trim()) return false;
        }

        const flImgs = captureFullPage ? Array.from(document.querySelectorAll('img[data-id="ProductImage"]')) : [];
        if (captureFullPage) {
          const hasFLLayout = !!document.querySelector('[class*="ProductGallery"],[class*="ProductDetails-form"],[class*="ProductDetails-tab"]');
          if (flImgs.length > 0) {
            const ready = flImgs.some(img => /^https?:\/\//.test(img.getAttribute('src') || ''));
            if (!ready) return false; // images in DOM but src not set yet â€" keep waiting
          } else {
            // No FL images yet â€" if page has FL product indicators, keep waiting
            if (hasFLLayout) return false;
            // Non-FL page: fall through and capture whatever is there
          }
          if (hasFLLayout) {
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

        // 4. DSG checks are only useful once live sizes hydrate. Price-only captures
        // look successful here but become monitor errors later, so keep waiting.
        if (isDSG && (liveData.inStock?.length || 0) + (liveData.outOfStock?.length || 0) === 0) return false;

        // 5. If FL images were in the DOM but extractProduct got none, keep waiting
        if (captureFullPage && flImgs.length > 0 && !fullPage?.productData?.images?.length) return false;

        if (!selectors || !selectors.length) {
          const pageText = fullPage ? fullPage.text : buildFullText();
          finish({ ok: true, summary: pageText, html: pageText, matched: "full-page", fullPage, liveData });
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

      // Heartbeat every 100 ms â€" needed for the case where FL images are already
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
  let interval = 25;
  while (Date.now() < deadline) {
    if (!captureTabIds.has(tabId)) return null;
    await sleep(interval);
    interval = Math.min(interval * 1.5, 250);
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

async function forceDsgCaptureFromTab(tabId, monitor, selectors, captureFullPage, priceAdjustment = 80) {
  try {
    const [{ result }] = await chrome.scripting.executeScript({
      target: { tabId },
      func: (selectors, monitorName, captureFullPage, priceAdjustment) => {
        const nt = (v) => (v ?? "").replace(/\s+/g, " ").replace(/\u00a0/g, " ").trim();
        const isDsg = /(^|\.)dickssportinggoods\.com\b/i.test(location.hostname);
        if (!isDsg) return null;

        function buildFullText() {
          const parts = [];
          const visible = nt(document.body?.innerText || "");
          if (visible) parts.push("=== PAGE TEXT ===\n" + visible);
          const metas = [];
          document.querySelectorAll("meta[name], meta[property]").forEach((m) => {
            const k = m.getAttribute("name") || m.getAttribute("property") || "";
            const v = m.getAttribute("content") || "";
            if (k && v && v.length < 600) metas.push(k + ": " + v);
          });
          if (metas.length) parts.push("=== META TAGS ===\n" + metas.join("\n"));
          return parts.join("\n\n");
        }

        const meta = (selector) => nt(document.querySelector(selector)?.getAttribute("content") || "");
        const bodyText = document.body?.textContent || "";
        const brandAliases = [
          ["Nike Sportswear", "Nike"],
          ["Nike SB", "Nike"],
          ["Air Jordan", "Jordan"],
          ["Jordan Brand", "Jordan"],
          ["Jordan", "Jordan"],
          ["adidas Originals", "Adidas"],
          ["adidas Sportswear", "Adidas"],
          ["adidas", "Adidas"],
          ["New Balance", "New Balance"],
          ["NewBalance", "New Balance"],
          ["Under Armour", "Under Armour"],
          ["Under Armor", "Under Armour"],
          ["Hoka One One", "Hoka"],
          ["Hoka", "Hoka"],
          ["On Running", "ON Cloud"],
          ["On Cloud Running", "ON Cloud"],
          ["On Cloud", "ON Cloud"],
          ["Reebok Classic", "Reebok"],
          ["Reebok", "Reebok"],
          ["Puma", "Puma"],
          ["Asics Tiger", "Asics"],
          ["Asics", "Asics"],
          ["Converse All Star", "Converse"],
          ["Converse CONS", "Converse"],
          ["Converse", "Converse"],
          ["Vans", "Vans"],
          ["Saucony", "Saucony"],
          ["Brooks Running", "Brooks"],
          ["Brooks", "Brooks"],
          ["Salomon Sportstyle", "Salomon"],
          ["Salomon", "Salomon"],
          ["Timberland Pro", "Timberland"],
          ["Timberland", "Timberland"],
          ["The North Face", "The North Face"],
          ["North Face", "The North Face"],
          ["UGG Australia", "UGG"],
          ["UGG", "UGG"],
          ["Dr. Martens", "Dr. Martens"],
          ["Dr Martens", "Dr. Martens"],
          ["Doc Martens", "Dr. Martens"],
          ["Birkenstock", "Birkenstock"],
          ["Clarks Originals", "Clarks"],
          ["Clarks", "Clarks"],
          ["Columbia Sportswear", "Columbia"],
          ["Columbia", "Columbia"],
          ["Yeezy", "Yeezy"],
          ["Fila", "Fila"],
          ["Skechers", "Skechers"],
          ["Skecher", "Skechers"],
          ["Crocs", "Crocs"],
          ["Merrell", "Merrell"],
          ["Keen", "Keen"],
          ["Teva", "Teva"]
        ].sort((a, b) => b[0].length - a[0].length);
        const inferBrandFromText = (value) => {
          const text = nt(value).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
          if (!text) return null;
          for (const [alias, canonical] of brandAliases) {
            const needle = alias.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
            if (!needle) continue;
            const re = new RegExp(`(^|\\s)${needle.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(\\s|$)`, "i");
            if (re.test(text)) return canonical;
          }
          return null;
        };
        const productData = {
          name: nt(document.querySelector("h1")?.textContent || "") ||
            meta('meta[property="og:title"]') ||
            meta('meta[name="twitter:title"]') ||
            nt(monitorName || document.title || ""),
          brand: null,
          type: null,
          color: null,
          colorRaw: null,
          colorFinal: null,
          gender: null,
          price: null,
          currency: null,
          sku: null,
          description: meta('meta[name="description"]') || meta('meta[property="og:description"]') || null,
          images: [],
          sizes: [],
          outOfStock: [],
          source: ["dsg-forced-capture"]
        };
        productData.brand = inferBrandFromText(productData.name) ||
          inferBrandFromText(meta('meta[property="og:title"]')) ||
          inferBrandFromText(document.title);
        if (productData.brand) productData.source.push("dsg-title-brand");

        const pathSku = location.pathname.match(/\/p\/[^/]+\/([A-Z0-9]{8,20})(?:\/|$|\?)/i);
        if (pathSku) productData.sku = pathSku[1].toUpperCase();
        if (!productData.sku) {
          const textSku = bodyText.match(/\b(?:SKU|Style\s*(?:No\.?|Code|#)|Product\s*#|Item\s*(?:No\.?|Code|#))\s*[:#]?\s*([A-Z0-9]{6,20}(?:-[A-Z0-9]{1,8}){0,3})/i);
          if (textSku) productData.sku = textSku[1].toUpperCase();
        }

        const colorParam = new URLSearchParams(location.search).get("color");
        if (colorParam) {
          productData.colorRaw = colorParam.split("/")[0].trim();
          productData.color = productData.colorRaw;
          productData.colorFinal = productData.colorRaw;
        }

        const priceMeta = document.querySelector('meta[itemprop="price"]')?.getAttribute("content") || "";
        const priceText = priceMeta || nt(Array.from(document.querySelectorAll('#offer-price .product-price, hmf-price .product-price, pdp-price .product-price, hmf-price, [class*="our-price"], [class*="final-price"], [class*="current-price"], [data-test*="price" i]'))
          .map((el) => el.textContent || "")
          .join(" "));
        const priceMatch = priceMeta ? ["", priceMeta] : priceText.match(/[$â‚¬Â£Â¥]\s*(\d{1,5}(?:[.,]\d{1,2})?)/);
        if (priceMatch) {
          const raw = parseFloat(String(priceMatch[1]).replace(",", "."));
          if (Number.isFinite(raw)) productData.price = String(Math.round(raw + priceAdjustment));
        }

        const genderText = Array.from(document.querySelectorAll('nav[aria-label*="breadcrumb" i] a, nav[aria-label*="breadcrumb" i] span, [class*="breadcrumb"] a, [class*="breadcrumb"] span'))
          .map((el) => el.textContent || el.getAttribute("href") || "")
          .join(" ");
        if (/women/i.test(genderText)) productData.gender = "Women";
        else if (/\bmen/i.test(genderText)) productData.gender = "Men";
        else if (/kids|youth|boys|girls/i.test(genderText)) productData.gender = "Kids";

        const normalizeSize = (value) => {
          const t = nt(value).replace(',', '.');
          if (!t) return "";
          const cleaned = t
            .replace(/^size\s*[:#-]?\s*/i, "")
            .replace(/\s*[-–]\s*(?:sold\s*out|out\s*of\s*stock|unavailable).*$/i, "")
            .replace(/\b(?:sold\s*out|out\s*of\s*stock|unavailable)\b/gi, "")
            .replace(/\s+/g, " ")
            .trim();
          const dual = cleaned.match(/\bM\s*(\d+(?:\.\d+)?)\s*\/\s*W\s*(\d+(?:\.\d+)?)/i);
          if (dual) {
            const raw = /women|girl|female/i.test(genderText) && !/men'?s/i.test(genderText) ? dual[2] : dual[1];
            const n = Number(raw);
            return Number.isFinite(n) ? String(n) : raw;
          }
          if (cleaned && cleaned.length <= 16 && /^(?:EU|EUR|US|UK|CM)?\s*\d+(?:\.\d+)?(?:\s+(?:1\/2|[12]\/3))?(?:\s*(?:US|EU|UK|CM|½))?$/i.test(cleaned)) {
            return cleaned.replace(/^(?:EU|EUR)\s*/i, "").replace(/\s*(?:US|EU|UK|CM)$/i, "").trim();
          }
          const m = cleaned.match(/(?:^|\b)(?:size\s*)?(\d+(?:\.\d+)?(?:\s+(?:1\/2|[12]\/3))?)(?:\b|$)/i);
          return m ? m[1].trim() : "";
        };
        const addDsgSize = (raw, isOos = false) => {
          const size = normalizeSize(raw);
          if (!size || productData.sizes.includes(size)) return false;
          productData.sizes.push(size);
          if (isOos && !productData.outOfStock.includes(size)) productData.outOfStock.push(size);
          return true;
        };

        // 1. JSON-LD offers — most reliable OOS source for DSG (schema.org availability)
        document.querySelectorAll('script[type="application/ld+json"]').forEach((script) => {
          try {
            const data = JSON.parse(script.textContent);
            const items = Array.isArray(data) ? data : [data];
            for (const item of items) {
              const tp = item["@type"];
              if (tp !== "Product" && !(Array.isArray(tp) && tp.includes("Product"))) continue;
              const rawOffers = item.offers ? (Array.isArray(item.offers) ? item.offers : [item.offers]) : [];
              // Unwrap AggregateOffer — DSG often nests per-size Offer objects inside it
              const offers = [];
              for (const o of rawOffers) {
                if (o.offers) {
                  const nested = Array.isArray(o.offers) ? o.offers : [o.offers];
                  offers.push(...nested);
                } else {
                  offers.push(o);
                }
              }
              offers.forEach((o) => {
                const label = nt(o.name || o.description || "");
                const av = String(o.availability || "").toLowerCase();
                const isOos = av && !av.includes("instock") && !av.includes("onlineonly") && !av.includes("limitedavailability") && !av.includes("preorder");
                addDsgSize(label, isOos);
              });
            }
          } catch (_) {}
        });

        // 2. Size buttons â€" fallback if JSON-LD had no offers, also catches real-time DOM state
        document.querySelectorAll('.selector-attribute-outer button, hmf-selectable button, common-product-availability button, hmf-size-selector button, [class*="size-selector" i] button, [class*="sizeTile"] button, [class*="size-tile"] button').forEach((btn) => {
          const raw = btn.getAttribute("aria-label") || btn.getAttribute("data-size") || btn.getAttribute("data-size-value") || btn.getAttribute("data-testid") || btn.textContent || "";
          const cls = ((btn.className || "") + " " + (btn.getAttribute("aria-label") || "") + " " + (btn.getAttribute("data-testid") || "")).toLowerCase();
          const oos = btn.disabled || btn.getAttribute("aria-disabled") === "true" || /\b(disabled|oos|out.?of.?stock|unavailable|sold.?out)\b/i.test(cls);
          addDsgSize(raw, oos);
        });
        if (!productData.sizes.length) {
          document.querySelectorAll('[aria-label],[data-size],[data-size-value],[data-testid*="size" i],[class*="size" i]').forEach((el) => {
            const raw = el.getAttribute("aria-label") || el.getAttribute("data-size") || el.getAttribute("data-size-value") || el.textContent || "";
            if (!raw || raw.length > 80) return;
            const cls = ((el.className || "") + " " + (el.getAttribute("aria-label") || "") + " " + (el.getAttribute("data-testid") || "")).toLowerCase();
            const oos = el.getAttribute("aria-disabled") === "true" || /\b(disabled|oos|out.?of.?stock|unavailable|sold.?out)\b/i.test(cls);
            addDsgSize(raw, oos);
          });
        }
        if (!productData.sizes.length) {
          for (const script of document.querySelectorAll('script')) {
            const text = script.textContent || "";
            if (!/size|Size|shoeSize|availability|variants/i.test(text) || text.length > 800000) continue;
            const re = /(?:"(?:size|Size|shoeSize|displaySize|label|name)"\s*:\s*"([^"]{1,40})")/g;
            let match;
            let guard = 0;
            while ((match = re.exec(text)) && guard++ < 300) addDsgSize(match[1], false);
            if (productData.sizes.length) break;
          }
        }

        const seen = new Set();
        const imageRoot = document.querySelector("main,[role='main']") || document.body;
        imageRoot.querySelectorAll("img").forEach((img) => {
          let src = img.currentSrc || img.src || "";
          if (!src && img.srcset) src = img.srcset.split(",").map((part) => part.trim().split(/\s+/)[0]).filter(Boolean).pop() || "";
          if (!/^https?:\/\//.test(src) || seen.has(src)) return;
          if (/logo|icon|badge|star|rating|sprite|pixel|placeholder|blank/i.test(src)) return;
          const w = img.naturalWidth || Number(img.getAttribute("width") || 0);
          const h = img.naturalHeight || Number(img.getAttribute("height") || 0);
          if ((w && w < 120) || (h && h < 120)) return;
          seen.add(src);
          productData.images.push(src);
        });
        productData.images = productData.images.slice(0, 8);

        const hasProductSignal = productData.sku || productData.sizes.length || /\/p\//i.test(location.pathname);
        if (!hasProductSignal) return null;

        const fullText = buildFullText();
        const liveData = {
          price: productData.price ? Number(productData.price) : null,
          compareAt: null,
          inStock: productData.sizes.filter((size) => !productData.outOfStock.includes(size)),
          outOfStock: productData.outOfStock
        };

        if (!selectors || !selectors.length) {
          const fullPage = captureFullPage ? { text: fullText, productData } : null;
          return { ok: true, summary: fullText, html: fullText, matched: "dsg-forced-full-page", fullPage, liveData };
        }

        const htmlParts = [], summaryParts = [];
        for (const selector of selectors) {
          try {
            const element = document.querySelector(selector);
            if (!element) continue;
            htmlParts.push(element.outerHTML || "");
            summaryParts.push(nt(element.innerText || element.textContent || ""));
          } catch (_) {}
        }
        if (!htmlParts.length) {
          const fullPage = captureFullPage ? { text: fullText, productData } : null;
          return { ok: true, summary: fullText, html: fullText, matched: "dsg-forced-full-page", fullPage, liveData };
        }
        return {
          ok: true,
          summary: summaryParts.join("\n\n"),
          html: htmlParts.join("\n"),
          matched: "dsg-forced-selectors",
          fullPage: captureFullPage ? { text: fullText, productData } : null,
          liveData
        };
      },
      args: [selectors, monitor.name, captureFullPage, priceAdjustment]
    });
    return result?.ok ? result : null;
  } catch (_) {
    return null;
  }
}

async function captureSnapshotInHiddenTab(monitor, captureFullPage = false, _retryCount = 0, isBatch = false, visibleTab = false) {
  const stoppedResult = { ok: false, stopped: true };
  let captureHostname = "";
  try {
    captureHostname = new URL(monitor.url || "").hostname || "";
  } catch (_) {}
  const captureCookieDomain = getCookieDomainForUrl(monitor.url || "");
  const isDsgCapture = /(^|\.)dickssportinggoods\.com\b/i.test(captureHostname);
  const isWowCapture = !isDsgCapture && /(^|\.)wayofwade\.com\b/i.test(captureHostname);
  const isRetailLimitedCapture = !isDsgCapture && !isWowCapture && /(^|\.)footlocker\.com\b/i.test(captureHostname);

  await acquireSlot(isBatch, isRetailLimitedCapture, isDsgCapture, isWowCapture);
  if (stopChecksFlag) { releaseSlot(isBatch, isRetailLimitedCapture, isDsgCapture, isWowCapture); return { ok: false, stopped: true }; }

  let selectors = Array.isArray(monitor.selectors)
    ? monitor.selectors
    : (monitor.selector ? [monitor.selector] : []);
  // FL: bypass stored selectors and use full-page mode like DSG for fast live checks
  if (/(^|\.)footlocker\.com\b/i.test(captureHostname)) selectors = [];

  let tab = null;
  try {
    tab = isDsgCapture
      ? await openDsgTab(monitor.url, visibleTab)
      : isWowCapture
        ? await openWowTab(monitor.url, visibleTab)
      : await chrome.tabs.create({ url: monitor.url, active: !!visibleTab });
  } catch (tabErr) {
    releaseSlot(isBatch, isRetailLimitedCapture, isDsgCapture, isWowCapture);
    if (stopChecksFlag || /stopped/i.test(tabErr?.message || "")) return stoppedResult;
    if (_retryCount < 2) return captureSnapshotInHiddenTab(monitor, captureFullPage, _retryCount + 1, isBatch, visibleTab);
    return { ok: false, error: tabErr.message || "Failed to open tab" };
  }
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
        if (stopChecksFlag) return stoppedResult;
        return earlyResult;
      }
    }

    // Step 1: wait for initial load
    await waitForTabLoad(tab.id);
    if (!captureTabIds.has(tab.id)) {
      if (stopChecksFlag) return stoppedResult;
      if (_retryCount < 2) return captureSnapshotInHiddenTab(monitor, captureFullPage, _retryCount + 1, isBatch, visibleTab);
      return { ok: false, error: "Tab closed unexpectedly" };
    }

    if (isDsgCapture) {
      if (!await waitWhileTabOpen(tab.id, 200)) {
        if (stopChecksFlag) return stoppedResult;
        if (_retryCount < 2) return captureSnapshotInHiddenTab(monitor, captureFullPage, _retryCount + 1, isBatch, visibleTab);
        return { ok: false, error: "Tab closed unexpectedly" };
      }
      // Poll forceDsgCaptureFromTab every 200ms up to 60s total.
      // At the 30s midpoint, reload the tab once if still no sizes — handles
      // soft blocks and redirects where the first load returns a bad page.
      const dsgPriceAdj = Number(monitor.priceAdjustment) || 80;
      const dsgStart = Date.now();
      const dsgDeadline = dsgStart + 60000;
      const dsgReloadAt = dsgStart + 30000;
      let dsgReloaded = false;
      while (captureTabIds.has(tab.id) && !stopChecksFlag && Date.now() < dsgDeadline) {
        const dsgForced = await forceDsgCaptureFromTab(tab.id, monitor, selectors, captureFullPage, dsgPriceAdj);
        if (dsgForced?.ok) {
          const ld = dsgForced.liveData;
          const hasSizes = (ld?.inStock?.length || 0) + (ld?.outOfStock?.length || 0) > 0;
          if (hasSizes) {
            captureTabIds.delete(tab.id);
            await chrome.tabs.remove(tab.id).catch(() => {});
            if (stopChecksFlag) return stoppedResult;
            return dsgForced;
          }
        }
        if (!dsgReloaded && Date.now() >= dsgReloadAt && captureTabIds.has(tab.id)) {
          dsgReloaded = true;
          chrome.tabs.reload(tab.id).catch(() => {});
        }
        await sleep(200);
      }
      // Deadline reached, tab closed, or stop requested
      captureTabIds.delete(tab.id);
      await chrome.tabs.remove(tab.id).catch(() => {});
      if (stopChecksFlag) return stoppedResult;
      return { ok: false, error: "DSG: sizes not found within timeout" };
    }

    // Step 2: do not burn time reloading on the initial first-run capture.
    // If we need a retry, a reload can still help recover.
    if (captureFullPage && _retryCount > 0) {
      await chrome.tabs.reload(tab.id);
      await waitForTabLoad(tab.id);
      if (!captureTabIds.has(tab.id)) {
        if (stopChecksFlag) return stoppedResult;
        if (_retryCount < 2) return captureSnapshotInHiddenTab(monitor, captureFullPage, _retryCount + 1, isBatch, visibleTab);
        return { ok: false, error: "Tab closed unexpectedly after reload" };
      }
    }

    // Step 3: inject observer and poll (non-DSG only).
    // 45s main window; one re-injection to recover from client-side navigation.
    const priceAdj = Number(monitor.priceAdjustment) || 80;
    const pollAfterInject = async (timeoutMs) => {
      await injectCaptureObserver(tab.id, selectors, captureFullPage, monitor.name, priceAdj);
      return pollForResult(tab.id, timeoutMs);
    };
    const refreshAndRetrySameTimeout = async (timeoutMs) => {
      await chrome.tabs.reload(tab.id);
      await waitForTabLoad(tab.id);
      if (!captureTabIds.has(tab.id) || stopChecksFlag) return stoppedResult;
      return pollAfterInject(timeoutMs);
    };
    let result = await pollAfterInject(45000);
    if (result === false && captureTabIds.has(tab.id)) {
      result = await refreshAndRetrySameTimeout(45000);
      if (result === false) result = { ok: false, error: "Timeout" };
    }

    // One extra re-injection to recover from client-side navigation
    if (result === false && captureTabIds.has(tab.id)) {
      await sleep(300);
      await injectCaptureObserver(tab.id, selectors, captureFullPage, monitor.name, priceAdj);
      result = await pollForResult(tab.id, 15000);
    }

    if (result === null) {
      if (stopChecksFlag) return stoppedResult;
      if (_retryCount < 2) return captureSnapshotInHiddenTab(monitor, captureFullPage, _retryCount + 1, isBatch, visibleTab);
      return { ok: false, error: "Tab closed unexpectedly" };
    }
    if (result?.error === "Timeout") {
      captureTabIds.delete(tab.id);
      await chrome.tabs.remove(tab.id).catch(() => {});
      return result;
    }
    if (result === false) {
      captureTabIds.delete(tab.id);
      await chrome.tabs.remove(tab.id).catch(() => {});
      return { ok: false, error: "Timeout" };
    }

    captureTabIds.delete(tab.id);
    await chrome.tabs.remove(tab.id).catch(() => {});
    if (stopChecksFlag) return stoppedResult;
    return result;

  } catch (error) {
    if (captureTabIds.has(tab.id)) {
      captureTabIds.delete(tab.id);
      await chrome.tabs.remove(tab.id).catch(() => {});
    }
    if (stopChecksFlag || /stopped/i.test(error?.message || "")) return stoppedResult;
    if (_retryCount < 2) return captureSnapshotInHiddenTab(monitor, captureFullPage, _retryCount + 1, isBatch, visibleTab);
    return { ok: false, error: error.message || "Capture failed" };
  } finally {
    if (!isDsgCapture) queueNonDsgCookieClear(captureCookieDomain);
    releaseSlot(isBatch, isRetailLimitedCapture, isDsgCapture, isWowCapture);
  }
}

async function runMonitor(monitorId, reason = "scheduled", currentTabId = null, isBatch = false, monitorHint = null) {
  _startKeepAlive();
  try {
    let monitor = await getMonitorById(monitorId).catch(() => null);
    if (!monitor && monitorHint?.url) {
      // IDB lookup failed or returned nothing — use the monitor data passed inline from the dashboard.
      // Save it to IDB so future lookups find it.
      monitor = monitorHint;
      _saveMonitorById(monitor).catch(() => {});
    }
    if (!monitor) return null;
    const next = { ...monitor };
    const watchedSelectors = Array.isArray(monitor.selectors)
      ? monitor.selectors.filter(Boolean)
      : (monitor.selector ? [monitor.selector] : []);
    const wasError = monitor.status === "error";
    let desiredShopifyStatus = null;

  try {
    let monitorHostname = "";
    try {
      monitorHostname = new URL(monitor.url || "").hostname || "";
    } catch (_) {}
    const isFootlockerMonitor = /(^|\.)footlocker\.com\b/i.test(monitorHostname);
    const isDsgMonitor = /(^|\.)dickssportinggoods\.com\b/i.test(monitorHostname);
    const hasFrozenProductData = !!monitor.productData && !monitor.pendingInitialCheck;
    const allowProductDataRefresh = reason === "created" || reason === "metadata-refresh";
    const needsFootlockerColorRefresh =
      !hasFrozenProductData &&
      isFootlockerMonitor && (
        !monitor.productData ||
        !monitor.productData.color ||
        !Array.isArray(monitor.productData.source) ||
        !monitor.productData.source.includes("fl-selected-style-color")
      );
    const needsDsgDataRefresh =
      !hasFrozenProductData &&
      isDsgMonitor && (
        !monitor.productData ||
        !monitor.productData.color ||
        !monitor.productData.sku
      );

    const _pd = monitor.productData;
    // "manual" (Check now) always means price+sizes only — never full metadata re-capture.
    // "metadata-refresh" always forces a full re-extraction regardless of existing data.
    // Scheduled checks and first runs still do full capture when metadata is missing.
    const needsFullCapture = reason === "metadata-refresh"
      ? true
      : reason === "manual"
        ? false
        : (allowProductDataRefresh || !hasFrozenProductData)
          ? (
            !monitor.lastHtmlSnapshot ||
            !_pd ||
            !_pd.name ||
            !_pd.sku ||
            !Array.isArray(_pd.images) ||
            !_pd.images.length ||
            !_pd.brand ||
            needsFootlockerColorRefresh ||
            needsDsgDataRefresh
          )
          : false;

    // On first run, capture from the tab the user already has open (faster,
    // already logged-in, no extra network request). Fall back to hidden tab if
    // the tab is gone or not scriptable.
    const preferHiddenTabCapture = needsFootlockerColorRefresh;
    const useCurrentTabCapture = needsFullCapture && currentTabId && !preferHiddenTabCapture;
    let usedHiddenFallback = false;
    const visibleManualCapture = reason === "manual" && !isBatch;
    const snapshot = useCurrentTabCapture
      ? await captureSnapshotFromCurrentTab(currentTabId, monitor).catch(async () => {
          usedHiddenFallback = true;
          return captureSnapshotInHiddenTab(monitor, true, 0, isBatch, visibleManualCapture);
        })
      : await captureSnapshotInHiddenTab(monitor, needsFullCapture, 0, isBatch, visibleManualCapture);


    if (snapshot.stopped) return { stopped: true };
    if (stopChecksFlag && isBatch) return { stopped: true };
    next.pendingInitialCheck = false;
    next.savedBatchId = "";

    // FL/DSG: treat as error only when the page returned no usable content at all.
    // Do NOT require specific fields here â€" incomplete data causes errorâ†'retry loops
    // that flood the capture queue and freeze the whole extension.
    if (snapshot.ok && (isFootlockerMonitor || isDsgMonitor)) {
      const ld = snapshot.liveData;
      const hasPrice = ld?.price != null;
      const hasSizes = (ld?.inStock?.length || 0) + (ld?.outOfStock?.length || 0) > 0;
      const pdName = snapshot.fullPage?.productData?.name;
      const pageHasContent = hasPrice || hasSizes || !!pdName || (snapshot.summary?.length > 200);
      if (!pageHasContent) {
        snapshot.ok = false;
        snapshot.error = "Incomplete extraction - page returned no product data";
      }
    }

    next.lastCheckedAt = new Date().toISOString();
    const ignoredSizes = monitor.ignoredSizes || [];
    if (snapshot.liveData) {
      const ld = applyIgnoredSizesToMonitorData(snapshot.liveData, ignoredSizes);
      next.lastExtractedData = ld;
      next.lastExtractedAt = next.lastCheckedAt;
    }

    // DSG: tab extraction often misses price (lazy-loaded); fall back to the stored
    // product-list price rather than flagging an error for price only.
    if (isDsgMonitor && next.lastExtractedData?.price == null) {
      const storedPrice = next.productDataOverrides?.price ?? next.productData?.price;
      if (storedPrice != null) {
        next.lastExtractedData = { ...next.lastExtractedData, price: Number(storedPrice) };
      }
    }

    if (!snapshot.ok) {
      next.status = "error";
      next.lastError = snapshot.error;
      if (!wasError) desiredShopifyStatus = "draft";
    } else {
      next.status = "ok";
      next.lastError = "";
      if (watchedSelectors.length) {
        // Don't overwrite the at-pick-time outerHTML when the capture ran in
        // full-page mode (FL bypasses stored selectors for live-check speed).
        if (snapshot.matched !== "full-page" && snapshot.matched !== "dsg-forced-full-page") {
          next.lastSelectedOuterHtmlSnapshot = snapshot.html || "";
        }
      } else {
        next.lastSelectedOuterHtmlSnapshot = "";
        next.previousSelectedOuterHtmlSnapshot = "";
      }

      if (needsFullCapture) {
        // "Fill-in" capture: monitor already has some data but is missing fields.
        // Preserve change history and snapshot state; only merge in new product data.
        const isFillIn = !!(monitor.lastHtmlSnapshot && monitor.productData);
        next.lastSnapshot = snapshot.summary;
        next.lastHtmlSnapshot = snapshot.html;
        next.previousSnapshot = monitor.previousSnapshot || "";
        next.previousHtmlSnapshot = monitor.previousHtmlSnapshot || "";
        next.lastHtmlDiff = monitor.lastHtmlDiff || "";
        next.changeHistory = monitor.changeHistory || [];
        if (isFillIn) {
          // Preserve existing change tracking â€" this is not a reset, just filling gaps
          next.lastChangedAt = monitor.lastChangedAt || "";
          next.changeCount = monitor.changeCount || 0;
        } else {
          next.lastChangedAt = "";
        }

        if (snapshot.fullPage) {
          const freshPd = applyIgnoredSizesToMonitorData(snapshot.fullPage.productData || null, ignoredSizes);
          if (isFillIn && freshPd && reason !== "metadata-refresh") {
            // Merge: keep existing fields, only fill in ones that are null/empty
            const existing = monitor.productData || {};
            const merged = { ...existing };
            for (const key of Object.keys(freshPd)) {
              const ev = existing[key];
              const fv = freshPd[key];
              const existingEmpty = ev == null || ev === "" || (Array.isArray(ev) && ev.length === 0);
              if (existingEmpty && fv != null && fv !== "" && !(Array.isArray(fv) && fv.length === 0)) {
                merged[key] = fv;
              }
            }
            next.productData = merged;
          } else {
            next.productData = freshPd;
          }
          next.initialFullPageText = snapshot.fullPage.text;
          if (!isFillIn || reason === "metadata-refresh") next.initialCapturedAt = new Date().toISOString();
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
          // Way of Wade: swap images[0] <-> images[1] only when coming from fresh scrape (no flag yet)
          if (next.productData &&
              !next.productData._wowImgSwapped &&
              Array.isArray(next.productData.images) &&
              next.productData.images.length >= 2 &&
              /way\s*of\s*wade|li[\s-]*ning|lining/i.test(String(next.productData.brand || ""))) {
            const [a, b, ...rest] = next.productData.images;
            next.productData = { ...next.productData, images: [b, a, ...rest], _wowImgSwapped: true };
          }
        }

      } else {
        const htmlChanged = snapshot.html !== monitor.lastHtmlSnapshot;
        const prevLive = monitor.lastExtractedData;
        const newLive = snapshot.liveData;
        const liveChanges = [];
        if (prevLive && newLive) {
          if (prevLive.price !== newLive.price) liveChanges.push(`Price: $${prevLive.price ?? "?"} â†' $${newLive.price ?? "?"}`);
          const prevOosSet = new Set(prevLive.outOfStock || []);
          const newOosSet = new Set(newLive.outOfStock || []);
          const prevInSet = new Set(prevLive.inStock || []);
          const newInSet = new Set(newLive.inStock || []);
          const prevAllSet = new Set([...prevInSet, ...prevOosSet]);
          const newAllSet = new Set([...newInSet, ...newOosSet]);
          const sizesChanged = [...prevOosSet].sort().join(",") !== [...newOosSet].sort().join(",") || [...prevInSet].sort().join(",") !== [...newInSet].sort().join(",");
          if (sizesChanged) {
            const backInStock = [...newInSet].filter(s => prevOosSet.has(s));
            const newlyOos = [...newOosSet].filter(s => prevInSet.has(s));
            const added = [...newInSet].filter(s => !prevAllSet.has(s));
            const dropped = [...prevAllSet].filter(s => !newAllSet.has(s));
            if (backInStock.length) liveChanges.push(`Back in stock: ${backInStock.join(", ")}`);
            if (newlyOos.length) liveChanges.push(`Out of stock: ${newlyOos.join(", ")}`);
            if (added.length) liveChanges.push(`Sizes added: ${added.join(", ")}`);
            if (dropped.length) liveChanges.push(`Sizes dropped: ${dropped.join(", ")}`);
            if (!backInStock.length && !newlyOos.length && !added.length && !dropped.length) liveChanges.push("Sizes changed");
          }
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
          next.previousSelectedOuterHtmlSnapshot = watchedSelectors.length
            ? (monitor.lastSelectedOuterHtmlSnapshot || "")
            : "";
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
            ? liveChanges.join(" Â· ")
            : (monitor.selector ? "The watched element changed." : "The monitored page content changed.");

          chrome.notifications.create(`change:${monitor.id}:${Date.now()}`, {
            type: "basic",
            iconUrl: chrome.runtime.getURL("icon.svg"),
            title: `Change detected: ${monitor.name}`,
            message: notifMsg
          }).catch(() => {});
        }

      }

      if (needsFullCapture) {
        // First capture â€" all fields must be present
        const missingFields = getMissingProductDataFields(next);
        if (missingFields.length) {
          next.status = "error";
          next.lastError = `First capture incomplete â€" missing: ${missingFields.join(", ")}`;
          if (!wasError) desiredShopifyStatus = "draft";
        } else if (snapshot.liveData) {
          const _allSizes = getMonitorSavedSizes(next);
          const _hasSizeConversionError = getUnconvertedSizes(_allSizes, next).length > 0 || getStaleFractionalEuSizes(_allSizes, next).length > 0;
          if (!_hasSizeConversionError) {
            const shopifyMonitor = normalizeDsgSoccerCleatsMonitor(next);
            chrome.storage.local.get("shopifyTestMode").then(({ shopifyTestMode }) => {
              if (!shopifyTestMode) updateShopifyForMonitor(shopifyMonitor, snapshot.liveData).catch(() => {});
            });
          }
        }
      } else {
        // Normal check â€" only live price and sizes are required (product data is frozen)
        const live = next.lastExtractedData || {};
        const missingLive = [];
        if (live.price == null) missingLive.push("price");
        const hasSizes = (live.inStock?.length || 0) + (live.outOfStock?.length || 0) > 0;
        if (!hasSizes) missingLive.push("sizes");
        if (missingLive.length) {
          next.status = "error";
          next.lastError = `Could not extract from page: ${missingLive.join(", ")}`;
          if (!wasError) desiredShopifyStatus = "draft";
        } else if (snapshot.liveData) {
          const _allSizes = getMonitorSavedSizes(next);
          const _hasSizeConversionError = getUnconvertedSizes(_allSizes, next).length > 0 || getStaleFractionalEuSizes(_allSizes, next).length > 0;
          if (_hasSizeConversionError) {
            if (!wasError) desiredShopifyStatus = "draft";
          } else {
            const shopifyMonitor = normalizeDsgSoccerCleatsMonitor(next);
            chrome.storage.local.get("shopifyTestMode").then(({ shopifyTestMode }) => {
              if (!shopifyTestMode) updateShopifyForMonitor(shopifyMonitor, snapshot.liveData).catch(() => {});
            });
          }
        } else if (wasError) {
          desiredShopifyStatus = "active";
        }
      }
    }

    // Size conversion check always runs last so it overrides any ok status.
    // Shopify public variants must be EU labels only; no raw US fallback is allowed.
    if (next.lastExtractedData) {
      const allSizes = getMonitorSavedSizes(next);
      const usOnly = getUnconvertedSizes(allSizes, next);
      const staleFractional = getStaleFractionalEuSizes(allSizes, next);
      next.hasUsOnlySizes = usOnly.length > 0;
      if (next.hasUsOnlySizes) {
        next.usOnlySizesList = usOnly;
        next.status = "error";
        next.lastError = `Sizes with no EU conversion: ${usOnly.join(", ")} â€" remove them before importing`;
        if (!wasError) desiredShopifyStatus = "draft";
      } else {
        next.hasUsOnlySizes = false;
        next.usOnlySizesList = [];
        if (staleFractional.length) {
          next.hasStaleFractionalEuSizes = true;
          next.staleFractionalEuSizesList = staleFractional;
          next.status = "error";
          next.lastError = getStaleFractionalEuError(staleFractional);
          if (!wasError) desiredShopifyStatus = "draft";
        } else {
          const hadStaleFractionError = /Fraction EU sizes showing as decimals/i.test(String(next.lastError || ""));
          next.hasStaleFractionalEuSizes = false;
          next.staleFractionalEuSizesList = [];
          if (hadStaleFractionError) {
            next.lastError = "";
            if (next.status === "error") next.status = "ok";
          }
        }
      }
    }
  } catch (error) {
    next.lastCheckedAt = new Date().toISOString();
    next.status = "error";
    next.lastError = error.message;
    if (!wasError) desiredShopifyStatus = "draft";
  }

    next.lastRunReason = reason;
    next.pendingInitialCheck = false;
    next.savedBatchId = "";
    if (stopChecksFlag && isBatch) return { stopped: true };

    // Each monitor has its own storage key â€" no lock needed, no full-array read/write.
    await saveMonitorFast(next);
    notifyDashboard("monitors-updated", { monitor: slimMonitor(next) }).catch(() => {});

    if (desiredShopifyStatus) {
      chrome.storage.local.get("shopifyTestMode").then(({ shopifyTestMode }) => {
        if (!shopifyTestMode) syncMonitorShopifyStatus(next, desiredShopifyStatus).catch(() => {});
      });
    }
    return next;
  } finally {
    _stopKeepAlive();
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
    lastSelectedOuterHtmlSnapshot: payload.lastSelectedOuterHtmlSnapshot || "",
    previousSelectedOuterHtmlSnapshot: "",
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

  const savedMonitor = normalizeDsgSoccerCleatsMonitor(monitor);
  const monitors = await getMonitors();
  monitors.unshift(savedMonitor);
  await saveMonitors(monitors);
  await ensureAlarm(savedMonitor);
  pushUndo(`Create: ${savedMonitor.name}`, async () => {
    await deleteMonitor(savedMonitor.id);
  });
  await runMonitor(savedMonitor.id, "created", currentTabId);
  return savedMonitor;
}

async function createMonitorsBatch(items, selectors, senderTabId, sharedOverrides = null, priceAdjustment = 80) {
  const created = [];
  const savedBatchId = `batch-${Date.now()}`;
  const adjustedPrice = (value) => {
    const raw = parseFloat(String(value ?? "").replace(",", "."));
    return Number.isFinite(raw) ? String(Math.round(raw + (Number(priceAdjustment) || 80))) : null;
  };
  await withStorageLock(async () => {
    const monitors = await getMonitors();
    for (const item of items) {
      const roundedPrice = adjustedPrice(item.price);
      const monitor = {
        id: uid(), name: item.name || "Untitled", url: item.url,
        selectors: selectors || [], autoCheck: false,
        intervalMinutes: DEFAULT_INTERVAL_MINUTES,
        createdAt: new Date().toISOString(), lastCheckedAt: "", lastChangedAt: "",
        lastSnapshot: "", previousSnapshot: "", lastHtmlSnapshot: "",
        previousHtmlSnapshot: "", lastHtmlDiff: "",
        lastSelectedOuterHtmlSnapshot: "", previousSelectedOuterHtmlSnapshot: "",
        changeHistory: [],
        lastError: "", status: "saved", changeCount: 0,
        initialFullPageText: "", initialCapturedAt: "",
        productData: null, lastExtractedData: null, previousExtractedData: null,
        lastExtractedAt: "", pendingInitialCheck: true,
        savedBatchId,
        productDataOverrides: roundedPrice
          ? { ...(sharedOverrides || {}), price: roundedPrice }
          : (sharedOverrides || null),
        priceAdjustment: Number(priceAdjustment) || 80
      };
      const savedMonitor = normalizeDsgSoccerCleatsMonitor(monitor);
      monitors.unshift(savedMonitor);
      created.push(savedMonitor);
      pushUndo(`Create: ${savedMonitor.name}`, async () => { await deleteMonitor(savedMonitor.id); });
    }
    await saveMonitors(monitors);
  });

  // Tell the dashboard immediately; saved batches are checked later from the dashboard.
  notifyDashboard("monitors-batch-saved");

  for (const monitor of created) ensureAlarm(monitor);
  return created;
}

async function updateMonitor(payload) {
  let previous = null;
  let updated = null;
  await withStorageLock(async () => {
    const monitors = await getMonitors();
    const index = monitors.findIndex((item) => item.id === payload.id);
    if (index === -1) {
      throw new Error("Monitor not found");
    }

    previous = { ...monitors[index] };
    updated = {
      ...previous,
      ...payload,
      intervalMinutes: Math.max(1, Number(payload.intervalMinutes) || previous.intervalMinutes || DEFAULT_INTERVAL_MINUTES)
    };

    monitors[index] = updated;
    await saveMonitors(monitors);
  });
  await ensureAlarm(updated);
  pushUndo(`Update: ${updated.name}`, async () => {
    await withStorageLock(async () => {
      const mons = await getMonitors();
      const idx = mons.findIndex((m) => m.id === previous.id);
      if (idx !== -1) { mons[idx] = previous; await saveMonitors(mons); await ensureAlarm(previous); }
    });
  });
  return updated;
}

async function deleteMonitor(monitorId) {
  await deleteMonitors([monitorId]);
}

async function deleteMonitors(monitorIds) {
  const ids = [...new Set(monitorIds)];
  const deletedItems = [];
  await withStorageLock(async () => {
    const reads = await Promise.all(ids.map(id => getMonitorById(id)));
    for (const m of reads) { if (m) deletedItems.push(m); }
    await deleteMonitorsByIds(ids);
  });
  _bumpMonitorsVersion();
  for (const m of deletedItems) {
    clearAlarm(m.id);
    const snapshot = { ...m };
    pushUndo(`Delete: ${m.name}`, async () => {
      await withStorageLock(async () => {
        const existing = await getMonitorById(snapshot.id);
        if (!existing) await _saveMonitorById(snapshot);
      });
      await ensureAlarm(snapshot);
    });
  }
  if (deletedItems.length) scheduleCloudAutoSync("monitors deleted");
}

const _BRAND_INFER_TYPES = new Set(["training", "basketball", "football", "running", "lifestyle"]);

async function normalizeLocalMonitorBrands() {
  let changed = false;
  await withStorageLock(async () => {
    const monitors = await getMonitors();
    for (const m of monitors) {
      const pd = m.productData;
      if (!pd) continue;
      const rawBrand = String(pd.brand || "").trim();
      if (!rawBrand) continue;
      const canonical = canonicalizeBrand(rawBrand);
      if (!canonical || canonical === rawBrand) continue;
      pd.brand = canonical;
      if (!pd.type) {
        const suffix = rawBrand.slice(canonical.length).trim();
        if (suffix && _BRAND_INFER_TYPES.has(suffix.toLowerCase())) {
          pd.type = suffix.charAt(0).toUpperCase() + suffix.slice(1).toLowerCase();
        }
      }
      changed = true;
    }
    if (changed) await _saveMonitors(monitors);
  });
  return changed;
}

async function normalizeMonitorBrandsAndShopify() {
  await normalizeLocalMonitorBrands().catch(() => {});
  const monitors = await getMonitors();
  await syncMonitorBrandsToShopify(monitors).catch(() => {});
  await normalizeAllShopifyVendors().catch(() => {});
}

chrome.tabs.onRemoved.addListener((tabId) => {
  captureTabIds.delete(tabId);
  _untrackDsgTab(tabId);
});

async function initializeMonitorState() {
  const monitors = await getMonitorsMarkingIncompleteAsErrors();
  await Promise.all(monitors.map((monitor) => ensureAlarm(monitor)));
  normalizeMonitorBrandsAndShopify().catch(() => {});
}

async function syncFromCloud() {
  try {
    const existing = await getMonitors();
    if (existing.length) return;
    const count = await restoreFromStorageBackup();
    if (count) {
      addLog({
        type: "cloud-sync",
        title: "Auto-restored from local backup",
        details: [`Recovered ${count} monitors from chrome.storage.local backup`]
      }).catch(() => {});
      notifyDashboard("cloud-sync-status", {
        ok: true,
        message: `Auto-restored ${count} monitors from local backup`
      }).catch(() => {});
    }
  } catch (_) {}
}

let recoveredSupabaseImportAttempted = false;
let recoveredBackupCache = null;
async function loadRecoveredLocalBackup() {
  if (recoveredBackupCache) return recoveredBackupCache;
  const response = await fetch(chrome.runtime.getURL("recovered/local-monitors-backup.json"), { cache: "no-store" });
  if (!response.ok) throw new Error("Recovered backup file not found");
  const monitors = await response.json();
  if (!Array.isArray(monitors) || !monitors.length) throw new Error("Recovered backup is empty or invalid");
  recoveredBackupCache = monitors;
  return monitors;
}

async function importRecoveredSupabaseBackupIfEmpty() {
  if (recoveredSupabaseImportAttempted) return 0;
  recoveredSupabaseImportAttempted = true;
  const existing = await getMonitors();
  if (existing.length) return existing.length;
  try {
    const monitors = await loadRecoveredLocalBackup();
    await _saveMonitors(monitors);
    await addLog({
      type: "cloud-sync",
      title: "Recovered local backup",
      details: [`Imported ${monitors.length} monitors from recovered/local-monitors-backup.json`]
    }).catch(() => {});
    return monitors.length;
  } catch (error) {
    await addLog({
      type: "cloud-sync",
      title: "Local backup recovery failed",
      details: [error.message || String(error)]
    }).catch(() => {});
    return 0;
  }
}

const _MODULE_COLORS = {"Black":["black","onyx","jet","ebony","obsidian","raven","coal","ink","shadow","noir","licorice","pitch","tripleblack","triple-black","coreblack","core-black","phantom","anthracite","soot","carbon"],"White":["white","ivory","snow","pearl","sail","cream","bone","eggshell","linen","frost","alabaster","porcelain","chalk","milk","cotton","ghost","offwhite","off-white","whisper","paper","shell","antique"],"Red":["red","crimson","scarlet","ruby","burgundy","maroon","wine","cherry","carmine","cardinal","tomato","garnet","vermillion","vermilion","brick","blood","firebrick","cranberry","raspberry","strawberry","rose","claret","mahogany","terra","cotta","sienna","auburn","rubyred","oxblood","merlot","poppy","coralred","sunsetred","chili","rubywine"],"Blue":["blue","navy","cobalt","royal","indigo","denim","sky","powder","midnight","steel","slate","sapphire","azure","thunder","ice","cornflower","periwinkle","iris","ultramarine","prussian","admiral","marine","federal","storm","glacier","arctic","aegean","obsidian-blue","turbo","polar","mistblue","oceanblue","deepblue","lightblue","darkblue","hyperblue","universityblue","carolinablue"],"Green":["green","olive","sage","forest","army","jade","emerald","mint","fern","moss","pine","volt","lime","hunter","bottle","kelly","shamrock","chartreuse","avocado","pistachio","pear","leaf","basil","seaweed","jungle","cucumber","matcha","celadon","viridian","malachite","voltgreen","neongreen","electricgreen","loden","spruce","evergreen","clover","pea","grassy","seaglass","seafoamgreen"],"Yellow":["yellow","gold","golden","mustard","lemon","canary","butter","banana","honey","sunflower","flaxen","straw","blonde","champagne","vanilla","daffodil","citrine","topaz","citrus","maize","corn","ambergold","sandgold","sulphur","mustardseed","dijon","neonyellow","electricyellow"],"Orange":["orange","amber","tangerine","apricot","rust","copper","pumpkin","saffron","coral","burnt","cinnamon","papaya","mango","melon","clay","ginger","tiger","marigold","bronze","peach","persimmon","nectarine","cantaloupe","sunset","burntorange","terracotta","carrot","kumquat"],"Violet":["purple","violet","lavender","lilac","plum","grape","mauve","amethyst","orchid","wisteria","heather","thistle","periwinkle","mulberry","eggplant","byzantium","aubergine","boysenberry","violetdust","deeppurple","royalpurple"],"Pink":["pink","blush","fuchsia","magenta","salmon","rose","bubblegum","flamingo","watermelon","peony","carnation","petal","flush","rouge","blossom","pastel","candy","lollipop","neon","cerise","hot","dusty","millennial","rosepink","powderpink","softpink","brightpink","shockpink"],"Brown":["brown","tan","beige","camel","mocha","chocolate","coffee","sand","taupe","nude","natural","khaki","wheat","stone","walnut","hazel","toffee","espresso","sepia","umber","fawn","oatmeal","biscuit","latte","ecru","buff","driftwood","chestnut","cacao","bark","leather","suede","caramel","pecan","almond","acorn","cocoa","mink","tobacco","saddle","oak","hickory","truffle","earth","mud","dune","bran"],"Gray":["gray","grey","silver","charcoal","ash","smoke","graphite","pewter","cement","concrete","cloud","wolf","pebble","flint","iron","lead","fossil","heather","marengo","dove","cool","smokey","stonegrey","stone-gray","coolgrey","cool-grey","neutralgray","neutralgrey","platinum","gunmetal"],"Turquoise":["turquoise","teal","aqua","cyan","seafoam","aquamarine","caribbean","lagoon","cerulean","peacock","ocean","pool","mintblue","tiffany","robinsegg","bluegreen","turq"],"Multicolor":["multi","multicolor","multi-color","assorted"]};

function pickColorFinalFromRaw(raw) {
  if (!raw) return "Multicolor";
  // Split CamelCase so "RedGreenWhite" → ["red","green","white"]
  const spaced = raw.replace(/([a-z])([A-Z])/g, "$1 $2");
  const words = spaced.toLowerCase().replace(/[^a-z0-9\s]/g, " ").split(/\s+/).filter(Boolean);
  // Iterate words in position order — first recognised color wins
  for (const word of words) {
    const clean = word.replace(/[^a-z0-9]/g, "");
    for (const [base, synonyms] of Object.entries(_MODULE_COLORS)) {
      if (base === "Multicolor") continue;
      if (synonyms.some((c) => String(c).toLowerCase().replace(/[^a-z0-9]/g, "") === clean)) return base;
    }
  }
  return "Multicolor";
}

async function migrateWowColorFinals() {
  const WOW_RE = /way\s*of\s*wade|li[\s-]*ning|lining/i;
  const DLO1_RE = /\bd['']?lo\s*1\b/i;
  await withStorageLock(async () => {
    const monitors = await getMonitors();
    let changed = false;
    for (const m of monitors) {
      const pd = m.productData;
      if (!pd) continue;
      if (!WOW_RE.test(String(pd.brand || ""))) continue;
      if (pd.colorRaw) {
        const correct = pickColorFinalFromRaw(pd.colorRaw);
        if (pd.colorFinal !== correct) { pd.colorFinal = correct; changed = true; }
      }
      if (!pd.player && DLO1_RE.test(String(pd.name || ""))) {
        pd.player = "D'Angelo Russell";
        changed = true;
      }
    }
    if (changed) { await saveMonitors(monitors); _bumpMonitorsVersion(); }
  });
}

chrome.runtime.onInstalled.addListener(async () => {
  await migrateToPerMonitorStorage();
  await syncFromCloud();
  await initializeMonitorState();
  await migrateWowColorFinals().catch(() => {});
});

chrome.runtime.onStartup.addListener(async () => {
  await migrateToPerMonitorStorage();
  await syncFromCloud();
  await initializeMonitorState();
  await migrateWowColorFinals().catch(() => {});
});

chrome.alarms.onAlarm.addListener(async (alarm) => {
  if (!alarm.name.startsWith(ALARM_PREFIX)) {
    return;
  }

  const { autoIntervalEnabled } = await chrome.storage.local.get("autoIntervalEnabled");
  if (!autoIntervalEnabled) return; // auto checks disabled by default

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
        sendResponse({ ok: true, monitor: slimMonitor(monitor) });
        return;
      }

      if (message.type === "prepare-dsg-session") {
        const cookies = await clearSiteCookies();
        await addLog({
          type: "dsg-reset",
          title: "Cookies cleared",
          details: [
            `Dick's: cleared ${cookies.dsg.cleared}/${cookies.dsg.attempted}`,
            `Foot Locker: cleared ${cookies.fl.cleared}/${cookies.fl.attempted}`
          ]
        }).catch(() => {});
        sendResponse({ ok: true, cookies });
        return;
      }

      if (message.type === "refresh-monitor") {
        const currentTabId = message.tabId ?? null;
        const monitorHint = message.monitorHint || null;
        const monitor = await runMonitor(message.monitorId, "manual", currentTabId, !!message.isBatch, monitorHint);
        if (monitor?.stopped) {
          sendResponse({ ok: false, stopped: true });
        } else {
          sendResponse({ ok: true, monitor: monitor ? slimMonitor(monitor) : null });
        }
        return;
      }

      if (message.type === "first-capture") {
        // Forces a full product metadata re-capture (name, brand, images, SKU, color, etc.)
        // regardless of whether the monitor already has productData.
        const monitor = await runMonitor(message.monitorId, "metadata-refresh", null, !!message.isBatch);
        if (monitor?.stopped) {
          sendResponse({ ok: false, stopped: true });
        } else {
          sendResponse({ ok: true, monitor: monitor ? slimMonitor(monitor) : null });
        }
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
        sendResponse({ ok: true, monitor: monitor ? slimMonitor(monitor) : null });
        return;
      }

      if (message.type === "fix-wow-colorfinal") {
        await migrateWowColorFinals();
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "bulk-update-monitors") {
        const ids = new Set((message.payload?.ids || []).map(String));
        const productPatch = message.payload?.productPatch || {};
        const monitorPatch = message.payload?.monitorPatch || {};
        let updatedCount = 0;
        await withStorageLock(async () => {
          const monitors = await getMonitors();
          for (let index = 0; index < monitors.length; index++) {
            const monitor = monitors[index];
            if (!ids.has(String(monitor.id))) continue;
            monitors[index] = {
              ...monitor,
              ...monitorPatch,
              productData: {
                ...(monitor.productData || {}),
                ...productPatch
              },
              intervalMinutes: Math.max(1, Number(monitorPatch.intervalMinutes) || monitor.intervalMinutes || DEFAULT_INTERVAL_MINUTES)
            };
            updatedCount += 1;
          }
          await saveMonitors(monitors);
        });
        notifyDashboard("monitors-batch-saved");
        sendResponse({ ok: true, count: updatedCount });
        return;
      }

      if (message.type === "get-monitors") {
        // Fast path: if caller already has this version, skip the IDB read entirely.
        if (message.knownVersion !== undefined && message.knownVersion === _monitorsVersion) {
          sendResponse({ ok: true, unchanged: true, version: _monitorsVersion });
          return;
        }
        let monitors = await getMonitorsMarkingIncompleteAsErrors();
        if (!monitors.length) {
          // 1st fallback: chrome.storage.local automatic backup (most recent, zero user interaction)
          await syncFromCloud().catch(() => {});
          monitors = await getMonitorsMarkingIncompleteAsErrors();
        }
        if (!monitors.length) {
          // 2nd fallback: static recovered/local-monitors-backup.json bundled with extension
          await importRecoveredSupabaseBackupIfEmpty().catch(() => 0);
          monitors = await getMonitorsMarkingIncompleteAsErrors();
        }
        if (!monitors.length) {
          const recovered = await loadRecoveredLocalBackup().catch(() => []);
          if (recovered.length) {
            monitors = recovered.map(withIncompleteDataError);
            _saveMonitors(monitors).catch(() => {});
          }
        }
        sendResponse({ ok: true, monitors: monitors.map(slimMonitor), version: _monitorsVersion });
        return;
      }

      if (message.type === "restore-status") {
        const [stored, recovered] = await Promise.all([
          getMonitors().catch(() => []),
          loadRecoveredLocalBackup().catch(() => [])
        ]);
        sendResponse({ ok: true, stored: stored.length, recovered: recovered.length });
        return;
      }

      if (message.type === "get-monitor-snapshots") {
        const { id, historyIndex } = message.payload || {};
        const monitors = await getMonitors();
        const m = monitors.find(x => x.id === id);
        if (!m) { sendResponse({ ok: false, error: "Monitor not found" }); return; }
        if (historyIndex !== undefined) {
          const entry = (m.changeHistory || [])[historyIndex];
          if (!entry) { sendResponse({ ok: false, error: "History entry not found" }); return; }
          sendResponse({ ok: true, previousHtml: entry.previousHtml || "", currentHtml: entry.currentHtml || "" });
        } else {
          sendResponse({
            ok: true,
            lastHtmlSnapshot: m.lastHtmlSnapshot || "",
            previousHtmlSnapshot: m.previousHtmlSnapshot || "",
            lastSelectedOuterHtmlSnapshot: m.lastSelectedOuterHtmlSnapshot || "",
            previousSelectedOuterHtmlSnapshot: m.previousSelectedOuterHtmlSnapshot || ""
          });
        }
        return;
      }

      if (message.type === "normalize-monitor-brands") {
        await normalizeMonitorBrandsAndShopify();
        const monitors = await getMonitorsMarkingIncompleteAsErrors();
        sendResponse({ ok: true, monitors: monitors.map(slimMonitor) });
        return;
      }

      if (message.type === "refresh-monitors-metadata") {
        const cardMap = message.payload?.cardMap || {};
        const matched = [];
        await withStorageLock(async () => {
          const monitors = await getMonitors();
          let changed = false;
          for (const m of monitors) {
            const card = cardMap[m.url];
            if (!card) continue;
            if (card.price) {
              const existingPrice = m.productDataOverrides?.price ?? m.productData?.price;
              const isMissing = existingPrice == null || existingPrice === "" || /see price in cart/i.test(String(existingPrice));
              if (isMissing) {
                const adj = Number(m.priceAdjustment) || 80;
                const raw = parseFloat(String(card.price).replace(",", "."));
                if (Number.isFinite(raw)) {
                  const adjustedPrice = String(Math.round(raw + adj));
                  m.productDataOverrides = { ...(m.productDataOverrides || {}), price: adjustedPrice };
                  changed = true;
                }
              }
            }
            matched.push(m.id);
          }
          if (changed) await saveMonitors(monitors);
        });
        sendResponse({ ok: true, started: matched.length });
        const senderTabId = sender?.tab?.id ?? null;
        let done = 0; let errors = 0;
        const sendProgress = () => {
          try { chrome.tabs.sendMessage(senderTabId, { type: "batch-progress", done, total: matched.length, errors }); } catch (_) {}
        };
        matched.forEach(async (id) => {
          try { await runMonitor(id, "metadata-refresh", senderTabId, true); } catch (_) { errors++; }
          done++;
          sendProgress();
        });
        return;
      }

      if (message.type === "refresh-dsg-oos") {
        // Backfill out-of-stock sizes for existing DSG monitors.
        // Opens each DSG monitor in a hidden tab, re-runs forceDsgCaptureFromTab,
        // then patches productData.sizes + productData.outOfStock + lastExtractedData.
        const senderTabId = sender?.tab?.id ?? null;
        const monitors = await getMonitors();
        const dsgMonitors = monitors.filter((m) => /(^|\.)dickssportinggoods\.com\b/i.test((m.url || "")));
        sendResponse({ ok: true, total: dsgMonitors.length });
        let done = 0; let errors = 0;
        const sendProg = () => {
          try { chrome.tabs.sendMessage(senderTabId, { type: "batch-progress", done, total: dsgMonitors.length, errors }); } catch (_) {}
        };
        const processOne = async (monitor) => {
          let tabId = null;
          await acquireSlot(false, false, true);
          try {
            const tab = await openDsgTab(monitor.url);
            tabId = tab.id;
            captureTabIds.add(tabId);
            if (!await waitWhileTabOpen(tabId, 1200)) return;
            const result = await forceDsgCaptureFromTab(tabId, monitor, [], true, Number(monitor.priceAdjustment) || 80);
            if (result?.ok && result.fullPage?.productData) {
              const pd = applyIgnoredSizesToMonitorData(result.fullPage.productData, monitor.ignoredSizes || []);
              const newSizes = pd.sizes || [];
              const newOos = pd.outOfStock || [];
              if (newSizes.length) {
                await withStorageLock(async () => {
                  const fresh = await getMonitors();
                  const idx = fresh.findIndex((m) => m.id === monitor.id);
                  if (idx !== -1) {
                    if (fresh[idx].productData) {
                      fresh[idx].productData.sizes = newSizes;
                      fresh[idx].productData.outOfStock = newOos;
                    }
                    const inStock = newSizes.filter((s) => !newOos.includes(s));
                    fresh[idx].lastExtractedData = { ...(fresh[idx].lastExtractedData || {}), inStock, outOfStock: newOos };
                    fresh[idx].lastExtractedAt = new Date().toISOString();
                    await saveMonitors(fresh);
                  }
                });
              }
            }
          } finally {
            if (tabId) { captureTabIds.delete(tabId); chrome.tabs.remove(tabId).catch(() => {}); }
            releaseSlot(false, false, true);
          }
        };

        // Process in parallel batches that share the DSG hidden-tab limiter.
        for (let i = 0; i < dsgMonitors.length; i += MAX_CONCURRENT_DSG_CAPTURES) {
          const batch = dsgMonitors.slice(i, i + MAX_CONCURRENT_DSG_CAPTURES);
          await Promise.allSettled(batch.map(async (monitor) => {
            try { await processOne(monitor); } catch (_) { errors++; }
            done++;
            sendProg();
          }));
        }
        notifyDashboard("monitors-batch-saved");
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

      if (message.type === "import-monitors") {
        const incoming = message.payload.monitors;
        if (!Array.isArray(incoming)) { sendResponse({ ok: false, error: "Invalid data" }); return; }
        if (message.payload.replace) {
          await saveMonitors(incoming);
        } else {
          const existing = await getMonitors();
          const existingIds = new Set(existing.map(m => m.id));
          const merged = [...existing, ...incoming.filter(m => !existingIds.has(m.id))];
          await saveMonitors(merged);
        }
        notifyDashboard("monitors-batch-saved");
        sendResponse({ ok: true, count: incoming.length });
        return;
      }

      if (message.type === "import-recovered-supabase-backup") {
        const incoming = await loadRecoveredLocalBackup();
        if (!Array.isArray(incoming)) { sendResponse({ ok: false, error: "Recovered backup is invalid" }); return; }
        if (message.payload?.replace) {
          await _saveMonitors(incoming);
        } else {
          const existing = await getMonitors();
          const existingIds = new Set(existing.map(m => m.id));
          const merged = [...existing, ...incoming.filter(m => !existingIds.has(m.id))];
          await _saveMonitors(merged);
        }
        notifyDashboard("monitors-batch-saved");
        sendResponse({ ok: true, count: incoming.length });
        return;
      }

      if (message.type === "remove-us-sizes") {
        const monitors = await getMonitors();
        const idx = monitors.findIndex(m => m.id === message.monitorId);
        if (idx === -1) { sendResponse({ ok: false, error: "Monitor not found" }); return; }
        const m = { ...monitors[idx] };
        const requestedSizes = Array.isArray(message.sizes)
          ? message.sizes.map(s => String(s || "").replace(/\s+/g, " ").trim()).filter(Boolean)
          : [];
        const usOnly = new Set(requestedSizes.length ? requestedSizes : getUnconvertedSizes([
          ...(m.lastExtractedData?.inStock || []),
          ...(m.lastExtractedData?.outOfStock || []),
          ...(m.productData?.sizes || [])
        ], m));
        const ignoredList = [...usOnly];
        Object.assign(m, removeIgnoredSizesFromMonitor(m, ignoredList));
        m.ignoredSizes = [...new Set([...(m.ignoredSizes || []), ...ignoredList])];
        const remainingBadSizes = getUnconvertedSizes([
          ...(m.lastExtractedData?.inStock || []),
          ...(m.lastExtractedData?.outOfStock || []),
          ...(m.productData?.sizes || [])
        ], m);
        m.hasUsOnlySizes = remainingBadSizes.length > 0;
        m.usOnlySizesList = remainingBadSizes;
        if (remainingBadSizes.length) {
          m.status = "error";
          m.lastError = `Sizes with no EU conversion: ${remainingBadSizes.join(", ")} â€" remove them before importing`;
        } else {
          m.status = "ok";
          m.lastError = "";
        }
        monitors[idx] = m;
        await saveMonitors(monitors);
        notifyDashboard("monitors-batch-saved");
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "local-backup-status") {
        try {
          const info = await getLocalBackupInfo();
          sendResponse({ ok: true, perm: info.perm, folderName: info.folderName, lastWriteAt: info.lastWriteAt });
        } catch (err) {
          sendResponse({ ok: false, error: err.message });
        }
        return;
      }

      if (message.type === "local-backup-write-now") {
        try {
          const monitors = await getMonitorsMarkingIncompleteAsErrors();
          await writeLocalBackup(monitors);
          sendResponse({ ok: true, count: monitors.length });
        } catch (err) {
          sendResponse({ ok: false, error: err.message });
        }
        return;
      }

      if (message.type === "drive-backup") {
        try {
          const monitors = await getMonitorsMarkingIncompleteAsErrors();
          const result = await backupToDrive(monitors);
          sendResponse({ ok: true, count: result.count });
        } catch (err) {
          sendResponse({ ok: false, error: err.message });
        }
        return;
      }

      if (message.type === "drive-restore") {
        try {
          const { monitors, modifiedTime } = await restoreFromDrive();
          if (message.payload?.replace) {
            await saveMonitors(monitors);
          } else {
            const existing = await getMonitors();
            const existingIds = new Set(existing.map(m => m.id));
            await saveMonitors([...existing, ...monitors.filter(m => !existingIds.has(m.id))]);
          }
          notifyDashboard("monitors-batch-saved");
          sendResponse({ ok: true, count: monitors.length, modifiedTime });
        } catch (err) {
          sendResponse({ ok: false, error: err.message });
        }
        return;
      }

      if (message.type === "drive-backup-info") {
        try {
          const info = await getDriveBackupInfo();
          sendResponse({ ok: true, info });
        } catch (err) {
          sendResponse({ ok: false, error: err.message });
        }
        return;
      }

      if (message.type === "cloud-status") {
        await ensureDefaultCloudSignIn().catch(() => {});
        const status = await getCloudStatus();
        sendResponse({ ok: true, status });
        return;
      }

      if (message.type === "cloud-configure") {
        const result = await configureCloud(message.payload || {});
        sendResponse({ ok: true, result });
        return;
      }

      if (message.type === "cloud-sign-up") {
        const result = await signUpCloud(message.payload?.email || "", message.payload?.password || "");
        sendResponse({ ok: true, result });
        return;
      }

      if (message.type === "cloud-sign-in") {
        const result = await signInCloud(message.payload?.email || "", message.payload?.password || "");
        scheduleCloudAutoSync("signed in", 5000);
        sendResponse({ ok: true, result });
        return;
      }

      if (message.type === "cloud-sign-out") {
        await signOutCloud();
        clearTimeout(_cloudAutoSyncTimer);
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "cloud-push") {
        clearTimeout(_cloudAutoSyncTimer);
        await ensureDefaultCloudSignIn().catch(() => {});
        const monitors = await getMonitorsMarkingIncompleteAsErrors();
        const result = await pushCloudMonitors(monitors);
        sendResponse({ ok: true, ...result });
        return;
      }

      if (message.type === "cloud-pull") {
        await ensureDefaultCloudSignIn().catch(() => {});
        const result = await pullCloudMonitors();
        const incoming = Array.isArray(result.monitors) ? result.monitors : [];
        if (message.payload?.replace) {
          await saveMonitors(incoming);
        } else {
          const existing = await getMonitors();
          const existingIds = new Set(existing.map((monitor) => monitor.id));
          await saveMonitors([...existing, ...incoming.filter((monitor) => !existingIds.has(monitor.id))]);
        }
        notifyDashboard("monitors-batch-saved");
        sendResponse({ ok: true, count: incoming.length, updatedAt: result.updatedAt || null });
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

      if (message.type === "ping") {
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "stop-checks") {
        stopAllChecks();
        sendResponse({ ok: true });
        return;
      }

      if (message.type === "resume-checks") {
        resumeChecks();
        resetCaptureQueues();
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

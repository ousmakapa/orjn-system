const STORAGE_KEY = "pageMonitors";

export const DEFAULT_INTERVAL_MINUTES = 1440;

const CANONICAL_BRAND_RULES = [
  { canonical: "Nike", patterns: [/^nike$/i, /^nike\s+sportswear$/i, /^nike\s+sb$/i] },
  { canonical: "Jordan", patterns: [/^air\s*jordan$/i, /^jordan$/i, /^jordan\s+brand$/i] },
  { canonical: "Adidas", patterns: [/^adidas$/i, /^adidas\s+originals$/i, /^adidas\s+sportswear$/i] },
  { canonical: "Puma", patterns: [/^puma$/i, /^puma\s+sportswear$/i] },
  { canonical: "Reebok", patterns: [/^reebok$/i, /^reebok\s+classic$/i] },
  { canonical: "New Balance", patterns: [/^new\s*balance$/i, /^newbalance$/i, /^nb$/i] },
  { canonical: "Converse", patterns: [/^converse$/i, /^converse\s+all\s*star$/i, /^converse\s+cons$/i] },
  { canonical: "Vans", patterns: [/^vans$/i] },
  { canonical: "Under Armour", patterns: [/^under\s*armou?r$/i, /^ua$/i] },
  { canonical: "Asics", patterns: [/^asics$/i, /^asics\s+tiger$/i] },
  { canonical: "Saucony", patterns: [/^saucony$/i] },
  { canonical: "Brooks", patterns: [/^brooks$/i, /^brooks\s+running$/i] },
  { canonical: "Hoka", patterns: [/^hoka$/i, /^hoka\s+one\s+one$/i, /^hokaoneone$/i] },
  { canonical: "ON Cloud", patterns: [/^on$/i, /^on\s*running$/i, /^on\s*cloud$/i, /^on\s*cloud\s*running$/i] },
  { canonical: "Salomon", patterns: [/^salomon$/i, /^salomon\s+sportstyle$/i] },
  { canonical: "Timberland", patterns: [/^timberland$/i, /^timberland\s+pro$/i] },
  { canonical: "UGG", patterns: [/^ugg$/i, /^ugg\s+australia$/i] },
  { canonical: "Dr. Martens", patterns: [/^dr\.?\s*martens$/i, /^doc\s*martens$/i, /^drmartens$/i] },
  { canonical: "Birkenstock", patterns: [/^birkenstock$/i] },
  { canonical: "Clarks", patterns: [/^clarks$/i, /^clarks\s+originals$/i] },
  { canonical: "The North Face", patterns: [/^the\s*north\s*face$/i, /^north\s*face$/i, /^tnf$/i] },
  { canonical: "Columbia", patterns: [/^columbia$/i, /^columbia\s+sportswear$/i] },
  { canonical: "Patagonia", patterns: [/^patagonia$/i] },
  { canonical: "Supreme", patterns: [/^supreme$/i] },
  { canonical: "Off-White", patterns: [/^off[\s-]*white$/i, /^offwhite$/i] },
  { canonical: "Balenciaga", patterns: [/^balenciaga$/i] },
  { canonical: "Gucci", patterns: [/^gucci$/i] },
  { canonical: "Louis Vuitton", patterns: [/^louis\s*vuitton$/i, /^lv$/i] },
  { canonical: "Yeezy", patterns: [/^yeezy$/i, /^adidas\s+yeezy$/i] },
  { canonical: "Fila", patterns: [/^fila$/i] },
  { canonical: "Tommy Hilfiger", patterns: [/^tommy\s*hilfiger$/i, /^tommy$/i] },
  { canonical: "Ralph Lauren", patterns: [/^ralph\s*lauren$/i, /^polo\s*ralph\s*lauren$/i, /^polo$/i] },
  { canonical: "Lacoste", patterns: [/^lacoste$/i] },
  { canonical: "Champion", patterns: [/^champion$/i] },
  { canonical: "Kappa", patterns: [/^kappa$/i] },
  { canonical: "Umbro", patterns: [/^umbro$/i] },
  { canonical: "Ellesse", patterns: [/^ellesse$/i] },
  { canonical: "Diadora", patterns: [/^diadora$/i] },
  { canonical: "Le Coq Sportif", patterns: [/^le\s*coq\s*sportif$/i, /^lecoqsportif$/i] },
  { canonical: "Mizuno", patterns: [/^mizuno$/i] },
  { canonical: "Karhu", patterns: [/^karhu$/i] },
  { canonical: "Crocs", patterns: [/^crocs$/i] },
  { canonical: "Skechers", patterns: [/^skechers$/i, /^skecher$/i] },
  { canonical: "Steve Madden", patterns: [/^steve\s*madden$/i] },
  { canonical: "Ecco", patterns: [/^ecco$/i] },
  { canonical: "Geox", patterns: [/^geox$/i] },
  { canonical: "Camper", patterns: [/^camper$/i] },
  { canonical: "Stussy", patterns: [/^stussy$/i, /^st[uü]ssy$/i] },
  { canonical: "Palace", patterns: [/^palace$/i, /^palace\s+skateboards$/i] },
  { canonical: "Kith", patterns: [/^kith$/i] },
  { canonical: "Carhartt", patterns: [/^carhartt$/i, /^carhartt\s+wip$/i] },
  { canonical: "Dickies", patterns: [/^dickies$/i] },
  { canonical: "Stone Island", patterns: [/^stone\s*island$/i] },
  { canonical: "Moncler", patterns: [/^moncler$/i] },
  { canonical: "Arc'teryx", patterns: [/^arc[\s'’]*teryx$/i] },
  { canonical: "Merrell", patterns: [/^merrell$/i] },
  { canonical: "Keen", patterns: [/^keen$/i] },
  { canonical: "Teva", patterns: [/^teva$/i] },
  { canonical: "Calvin Klein", patterns: [/^calvin\s*klein$/i, /^ck$/i] },
  { canonical: "Hugo Boss", patterns: [/^hugo\s*boss$/i, /^boss$/i, /^boss\s+by\s+hugo\s+boss$/i] }
];

export function uid(prefix = "monitor") {
  const random = Math.random().toString(36).slice(2, 10);
  return `${prefix}_${Date.now()}_${random}`;
}

export function normalizeText(value) {
  return (value ?? "")
    .replace(/\s+/g, " ")
    .replace(/\u00a0/g, " ")
    .trim();
}

export function canonicalizeBrand(value) {
  const normalized = normalizeText(value);
  if (!normalized) return "";
  for (const rule of CANONICAL_BRAND_RULES) {
    if (rule.patterns.some((pattern) => pattern.test(normalized))) {
      return rule.canonical;
    }
  }
  return normalized;
}

function normalizeMonitorBrandData(monitor) {
  if (!monitor || typeof monitor !== "object") return monitor;
  let changed = false;
  const next = { ...monitor };

  if (monitor.productData && typeof monitor.productData === "object") {
    const normalizedBrand = canonicalizeBrand(monitor.productData.brand);
    if (normalizedBrand && normalizedBrand !== monitor.productData.brand) {
      next.productData = { ...monitor.productData, brand: normalizedBrand };
      changed = true;
    }
  }

  if (monitor.productDataOverrides && typeof monitor.productDataOverrides === "object") {
    const normalizedOverrideBrand = canonicalizeBrand(monitor.productDataOverrides.brand);
    if (normalizedOverrideBrand && normalizedOverrideBrand !== monitor.productDataOverrides.brand) {
      next.productDataOverrides = { ...monitor.productDataOverrides, brand: normalizedOverrideBrand };
      changed = true;
    }
  }

  return changed ? next : monitor;
}

function normalizeMonitors(monitors) {
  let changed = false;
  const normalized = (Array.isArray(monitors) ? monitors : []).map((monitor) => {
    const next = normalizeMonitorBrandData(monitor);
    if (next !== monitor) changed = true;
    return next;
  });
  return { changed, monitors: normalized };
}

export function buildSelector(element) {
  if (!element || element.nodeType !== Node.ELEMENT_NODE) {
    return "";
  }

  if (element.id) {
    return `#${CSS.escape(element.id)}`;
  }

  const parts = [];
  let current = element;

  while (current && current.nodeType === Node.ELEMENT_NODE && current !== document.body) {
    let part = current.localName;
    if (!part) {
      break;
    }

    const classNames = Array.from(current.classList).slice(0, 2);
    if (classNames.length) {
      part += classNames.map((name) => `.${CSS.escape(name)}`).join("");
    }

    const parent = current.parentElement;
    if (parent) {
      const siblings = Array.from(parent.children).filter((child) => child.localName === current.localName);
      if (siblings.length > 1) {
        const index = siblings.indexOf(current) + 1;
        part += `:nth-of-type(${index})`;
      }
    }

    parts.unshift(part);

    const selector = parts.join(" > ");
    try {
      const resolved = document.querySelector(selector);
      if (resolved === element) {
        return selector;
      }
    } catch (error) {
      // Keep walking up until the selector is valid and unique.
    }

    current = parent;
  }

  return parts.join(" > ");
}

export function extractSnapshot(doc, monitor) {
  if (!doc) {
    return {
      ok: false,
      summary: "",
      error: "Document unavailable"
    };
  }

  if (!monitor.selector) {
    return {
      ok: true,
      summary: normalizeText(doc.body?.innerText || doc.documentElement?.textContent || ""),
      matched: "full-page"
    };
  }

  try {
    const element = doc.querySelector(monitor.selector);
    if (!element) {
      return {
        ok: false,
        summary: "",
        error: "Selector not found"
      };
    }

    return {
      ok: true,
      summary: normalizeText(element.innerText || element.textContent || ""),
      matched: monitor.selector
    };
  } catch (error) {
    return {
      ok: false,
      summary: "",
      error: `Invalid selector: ${error.message}`
    };
  }
}

// ── Activity log ──────────────────────────────────────────────────────────
const LOGS_KEY = "monitorLogs";
const MAX_LOGS = 500;

export async function addLog(entry) {
  const { [LOGS_KEY]: logs = [] } = await chrome.storage.local.get(LOGS_KEY);
  logs.unshift({ id: uid("log"), timestamp: new Date().toISOString(), ...entry });
  if (logs.length > MAX_LOGS) logs.length = MAX_LOGS;
  await chrome.storage.local.set({ [LOGS_KEY]: logs });
}

export async function getLogs() {
  const { [LOGS_KEY]: logs = [] } = await chrome.storage.local.get(LOGS_KEY);
  return logs;
}

export async function clearLogs() {
  await chrome.storage.local.set({ [LOGS_KEY]: [] });
}

export async function getMonitors() {
  const stored = await chrome.storage.local.get(STORAGE_KEY);
  const normalized = normalizeMonitors(stored[STORAGE_KEY] || []);
  if (normalized.changed) {
    await persistMonitors(normalized.monitors);
  }
  return normalized.monitors;
}

export async function saveMonitors(monitors) {
  const normalized = normalizeMonitors(monitors);
  await persistMonitors(normalized.monitors);
}

function isQuotaError(error) {
  const message = String(error?.message || error || "").toLowerCase();
  return message.includes("quota") || message.includes("kquotabytes");
}

function trimText(value, maxLength) {
  const text = typeof value === "string" ? value : "";
  if (!maxLength || text.length <= maxLength) return text;
  return `${text.slice(0, maxLength)}…`;
}

function compactChangeHistoryEntry(entry, dropText = false) {
  if (!entry || typeof entry !== "object") return entry;
  return {
    ...entry,
    previousText: dropText ? "" : trimText(entry.previousText, 500),
    currentText: dropText ? "" : trimText(entry.currentText, 500),
    previousHtml: "",
    currentHtml: "",
    htmlDiff: ""
  };
}

function compactMonitorForStorage(monitor, level = 1) {
  if (!monitor || typeof monitor !== "object") return monitor;
  if (level <= 0) return monitor;

  const compacted = { ...monitor };

  compacted.lastHtmlSnapshot = "";
  compacted.previousHtmlSnapshot = "";
  compacted.lastHtmlDiff = "";

  if (level >= 1) {
    compacted.initialFullPageText = trimText(compacted.initialFullPageText, 4000);
    compacted.lastSnapshot = trimText(compacted.lastSnapshot, 4000);
    compacted.previousSnapshot = trimText(compacted.previousSnapshot, 4000);
    compacted.changeHistory = Array.isArray(compacted.changeHistory)
      ? compacted.changeHistory.slice(0, 5).map((entry) => compactChangeHistoryEntry(entry, false))
      : [];
  }

  if (level >= 2) {
    compacted.initialFullPageText = "";
    compacted.lastSnapshot = trimText(compacted.lastSnapshot, 1000);
    compacted.previousSnapshot = trimText(compacted.previousSnapshot, 1000);
    compacted.changeHistory = Array.isArray(compacted.changeHistory)
      ? compacted.changeHistory.slice(0, 2).map((entry) => compactChangeHistoryEntry(entry, true))
      : [];
  }

  return compacted;
}

async function persistMonitors(monitors) {
  try {
    await chrome.storage.local.set({ [STORAGE_KEY]: monitors });
  } catch (error) {
    if (!isQuotaError(error)) throw error;
    try {
      const compacted = monitors.map((monitor) => compactMonitorForStorage(monitor, 1));
      await chrome.storage.local.set({ [STORAGE_KEY]: compacted });
    } catch (secondError) {
      if (!isQuotaError(secondError)) throw secondError;
      const compacted = monitors.map((monitor) => compactMonitorForStorage(monitor, 2));
      await chrome.storage.local.set({ [STORAGE_KEY]: compacted });
    }
  }
}

export function intervalLabel(minutes) {
  if (minutes >= 1440 && minutes % 1440 === 0) {
    const days = minutes / 1440;
    return days === 1 ? "1 day" : `${days} days`;
  }

  if (minutes >= 60 && minutes % 60 === 0) {
    const hours = minutes / 60;
    return hours === 1 ? "1 hr" : `${hours} hr`;
  }

  return `${minutes} min`;
}

export function buildHtmlDiff(previousHtml, currentHtml) {
  return buildDiffRows(previousHtml, currentHtml)
    .flatMap((row) => {
      const lines = [];

      if (row.leftType === "removed" && row.left) {
        lines.push(`- ${row.left}`);
      }

      if (row.rightType === "added" && row.right) {
        lines.push(`+ ${row.right}`);
      }

      return lines;
    })
    .join("\n");
}

export function buildDiffRows(previousHtml, currentHtml) {
  const beforeLines = splitHtmlLines(previousHtml);
  const afterLines = splitHtmlLines(currentHtml);
  const lcs = buildLcsMatrix(beforeLines, afterLines);
  const operations = [];
  let beforeIndex = beforeLines.length;
  let afterIndex = afterLines.length;

  while (beforeIndex > 0 && afterIndex > 0) {
    if (beforeLines[beforeIndex - 1] === afterLines[afterIndex - 1]) {
      operations.push({
        kind: "same",
        value: beforeLines[beforeIndex - 1]
      });
      beforeIndex -= 1;
      afterIndex -= 1;
    } else if (lcs[beforeIndex - 1][afterIndex] >= lcs[beforeIndex][afterIndex - 1]) {
      operations.push({
        kind: "removed",
        value: beforeLines[beforeIndex - 1]
      });
      beforeIndex -= 1;
    } else {
      operations.push({
        kind: "added",
        value: afterLines[afterIndex - 1]
      });
      afterIndex -= 1;
    }
  }

  while (beforeIndex > 0) {
    operations.push({
      kind: "removed",
      value: beforeLines[beforeIndex - 1]
    });
    beforeIndex -= 1;
  }

  while (afterIndex > 0) {
    operations.push({
      kind: "added",
      value: afterLines[afterIndex - 1]
    });
    afterIndex -= 1;
  }

  operations.reverse();

  const rows = [];
  for (let index = 0; index < operations.length; index += 1) {
    const current = operations[index];

    if (current.kind === "same") {
      rows.push({
        left: current.value,
        right: current.value,
        leftType: "same",
        rightType: "same"
      });
      continue;
    }

    if (current.kind === "removed") {
      const next = operations[index + 1];
      if (next?.kind === "added") {
        rows.push({
          left: current.value,
          right: next.value,
          leftType: "removed",
          rightType: "added"
        });
        index += 1;
        continue;
      }

      rows.push({
        left: current.value,
        right: "",
        leftType: "removed",
        rightType: "empty"
      });
      continue;
    }

    rows.push({
      left: "",
      right: current.value,
      leftType: "empty",
      rightType: "added"
    });
  }

  return rows;
}

export function summarizeDiff(rows) {
  const summary = {
    added: 0,
    removed: 0,
    changed: 0,
    unchanged: 0
  };

  for (const row of rows) {
    if (row.leftType === "same" && row.rightType === "same") {
      summary.unchanged += 1;
      continue;
    }

    if (row.leftType === "removed" && row.rightType === "added") {
      summary.changed += 1;
      continue;
    }

    if (row.leftType === "removed") {
      summary.removed += 1;
    }

    if (row.rightType === "added") {
      summary.added += 1;
    }
  }

  return summary;
}

function splitHtmlLines(html) {
  return (html ?? "")
    .replace(/></g, ">\n<")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function buildLcsMatrix(beforeLines, afterLines) {
  const rows = beforeLines.length + 1;
  const cols = afterLines.length + 1;
  const matrix = Array.from({ length: rows }, () => Array(cols).fill(0));

  for (let i = 1; i < rows; i += 1) {
    for (let j = 1; j < cols; j += 1) {
      if (beforeLines[i - 1] === afterLines[j - 1]) {
        matrix[i][j] = matrix[i - 1][j - 1] + 1;
      } else {
        matrix[i][j] = Math.max(matrix[i - 1][j], matrix[i][j - 1]);
      }
    }
  }

  return matrix;
}

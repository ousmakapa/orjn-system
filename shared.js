const STORAGE_KEY = "pageMonitors";

export const DEFAULT_INTERVAL_MINUTES = 1440;

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
  return stored[STORAGE_KEY] || [];
}

export async function saveMonitors(monitors) {
  await chrome.storage.local.set({
    [STORAGE_KEY]: monitors
  });
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

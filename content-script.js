(function () {
  const STATE = {
    active: false,
    hovered: null,
    overlay: null,
    label: null,
    panel: null,
    pickedSelectors: []
  };

  function buildSelector(element) {
    if (!element || element.nodeType !== Node.ELEMENT_NODE) return "";

    if (element.id) return `#${CSS.escape(element.id)}`;

    const parts = [];
    let current = element;

    while (current && current.nodeType === Node.ELEMENT_NODE && current !== document.body) {
      let part = current.localName;
      if (!part) break;

      const classNames = Array.from(current.classList).slice(0, 2);
      if (classNames.length) part += classNames.map((n) => `.${CSS.escape(n)}`).join("");

      const parent = current.parentElement;
      if (parent) {
        const siblings = Array.from(parent.children).filter((c) => c.localName === current.localName);
        if (siblings.length > 1) part += `:nth-of-type(${siblings.indexOf(current) + 1})`;
      }

      parts.unshift(part);

      try {
        const selector = parts.join(" > ");
        if (document.querySelector(selector) === element) return selector;
      } catch (_) {}

      current = parent;
    }

    return parts.join(" > ");
  }

  function ensureOverlay() {
    if (STATE.overlay) return;

    const overlay = document.createElement("div");
    overlay.style.cssText = "position:fixed;z-index:2147483646;pointer-events:none;border:2px solid #ff5a36;background:rgba(255,90,54,0.14);display:none;box-sizing:border-box;";

    const label = document.createElement("div");
    label.style.cssText = "position:fixed;z-index:2147483647;pointer-events:none;padding:5px 8px;border-radius:8px;font:11px/1.4 system-ui,sans-serif;color:#fff;background:#1e293b;display:none;max-width:260px;word-break:break-all;";

    document.documentElement.appendChild(overlay);
    document.documentElement.appendChild(label);
    STATE.overlay = overlay;
    STATE.label = label;
  }

  function showHighlight(element) {
    if (!element || !STATE.overlay || !STATE.label) return;
    const rect = element.getBoundingClientRect();
    STATE.overlay.style.cssText += `display:block;left:${rect.left}px;top:${rect.top}px;width:${rect.width}px;height:${rect.height}px;`;
    STATE.label.style.display = "block";
    STATE.label.style.left = `${Math.max(8, rect.left)}px`;
    STATE.label.style.top = `${Math.max(8, rect.top - 32)}px`;
    STATE.label.textContent = buildSelector(element) || element.tagName.toLowerCase();
  }

  function hideHighlight() {
    if (STATE.overlay) STATE.overlay.style.display = "none";
    if (STATE.label) STATE.label.style.display = "none";
  }

  function escHtml(v) {
    return (v ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
  }

  function getOrCreatePanel() {
    if (STATE.panel) return STATE.panel;

    const panel = document.createElement("div");
    panel.setAttribute("data-monitor-panel", "true");
    panel.style.cssText = `
      position: fixed;
      bottom: 20px;
      right: 20px;
      z-index: 2147483647;
      background: #fff;
      border: 2px solid #ff5a36;
      border-radius: 16px;
      padding: 16px;
      width: 310px;
      box-shadow: 0 12px 40px rgba(0,0,0,0.22);
      font: 14px/1.5 "Segoe UI", system-ui, sans-serif;
      color: #17202a;
    `;

    panel.innerHTML = `
      <p style="font-weight:700;margin:0 0 4px;font-size:15px">Add monitor</p>
      <p style="font-size:12px;color:#5f6c7b;margin:0 0 10px">Click elements on the page · ESC to cancel</p>
      <div id="mpanel-selectors" style="display:flex;flex-wrap:wrap;gap:6px;min-height:24px;margin-bottom:8px"></div>
      <p id="mpanel-hint" style="font-size:12px;color:#9ca3af;margin:0 0 10px">No elements selected yet — click anything on the page</p>
      <input id="mpanel-name" type="text" placeholder="Monitor name" style="width:100%;box-sizing:border-box;padding:9px 11px;border:1px solid rgba(23,32,42,0.18);border-radius:10px;font:inherit;color:#17202a;margin-bottom:8px;outline:none;">
      <select id="mpanel-type" style="width:100%;box-sizing:border-box;padding:9px 11px;border:1px solid rgba(23,32,42,0.18);border-radius:10px;font:inherit;color:#17202a;margin-bottom:10px;outline:none;background:#fff;cursor:pointer;">
        <option value="">Type — auto detect</option>
        <option value="Lifestyle">Lifestyle</option>
        <option value="Basketball">Basketball</option>
        <option value="Running">Running</option>
        <option value="Training">Training</option>
        <option value="Football">Football</option>
        <option value="Soccer">Soccer</option>
        <option value="Tennis">Tennis</option>
        <option value="Golf">Golf</option>
        <option value="Hiking">Hiking</option>
        <option value="Trail">Trail</option>
        <option value="Skate">Skate</option>
        <option value="Sandal">Sandal</option>
        <option value="Boot">Boot</option>
      </select>
      <div style="display:flex;gap:8px;">
        <button id="mpanel-cancel" style="flex:1;padding:9px 0;border:1px solid rgba(23,32,42,0.15);background:#fff;border-radius:10px;cursor:pointer;font:inherit;color:#17202a;">Cancel</button>
        <button id="mpanel-create" style="flex:1;padding:9px 0;background:#ff5a36;color:#fff;border:none;border-radius:10px;cursor:pointer;font:inherit;font-weight:600;">Create monitor</button>
      </div>
    `;

    document.documentElement.appendChild(panel);
    STATE.panel = panel;

    panel.querySelector("#mpanel-name").value = document.title || "";

    panel.querySelector("#mpanel-cancel").addEventListener("click", (e) => {
      e.stopPropagation();
      stopPicking();
    });

    panel.querySelector("#mpanel-create").addEventListener("click", (e) => {
      e.stopPropagation();
      createMonitorFromPanel();
    });

    return panel;
  }

  function updatePanelHint() {
    const hint = STATE.panel?.querySelector("#mpanel-hint");
    if (!hint) return;
    hint.style.display = STATE.pickedSelectors.length === 0 ? "" : "none";
  }

  function addSelectorChip(selector) {
    if (STATE.pickedSelectors.includes(selector)) return;
    STATE.pickedSelectors.push(selector);

    const list = STATE.panel.querySelector("#mpanel-selectors");
    const chip = document.createElement("span");
    chip.style.cssText = "display:inline-flex;align-items:center;gap:3px;background:rgba(255,90,54,0.12);color:#d63f1d;padding:3px 8px;border-radius:999px;font-size:11px;max-width:220px;";
    chip.title = selector;

    const text = document.createElement("span");
    text.style.cssText = "overflow:hidden;text-overflow:ellipsis;white-space:nowrap;";
    text.textContent = selector;

    const removeBtn = document.createElement("button");
    removeBtn.textContent = "×";
    removeBtn.style.cssText = "background:none;border:none;cursor:pointer;padding:0 0 0 3px;color:inherit;font-size:14px;line-height:1;flex-shrink:0;";
    removeBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      STATE.pickedSelectors = STATE.pickedSelectors.filter((s) => s !== selector);
      chip.remove();
      updatePanelHint();
    });

    chip.appendChild(text);
    chip.appendChild(removeBtn);
    list.appendChild(chip);
    updatePanelHint();
  }

  function createMonitorFromPanel() {
    const name = STATE.panel.querySelector("#mpanel-name").value.trim() || document.title || "Page monitor";
    const selectors = [...STATE.pickedSelectors];
    const selectedType = STATE.panel.querySelector("#mpanel-type").value || null;
    const createBtn = STATE.panel.querySelector("#mpanel-create");
    const hint = STATE.panel.querySelector("#mpanel-hint");

    createBtn.textContent = "Creating…";
    createBtn.disabled = true;

    chrome.runtime.sendMessage(
      {
        type: "create-monitor",
        payload: {
          name, url: location.href, selectors, autoCheck: false, intervalMinutes: 1440,
          productDataOverrides: selectedType ? { type: selectedType } : null
        }
      },
      (response) => {
        if (!response?.ok) {
          createBtn.textContent = "Create monitor";
          createBtn.disabled = false;
          if (hint) {
            hint.textContent = `Error: ${response?.error || "Unknown error"}`;
            hint.style.display = "";
            hint.style.color = "#b41f1f";
          }
        } else {
          createBtn.textContent = "Created!";
          createBtn.style.background = "#0f766e";
          setTimeout(() => stopPicking(), 1400);
        }
      }
    );
  }

  function handleMove(event) {
    if (!STATE.active) return;
    const target = event.target;
    if (!(target instanceof Element)) return;
    if (STATE.panel?.contains(target)) return;
    if (target === STATE.overlay || target === STATE.label) return;
    STATE.hovered = target;
    showHighlight(target);
  }

  function handleKeydown(event) {
    if (event.key === "Escape") stopPicking();
  }

  function handleClick(event) {
    if (!STATE.active) return;
    if (STATE.panel?.contains(event.target)) return;

    event.preventDefault();
    event.stopPropagation();

    const element = event.target;
    if (!(element instanceof Element)) return;
    if (element === STATE.overlay || element === STATE.label) return;

    const selector = buildSelector(element);
    if (selector) {
      getOrCreatePanel();
      addSelectorChip(selector);
    }
  }

  function startPicking() {
    if (STATE.active) return;
    ensureOverlay();
    getOrCreatePanel();
    STATE.active = true;
    document.addEventListener("mousemove", handleMove, true);
    document.addEventListener("click", handleClick, true);
    document.addEventListener("keydown", handleKeydown, true);
  }

  function stopPicking() {
    STATE.active = false;
    STATE.hovered = null;
    STATE.pickedSelectors = [];
    hideHighlight();
    if (STATE.panel) {
      STATE.panel.remove();
      STATE.panel = null;
    }
    document.removeEventListener("mousemove", handleMove, true);
    document.removeEventListener("click", handleClick, true);
    document.removeEventListener("keydown", handleKeydown, true);
  }

  // ── Multi-picker (select several product cards) ──────────────────────────
  const MULTI = { active: false, selected: new Map(), panel: null };
  const FL_SELECTORS = [".ProductPrice", "#tabPanel"];

  function startMultiPicker() {
    if (MULTI.active) return;
    MULTI.active = true;
    MULTI.selected.clear();
    document.querySelectorAll(".ProductCard").forEach(card => {
      const link = card.querySelector("a.ProductCard-link");
      if (!link) return;
      card.style.position = "relative";
      const chk = document.createElement("div");
      chk.className = "__mchk";
      chk.style.cssText = "position:absolute;top:8px;left:8px;z-index:2147483640;width:22px;height:22px;border-radius:50%;border:2px solid #ff5a36;background:#fff;box-sizing:border-box;pointer-events:none;";
      card.appendChild(chk);
      card.addEventListener("click", onCardClick, true);
    });
    createMultiPanel();
    document.addEventListener("keydown", onMultiKey, true);
  }

  function stopMultiPicker() {
    MULTI.active = false;
    MULTI.selected.clear();
    document.querySelectorAll(".ProductCard").forEach(card => {
      card.removeEventListener("click", onCardClick, true);
      card.querySelector(".__mchk")?.remove();
      card.style.outline = "";
    });
    MULTI.panel?.remove();
    MULTI.panel = null;
    document.removeEventListener("keydown", onMultiKey, true);
  }

  function onMultiKey(e) { if (e.key === "Escape") stopMultiPicker(); }

  function onCardClick(e) {
    if (MULTI.panel?.contains(e.target)) return;
    e.preventDefault(); e.stopPropagation();
    const card = e.currentTarget;
    const link = card.querySelector("a.ProductCard-link");
    if (!link) return;
    const href = link.getAttribute("href") || "";
    const url = href.startsWith("http") ? href : `https://www.footlocker.com${href}`;
    const name = card.querySelector(".ProductName-primary")?.textContent?.trim() || url;
    const chk = card.querySelector(".__mchk");
    if (MULTI.selected.has(url)) {
      MULTI.selected.delete(url);
      card.style.outline = "";
      if (chk) { chk.style.background = "#fff"; chk.textContent = ""; }
    } else {
      MULTI.selected.set(url, { url, name });
      card.style.outline = "2px solid #ff5a36";
      if (chk) { chk.style.background = "#ff5a36"; chk.style.cssText += "display:flex;align-items:center;justify-content:center;color:#fff;font-size:13px;font-weight:700;"; chk.textContent = "✓"; }
    }
    updateMultiPanel();
  }

  function createMultiPanel() {
    if (MULTI.panel) return;
    const panel = document.createElement("div");
    panel.style.cssText = "position:fixed;bottom:20px;right:20px;z-index:2147483647;background:#fff;border:2px solid #ff5a36;border-radius:16px;padding:16px;width:280px;box-shadow:0 12px 40px rgba(0,0,0,0.22);font:14px/1.5 \"Segoe UI\",system-ui,sans-serif;color:#17202a;";
    panel.innerHTML = `
      <p style="font-weight:700;margin:0 0 4px;font-size:15px">Monitor several shoes</p>
      <p style="font-size:12px;color:#5f6c7b;margin:0 0 10px">Click shoes to select · ESC to cancel</p>
      <p id="mpanel-multi-count" style="font-size:13px;color:#ff5a36;font-weight:600;margin:0 0 10px">0 shoes selected</p>
      <select id="mpanel-multi-type" style="width:100%;box-sizing:border-box;padding:9px 11px;border:1px solid rgba(23,32,42,0.18);border-radius:10px;font:inherit;color:#17202a;margin-bottom:12px;outline:none;background:#fff;cursor:pointer;">
        <option value="">Type — auto detect</option>
        <option value="Lifestyle">Lifestyle</option>
        <option value="Basketball">Basketball</option>
        <option value="Running">Running</option>
        <option value="Training">Training</option>
        <option value="Football">Football</option>
        <option value="Soccer">Soccer</option>
        <option value="Tennis">Tennis</option>
        <option value="Golf">Golf</option>
        <option value="Hiking">Hiking</option>
        <option value="Trail">Trail</option>
        <option value="Skate">Skate</option>
        <option value="Sandal">Sandal</option>
        <option value="Boot">Boot</option>
      </select>
      <div style="display:flex;gap:8px;">
        <button id="mpanel-multi-cancel" style="flex:1;padding:9px 0;border:1px solid rgba(23,32,42,0.15);background:#fff;border-radius:10px;cursor:pointer;font:inherit;color:#17202a;">Cancel</button>
        <button id="mpanel-multi-create" style="flex:1;padding:9px 0;background:#ff5a36;color:#fff;border:none;border-radius:10px;cursor:pointer;font:inherit;font-weight:600;">Create monitors</button>
      </div>
      <p id="mpanel-multi-status" style="font-size:11px;color:#9ca3af;margin:8px 0 0;min-height:16px;"></p>
    `;
    document.documentElement.appendChild(panel);
    MULTI.panel = panel;
    panel.querySelector("#mpanel-multi-cancel").addEventListener("click", e => { e.stopPropagation(); stopMultiPicker(); });
    panel.querySelector("#mpanel-multi-create").addEventListener("click", e => { e.stopPropagation(); createMonitorsBatch(); });
  }

  function updateMultiPanel() {
    const c = MULTI.panel?.querySelector("#mpanel-multi-count");
    if (c) c.textContent = `${MULTI.selected.size} shoe${MULTI.selected.size !== 1 ? "s" : ""} selected`;
  }

  function createMonitorsBatch() {
    const items = [...MULTI.selected.values()];
    if (!items.length) return;
    const createBtn = MULTI.panel.querySelector("#mpanel-multi-create");
    const cancelBtn = MULTI.panel.querySelector("#mpanel-multi-cancel");
    const status = MULTI.panel.querySelector("#mpanel-multi-status");
    const selectedType = MULTI.panel.querySelector("#mpanel-multi-type")?.value || "";
    const productDataOverrides = selectedType ? { type: selectedType } : null;
    createBtn.disabled = true;
    createBtn.textContent = "Creating…";
    if (status) status.textContent = `0/${items.length} — opening tabs…`;
    chrome.runtime.sendMessage({
      type: "create-monitors-batch",
      payload: { items, selectors: FL_SELECTORS, productDataOverrides }
    }, (resp) => {
      if (!resp?.ok) {
        createBtn.textContent = "Error";
        if (status) { status.textContent = resp?.error || "Failed"; status.style.color = "#b41f1f"; }
        createBtn.disabled = false;
      }
    });
  }

  function handleBatchProgress(done, total, errors) {
    if (!MULTI.panel) return;
    const createBtn = MULTI.panel.querySelector("#mpanel-multi-create");
    const status = MULTI.panel.querySelector("#mpanel-multi-status");
    if (done < total) {
      if (status) status.textContent = `${done}/${total} done${errors ? ` · ${errors} error${errors > 1 ? "s" : ""}` : ""}`;
    } else {
      if (createBtn) { createBtn.textContent = errors ? `Done (${errors} error${errors > 1 ? "s" : ""})` : "All created!"; createBtn.style.background = errors ? "#b45309" : "#0f766e"; createBtn.disabled = false; }
      if (status) { status.textContent = errors ? `${total - errors} ok · ${errors} failed (check errors tab)` : `${total} monitors ready`; status.style.color = errors ? "#b41f1f" : "#16a34a"; }
      const cancelBtn = MULTI.panel.querySelector("#mpanel-multi-cancel");
      if (cancelBtn) { cancelBtn.textContent = "Close"; }
    }
  }

  // ── Message listener ──────────────────────────────────────────────────────
  chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    if (message.type === "start-picker") {
      startPicking();
      sendResponse({ ok: true });
      return;
    }
    if (message.type === "stop-picker") {
      stopPicking();
      sendResponse({ ok: true });
      return;
    }
    if (message.type === "start-multi-picker") {
      startMultiPicker();
      sendResponse({ ok: true });
      return;
    }
    if (message.type === "stop-multi-picker") {
      stopMultiPicker();
      sendResponse({ ok: true });
      return;
    }
    if (message.type === "batch-progress") {
      handleBatchProgress(message.done, message.total, message.errors);
      sendResponse({ ok: true });
    }
  });
})();

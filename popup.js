async function getCurrentTab() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab;
}

function isMonitorableUrl(url) {
  return /^https?:\/\//i.test(url || "");
}

async function startPicker() {
  const tab = await getCurrentTab();
  if (!tab?.id || !isMonitorableUrl(tab.url)) {
    window.alert("Open a regular http or https page before starting the picker.");
    return;
  }
  await chrome.tabs.sendMessage(tab.id, { type: "start-picker" });
  window.close();
}

async function monitorFullPage() {
  const tab = await getCurrentTab();
  if (!tab?.id || !isMonitorableUrl(tab.url)) {
    window.alert("Open a regular http or https page before creating a monitor.");
    return;
  }

  const btn = document.getElementById("monitor-page");
  btn.disabled = true;
  btn.querySelector("strong").textContent = "Creating…";

  const response = await chrome.runtime.sendMessage({
    type: "create-monitor",
    payload: {
      name: tab.title ? `${tab.title} – full page` : "Full page monitor",
      url: tab.url,
      selectors: [],
      autoCheck: false,
      intervalMinutes: 1440,
      tabId: tab.id
    }
  });

  if (!response?.ok) {
    window.alert(response?.error || "Unable to create monitor");
    btn.disabled = false;
    btn.querySelector("strong").textContent = "Monitor full page";
    return;
  }

  window.close();
}

document.getElementById("pick-element").addEventListener("click", startPicker);
document.getElementById("monitor-page").addEventListener("click", monitorFullPage);
document.getElementById("open-dashboard").addEventListener("click", () => chrome.runtime.openOptionsPage());

document.getElementById("monitor-several").addEventListener("click", async () => {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.id) return;
  await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ["content-script.js"] }).catch(() => {});
  chrome.tabs.sendMessage(tab.id, { type: "start-multi-picker" });
  window.close();
});

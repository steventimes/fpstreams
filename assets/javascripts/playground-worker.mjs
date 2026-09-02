import { loadPyodide } from "https://cdn.jsdelivr.net/pyodide/v314.0.4/full/pyodide.mjs";

const PYODIDE_INDEX = "https://cdn.jsdelivr.net/pyodide/v314.0.4/full/";
const manifestUrl = new URL("../packages/browser-wheel.json", import.meta.url);

let activeId = null;
let namespace = null;

async function initialize() {
  self.postMessage({ type: "status", message: "Loading CPython 3.14 and WebAssembly…" });
  const pyodide = await loadPyodide({ indexURL: PYODIDE_INDEX });

  self.postMessage({ type: "status", message: "Loading the fpstreams browser wheel…" });
  const response = await fetch(manifestUrl);
  if (!response.ok) {
    throw new Error(`Browser wheel manifest returned HTTP ${response.status}.`);
  }
  const manifest = await response.json();
  const wheelUrl = new URL(`../packages/${manifest.wheel}`, import.meta.url).href;
  await pyodide.loadPackage("micropip");
  pyodide.globals.set("_fpstreams_wheel_url", wheelUrl);
  await pyodide.runPythonAsync(`
import micropip
await micropip.install(_fpstreams_wheel_url)
del _fpstreams_wheel_url
`);

  namespace = pyodide.globals.get("dict")();
  namespace.set("__name__", "__main__");
  pyodide.setStdout({
    batched(text) {
      self.postMessage({ type: "stdout", id: activeId, text });
    },
  });
  pyodide.setStderr({
    batched(text) {
      self.postMessage({ type: "stderr", id: activeId, text });
    },
  });
  self.postMessage({ type: "ready", version: manifest.version });
  return pyodide;
}

const pyodidePromise = initialize().catch((error) => {
  self.postMessage({
    type: "fatal",
    error: `Unable to initialize the browser runtime.\n${error.stack || error.message}`,
  });
  throw error;
});

self.addEventListener("message", async (event) => {
  const payload = event.data ?? {};
  if (payload.type !== "run") {
    return;
  }
  activeId = payload.id;
  try {
    const pyodide = await pyodidePromise;
    const value = await pyodide.runPythonAsync(payload.code, { globals: namespace });
    let rendered = "";
    if (value !== null && value !== undefined) {
      namespace.set("__fpstreams_browser_result__", value);
      rendered = pyodide.runPython("repr(__fpstreams_browser_result__)", {
        globals: namespace,
      });
      namespace.delete("__fpstreams_browser_result__");
    }
    if (value && typeof value.destroy === "function") {
      value.destroy();
    }
    self.postMessage({ type: "result", id: payload.id, result: rendered });
  } catch (error) {
    self.postMessage({
      type: "error",
      id: payload.id,
      error: error.stack || error.message || String(error),
    });
  }
});

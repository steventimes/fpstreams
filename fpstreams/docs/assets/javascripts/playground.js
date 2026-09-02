const root = document.querySelector("[data-fp-playground]");

if (root) {
  const examples = {
    flow: `from fpstreams import flow, item

(
    flow(range(12))
    .map(item * 3)
    .filter(item % 2 == 0)
    .chunk(3)
    .to_list()
)`,
    rows: `from fpstreams import col, flow

orders = [
    {"region": "eu", "status": "paid", "amount": 24},
    {"region": "us", "status": "open", "amount": 18},
    {"region": "eu", "status": "paid", "amount": 31},
]

(
    flow(orders)
    .filter(col("status") == "paid")
    .select("region", "amount")
    .sort_by("amount", reverse=True)
    .to_list()
)`,
    group_join: `from fpstreams import agg, rows

orders = rows([
    {"region": "eu", "amount": 24},
    {"region": "us", "amount": 18},
    {"region": "eu", "amount": 31},
])
regions = rows([
    {"region": "eu", "label": "Europe"},
    {"region": "us", "label": "United States"},
])

(
    orders.group_by("region")
    .aggregate(total=agg.sum("amount"), orders=agg.count())
    .join(regions, on="region", validate="m:1")
    .sort_by("region")
    .to_list()
)`,
    pairs: `from fpstreams import pairs

(
    pairs([("tea", 2), ("coffee", 4), ("water", 1)])
    .map_values(lambda value: value * 3)
    .filter_pairs(lambda _key, value: value >= 6)
    .to_dict()
)`,
    async: `import asyncio
from fpstreams import aflow

async def enrich(value: int) -> dict[str, int]:
    await asyncio.sleep(0.02)
    return {"value": value, "square": value * value}

await aflow(range(6)).map_async(enrich, concurrency=3).to_list()`,
  };

  const code = root.querySelector("[data-code]");
  const status = root.querySelector(".fp-playground__status");
  const statusText = root.querySelector("[data-status-text]");
  const stdout = root.querySelector("[data-stdout]");
  const result = root.querySelector("[data-result]");
  const error = root.querySelector("[data-error]");
  const stdoutChannel = root.querySelector('[data-channel="stdout"]');
  const errorChannel = root.querySelector('[data-channel="error"]');
  const runButton = root.querySelector("[data-run]");
  const stopButton = root.querySelector("[data-stop]");
  const resetButton = root.querySelector("[data-reset]");
  const elapsed = root.querySelector("[data-elapsed]");
  const exampleButtons = [...root.querySelectorAll("[data-example]")];
  const workerUrl = new URL("./playground-worker.mjs", import.meta.url);

  let worker;
  let ready = false;
  let running = false;
  let requestId = 0;
  let startedAt = 0;

  function setStatus(kind, message) {
    status.dataset.status = kind;
    statusText.textContent = message;
  }

  function setControls() {
    runButton.disabled = !ready || running;
    stopButton.disabled = !worker || (!running && ready);
    resetButton.disabled = running;
  }

  function clearOutput() {
    stdout.textContent = "";
    result.textContent = "";
    error.textContent = "";
    stdoutChannel.hidden = true;
    errorChannel.hidden = true;
    elapsed.textContent = "running";
  }

  function appendStdout(text) {
    stdoutChannel.hidden = false;
    stdout.textContent += `${text}\n`;
  }

  function finish(kind, message) {
    running = false;
    const seconds = (performance.now() - startedAt) / 1000;
    elapsed.textContent = `${seconds.toFixed(2)} s`;
    setStatus(kind, message);
    setControls();
  }

  function startWorker(message = "Loading Python runtime…") {
    if (worker) {
      worker.terminate();
    }
    ready = false;
    running = false;
    setStatus("loading", message);
    elapsed.textContent = "idle";
    setControls();
    worker = new Worker(workerUrl, { type: "module" });

    worker.addEventListener("message", (event) => {
      const payload = event.data ?? {};
      if (payload.type === "status") {
        setStatus("loading", payload.message);
        return;
      }
      if (payload.type === "ready") {
        ready = true;
        setStatus("ready", `Ready · fpstreams ${payload.version}`);
        setControls();
        return;
      }
      if (payload.type === "fatal") {
        ready = false;
        running = false;
        errorChannel.hidden = false;
        error.textContent = payload.error;
        setStatus("error", "Runtime unavailable");
        elapsed.textContent = "failed";
        setControls();
        return;
      }
      if (payload.id !== requestId) {
        return;
      }
      if (payload.type === "stdout" || payload.type === "stderr") {
        appendStdout(payload.text);
        return;
      }
      if (payload.type === "result") {
        result.textContent = payload.result || "None";
        finish("ready", "Ready");
        return;
      }
      if (payload.type === "error") {
        errorChannel.hidden = false;
        error.textContent = payload.error;
        finish("error", "Execution failed");
      }
    });

    worker.addEventListener("error", (event) => {
      ready = false;
      running = false;
      errorChannel.hidden = false;
      error.textContent = event.message || "The browser worker failed to load.";
      setStatus("error", "Runtime unavailable");
      elapsed.textContent = "failed";
      setControls();
    });
  }

  function runCode() {
    if (!ready || running) {
      return;
    }
    requestId += 1;
    startedAt = performance.now();
    running = true;
    clearOutput();
    setStatus("running", "Executing in browser worker…");
    setControls();
    worker.postMessage({ type: "run", id: requestId, code: code.value });
  }

  function chooseExample(name) {
    code.value = examples[name];
    for (const button of exampleButtons) {
      button.setAttribute("aria-pressed", String(button.dataset.example === name));
    }
    code.focus();
  }

  runButton.addEventListener("click", runCode);
  stopButton.addEventListener("click", () => {
    requestId += 1;
    startWorker("Stopped · rebuilding clean runtime…");
    result.textContent = "Execution stopped. The namespace was reset.";
    elapsed.textContent = "stopped";
  });
  resetButton.addEventListener("click", () => {
    requestId += 1;
    chooseExample("flow");
    clearOutput();
    result.textContent = "Runtime reset. Run the example when Ready appears.";
    startWorker("Resetting Python runtime…");
  });
  code.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) {
      event.preventDefault();
      runCode();
    }
  });
  for (const button of exampleButtons) {
    button.addEventListener("click", () => chooseExample(button.dataset.example));
  }

  chooseExample("flow");
  startWorker();
}

# Try fpstreams in your browser

The page installs fpstreams' pure-Python wheel into a dedicated Pyodide worker.
Code runs in that worker and is not sent to an fpstreams server. Initial load
time depends on the network and browser cache.

<div class="fp-playground" data-fp-playground>
  <div class="fp-playground__masthead">
    <div>
      <p class="fp-playground__eyebrow">Runs locally in your browser</p>
      <h2>Inspect a pipeline while you learn it</h2>
    </div>
    <div class="fp-playground__status" data-status="loading" role="status" aria-live="polite">
      <span class="fp-playground__status-light" aria-hidden="true"></span>
      <span data-status-text>Loading Python runtime…</span>
    </div>
  </div>

  <div class="fp-playground__examples" role="group" aria-label="Example programs">
    <button type="button" data-example="flow" aria-pressed="true">Flow</button>
    <button type="button" data-example="rows" aria-pressed="false">Rows</button>
    <button type="button" data-example="group_join" aria-pressed="false">Group + join</button>
    <button type="button" data-example="pairs" aria-pressed="false">Pairs</button>
    <button type="button" data-example="async" aria-pressed="false">AsyncFlow</button>
  </div>

  <div class="fp-playground__workspace">
    <section class="fp-playground__pane fp-playground__editor-pane" aria-labelledby="fp-editor-title">
      <div class="fp-playground__pane-heading">
        <span id="fp-editor-title">Python</span>
        <span class="fp-playground__shortcut">Ctrl/⌘ + Enter</span>
      </div>
      <label class="sr-only" for="fp-playground-code">Python source code</label>
      <textarea id="fp-playground-code" data-code spellcheck="false" autocapitalize="off" autocomplete="off" aria-describedby="fp-playground-help"></textarea>
      <p id="fp-playground-help" class="fp-playground__help">The namespace is retained between runs. Reset starts a clean interpreter.</p>
    </section>

    <section class="fp-playground__pane fp-playground__output-pane" aria-labelledby="fp-output-title">
      <div class="fp-playground__pane-heading">
        <span id="fp-output-title">Execution output</span>
        <span data-elapsed>idle</span>
      </div>
      <div class="fp-playground__channel" data-channel="stdout" hidden>
        <span>stdout</span>
        <pre data-stdout></pre>
      </div>
      <div class="fp-playground__channel fp-playground__channel--result" data-channel="result">
        <span>result</span>
        <pre data-result>Run the example to see its value.</pre>
      </div>
      <div class="fp-playground__channel fp-playground__channel--error" data-channel="error" hidden>
        <span>error</span>
        <pre data-error></pre>
      </div>
    </section>
  </div>

  <div class="fp-playground__controls">
    <button type="button" class="fp-playground__run" data-run disabled>Run code</button>
    <button type="button" data-stop disabled>Stop</button>
    <button type="button" data-reset>Reset runtime</button>
    <span>Python 3.14 · fpstreams pure-Python engine</span>
  </div>
</div>

<noscript>This playground requires JavaScript. All ordinary documentation remains available without it.</noscript>

## Browser scope

The playground is intended for core pipeline exploration:

- `Flow`, `Rows`, `Pairs`, collectors, expressions, and `AsyncFlow` run locally;
- the `auto` engine selects the canonical Python path because the CPython/Rust
  extension is not a WebAssembly wheel;
- stopping code terminates the worker, including an accidental infinite loop;
- local operating-system paths, process pools, and native-only execution are not
  available inside the browser sandbox;
- the runtime and wheel are fetched from the network on first use. Resetting the
  runtime downloads them again when they are not already in the browser cache.

For production workloads, install the regular package to gain Rust acceleration,
filesystem access, optional data-system adapters, and normal profiling tools.

<script type="module" src="../assets/javascripts/playground.js"></script>

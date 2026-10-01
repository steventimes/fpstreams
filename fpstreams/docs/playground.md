# Try fpstreams in your browser

Edit an example and run it in your browser. The page downloads Python and the
fpstreams pure-Python wheel, then runs your code in a separate worker. It does
not send your code to an fpstreams server. The first load can take longer while
the runtime downloads. The status line identifies whether the wheel came from a
clean release-tag checkout or a development checkout. A development build can
contain changes absent from the PyPI wheel.

Run code you trust. Browser Python can make network requests through browser APIs;
the worker keeps execution off the page's main thread, but does not make pasted
code safe or suitable for secrets.

<div class="fp-playground" data-fp-playground>
  <div class="fp-playground__masthead">
    <div>
      <p class="fp-playground__eyebrow">Runs locally in your browser</p>
      <h2>Run a Python pipeline</h2>
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
      <p id="fp-playground-help" class="fp-playground__help">Variables stay available between runs. Reset runtime clears them and starts Python again.</p>
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

<noscript>Enable JavaScript to run these examples. You can still read the documentation without it.</noscript>

## Browser scope

You can try the core APIs here:

- `Flow`, `Rows`, `Pairs`, collectors, expressions, and `AsyncFlow` run locally;
- the `auto` engine selects the canonical Python path because the CPython/Rust
  extension is not a WebAssembly wheel;
- the status line shows the wheel version, build commit, working-tree state, and
  Python engine;
- stopping code terminates the worker, including an accidental infinite loop;
- local operating-system paths, process pools, and native-only execution are not
  available inside the browser sandbox;
- the runtime and wheel are fetched from the network on first use. Resetting the
  runtime downloads them again when they are not already in the browser cache.

For production workloads, install the regular package to gain Rust acceleration,
filesystem access, optional data-system adapters, and normal profiling tools.

<script type="module" src="../assets/javascripts/playground.js"></script>

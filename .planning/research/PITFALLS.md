# Domain Pitfalls: WASM Validation Module Extraction

**Domain:** Extracting a C++ validation library from a large codebase (NebulaStream) and compiling it to WebAssembly via Emscripten for browser-based topology validation.
**Researched:** 2026-03-15

## Critical Pitfalls

Mistakes that cause rewrites, multi-week delays, or make the approach unviable.

### Pitfall 1: C++23 Incompatibility with Emscripten

**What goes wrong:** NES uses C++23 (`-std=c++23`) throughout the codebase. Emscripten does not support C++23 -- it maxes out at C++20. Any code extracted for the WASM module that relies on C++23 features will fail to compile.

**Why it happens:** The assumption that "Emscripten uses Clang, and Clang supports C++23, so Emscripten must too" is wrong. Emscripten's libc++ and runtime support lag behind upstream Clang.

**Consequences:** The `nes-sql-parser` module -- a core piece of the validation pipeline -- uses `std::expected<T, E>` (a C++23 feature) in at least 12 locations across `AntlrSQLQueryParser.hpp`, `StatementBinder.hpp`, and their implementations. This is not a surface-level issue; it is threaded through the return types of the parser's public API.

**Prevention:**
- The extracted validation library must target C++20 maximum. All `std::expected` usage in extracted code must be replaced with a C++20-compatible polyfill (e.g., `tl::expected` or a hand-rolled Result type).
- Audit ALL C++23 features used in the dependency chain: `std::expected`, `std::print`, `std::format`, deducing `this`, `if consteval`, `std::stacktrace`, `std::flat_map`, etc.
- Create a dedicated CMake target for the WASM library that enforces `-std=c++20` to catch violations at compile time.

**Detection:** Compilation failure with Emscripten will immediately surface this, but by then you have already invested time in extraction. Audit BEFORE extracting.

**Confidence:** HIGH (verified via Emscripten issue tracker and documentation)

### Pitfall 2: Dependency Explosion via nes-common -> gRPC -> Protobuf

**What goes wrong:** The validation pipeline (nes-sql-parser -> nes-logical-operators -> nes-sources -> nes-common) transitively pulls in gRPC and Protobuf because `nes-common` links `nes-grpc` as a PUBLIC dependency. gRPC is fundamentally incompatible with WebAssembly -- it requires native networking, threading, and system calls that do not exist in the WASM sandbox.

**Why it happens:** `nes-common` was designed as a monolithic shared utilities module. Its CMakeLists.txt links `nes-grpc` publicly, meaning anything that depends on `nes-common` transitively depends on gRPC. The full dependency chain for just the SQL parser is:

```
nes-sql-parser
  -> nes-common (PUBLIC) -> nes-grpc -> gRPC, Protobuf
  -> nes-common -> folly (Facebook's library -- WASM-incompatible)
  -> nes-common -> boost-asio (networking -- WASM-incompatible)
  -> nes-common -> spdlog, fmt, cpptrace, libcuckoo, libuuid, yaml-cpp, magic-enum
  -> nes-sources (PUBLIC) -> nes-memory -> nes-executable -> nes-runtime (!!!)
  -> nes-sinks (PUBLIC) -> nes-memory, nes-executable
  -> nes-logical-operators (PUBLIC) -> nes-query-optimizer -> nes-single-node-worker-interface, nes-distributed, highs
  -> antlr4_static
```

**Consequences:** If you try to compile the existing `nes-sql-parser` target with Emscripten, you will need to compile gRPC, Protobuf, Folly, Boost.Asio, and the entire NES runtime for WebAssembly. This is effectively impossible. gRPC alone requires native sockets and threading. Folly assumes a full POSIX environment.

**Prevention:**
- Do NOT try to cross-compile the existing CMake targets. Instead, create a new, minimal CMake target (`nes-validation-wasm`) that cherry-picks only the source files and headers needed.
- The extracted target must NOT link against `nes-common` as-is. Instead, create a `nes-common-lite` or inline the specific utilities needed (identifiers, data types, logging stubs).
- Break the `nes-common -> nes-grpc` dependency by either (a) splitting `nes-common` into `nes-common-core` and `nes-common-grpc`, or (b) creating wrapper headers that provide the needed types without the gRPC dependency.
- Similarly, the `nes-logical-operators -> nes-query-optimizer -> nes-distributed` chain must be severed. The validation library should only need logical operator definitions, not the optimizer or distributed systems.

**Detection:** CMake configure step will fail spectacularly when Emscripten cannot find gRPC. The danger is spending weeks trying to "fix" each dependency rather than recognizing the need for a clean extraction.

**Confidence:** HIGH (verified by reading every CMakeLists.txt in the dependency chain)

### Pitfall 3: ANTLR4 C++ Runtime Emscripten Compatibility

**What goes wrong:** The ANTLR4 C++ runtime is a complex library that uses threading (mutexes for thread-safe grammar initialization), dynamic memory allocation patterns, and RTTI extensively. It was not designed for WebAssembly compilation and may have subtle incompatibilities.

**Why it happens:** ANTLR4's C++ runtime assumes a full POSIX environment. It uses `std::mutex` for lazy initialization of ATN data structures, which works in single-threaded WASM but adds unnecessary overhead. More critically, the ANTLR JAR must generate parser/lexer code, and that generated code makes assumptions about the runtime that may conflict with Emscripten's libc++.

**Consequences:** Compilation may succeed but runtime behavior could be incorrect (e.g., static initialization order issues in WASM). The ANTLR4 runtime also contributes significant binary size (often 500KB-1MB in optimized WASM builds).

**Prevention:**
- Compile the ANTLR4 C++ runtime separately with Emscripten as a proof-of-concept BEFORE attempting the full extraction. This is the single highest-risk dependency.
- Use `-pthread` flag if needed for mutex stubs, but target single-threaded execution.
- Consider `-fno-rtti` to reduce binary size if ANTLR4 runtime allows it (test this).
- Alternative: If ANTLR4 C++ runtime proves too problematic, consider generating the parser in TypeScript using ANTLR4's TypeScript target and only sending the parsed AST to the WASM module for semantic validation. This splits the work but avoids the hardest compatibility problem.

**Detection:** Build the ANTLR4 runtime with Emscripten in isolation first. Run the parser on a simple SQL string and verify output.

**Confidence:** MEDIUM (antlr4wasm project exists but was abandoned in favor of the JS runtime, suggesting the C++ WASM path is painful)

### Pitfall 4: WASM Binary Size Explosion

**What goes wrong:** An unoptimized extraction of the validation pipeline produces a WASM binary in the 5-20MB range (or larger), making the topology editor unusably slow to load. Users will wait 3-10 seconds just to download and compile the WASM module.

**Why it happens:** C++ produces large WASM binaries because: (1) C++ exceptions add significant overhead (JavaScript-based exception handling can double binary size), (2) ANTLR4 runtime is large, (3) template instantiation in NES data types generates lots of code, (4) RTTI metadata, (5) Emscripten's runtime support adds overhead, (6) debug symbols and unused code are not stripped by default.

**Consequences:** A 5MB+ WASM module over a typical connection takes several seconds to download and several more seconds to compile, making the editor feel broken on first load.

**Prevention:**
- Set a hard binary size budget: target under 1MB gzipped for the WASM module. Track this in CI.
- Use `-Oz` (optimize for size) and `-flto` (link-time optimization) for the WASM build.
- Use `-fno-exceptions` and convert all exception-based error handling to error codes or Result types in the extracted code. If exceptions are unavoidable, use `-fwasm-exceptions` (native WASM exceptions) instead of the JavaScript-based implementation -- this has significantly less size overhead but requires modern browsers.
- Use `-fno-rtti` unless the code requires `dynamic_cast` or `typeid`.
- Enable Emscripten's closure compiler for JS glue code: `--closure 1`.
- Strip debug info: `-s ASSERTIONS=0` in release builds.
- Use `-sEVAL_CTORS` to evaluate constructors at compile time.
- Consider WASM module splitting if the module exceeds 2MB: load a thin validation stub immediately and lazy-load the full parser on first use.

**Detection:** Measure binary size after every extraction change. Track both raw and gzipped sizes. Set up a CI check that fails if the binary exceeds the budget.

**Confidence:** HIGH (well-documented Emscripten behavior)

## Moderate Pitfalls

### Pitfall 5: Folly Dependency in nes-common and nes-memory

**What goes wrong:** Facebook's Folly library is linked by both `nes-common` and `nes-memory`. Folly is essentially impossible to compile with Emscripten -- it depends on platform-specific threading primitives, assembly instructions, and system calls.

**Prevention:**
- The extracted validation library must not depend on Folly at all. Identify which Folly features are actually used in the code paths needed for validation (likely `folly::small_vector`, `folly::Expected`, or similar containers) and replace them with standard library equivalents or minimal reimplementations.
- This is closely related to Pitfall 2 but warrants separate attention because Folly appears in `nes-memory` as well, meaning even the TupleBuffer/BufferManager code cannot be used as-is.

**Confidence:** HIGH (Folly has no Emscripten support)

### Pitfall 6: Async WASM Loading Blocks UI

**What goes wrong:** The WASM module is loaded synchronously or without proper user feedback, causing the topology editor to freeze or show a blank screen for several seconds on first load.

**Why it happens:** Developers test with the module cached locally and never experience the cold-load path. The default Emscripten JS glue code does not integrate well with React's lifecycle.

**Prevention:**
- Use `WebAssembly.instantiateStreaming()` for loading -- it compiles while downloading.
- Serve `.wasm` files with `Content-Type: application/wasm` and proper cache headers (the server must be configured for this).
- Show a loading indicator in the UI while the WASM module initializes. The topology editor should be functional (with validation disabled) before the WASM module finishes loading.
- Implement a "validation ready" state in the React app. Validation calls before the module is ready should be queued, not dropped.
- Lazy-load the WASM module: do not load it at app startup. Load it when the user first opens a topology that needs validation, or prefetch it after the initial render is complete.

**Detection:** Test on throttled network connections (Chrome DevTools: "Slow 3G" profile). If first-load takes more than 2 seconds to show a usable UI, the loading strategy needs work.

**Confidence:** HIGH (standard web performance concern)

### Pitfall 7: Memory Leaks Across the JS-WASM Boundary

**What goes wrong:** C++ objects allocated in WASM heap memory are not freed because JavaScript's garbage collector does not manage WASM memory. Each validation call leaks memory until the page is refreshed.

**Why it happens:** Embind and manual memory management both require explicit cleanup. Developers familiar with garbage-collected environments forget that strings passed to WASM are copied into the WASM heap and must be freed.

**Prevention:**
- Use Embind with value types for the interface (strings, vectors of error objects). Embind handles the memory lifecycle for value types automatically.
- Keep the WASM interface extremely narrow: one function that takes a `std::string` (YAML) and returns a `std::vector<ValidationError>`. Embind will handle the string conversion and vector marshaling.
- Do NOT expose C++ objects with pointers or shared_ptr across the boundary. If you must, use Embind's smart pointer support and ensure JavaScript code calls `.delete()` on C++ objects.
- Write a thin TypeScript wrapper that encapsulates the WASM module and handles cleanup in a `finally` block or RAII-style pattern.

**Detection:** Use Chrome's memory profiler. Run 100 consecutive validation calls and check if WASM memory (the `WebAssembly.Memory` buffer) grows monotonically.

**Confidence:** HIGH (well-known Emscripten pitfall)

### Pitfall 8: spdlog/cpptrace/libuuid -- Unnecessary Heavyweight Dependencies

**What goes wrong:** The extracted code transitively pulls in spdlog (logging), cpptrace (stack traces), and libuuid (UUID generation) via `nes-common`. These are unnecessary for validation in a browser context and add binary size and compatibility issues.

**Why it happens:** NES code uses `NES_INFO()`, `NES_ERROR()` logging macros throughout, including in the SQL parser and logical operator code. These macros expand to spdlog calls. cpptrace captures stack traces on errors. libuuid generates unique identifiers.

**Prevention:**
- Create stub implementations of the logging macros for the WASM build that compile to no-ops (or `console.log` via Emscripten's `EM_ASM`).
- Replace `libuuid` with a simple counter or browser-side UUID generation passed in via the interface.
- cpptrace is completely unnecessary in WASM -- stub it out.
- Define a preprocessor macro like `NES_WASM_BUILD` that triggers these stubs at compile time.

**Confidence:** HIGH (verified by reading nes-common CMakeLists.txt)

### Pitfall 9: Build System Integration -- Two Toolchains, One Repo

**What goes wrong:** Maintaining a CMake build that works with both the native Clang toolchain (for the NES engine) and the Emscripten toolchain (for the WASM module) in the same repository causes constant friction. Developers break the WASM build without realizing it because they only test with the native toolchain.

**Why it happens:** Emscripten provides its own CMake toolchain file that overrides the C/C++ compiler, and the existing NES CMake setup assumes Clang 19 and specific flags (`-march=native`, AVX2 detection, `-Werror` with NES-specific warning flags). These conflict with Emscripten's constraints.

**Prevention:**
- Create a completely separate CMake build for the WASM target. Do NOT try to conditionally compile the same CMakeLists.txt for both native and WASM. Use a top-level `CMakeLists.txt` in a subdirectory (e.g., `nes-validation-wasm/CMakeLists.txt`) that pulls in only the needed source files.
- Add a CI job that builds the WASM module on every PR that touches files in the validation pipeline. This catches breakage immediately.
- Use vcpkg's `wasm32-emscripten` community triplet for dependencies that need to be cross-compiled (yaml-cpp, antlr4-runtime). Be aware that not all vcpkg ports support this triplet.
- Pin the Emscripten SDK version in the repo (e.g., via `.emscripten-version` file or in the Docker dev image).

**Detection:** A PR that modifies `nes-sql-parser`, `nes-logical-operators`, `nes-common`, or `nes-data-types` should trigger the WASM CI build. If it does not, WASM builds will silently break.

**Confidence:** MEDIUM (standard cross-compilation challenge, but NES's complex build setup makes it worse)

### Pitfall 10: Testing the WASM Module End-to-End

**What goes wrong:** The WASM module passes unit tests in the C++ build but produces different results in the browser due to differences in floating-point behavior, string encoding, or memory layout. Alternatively, the validation works but the JS interface returns garbled results due to encoding mismatches.

**Why it happens:** WASM has specific numeric behavior (IEEE 754 strict compliance), and string encoding between JS (UTF-16) and C++ (UTF-8) can cause issues. The NES test infrastructure uses GTest which cannot run in a browser.

**Prevention:**
- Create three levels of tests:
  1. **C++ unit tests**: Test the extracted validation library with the native compiler. Use the same GTest infrastructure as NES. These run fast and catch logic errors.
  2. **Emscripten headless tests**: Build the GTest tests with Emscripten and run them in Node.js via `emcc --emrun`. This catches Emscripten-specific compilation issues.
  3. **Browser integration tests**: Use Playwright or similar to load the topology editor, submit YAML, and verify validation results match expected output. This catches JS-WASM boundary issues.
- Create a shared test fixture of YAML inputs and expected validation results that is used by all three levels.
- Test with non-ASCII characters in source names, sink names, and SQL strings. UTF-8/UTF-16 boundary issues are common.

**Detection:** If C++ tests pass but browser tests fail, the bug is at the JS-WASM boundary or in Emscripten-specific behavior.

**Confidence:** MEDIUM (standard WASM testing challenge)

## Minor Pitfalls

### Pitfall 11: Emscripten Version Drift

**What goes wrong:** The WASM build works with one Emscripten version but breaks with updates due to changed defaults, removed flags, or libc++ changes.

**Prevention:** Pin Emscripten version in CI and documentation. Update deliberately and test thoroughly. Current recommendation: use Emscripten 3.1.50+ for best WASM exception support.

**Confidence:** MEDIUM

### Pitfall 12: Main Thread Blocking During Validation

**What goes wrong:** A complex topology with many queries takes 500ms+ to validate in WASM, blocking the browser's main thread and causing the UI to freeze.

**Prevention:**
- Run WASM validation in a Web Worker, not on the main thread.
- Implement a debounce on validation calls (validate 300ms after the user stops editing, not on every keystroke).
- For the initial implementation, main-thread execution is acceptable IF validation completes in under 50ms. Measure actual validation times with realistic topologies and move to a Web Worker if needed.

**Confidence:** LOW (depends on actual validation complexity, which is unknown until implemented)

### Pitfall 13: yaml-cpp Emscripten Compatibility

**What goes wrong:** yaml-cpp uses features or system calls that do not compile with Emscripten, or the vcpkg wasm32-emscripten port is broken/unavailable.

**Prevention:** yaml-cpp is a relatively portable library and is likely to compile. However, verify early by building yaml-cpp with Emscripten standalone. If it fails, consider using a JS-side YAML parser (js-yaml) and passing a pre-parsed structure to WASM instead of raw YAML strings.

**Confidence:** LOW (yaml-cpp is generally portable, but vcpkg cross-compilation can have issues)

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Library extraction from NES | Pitfall 2 (dependency explosion) | Map ALL transitive deps first; create clean new CMake target |
| C++23 -> C++20 downgrade | Pitfall 1 (std::expected) | Replace with tl::expected or Result type polyfill |
| ANTLR4 WASM compilation | Pitfall 3 (runtime compat) | Build ANTLR4 runtime with Emscripten as first spike |
| Emscripten build setup | Pitfall 9 (two toolchains) | Separate CMakeLists.txt, dedicated CI job |
| JS-WASM interface | Pitfall 7 (memory leaks) | Use Embind with value types, narrow interface |
| Binary size optimization | Pitfall 4 (size explosion) | Set 1MB gzipped budget, -Oz, -fno-exceptions, -flto |
| Browser integration | Pitfall 6 (async loading) | instantiateStreaming, lazy load, loading indicator |
| Testing | Pitfall 10 (three-level testing) | Shared test fixtures across native, Node.js, and browser |
| Stubbing NES infrastructure | Pitfall 8 (spdlog/folly) | NES_WASM_BUILD preprocessor flag, no-op stubs |

## NES-Specific Dependency Risk Matrix

| Dependency | Used By | WASM Compatible | Action Required |
|------------|---------|-----------------|-----------------|
| gRPC + Protobuf | nes-common (transitively) | NO | Sever dependency completely |
| Folly | nes-common, nes-memory | NO | Replace used features with stdlib equivalents |
| Boost.Asio | nes-common | NO | Sever dependency completely |
| spdlog | nes-common (logging) | MAYBE (heavy) | Stub with no-op macros |
| cpptrace | nes-common (stack traces) | NO | Stub out entirely |
| libuuid | nes-common (UUID gen) | NO (system dep) | Replace with simple counter |
| libcuckoo | nes-common (hash tables) | MAYBE | Likely not needed for validation path |
| yaml-cpp | nes-common (YAML parsing) | YES (likely) | Test early; fallback to JS-side parsing |
| ANTLR4 runtime | nes-sql-parser | MAYBE (test first) | Build with Emscripten as proof-of-concept |
| fmt | nes-common (formatting) | YES | Compiles with Emscripten |
| magic-enum | nes-common (enum utils) | YES | Header-only, no issues |
| reflectcpp | nes-data-types | UNKNOWN | Test; may need stubbing |
| highs | nes-query-optimizer | NO (not needed) | Sever; optimizer not needed for validation |
| nautilus | nes-query-optimizer (optional) | NO | Sever; not needed for validation |

## Recommended Spike Order

Before committing to the full extraction, run these three spikes to validate the approach:

1. **Spike: ANTLR4 + Emscripten** (1-2 days) -- Compile antlr4-runtime with Emscripten, generate a trivial parser, parse a SQL string in Node.js. If this fails, the entire approach needs rethinking.

2. **Spike: Dependency boundary map** (1 day) -- Use CMake's `--graphviz` output to generate the full dependency graph for `nes-sql-parser`. Identify exactly which source files are needed and which dependencies they pull in. Count the files that need modification.

3. **Spike: Minimal WASM binary** (1 day) -- Compile a trivial C++ function (that takes a string and returns a string) with Emscripten + Embind. Measure baseline WASM size. Load it in the topology editor. Verify the loading/integration pattern works.

## Sources

- [Emscripten API Limitations](https://emscripten.org/docs/porting/guidelines/api_limitations.html)
- [Emscripten C++ Exceptions Support](https://emscripten.org/docs/porting/exceptions.html)
- [Emscripten Code Optimization](https://emscripten.org/docs/optimizing/Optimizing-Code.html)
- [Emscripten Embind](https://emscripten.org/docs/porting/connecting_cpp_and_javascript/embind.html)
- [Emscripten Pthreads Support](https://emscripten.org/docs/porting/pthreads.html)
- [Emscripten C++23 module support issue #21143](https://github.com/emscripten-core/emscripten/issues/21143)
- [Emscripten C++23 via Meson issue #13628](https://github.com/mesonbuild/meson/issues/13628)
- [gRPC WASM support issue #24166](https://github.com/grpc/grpc/issues/24166)
- [vcpkg wasm32-emscripten triplet](https://gist.github.com/jeronimonunes/4769f512b51eeedb2b146cbd3e5ceebf)
- [Loading WASM modules efficiently (web.dev)](https://web.dev/articles/loading-wasm)
- [ANTLR4 WASM project (abandoned)](https://github.com/mike-lischke/antlr4wasm)
- NES codebase: CMakeLists.txt files for nes-common, nes-sql-parser, nes-logical-operators, nes-query-optimizer, nes-sources, nes-sinks, nes-memory, nes-executable

---

*Pitfalls research: 2026-03-15*

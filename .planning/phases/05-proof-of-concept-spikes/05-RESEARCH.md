# Phase 5: Proof-of-Concept Spikes - Research

**Researched:** 2026-03-15
**Domain:** Emscripten / WebAssembly / C++23 / ANTLR4 / Embind
**Confidence:** MEDIUM-HIGH

## Summary

This phase validates three technical unknowns before committing to the full validation library extraction: (1) whether C++23 features used by NES (std::expected, concepts, ranges) compile under Emscripten, (2) whether the ANTLR4 C++ runtime compiles to WASM and can parse NES SQL, and (3) whether Embind can expose a C++ function to JavaScript in a Node.js environment.

The prognosis is positive. Emscripten 4.0+ ships LLVM 20-based libc++ which has complete std::expected support (libc++ has had std::expected since LLVM 16). The ANTLR4 C++ runtime is a standard C++ library available in vcpkg and should compile to WASM as a static library -- the abandoned antlr4wasm project is irrelevant because it tried a different approach (precompiled WASM runtime), while we are compiling our own C++ code including the ANTLR4 runtime directly. Embind is mature and well-documented with straightforward CMake integration.

**Primary recommendation:** Use Emscripten 4.0.x (latest stable) with the vcpkg `wasm32-emscripten` community triplet pattern adapted to NES's custom triplet structure. Each spike is a standalone CMake target under `nes-topology-editor/wasm/` that compiles to a .js+.wasm pair and is tested with a Node.js script.

<user_constraints>

## User Constraints (from CONTEXT.md)

### Locked Decisions
- Test Emscripten first -- user expects it to work since Emscripten ships LLVM 19 (same as NES)
- If C++23 doesn't work: the WASM target can use a different standard (C++20) from the rest of NES
- No preemptive polyfill -- only if Emscripten actually fails
- Hybrid architecture: keep TypeScript ANTLR parser for fast inline syntax feedback, use WASM module for full semantic validation
- The WASM module still needs the C++ ANTLR4 runtime internally
- Minimal spikes: compile C++ to WASM, call from Node.js, verify output -- pass/fail on feasibility
- No browser/Vite integration in spikes (that's Phase 7)
- No binary size measurement in spikes (optimization is Phase 6)
- Three discrete spikes: (1) C++23 features, (2) ANTLR4 parse, (3) Embind interop
- Spike code lives inside `nes-topology-editor/wasm/` with its own CMakeLists.txt
- Use vcpkg with a custom Emscripten/WASM triplet for dependency management
- Production .wasm artifact built in CI, not committed to repo

### Claude's Discretion
- Exact Emscripten version to use
- vcpkg triplet configuration for WASM
- How to structure the three spike directories
- Node.js test harness approach

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope

</user_constraints>

<phase_requirements>

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| WASM-01 | C++23 / Emscripten compatibility validated via proof-of-concept spike | Emscripten 4.0+ has LLVM 20 libc++ with full std::expected support; spike tests std::expected, concepts, ranges |
| WASM-02 | ANTLR4 C++ runtime compiles to WebAssembly | ANTLR4 is in vcpkg, can be built with wasm32-emscripten triplet as static library; spike parses NES SQL via generated parser |

</phase_requirements>

## Standard Stack

### Core
| Tool | Version | Purpose | Why Standard |
|------|---------|---------|--------------|
| Emscripten (emsdk) | 4.0.x (latest) | C++ to WASM compiler | Ships LLVM 20 + libc++ 20, full C++23 support |
| vcpkg | (NES baseline) | Dependency management | Consistent with NES project; has wasm32-emscripten community triplet |
| CMake | 3.21+ | Build system | Existing NES build system; Emscripten provides CMake toolchain file |
| Node.js | 18+ | Test runner for spikes | Available in dev environment; can load WASM modules directly |

### Supporting
| Tool | Purpose | When to Use |
|------|---------|-------------|
| ANTLR4 C++ runtime | Parser runtime for NES SQL grammar | Spike 2: compile grammar + runtime to WASM |
| antlr4-generator (vcpkg) | Generate parser/lexer C++ from .g4 | Spike 2: generate C++ sources from AntlrSQL.g4 |
| Embind | C++ <-> JS binding layer | Spike 3: expose C++ functions to JavaScript |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Embind | WebIDL / cwrap | Embind handles std::string, structured types automatically; cwrap is lower-level |
| vcpkg for ANTLR4 | Manual build | vcpkg gives consistent dependency management matching NES pattern |
| Node.js test | Browser test | Node.js is simpler for CI; browser integration is Phase 7 |

**Installation:**
```bash
# Install Emscripten SDK
git clone https://github.com/emscripten-core/emsdk.git
cd emsdk && ./emsdk install latest && ./emsdk activate latest
source ./emsdk_env.sh

# vcpkg install with WASM triplet (once triplet is configured)
vcpkg install antlr4:wasm32-emscripten
```

## Architecture Patterns

### Recommended Project Structure
```
nes-topology-editor/wasm/
  CMakeLists.txt              # Top-level CMake for all spikes
  vcpkg.json                  # Minimal manifest (antlr4 only)
  triplets/
    wasm32-emscripten.cmake   # Custom WASM triplet
  spike-cpp23/
    main.cpp                  # Tests std::expected, concepts, ranges
    CMakeLists.txt
  spike-antlr4/
    main.cpp                  # Parses NES SQL string via ANTLR4
    CMakeLists.txt
  spike-embind/
    main.cpp                  # Exposes function via Embind
    CMakeLists.txt
  test/
    test-cpp23.mjs            # Node.js test for spike 1
    test-antlr4.mjs           # Node.js test for spike 2
    test-embind.mjs           # Node.js test for spike 3
```

### Pattern 1: Emscripten CMake Integration
**What:** Use `emcmake` wrapper or `CMAKE_TOOLCHAIN_FILE` to invoke Emscripten's toolchain
**When to use:** All spike CMake builds
**Example:**
```cmake
# Top-level CMakeLists.txt
cmake_minimum_required(VERSION 3.21)
project(nes-wasm-spikes LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(spike-cpp23)
add_subdirectory(spike-antlr4)
add_subdirectory(spike-embind)
```

```bash
# Build invocation
emcmake cmake -B build \
  -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
  -DVCPKG_TARGET_TRIPLET=wasm32-emscripten \
  -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$EMSDK/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake
cmake --build build
```

### Pattern 2: Embind Module Pattern
**What:** Expose C++ functions to JS using EMSCRIPTEN_BINDINGS macro
**When to use:** Spike 3 and eventually the full validation library
**Example:**
```cpp
// spike-embind/main.cpp
#include <emscripten/bind.h>
#include <string>

std::string greet(const std::string& name) {
    return "Hello from WASM, " + name + "!";
}

EMSCRIPTEN_BINDINGS(spike_embind) {
    emscripten::function("greet", &greet);
}
```

```cmake
# spike-embind/CMakeLists.txt
add_executable(spike-embind main.cpp)
target_link_options(spike-embind PRIVATE -lembind)
set_target_properties(spike-embind PROPERTIES
    SUFFIX ".js"
    LINK_FLAGS "-sMODULARIZE=1 -sEXPORT_ES6=0 -sENVIRONMENT=node"
)
```

```javascript
// test/test-embind.mjs
import createModule from '../build/spike-embind/spike-embind.js';
const Module = await createModule();
const result = Module.greet("NebulaStream");
console.assert(result === "Hello from WASM, NebulaStream!", `Unexpected: ${result}`);
console.log("PASS: Embind spike");
```

### Pattern 3: ANTLR4 WASM Compilation
**What:** Compile the ANTLR4 C++ runtime + generated parser to WASM, parse a SQL string
**When to use:** Spike 2
**Example:**
```cpp
// spike-antlr4/main.cpp
#include <emscripten/bind.h>
#include <antlr4-runtime.h>
#include "AntlrSQLLexer.h"
#include "AntlrSQLParser.h"
#include <string>

std::string parseSql(const std::string& sql) {
    antlr4::ANTLRInputStream input(sql);
    AntlrSQLLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    AntlrSQLParser parser(&tokens);
    auto* tree = parser.statement();
    if (parser.getNumberOfSyntaxErrors() > 0) {
        return "PARSE_ERROR";
    }
    return tree->toStringTree(&parser);
}

EMSCRIPTEN_BINDINGS(spike_antlr4) {
    emscripten::function("parseSql", &parseSql);
}
```

### Pattern 4: vcpkg WASM Triplet
**What:** Custom triplet matching NES pattern but targeting Emscripten
**When to use:** All spikes that need vcpkg dependencies (antlr4)
**Example:**
```cmake
# triplets/wasm32-emscripten.cmake
set(VCPKG_TARGET_ARCHITECTURE wasm32)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Emscripten)
set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE "$ENV{EMSDK}/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake")
set(VCPKG_ENV_PASSTHROUGH_UNTRACKED EMSCRIPTEN_ROOT EMSDK PATH)
```

### Anti-Patterns to Avoid
- **Using antlr4wasm package:** That project is abandoned and uses a different approach (precompiled WASM runtime). We compile the ANTLR4 C++ runtime directly as part of our build.
- **Testing in browser during spikes:** Node.js testing is simpler and sufficient for feasibility validation. Browser/Vite integration is Phase 7.
- **Using SINGLE_FILE=1 in spikes:** While useful later, it increases binary size. Keep .js+.wasm separate for clear size measurement.
- **Generating parser at build time in WASM build:** The ANTLR4 code generator needs Java. Pre-generate the C++ parser sources and include them directly, or use the existing NES build to generate them first.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| C++ to JS binding | Manual cwrap/ccall | Embind | Handles std::string, classes, error types automatically |
| WASM module loading | Custom fetch+instantiate | Emscripten's generated JS glue | Handles memory init, async loading, environment detection |
| ANTLR4 parser generation | Manual lexer/parser | antlr4-generator vcpkg port | Consistent with NES, handles all edge cases |
| vcpkg Emscripten integration | Manual dependency builds | vcpkg wasm32-emscripten triplet | Reproducible, matches NES dependency management |

**Key insight:** The spikes should use the exact same tooling patterns that the production WASM build (Phase 6) will use, just with trivial test code. This validates the infrastructure, not just the features.

## Common Pitfalls

### Pitfall 1: ANTLR4 Code Generator Requires Java at Build Time
**What goes wrong:** The ANTLR4 grammar-to-C++ code generator is a Java JAR file. Emscripten build environments may not have Java.
**Why it happens:** NES uses `antlr4_generate()` CMake function which invokes the JAR at configure time.
**How to avoid:** Pre-generate the C++ parser/lexer sources from AntlrSQL.g4 using the host build, then include the generated files directly in the WASM spike's CMakeLists.txt. Or run the ANTLR4 generator as a separate step before the Emscripten build.
**Warning signs:** CMake configure fails looking for Java or the ANTLR4 JAR.

### Pitfall 2: vcpkg + Emscripten Toolchain Chain-loading
**What goes wrong:** CMake uses either vcpkg's toolchain OR Emscripten's toolchain, but not both.
**Why it happens:** CMAKE_TOOLCHAIN_FILE can only point to one file. vcpkg and Emscripten both want to be the toolchain.
**How to avoid:** Use `VCPKG_CHAINLOAD_TOOLCHAIN_FILE` to load Emscripten's toolchain through vcpkg. Set this in the triplet file or pass via `-DVCPKG_CHAINLOAD_TOOLCHAIN_FILE`.
**Warning signs:** Libraries install but the main project can't find them, or the compiler isn't `emcc`.

### Pitfall 3: AntlrSQL.g4 Has NES-Specific Include Directives
**What goes wrong:** The grammar file includes `<Util/DisableWarningsPragma.hpp>` in `@lexer::postinclude` and `@parser::postinclude` blocks.
**Why it happens:** NES-specific warning suppression macros are injected into the generated C++ code.
**How to avoid:** Either provide stub headers for `DisableWarningsPragma.hpp` (with empty DISABLE_WARNING macros) or modify the grammar for the spike to remove these postinclude blocks. A stub is preferable to avoid modifying the grammar.
**Warning signs:** Compilation fails with "file not found: Util/DisableWarningsPragma.hpp".

### Pitfall 4: Emscripten Exception Handling
**What goes wrong:** ANTLR4 runtime uses C++ exceptions. By default, Emscripten uses a JavaScript-based exception mechanism that can be slow.
**Why it happens:** Native WASM exceptions are newer and not the default.
**How to avoid:** Use `-fwasm-exceptions` flag for native WASM exception handling (supported in modern browsers and Node.js 17+). If that causes issues, fall back to `-fexceptions` (JS-based, slower but compatible).
**Warning signs:** ANTLR4 parse errors crash instead of being caught; or significant performance degradation.

### Pitfall 5: MODULARIZE Flag Required for Node.js Module Import
**What goes wrong:** Generated .js file pollutes global scope and can't be imported as a module.
**Why it happens:** Default Emscripten output assumes browser script tag loading.
**How to avoid:** Always use `-sMODULARIZE=1` and `-sENVIRONMENT=node` for spike testing. The generated JS exports a factory function.
**Warning signs:** `import` or `require` doesn't work; "Module is not a constructor" errors.

### Pitfall 6: wasm32-emscripten Is a Community Triplet
**What goes wrong:** Some vcpkg ports don't build with the wasm32-emscripten triplet.
**Why it happens:** Community triplets are not tested in vcpkg CI. Not all ports handle Emscripten's constraints.
**How to avoid:** Test `vcpkg install antlr4:wasm32-emscripten` early. If it fails, build ANTLR4 C++ runtime manually with Emscripten as a fallback. The runtime is a single CMakeLists.txt with no external dependencies.
**Warning signs:** vcpkg install fails with build errors for the antlr4 port.

## Code Examples

### Spike 1: C++23 Feature Test
```cpp
// spike-cpp23/main.cpp
#include <concepts>
#include <expected>
#include <ranges>
#include <string>
#include <vector>
#include <cstdio>

// Test std::expected (heavily used in NES StatementHandler)
std::expected<int, std::string> divide(int a, int b) {
    if (b == 0) return std::unexpected("division by zero");
    return a / b;
}

// Test concepts (used in NES templates)
template<std::integral T>
T double_it(T val) { return val * 2; }

// Test ranges (used in NES StatementHandler)
int sum_even(const std::vector<int>& v) {
    auto result = v | std::views::filter([](int x) { return x % 2 == 0; })
                    | std::views::transform([](int x) { return x; });
    int sum = 0;
    for (int x : result) sum += x;
    return sum;
}

int main() {
    // std::expected
    auto ok = divide(10, 3);
    auto err = divide(10, 0);
    if (!ok.has_value() || ok.value() != 3) return 1;
    if (err.has_value() || err.error() != "division by zero") return 2;

    // concepts
    if (double_it(21) != 42) return 3;

    // ranges
    std::vector<int> nums = {1, 2, 3, 4, 5, 6};
    if (sum_even(nums) != 12) return 4;

    std::printf("ALL C++23 TESTS PASSED\n");
    return 0;
}
```

```cmake
# spike-cpp23/CMakeLists.txt
add_executable(spike-cpp23 main.cpp)
set_target_properties(spike-cpp23 PROPERTIES
    SUFFIX ".js"
    LINK_FLAGS "-sENVIRONMENT=node"
)
```

```bash
# Test with Node.js
node build/spike-cpp23/spike-cpp23.js
# Expected output: "ALL C++23 TESTS PASSED"
# Exit code 0 = pass
```

### Node.js Test Harness
```javascript
// test/run-spikes.mjs
import { execSync } from 'child_process';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const buildDir = join(__dirname, '..', 'build');

const spikes = [
    { name: 'C++23 Features', cmd: `node ${join(buildDir, 'spike-cpp23/spike-cpp23.js')}` },
    // Spike 2 and 3 use module imports, tested separately
];

for (const spike of spikes) {
    try {
        const output = execSync(spike.cmd, { encoding: 'utf-8' });
        console.log(`PASS: ${spike.name}`);
        console.log(output);
    } catch (e) {
        console.error(`FAIL: ${spike.name}`);
        console.error(e.stderr || e.message);
        process.exit(1);
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Emscripten 3.x + LLVM 17 libc++ | Emscripten 4.0+ with LLVM 20 libc++ | 2025 | Full C++23 std::expected, ranges, concepts support |
| `--bind` flag for Embind | `-lembind` flag | Emscripten 3.1.44+ | `--bind` still works but `-lembind` is preferred |
| JS-based exceptions (`-fexceptions`) | Native WASM exceptions (`-fwasm-exceptions`) | 2024 | Better performance for exception-heavy code like ANTLR4 |
| antlr4wasm (precompiled WASM runtime) | Compile ANTLR4 C++ runtime directly | N/A (antlr4wasm abandoned) | Direct compilation gives us full control and latest ANTLR4 version |
| Manual Emscripten builds | vcpkg wasm32-emscripten community triplet | 2023+ | Reproducible dependency management |

**Deprecated/outdated:**
- antlr4wasm: Abandoned project, JS/TS runtime proved faster than their approach. Not relevant to our approach of compiling C++ directly.
- `--bind` linker flag: Still works but `-lembind` is the modern equivalent.

## Open Questions

1. **ANTLR4 vcpkg port compatibility with wasm32-emscripten**
   - What we know: antlr4 is in vcpkg, wasm32-emscripten is a community triplet, not all ports support it
   - What's unclear: Whether `vcpkg install antlr4:wasm32-emscripten` succeeds without patches
   - Recommendation: Try it first. Fallback: build ANTLR4 C++ runtime manually from source with Emscripten (it has minimal dependencies -- just a standard C++ compiler)

2. **ANTLR4 JAR for code generation in spike environment**
   - What we know: NES uses `antlr4_generate()` which requires Java + the ANTLR4 JAR
   - What's unclear: Whether the spike should generate parser C++ itself or copy from NES build
   - Recommendation: Copy the generated C++ sources from the NES build output directory. Avoids Java dependency in the WASM build. The grammar doesn't change during the spike.

3. **NES-specific header dependencies in generated ANTLR4 code**
   - What we know: AntlrSQL.g4 has `@postinclude` blocks referencing `<Util/DisableWarningsPragma.hpp>`
   - What's unclear: Exact macros needed to stub
   - Recommendation: Create minimal stub headers (DISABLE_WARNING_PUSH -> no-op, DISABLE_WARNING -> no-op) in the spike's include path

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Node.js scripts (spike validation); vitest for any TS integration tests |
| Config file | None needed for spike Node.js tests |
| Quick run command | `node test/run-spikes.mjs` |
| Full suite command | `cd nes-topology-editor/wasm && cmake --build build && node test/run-spikes.mjs` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| WASM-01 | C++23 features compile and run in WASM | smoke | `node build/spike-cpp23/spike-cpp23.js` | No -- Wave 0 |
| WASM-01 | std::expected error propagation works | smoke | `node build/spike-cpp23/spike-cpp23.js` (exit code) | No -- Wave 0 |
| WASM-02 | ANTLR4 C++ runtime compiles to WASM | smoke | `cmake --build build --target spike-antlr4` (build succeeds) | No -- Wave 0 |
| WASM-02 | ANTLR4 parses NES SQL string in WASM | smoke | `node test/test-antlr4.mjs` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** Build affected spike + run its test
- **Per wave merge:** Full rebuild + all spike tests
- **Phase gate:** All three spikes pass: C++23, ANTLR4, Embind

### Wave 0 Gaps
- [ ] `nes-topology-editor/wasm/CMakeLists.txt` -- top-level CMake for spikes
- [ ] `nes-topology-editor/wasm/triplets/wasm32-emscripten.cmake` -- vcpkg triplet
- [ ] `nes-topology-editor/wasm/test/test-cpp23.mjs` -- Node.js test for spike 1
- [ ] `nes-topology-editor/wasm/test/test-antlr4.mjs` -- Node.js test for spike 2
- [ ] `nes-topology-editor/wasm/test/test-embind.mjs` -- Node.js test for spike 3
- [ ] Emscripten SDK installation (emsdk)
- [ ] Stub headers for NES-specific ANTLR4 postinclude macros

## Sources

### Primary (HIGH confidence)
- Emscripten official docs (embind) -- https://emscripten.org/docs/porting/connecting_cpp_and_javascript/embind.html
- Emscripten official docs (modularized output) -- https://emscripten.org/docs/compiling/Modularized-Output.html
- vcpkg wasm32-emscripten triplet -- https://github.com/microsoft/vcpkg/blob/master/triplets/community/wasm32-emscripten.cmake
- NES project source: `vcpkg/vcpkg.json`, `vcpkg/custom-triplets/`, `nes-sql-parser/CMakeLists.txt`, `nes-sql-parser/AntlrSQL.g4`

### Secondary (MEDIUM confidence)
- Emscripten 4.0.0 changelog: libc++ updated to LLVM 20.1.8 (from GitHub ChangeLog.md search results)
- vcpkg + Emscripten integration patterns -- https://github.com/microsoft/vcpkg/discussions/40368
- antlr4wasm abandoned status -- https://github.com/mike-lischke/antlr4wasm (on hold, JS runtime faster)

### Tertiary (LOW confidence)
- Exact C++23 feature coverage in Emscripten 4.0 libc++: inferred from LLVM 20 libc++ having full C++23 library support, but not directly tested in Emscripten context

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- Emscripten, vcpkg, CMake, Embind are all well-documented and stable
- Architecture: MEDIUM-HIGH -- spike structure is straightforward, vcpkg+Emscripten chain-loading pattern is documented but community-tier
- Pitfalls: HIGH -- identified from real project structure analysis (AntlrSQL.g4 postinclude, Java dependency, community triplet gaps)
- C++23 compatibility: MEDIUM -- LLVM 20 libc++ should have full support but this is the exact thing the spike validates

**Research date:** 2026-03-15
**Valid until:** 2026-04-15 (Emscripten releases frequently but major version is stable)

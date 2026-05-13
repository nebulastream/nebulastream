/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include "FormatPhysicalFunction.hpp"

#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

FormatPhysicalFunction::FormatPhysicalFunction(std::vector<PhysicalFunction> childPhysicalFunctions)
    : childPhysicalFunctions(std::move(childPhysicalFunctions))
{
}

VarVal FormatPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /// Skeleton: return the format string verbatim, ignoring placeholder args.
    const auto formatStringValue = childPhysicalFunctions.front().execute(record, arena);

    /// TODO(student): replace the passthrough above with a real fmt::format-style
    /// substitution that fills `{}` placeholders in the format string with the
    /// stringified values of `childPhysicalFunctions[1..]` (in order).
    /// The returned VarVal must wrap a VARSIZED allocated from `arena`.
    ///
    /// Where to start:
    ///   * Systest for this function (extend it with cases as you go):
    ///       nes-systests/function/dspro/Format.test
    ///     Once Format actually substitutes placeholders, update the expected rows there.
    ///
    ///   * Reference physical functions worth studying:
    ///       - nes-physical-operators/src/Functions/ConcatPhysicalFunction.cpp
    ///         Shows how to allocate a new VARSIZED in the arena and memcpy bytes into
    ///         it. The natural primitive for emitting "literal | stringified arg |
    ///         literal | ..." fragments.
    ///       - nes-physical-operators/src/Functions/ToBase64PhysicalFunction.cpp
    ///         Shows how to call a native C/C++ helper from Nautilus code via
    ///         `nautilus::invoke(...)` — useful if you stringify numerics with
    ///         std::to_chars or sprintf rather than reimplementing in Nautilus.
    ///       - nes-physical-operators/src/Functions/ArithmeticalFunctions/AbsolutePhysicalFunction.cpp
    ///         Minimal example of operating on a Nautilus `VarVal`.
    ///     Their logical-side counterparts live under
    ///       nes-logical-operators/src/Functions/...
    ///
    ///   * Prior prototype (ls-1801 / Lukas Schwerdtfeger, not on main):
    ///       Branch:  origin/retrospective-study-charite-mlife-rebased-v2
    ///       Commit:  e88b3b46ec  "feat(Functions): add FMT printf-style varsized formatter plugin"
    ///       Files:   nes-plugins/Functions/Fmt/{FmtLogicalFunction,FmtPhysicalFunction,
    ///                                          ToStringLogicalFunction,ToStringPhysicalFunction}.{hpp,cpp}
    ///                nes-systests/function/varsized/FMT.test
    ///     The prototype factors out a `to_string` function that lifts non-VARSIZED
    ///     args to VARSIZED (via VarVal::customVisit + std::to_chars), and inserts
    ///     those wrappers during data type inference so the physical side only
    ///     concatenates VARSIZED fragments. That two-function split is one viable
    ///     design; you are free to do it differently.
    ///
    ///   * Building / running NebulaStream:
    ///       - docs/development/development.md  (dev container, CMake profiles, build steps)
    ///       - docs/guide/extensibility.md      (plugin & registry model — read this first)
    ///       - docs/development/systests.md     (running systests from the CLI)
    ///       - README.md "Quick Start" section
    ///
    ///   * Running this systest from CLion (recommended):
    ///       - CLion plugin (gutter icons to run/debug individual systests):
    ///           nes-systests/utils/SystestPlugin/README.md
    ///         Releases (download the .zip and install from disk):
    ///           https://github.com/nebulastream/systest-plugin/releases/
    ///       - Optional: enable systest syntax highlighting via
    ///           nes-systests/utils/SyntaxHighlighting/README.md
    ///       From the command line you can also run the `systest` target directly,
    ///       e.g. `systest -t .../Format.test`.
    ///
    ///   * Adding a vcpkg dependency to this plugin (e.g. fmt itself if you decide
    ///     to delegate formatting to libfmt; analogous to `simdjson` in the JSON
    ///     input formatter plugin):
    ///       1. Add the port to the project's vcpkg manifest:
    ///            vcpkg.json  ->  "dependencies": [ ..., "fmt" ]
    ///       2. Switch this plugin from `add_plugin(...)` (which injects sources into
    ///          the host registry component) to `add_plugin_as_library(...)`, which
    ///          creates a real library target you can link against by name. Then add
    ///          in CMakeLists.txt:
    ///            find_package(fmt CONFIG REQUIRED)
    ///            target_link_libraries(<plugin_library> PRIVATE fmt::fmt)
    ///       3. Reference example with a vcpkg dep:
    ///            nes-plugins/InputFormatters/JSONInputFormatter/CMakeLists.txt
    ///            (uses `find_package(simdjson CONFIG REQUIRED)` + target_link_libraries)
    return formatStringValue;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterFormatPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        not physicalFunctionRegistryArguments.childFunctions.empty(),
        "Format function must have at least one child function (the format string)");
    return FormatPhysicalFunction(std::move(physicalFunctionRegistryArguments.childFunctions));
}

}

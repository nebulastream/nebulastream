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

#include "PseudonymizePhysicalFunction.hpp"

#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <cstring>

namespace NES
{

PseudonymizePhysicalFunction::PseudonymizePhysicalFunction(PhysicalFunction childPhysicalFunction)
    : childPhysicalFunction(std::move(childPhysicalFunction))
{
}

namespace {
int32_t generateHMAC(int32_t inputId) {
    const char* secret_key= "dummy_key";
    int key_length = std::strlen(secret_key);

    unsigned char hmacResult[EVP_MAX_MD_SIZE];
    unsigned int hmacLen = 0;

    HMAC(
        EVP_sha256(),
        secret_key,
        key_length,
        reinterpret_cast<unsigned char*>(&inputId),
        sizeof(inputId),
        hmacResult,
        &hmacLen
    );

    int32_t result = 0;
    std::memcpy(&result, hmacResult, sizeof(result));

    return result > 0 ? result : -result;
}
}


VarVal PseudonymizePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto inputValue = childPhysicalFunction.execute(record, arena);
    // auto inputSize = inputValue.getSize();
    // auto maxOutputSize = (inputSize + nautilus::val<uint32_t>(2)) / nautilus::val<uint32_t>(3) * nautilus::val<uint32_t>(4);
    //auto output = arena.allocateVariableSizedData(maxOutputSize);
    // auto actualOutputSize = nautilus::invoke()
    auto inputInt = inputValue.getRawValueAs<nautilus::val<int32_t>>();
    auto opensslResult = nautilus::invoke(generateHMAC, inputInt);

    return VarVal(opensslResult);
    /// TODO(student): replace the identity passthrough below with the actual pseudonymization
    /// of `inputValue` (e.g. keyed hash, format-preserving encryption, ...).
    /// The returned VarVal must have the same integer type as `inputValue`.
    ///
    /// Where to start:
    ///   * Systest for this function (extend it with cases as you go):
    ///       nes-systests/function/dspro/Pseudonymize.test
    ///     Once Pseudonymize stops being the identity, update the expected rows there.
    ///
    ///   * Reference physical functions worth studying:
    ///       - nes-physical-operators/src/Functions/ArithmeticalFunctions/AbsolutePhysicalFunction.cpp
    ///         Minimal example of operating on a Nautilus `VarVal` (no external libs).
    ///       - nes-physical-operators/src/Functions/ToBase64PhysicalFunction.cpp
    ///         Shows how to call a native C/C++ helper from Nautilus code via
    ///         `nautilus::invoke(...)` and how to allocate output via the arena.
    ///       - nes-physical-operators/src/Functions/FromBase64PhysicalFunction.cpp
    ///         Companion to ToBase64; useful for round-trip / decoding patterns.
    ///     Their logical-side counterparts live under
    ///       nes-logical-operators/src/Functions/...
    ///
    ///   * Building / running NebulaStream:
    ///       - docs/development/development.md  (dev container, CMake profiles, build steps)
    ///       - docs/guide/extensibility.md      (plugin & registry model — read this first)
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
    ///       e.g. `systest -t .../Pseudonymize.test` (see docs/development/systests.md).
    ///
    ///   * Adding a vcpkg dependency to this plugin (e.g. OpenSSL for HMAC, or any other
    ///     library; analogous to `simdjson` in the JSON input formatter plugin):
    ///       1. Add the port name to the project's vcpkg manifest:
    ///            vcpkg.json  ->  "dependencies": [ ..., "openssl" ]
    ///       2. In this plugin's CMakeLists.txt, after the `add_plugin(...)` calls, add:
    ///            find_package(OpenSSL CONFIG REQUIRED)
    ///            target_link_libraries(<plugin_library> PRIVATE OpenSSL::Crypto)
    ///          For an `add_plugin(...)` (not `add_plugin_as_library`) the plugin sources
    ///          are compiled into the host registry component, so prefer
    ///          `add_plugin_as_library(...)` (see JSONInputFormatter below) when you need
    ///          a private dependency you can link against by name.
    ///       3. Reference example with a vcpkg dep:
    ///            nes-plugins/InputFormatters/JSONInputFormatter/CMakeLists.txt
    ///            (uses `find_package(simdjson CONFIG REQUIRED)` + target_link_libraries)
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterPseudonymizePhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1, "Pseudonymize function must have exactly one child function");
    return PseudonymizePhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}

}

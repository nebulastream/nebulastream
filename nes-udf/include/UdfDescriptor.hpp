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

#pragma once

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

class UdfCatalog;

/// Whether a NES data type may appear as a scalar-UDF argument or return type. v1 supports the numeric
/// widths, BOOLEAN, and VARSIZED; CHAR and UNDEFINED are rejected at registration (see docs/design/20260708_Scalar_UDF_Support.md, NG4).
[[nodiscard]] bool isSupportedUdfType(DataType::Type type);

/// The language a UDF is written in.
enum class UdfLanguage : std::uint8_t
{
    Python
};

/// Where a registered UDF runs. Inline compiles it ahead of time into the query pipeline (Codon for Python; no `.so`, no
/// ABI call per row). InProcess loads a bridge `.so` (e.g. CPython) that implements the UDF C ABI. OutOfProcess runs it
/// in a separate process.
enum class UdfExecution : std::uint8_t
{
    Inline,
    InProcess,
    OutOfProcess
};

/// The LANGUAGE and EXECUTION values of CREATE FUNCTION, in any case. Return nullopt for an unknown value.
[[nodiscard]] std::optional<UdfLanguage> parseUdfLanguage(std::string_view language);
[[nodiscard]] std::optional<UdfExecution> parseUdfExecution(std::string_view execution);

/// Splits an entry point "module.function" (the module may be a dotted package path) at its last dot.
/// Returns nullopt unless both parts are non-empty.
[[nodiscard]] std::optional<std::pair<std::string, std::string>> splitEntrypoint(std::string_view entrypoint);

/// The source of a UDF that carries its code instead of naming an entry point: the function body and the names its
/// arguments have inside that body. It travels with the plan, so the worker needs no file for it.
struct InlineUdfSource
{
    std::vector<std::string> parameterNames;
    std::string body;

    bool operator==(const InlineUdfSource&) const = default;
};

/// An Inline UDF that the catalog compiled when it was registered: LLVM bitcode of the function for the query pipeline
/// and for the interpreter. Its entry points are `symbol` and the interpreter's variant of it; the worker gives each call
/// its own name. It travels with the plan, so the worker does not compile the UDF again.
struct CompiledInlineUdf
{
    std::string symbol;
    std::string pipelineBitcode;
    std::string interpreterBitcode;

    bool operator==(const CompiledInlineUdf&) const = default;
};

/// A registered scalar UDF: name, language, where it executes, path to the `.so` exposing the UDF C ABI (InProcess only;
/// empty otherwise), the code to run (an entry point inside the `.so`, e.g. "module.function", or an inline source), and the
/// declared signature. No public constructor -- only reachable through `UdfCatalog::registerUdf` (which validates),
/// `fromSource`, or reflection (which trusts prior validation).
class UdfDescriptor
{
    std::string name;
    std::filesystem::path path;
    std::string entrypoint;
    std::optional<InlineUdfSource> source;
    std::vector<DataType> argTypes;
    /// Positions of the VARSIZED or CHAR arguments that a Python function receives as bytes instead of str, in ascending order.
    std::vector<uint64_t> bytesArguments;
    DataType returnType;
    UdfExecution execution;
    UdfLanguage language;
    std::optional<CompiledInlineUdf> compiled;
    /// The compiler of an Inline UDF ("Codon" or "Numba"), which also selects how the worker runs the compiled code.
    std::string compiler;
    /// PYTHON_COMPAT: Codon compiles with Python numerics (// and % floor, checked math and int(float), NaN in lists).
    bool pythonCompat;

    UdfDescriptor(
        std::string name,
        std::filesystem::path path,
        std::string entrypoint,
        std::optional<InlineUdfSource> source,
        std::vector<DataType> argTypes,
        std::vector<uint64_t> bytesArguments,
        DataType returnType,
        UdfExecution execution,
        UdfLanguage language,
        std::optional<CompiledInlineUdf> compiled = std::nullopt,
        std::string compiler = "Codon",
        bool pythonCompat = false)
        : name(std::move(name))
        , path(std::move(path))
        , entrypoint(std::move(entrypoint))
        , source(std::move(source))
        , argTypes(std::move(argTypes))
        , bytesArguments(std::move(bytesArguments))
        , returnType(returnType)
        , execution(execution)
        , language(language)
        , compiled(std::move(compiled))
        , compiler(std::move(compiler))
        , pythonCompat(pythonCompat)
    {
    }

    friend class NES::UdfCatalog;
    friend struct Reflector<UdfDescriptor>;
    friend struct Unreflector<UdfDescriptor>;

public:
    /// A UDF defined in code rather than through the catalog, e.g. by a function or operator that is implemented as a
    /// UDF. `path` is the bridge `.so` of an InProcess UDF and empty otherwise. The caller vouches for the signature.
    [[nodiscard]] static UdfDescriptor fromSource(
        std::string name,
        std::filesystem::path path,
        InlineUdfSource source,
        std::vector<DataType> argTypes,
        DataType returnType,
        UdfExecution execution,
        /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
        UdfLanguage language = UdfLanguage::Python);

    /// An Inline UDF defined in code that is compiled already, e.g. a stage of a UDF that HiPyChainRule split. `compiler`
    /// selects its UdfLowering ("HiPy"). The caller vouches for the signature.
    [[nodiscard]] static UdfDescriptor fromCompiled(
        std::string name, std::vector<DataType> argTypes, DataType returnType, std::string compiler, CompiledInlineUdf compiled);

    [[nodiscard]] const std::string& getName() const { return name; }

    [[nodiscard]] const std::filesystem::path& getPath() const { return path; }

    [[nodiscard]] const std::string& getEntrypoint() const { return entrypoint; }

    /// Set if the UDF carries its code; `getEntrypoint` is empty then.
    [[nodiscard]] const std::optional<InlineUdfSource>& getSource() const { return source; }

    [[nodiscard]] const std::vector<DataType>& getArgTypes() const { return argTypes; }

    [[nodiscard]] const std::vector<uint64_t>& getBytesArguments() const { return bytesArguments; }

    /// If the argument at `position` was declared with BYTES: the function receives it as bytes instead of str.
    [[nodiscard]] bool isBytesArgument(uint64_t position) const { return std::ranges::binary_search(bytesArguments, position); }

    [[nodiscard]] const DataType& getReturnType() const { return returnType; }

    [[nodiscard]] UdfExecution getExecution() const { return execution; }

    [[nodiscard]] UdfLanguage getLanguage() const { return language; }

    /// Set for an Inline UDF that the catalog compiled at registration.
    [[nodiscard]] const std::optional<CompiledInlineUdf>& getCompiled() const { return compiled; }

    /// "Codon" or "Numba"; meaningful for an Inline UDF.
    [[nodiscard]] const std::string& getCompiler() const { return compiler; }

    /// True if the UDF was created with COMPILER CODON PYTHON_COMPAT.
    [[nodiscard]] bool getPythonCompat() const { return pythonCompat; }

    bool operator==(const UdfDescriptor&) const = default;
};

template <>
struct Reflector<UdfDescriptor>
{
    Reflected operator()(const UdfDescriptor& descriptor, const ReflectionContext& context) const;
};

template <>
struct Unreflector<UdfDescriptor>
{
    UdfDescriptor operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

}

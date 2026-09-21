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

/// How a registered UDF executes. InProcess loads a bridge `.so` (CPython, PyPy, ...) that implements the UDF C ABI.
/// Codon compiles the entry point's module ahead of time into the query pipeline (no `.so`, no ABI call per row).
enum class UdfExecution : std::uint8_t
{
    InProcess,
    Codon
};

/// Splits an entry point "module.function" (the module may be a dotted package path) at its last dot.
/// Returns nullopt unless both parts are non-empty.
[[nodiscard]] std::optional<std::pair<std::string, std::string>> splitEntrypoint(std::string_view entrypoint);

/// A registered scalar UDF: name, how it executes, path to the `.so` exposing the UDF C ABI (InProcess only; empty
/// for Codon), entry point inside it (e.g. "module.function"), and the declared signature. No public constructor --
/// only reachable through `UdfCatalog::registerUdf` (which validates) or reflection (which trusts prior validation).
class UdfDescriptor
{
    std::string name;
    std::filesystem::path path;
    std::string entrypoint;
    std::vector<DataType> argTypes;
    DataType returnType;
    UdfExecution execution;

    UdfDescriptor(
        std::string name,
        std::filesystem::path path,
        std::string entrypoint,
        std::vector<DataType> argTypes,
        DataType returnType,
        UdfExecution execution)
        : name(std::move(name))
        , path(std::move(path))
        , entrypoint(std::move(entrypoint))
        , argTypes(std::move(argTypes))
        , returnType(returnType)
        , execution(execution)
    {
    }

    friend class NES::UdfCatalog;
    friend struct Reflector<UdfDescriptor>;
    friend struct Unreflector<UdfDescriptor>;

public:
    [[nodiscard]] const std::string& getName() const { return name; }

    [[nodiscard]] const std::filesystem::path& getPath() const { return path; }

    [[nodiscard]] const std::string& getEntrypoint() const { return entrypoint; }

    [[nodiscard]] const std::vector<DataType>& getArgTypes() const { return argTypes; }

    [[nodiscard]] const DataType& getReturnType() const { return returnType; }

    [[nodiscard]] UdfExecution getExecution() const { return execution; }

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

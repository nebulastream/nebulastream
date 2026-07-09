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

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

class UdfCatalog;

/// Whether a NES data type may appear as a scalar-UDF argument or return type.
/// v1 supports the numeric widths, BOOLEAN, and VARSIZED; CHAR and UNDEFINED are
/// rejected at registration (see docs/design/20260708_Scalar_UDF_Support.md, NG4).
[[nodiscard]] bool isSupportedUdfType(DataType::Type type);

/// A registered scalar UDF: the user-given name, the path to the `.so` exposing
/// the UDF C ABI, the entry point inside it (e.g. a "module.function" string),
/// and the declared signature (ordered argument types + return type).
///
/// Constructible only through `UdfCatalog::registerUdf` (which validates) or
/// through reflection (which trusts the coordinator-side checks). There is no
/// public constructor — callers route through those paths.
class UdfDescriptor
{
    std::string name;
    std::filesystem::path path;
    std::string entrypoint;
    std::vector<DataType> argTypes;
    DataType returnType;

    UdfDescriptor(std::string name, std::filesystem::path path, std::string entrypoint, std::vector<DataType> argTypes, DataType returnType)
        : name(std::move(name))
        , path(std::move(path))
        , entrypoint(std::move(entrypoint))
        , argTypes(std::move(argTypes))
        , returnType(returnType)
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

    bool operator==(const UdfDescriptor&) const = default;
};

template <>
struct Reflector<UdfDescriptor>
{
    Reflected operator()(const UdfDescriptor& descriptor) const;
};

template <>
struct Unreflector<UdfDescriptor>
{
    UdfDescriptor operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

}

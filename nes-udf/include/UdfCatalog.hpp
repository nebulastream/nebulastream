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
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Util/NamedCatalog.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

/// Stores registered scalar UDFs keyed by name: validated metadata (path, entry point, signature), and for Inline UDFs the
/// code that the compiler produced at registration (see UdfCompilerRegistry). Loading the `.so` of an InProcess UDF is
/// deferred to worker-side lowering. Storage/lookup is delegated to NamedCatalog.
///
/// Not thread-safe — registration is serialized through DDL statement handling, mirroring ModelCatalog.
class UdfCatalog
{
    NamedCatalog<UdfDescriptor> catalog;
    std::vector<std::string> importPaths;

public:
    UdfCatalog() = default;

    /// `importPaths`: where the compiler of Inline UDFs looks for the modules they import, besides NES_UDF_PATH.
    explicit UdfCatalog(std::vector<std::string> importPaths) : importPaths(std::move(importPaths)) { }

    /// Applies to UDFs registered from now on.
    void setImportPaths(std::vector<std::string> paths) { importPaths = std::move(paths); }

    [[nodiscard]] const std::vector<std::string>& getImportPaths() const { return importPaths; }

    /// `path` is the bridge `.so` for InProcess UDFs and unused (empty) for Inline UDFs, whose `entrypoint` names an
    /// importable "module.function". An Inline UDF is compiled here; a compile error is thrown, and the UDF is not registered.
    /// Later changes to the modules it imports do not affect it until it is registered again.
    void registerUdf(
        std::string name,
        std::filesystem::path path,
        std::string entrypoint,
        std::vector<DataType> argTypes,
        DataType returnType,
        /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
        UdfExecution execution = UdfExecution::InProcess,
        /// The UDF's code if it carries it, in which case `entrypoint` is empty.
        /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
        std::optional<InlineUdfSource> source = std::nullopt,
        /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
        UdfLanguage language = UdfLanguage::Python,
        /// Positions of the VARSIZED or CHAR arguments that the function receives as bytes instead of str.
        /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
        std::vector<uint64_t> bytesArguments = {});
    void removeUdf(const std::string& udfName);
    [[nodiscard]] bool hasUdf(const std::string& udfName) const;
    [[nodiscard]] std::vector<std::string> getUdfNames() const;
    [[nodiscard]] std::vector<UdfDescriptor> getRegisteredUdfs() const;
    [[nodiscard]] UdfDescriptor load(const std::string& udfName) const;
};

}

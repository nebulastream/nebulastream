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
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

/// Stores registered scalar UDFs keyed by name. Loading the `.so` and preparing
/// it for execution is deferred to worker-side lowering — the catalog only holds
/// validated metadata (path, entry point, signature).
///
/// Not thread-safe — registration is serialized through DDL statement handling,
/// mirroring ModelCatalog. Concurrent access requires external synchronization.
class UdfCatalog
{
    std::unordered_map<std::string, UdfDescriptor> entries;

public:
    void
    registerUdf(std::string name, std::filesystem::path path, std::string entrypoint, std::vector<DataType> argTypes, DataType returnType);
    void removeUdf(const std::string& udfName);
    [[nodiscard]] bool hasUdf(const std::string& udfName) const;
    [[nodiscard]] std::vector<std::string> getUdfNames() const;
    [[nodiscard]] std::vector<UdfDescriptor> getRegisteredUdfs() const;
    [[nodiscard]] UdfDescriptor load(const std::string& udfName) const;
};

}

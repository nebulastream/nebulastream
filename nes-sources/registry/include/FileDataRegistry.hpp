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
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <Sources/SourceDataProvider.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using FileDataRegistryReturnType = PhysicalSourceConfig;

struct FileDataRegistryArguments
{
    PhysicalSourceConfig physicalSourceConfig;
    std::shared_ptr<std::vector<std::jthread>> serverThreads;
    std::filesystem::path testFilePath;
};

using FileDataFn = std::function<FileDataRegistryReturnType(FileDataRegistryArguments)>;

/// Filled by loadBuiltinPlugins() / plugin registration (see cmake/RuntimeRegistrationUtil.cmake).
/// Case-insensitive to mirror the retired BaseRegistry: lookups use canonical upper-case type
/// names while plugins register under their spelled name.
class FileDataRegistry : public RuntimeRegistry<FileDataRegistry, std::string, FileDataFn, /*CaseSensitive*/ false>
{
public:
    /// Defined out-of-line (FileDataRegistry.cpp) instead of inheriting the base's inline
    /// definition, so exactly one instance exists process-wide even with plugins loaded as
    /// shared objects.
    static FileDataRegistry& instance();
};

}

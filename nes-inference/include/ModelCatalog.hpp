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
#include <utility>
#include <vector>

#include <Util/Reflection.hpp>
#include <Model.hpp>

namespace NES
{

/// Manages model registration and stores imported model bodies keyed by name.
/// Compilation to executable bytecode is deferred until worker-side lowering.
/// Not thread-safe — concurrent access requires external synchronization.
class ModelCatalog
{
    std::unordered_map<std::string, RegisteredModel> entries;

public:
    void registerModel(std::string name, std::filesystem::path path, ModelSchema schema);
    void removeModel(const std::string& modelName);
    [[nodiscard]] bool hasModel(const std::string& modelName) const;
    [[nodiscard]] std::vector<std::string> getModelNames() const;
    [[nodiscard]] std::vector<RegisteredModel> getRegisteredModels() const;
    [[nodiscard]] RegisteredModel load(const std::string& modelName) const;
};

}

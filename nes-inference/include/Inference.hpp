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

#include <expected>
#include <filesystem>
#include <string>

#include <Model.hpp>

namespace NES
{

struct ImportError
{
    std::string message;
};

struct CompileError
{
    std::string message;
};

/// Import a model from a file.
std::expected<ImportedModel, ImportError> importModel(const std::filesystem::path& modelPath);

/// Compile a previously imported model so it can be executed by the runtime.
std::expected<CompiledModel, CompileError> compileModel(const ImportedModel& imported);

}

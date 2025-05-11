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
#include <optional>
#include <string>
#include <Model.hpp>

namespace NES::Nebuli::Inference
{

/**
* Check if Nebuli was built with Inference support and all relevant tools are available
*/
bool enabled();

struct ModelOptions
{
    std::optional<size_t> opset;
};

struct ModelLoadError
{
    std::string message;
};

std::expected<Model, ModelLoadError> load(const std::filesystem::path& path, const ModelOptions& options);

}

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

namespace NES
{

/// Reads a test file into memory.
/// The only place the pipeline opens a test file, so every stage behind it works on text.
/// Throws when the file cannot be read, which fails that one test file rather than the run.
[[nodiscard]] std::string readTestFile(const std::filesystem::path& path);

}

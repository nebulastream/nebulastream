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

#include <Model/TestFile.hpp>
#include <Parser/SystestParser.hpp>

namespace NES
{

/// Builds the structured form of a test file from the sections the parser reports.
/// It parses no SQL of its own.
/// Registers its callbacks on the parser and then runs it, so the parser must not have been run yet.
[[nodiscard]] TestFile parseTestFile(SystestParser& parser, const std::filesystem::path& path);

}

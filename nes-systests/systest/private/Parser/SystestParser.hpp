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

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <Model/ParsedTestFile.hpp>

namespace NES
{

/// Replaces every whole-word occurrence of the keyword in the file before parsing.
struct Substitution
{
    std::string keyword;
    std::string replacement;
};

/// Parses a systest file, a dialect of the sqllogictest format, into its statements in file order.
/// Throws an exception with error code SLTUnexpectedToken or InvalidQuerySyntax on a malformed file.
[[nodiscard]] std::vector<TestStatement> parseTestFile(std::string_view content, std::span<const Substitution> substitutions);

}

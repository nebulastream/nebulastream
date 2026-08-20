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

#include <string>
#include <vector>

#include <Model/Verdict.hpp>

namespace NES
{

/// One explain check whose expected lines are compared with the printed plan verbatim, in order.
struct ExplainLinesCheck
{
    std::vector<std::string> expected;
    std::string actual;

    /// Compares line by line. Indentation counts; trailing whitespace and blank lines do not.
    [[nodiscard]] Verdict check() const;
};

/// One explain check whose expected lines are `<REGEX>` and `<!REGEX>` assertions, each matched against the printed plan.
struct ExplainRegexCheck
{
    std::vector<std::string> expected;
    std::string actual;

    /// A malformed tag or an untagged line among the assertions fails the check.
    [[nodiscard]] Verdict check() const;
};

/// Whether the expected lines of an EXPLAIN have regex tags, which decides which of the checks to apply.
[[nodiscard]] bool hasExplainRegexTags(const std::vector<std::string>& expected);

}

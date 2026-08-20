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

#include <Model/Expectation.hpp>
#include <Model/Verdict.hpp>

namespace NES
{

/// One explain check: the plan that the optimizer printed, and the lines that the test expects.
/// A `<REGEX>` or `<!REGEX>` tag on the expected lines makes the checker match each as a regex instead of comparing it directly.
struct ExplainCheck
{
    std::string actualOutput;
    ExpectedResult expected;

    /// Compares the plan that the EXPLAIN printed against the expected lines, in order.
    /// The checker right-trims every line and drops the empty ones, because indentation matters but an empty line ends an expected block.
    /// The check passes when every line matches, or every regex assertion holds.
    [[nodiscard]] Verdict check() const;
};

}

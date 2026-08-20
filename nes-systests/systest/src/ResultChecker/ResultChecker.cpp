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

#include <ResultChecker/ResultChecker.hpp>

#include <expected>

#include <fmt/format.h>

#include <Model/Verdict.hpp>
#include <ResultChecker/ResultComparison.hpp>
#include <ResultChecker/ResultFile.hpp>

namespace NES
{

Verdict ResultCheck::check() const
{
    const auto loaded = loadQueryResult(resultFile);
    if (not loaded)
    {
        return std::unexpected(Mismatch{fmt::format("result file was not written: {}", resultFile.string())});
    }

    return toVerdict(compare(expectedSchema, expected, loaded->schema, loaded->result), false);
}

}

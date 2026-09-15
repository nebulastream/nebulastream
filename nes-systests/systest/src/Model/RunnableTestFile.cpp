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

#include <Model/RunnableTestFile.hpp>

#include <unordered_set>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Util/Overloaded.hpp>

namespace NES
{

void keepSelectedCases(RunnableTestFile& runnable, const std::unordered_set<SystestQueryId>& selected)
{
    if (selected.empty())
    {
        return;
    }
    std::erase_if(
        runnable.cases,
        [&](const RewrittenCase& testCase)
        {
            return not std::visit(
                Overloaded{
                    [&](const RewrittenQuery& query) { return selected.contains(query.id); },
                    [&](const RewrittenDifferential& block)
                    { return selected.contains(block.firstId) or selected.contains(block.secondId); },
                    [&](const RewrittenExplain& explain) { return selected.contains(explain.id); }},
                testCase.action);
        });
}

}

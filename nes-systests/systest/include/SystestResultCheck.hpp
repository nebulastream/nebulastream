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

#include <SLTParser.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
/// loads the query result for a sep
/// TODO #373: refactor result checking to be type save
std::optional<std::vector<std::string>> loadQueryResult(const Query& query);

/// TODO #373: refactor result checking to be type save
bool checkResult(const Query& query);
}

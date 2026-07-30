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

#include <compare>
#include <cstdint>
#include <filesystem>
#include <string>

#include <Identifiers/NESStrongType.hpp>

namespace NES::Systest
{

using TestName = std::string;
using TestGroup = std::string;

using SystestQueryId = NESStrongType<uint64_t, struct SystestQueryId_, 0, 1>;
static constexpr SystestQueryId INVALID_SYSTEST_QUERY_ID = INVALID<SystestQueryId>;
static constexpr SystestQueryId INITIAL_SYSTEST_QUERY_ID = INITIAL<SystestQueryId>;

struct CaseKey
{
    std::filesystem::path relativeTestFile;
    SystestQueryId queryNumber = INVALID_SYSTEST_QUERY_ID;

    auto operator<=>(const CaseKey&) const = default;
};

struct TestCaseId
{
    CaseKey source;
    uint32_t configurationVariant = 0;

    auto operator<=>(const TestCaseId&) const = default;
};

struct EnvironmentId
{
    uint64_t value = 0;

    auto operator<=>(const EnvironmentId&) const = default;
};

}

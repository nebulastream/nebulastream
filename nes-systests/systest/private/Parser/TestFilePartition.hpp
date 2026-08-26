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

#include <vector>

#include <Model/ConfigurationOverride.hpp>
#include <Model/ParsedTestFile.hpp>

namespace NES
{

/// Each partition runs on its own worker, because a worker takes its configuration only at startup.
struct TestFilePartition
{
    ConfigurationOverride overrides;
    ParsedTestFile file;
};

/// Splits a test file into one partition per distinct configuration override, in declaration order.
/// E.g., CREATE a, Q1 [x=1], Q2 [x=2], Q3 [x=1] -> {CREATE a, Q1, Q3} and {CREATE a, Q2}.
[[nodiscard]] std::vector<TestFilePartition> partitionByOverrides(const ParsedTestFile& testFile);

}

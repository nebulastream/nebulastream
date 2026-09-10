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

#include <Model/Verdict.hpp>

namespace NES::Systest
{

/// One differential check: the two result files that have to agree.
/// Neither file is the expected answer, because a differential block asserts only that its queries agree.
struct DifferentialCheck
{
    std::filesystem::path resultFile;
    std::filesystem::path differentialResultFile;
};

/// Compares the two result files of a differential block against each other, comparing values by type as a result check does.
[[nodiscard]] Verdict checkDifferential(const DifferentialCheck& check);

}

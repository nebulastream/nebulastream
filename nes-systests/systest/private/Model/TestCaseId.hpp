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

#include <ostream>
#include <string>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// Identifies one case in a run's reports.
/// A report line prints this rather than the file path, because one run may report many cases from the same file,
/// and a query with multiple configuration overrides runs once per alternative.
struct TestCaseId
{
    std::string originFile;
    SystestQueryId queryIdInFile = INVALID<SystestQueryId>;
    ConfigurationOverride overrides;
};

/// Prints `file:N`, followed by ` [key=value, ...]` when the run has overrides.
std::ostream& operator<<(std::ostream& os, const TestCaseId& id);

}

FMT_OSTREAM(NES::TestCaseId);

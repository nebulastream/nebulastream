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

namespace NES::Validator
{

/// Run the full optimizer pipeline on a YAML topology and return explain output.
/// Equivalent to `nes-cli dump`: populates catalogs, runs LegacyOptimizer, formats results.
/// Returns optimizer explain text on success, or an error message prefixed with "Error: ".
std::string explainTopology(const std::string& yamlString);

} // namespace NES::Validator

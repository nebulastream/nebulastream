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
#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>
#include <GeneratorRate.hpp>

namespace NES
{

class FixedGeneratorRate final : public GeneratorRate
{
    double emitRateTuplesPerSecond;

public:
    static std::optional<double> parseAndValidateConfigString(std::string_view configString);
    explicit FixedGeneratorRate(std::string_view configString);
    ~FixedGeneratorRate() override = default;
    uint64_t calcNumberOfTuplesForInterval(
        const std::chrono::time_point<std::chrono::system_clock>& start,
        const std::chrono::time_point<std::chrono::system_clock>& end) override;
};
}

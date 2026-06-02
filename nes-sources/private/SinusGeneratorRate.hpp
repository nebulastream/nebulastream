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
#include <tuple>
#include <GeneratorRate.hpp>

namespace NES
{

/// Provides an emit rate that resembles a sinus, specified by the amplitude and the frequency.
/// As a negative emit rate is not clearly defined, we take the absolute values of a sinus, i.e., |sin(x)| or std::abs(std::sin(x))
class SinusGeneratorRate final : public GeneratorRate
{
    double frequency = 0;
    double amplitude = 0;

public:
    /// @brief Tries to parse the amplitude and the frequency. If successful returns them in this order
    static std::optional<std::tuple<double, double>> parseAndValidateConfigString(std::string_view configString);
    explicit SinusGeneratorRate(double frequency, double amplitude);
    ~SinusGeneratorRate() override = default;
    uint64_t calcNumberOfTuplesForInterval(
        const std::chrono::time_point<std::chrono::system_clock>& start,
        const std::chrono::time_point<std::chrono::system_clock>& end) override;
};
}

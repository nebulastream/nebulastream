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
#include <expected>
#include <optional>
#include <string_view>
#include <variant>
#include <ErrorHandling.hpp>

namespace NES
{
/// @brief Specifies the tuples should be emitted for the generator source
class GeneratorRate
{
public:
    enum class Type : uint8_t
    {
        FIXED,
        SINUS
    };

    virtual ~GeneratorRate() = default;

    /// @brief This method calculates the number of tuples to be generated in the provided interval
    /// This method should be called in the fillTupleBuffer of the GeneratorSource before emitting the tuple buffer
    virtual uint64_t calcNumberOfTuplesForInterval(
        const std::chrono::time_point<std::chrono::system_clock>& start, const std::chrono::time_point<std::chrono::system_clock>& end)
        = 0;
};

/// @brief Rate configs for the generator source. Parsed from the rate config string.
struct FixedGeneratorRateConfig
{
    double emitRate;
};

struct SinusGeneratorRateConfig
{
    double amplitude;
    double frequency;
};

using GeneratorRateVariant = std::variant<FixedGeneratorRateConfig, SinusGeneratorRateConfig>;

/// Parsers for the rate config string. The outer optional signals "not this rate type", the inner
/// expected carries parse/validation errors for the matched type.
std::optional<std::expected<FixedGeneratorRateConfig, Exception>> parseValidateFixedRateConfigString(std::string_view configString);
std::optional<std::expected<SinusGeneratorRateConfig, Exception>> parseValidateSinusRateConfigString(std::string_view configString);

uint64_t calcNumberOfTuplesForInterval(
    const FixedGeneratorRateConfig& rate,
    const std::chrono::time_point<std::chrono::system_clock>& start,
    const std::chrono::time_point<std::chrono::system_clock>& end);
uint64_t calcNumberOfTuplesForInterval(
    const SinusGeneratorRateConfig& rate,
    const std::chrono::time_point<std::chrono::system_clock>& start,
    const std::chrono::time_point<std::chrono::system_clock>& end);
}

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

#include <BloomFilterConfiguration.hpp>

#include <cstdint>
#include <expected>
#include <utility>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<bool> ENABLE_BLOOM_FILTER{
    Identifier::parse("enable_bloom_filter"),
    "Enabling the in-map BloomFilter of a hash join's hash maps, which skips walking a chain for keys the map cannot contain. "
    "Costs one bit array per hash map, sized from expected_entries and the false positive rate below.",
    true};

const ConfigField<double> FALSE_POSITIVE_RATE{
    Identifier::parse("bloom_filter_false_positive_rate"),
    "False positive rate the BloomFilter is sized for. Lower rates skip more chain walks but grow the bit array and the number of "
    "hash positions probed per lookup.",
    [](const ConfigLiteral& literal) -> std::expected<double, Exception>
    {
        auto result = tryGetOr<double>(literal, expectedType<double>());
        if (!result)
        {
            return std::unexpected{std::move(result).error()};
        }
        const auto value = *result;
        if (value <= 0.0 || value >= 1.0)
        {
            return std::unexpected{InvalidConfigParameter("bloom_filter_false_positive_rate must be in (0, 1), got {}", value)};
        }
        if (value > 0.5)
        {
            return std::unexpected{
                InvalidConfigParameter("bloom_filter_false_positive_rate above 0.5 collapses to a useless filter, got {}", value)};
        }
        return value;
    },
    0.01};

const ConfigField<uint64_t> EXPECTED_ENTRIES{
    Identifier::parse("expected_entries"),
    "Number of keys one hash map is expected to hold, which is what the bit array is sized for. This is not bounded by "
    "number_of_partitions: the hash maps never rehash, they just grow their chains, so the entry count is a property of the "
    "workload. Too low saturates the filter and it stops skipping anything; too high wastes memory per hash map.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(narrowConfigValue<int64_t, uint64_t>); },
    uint64_t{10'000}};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> BloomFilterConfiguration::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("bloom_filter"), ENABLE_BLOOM_FILTER, FALSE_POSITIVE_RATE, EXPECTED_ENTRIES);
}

BloomFilterConfiguration BloomFilterConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {config.get(ENABLE_BLOOM_FILTER), config.get(FALSE_POSITIVE_RATE), config.get(EXPECTED_ENTRIES)};
}
}

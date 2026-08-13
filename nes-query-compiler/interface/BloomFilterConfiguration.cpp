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

#include <expected>
#include <utility>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
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
    "Costs one bit array per hash map, sized from max_number_of_buckets and the false positive rate below.",
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
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> BloomFilterConfiguration::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("bloom_filter"), ENABLE_BLOOM_FILTER, FALSE_POSITIVE_RATE);
}

BloomFilterConfiguration BloomFilterConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {.enableBloomFilter = config.get(ENABLE_BLOOM_FILTER), .falsePositiveRate = config.get(FALSE_POSITIVE_RATE)};
}
}

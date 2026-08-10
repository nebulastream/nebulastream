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

#include <memory>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/FloatValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>

namespace NES
{

static constexpr auto DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;
/// FloatValidation compares inclusively, so the bounds have to stay strictly inside (0, 1): BloomFilterParams
/// treats an endpoint rate as a programming error and terminates on it. Above 0.5 the derived sizing collapses
/// to a single hash over a near-saturated bit array, which no longer filters anything worth the probe.
static constexpr auto MIN_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.000001;
static constexpr auto MAX_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.5;
static constexpr auto DEFAULT_BLOOM_FILTER_EXPECTED_ENTRIES = 10'000;

/// Configuration for the ChainedHashMap's optional in-map BloomFilter
class BloomFilterConfiguration final : public BaseConfiguration
{
public:
    BloomFilterConfiguration() = default;
    BloomFilterConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    BoolOption enableBloomFilter
        = {"enable_bloom_filter",
           "true",
           "Enabling the in-map BloomFilter of a hash join's hash maps, which skips walking a chain for keys the map cannot contain. "
           "Costs one bit array per hash map, sized from expected_entries and the false positive rate below."};
    FloatOption falsePositiveRate
        = {"bloom_filter_false_positive_rate",
           std::to_string(DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE),
           "False positive rate the BloomFilter is sized for. Lower rates skip more chain walks but grow the bit array and the number of "
           "hash positions probed per lookup.",
           {std::make_shared<FloatValidation>(MIN_BLOOM_FILTER_FALSE_POSITIVE_RATE, MAX_BLOOM_FILTER_FALSE_POSITIVE_RATE)}};
    UIntOption expectedEntries
        = {"expected_entries",
           std::to_string(DEFAULT_BLOOM_FILTER_EXPECTED_ENTRIES),
           "Number of keys one hash map is expected to hold, which is what the bit array is sized for. This is not bounded by "
           "number_of_partitions: the hash maps never rehash, they just grow their chains, so the entry count is a property of the "
           "workload. Too low saturates the filter and it stops skipping anything; too high wastes memory per hash map.",
           {std::make_shared<NumberValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override { return {&enableBloomFilter, &falsePositiveRate, &expectedEntries}; }
};
}

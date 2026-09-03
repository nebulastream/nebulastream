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

#include <SliceCacheConfiguration.hpp>

#include <cstdint>
#include <expected>
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
const ConfigField<bool> ENABLE_SLICE_CACHE{
    Identifier::parse("enable_slice_cache"),
    "Enabling Slice Cache for increased performance by caching the data structure access per thread. Unless weird behavior happens, "
    "it should not be necessary to disable the slice cache.",
    true};

const ConfigField<uint64_t> NUMBER_OF_ENTRIES{
    Identifier::parse("number_of_entries_sliceCache"),
    "Size of the slice cache",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<uint64_t>())
            .and_then(narrowConfigValue<int64_t, uint64_t>)
            .and_then(
                [](const uint64_t value) -> std::expected<uint64_t, Exception>
                {
                    if (value == 0)
                    {
                        return std::unexpected{InvalidConfigParameter("Slice cache size cannot be zero")};
                    }
                    return value;
                });
    },
    uint64_t{10}};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> SliceCacheConfiguration::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("slice_cache"), ENABLE_SLICE_CACHE, NUMBER_OF_ENTRIES);
}

SliceCacheConfiguration SliceCacheConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {config.get(ENABLE_SLICE_CACHE), config.get(NUMBER_OF_ENTRIES)};
}
}

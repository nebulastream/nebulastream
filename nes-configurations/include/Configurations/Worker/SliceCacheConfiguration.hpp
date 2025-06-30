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

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NonZeroValidation.hpp>
#include <fmt/ranges.h>


namespace NES::Configurations
{

enum class SliceCacheType : uint8_t
{
    NONE,
    TWO_QUEUES,
    LRU,
    FIFO,
    SECOND_CHANCE
};

struct SliceCacheOptions
{
    SliceCacheType sliceCacheType;
    uint64_t numberOfEntries;
};

class SliceCacheConfiguration final : public BaseConfiguration
{
public:
    SliceCacheConfiguration() = default;
    SliceCacheConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    NES::Configurations::EnumOption<SliceCacheType> sliceCacheType
        = {"sliceCacheType",
           SliceCacheType::NONE,
           fmt::format("Type of slice cache {}", fmt::join(magic_enum::enum_names<SliceCacheType>(), ", "))};
    NES::Configurations::UIntOption numberOfEntriesSliceCache
        = {"numberOfEntriesSliceCache", "1", "Size of the slice cache", {std::make_shared<NES::Configurations::NonZeroValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override { return {&sliceCacheType, &numberOfEntriesSliceCache}; }
};
}

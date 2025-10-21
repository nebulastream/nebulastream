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

enum class PredictionCacheType : uint8_t
{
    NONE,
    TWO_QUEUES,
    FIFO,
    LFU,
    LRU,
    SECOND_CHANCE
};

struct PredictionCacheOptions
{
    PredictionCacheType predictionCacheType;
    uint64_t numberOfEntries;
};

class PredictionCacheConfiguration final : public BaseConfiguration
{
public:
    PredictionCacheConfiguration() = default;
    PredictionCacheConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    EnumOption<PredictionCacheType> predictionCacheType
        = {"PredictionCacheType",
           PredictionCacheType::FIFO,
           fmt::format("Type of prediction cache {}", fmt::join(magic_enum::enum_names<PredictionCacheType>(), ", "))};
    UIntOption numberOfEntriesPredictionCache
        = {"numberOfEntriesPredictionCache", "16", "Size of the prediction cache", {std::make_shared<NonZeroValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override { return {&predictionCacheType, &numberOfEntriesPredictionCache}; }
};
}

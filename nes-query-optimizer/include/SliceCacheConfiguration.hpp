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
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NonZeroValidation.hpp>

namespace NES
{

class SliceCacheConfiguration final : public BaseConfiguration
{
public:
    SliceCacheConfiguration() = default;
    SliceCacheConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    BoolOption enableSliceCache = {"sliceCache", "true", "Enabling Slice Cache for increased performance"};
    UIntOption numberOfEntries
        = {"numberOfEntriesSliceCache", "10", "Size of the slice cache", {std::make_shared<NonZeroValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override { return {&enableSliceCache, &numberOfEntries}; }
};
}

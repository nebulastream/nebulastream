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
#include <Traits/TraitSet.hpp>

namespace NES::Optimizer
{

struct OriginIdTrait
{
    const std::vector<OriginId> originIds;

public:
    explicit OriginIdTrait(std::vector<OriginId> originIds) : originIds(originIds) { }
    explicit OriginIdTrait(OriginId originId) : originIds({originId}) { }
    explicit OriginIdTrait() : originIds() {};
    bool operator==(const OriginIdTrait& other) const { return originIds == other.originIds; }
    static constexpr bool atNode() { return true; }
    static std::string getName()
    {
        return "OriginId";
    }

    std::string toString() const
    {
        std::ostringstream oss;
        oss << "[";
        for (size_t i = 0; i < originIds.size(); ++i)
        {
            if (i != 0)
            {
                oss << ", ";
            }
            oss << originIds[i];
        }
        oss << "]";
        return oss.str();
    }

    std::vector<OriginId> getOriginIds()
    {
        return originIds;
    }
};

static_assert(Trait<OriginIdTrait>);

}

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

#include <cstddef>
#include <cstdint>
#include <typeinfo>
#include <Traits/Trait.hpp>
#include <magic_enum/magic_enum.hpp>
#include <SerializableTrait.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{


/// Struct that stores memoryLayout types as traits. For now, we simply have a choice/implementation type for the memoryLayouts
struct MemoryLayoutTypeTrait final : public TraitConcept
{
    Schema::MemoryLayoutType incomingLayoutType;
    Schema::MemoryLayoutType targetLayoutType;

    explicit MemoryLayoutTypeTrait(Schema::MemoryLayoutType incomingLayoutType, Schema::MemoryLayoutType targetLayoutType) : incomingLayoutType(incomingLayoutType), targetLayoutType(targetLayoutType) { }

    [[nodiscard]] const std::type_info& getType() const override { return typeid(MemoryLayoutTypeTrait); }


    bool operator==(const TraitConcept& other) const override
    {
        const auto casted = dynamic_cast<const MemoryLayoutTypeTrait*>(&other);
        if (casted == nullptr)
        {
            return false;
        }
        return incomingLayoutType == casted->incomingLayoutType && targetLayoutType == casted->targetLayoutType;
    };

};
}

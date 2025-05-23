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

#include <string>
#include <vector>
#include <Traits/Trait.hpp>

namespace NES
{

/// Marks an operator as creator of new origin ids
struct OriginIdAssignerTrait final : TraitConcept
{
    bool operator==(const TraitConcept& other) const override;
    friend std::ostream& operator<<(std::ostream& os, const TraitConcept& trait);
    [[nodiscard]] const std::type_info& getType() const override;
    [[nodiscard]] SerializableTrait serialize() const override;
};

}

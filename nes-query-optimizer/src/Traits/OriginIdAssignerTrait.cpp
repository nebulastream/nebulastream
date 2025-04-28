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

#include <memory>
#include <ostream>
#include <Traits/OriginIdAssignerTrait.hpp>

namespace NES::Optimizer
{

bool OriginIdAssignerTrait::operator==(const TraitConcept& other) const
{
    if (dynamic_cast<const OriginIdAssignerTrait*>(&other))
    {
        return true;
    }
    return false;
}

std::string OriginIdAssignerTrait::toString() const
{
    return "OriginIdAssignerTrait";
}

const std::type_info& OriginIdAssignerTrait::getType() const
{
    return typeid(this);
}

SerializableTrait OriginIdAssignerTrait::serialize() const
{
    SerializableTrait trait;
    trait.set_trait_type(getType().name());
    return trait;
}
}

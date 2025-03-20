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
#include <set>

namespace NES::Optimizer
{

struct AbstractTrait
{
    virtual ~AbstractTrait() = default;
    virtual bool operator==(const AbstractTrait& other) const = 0;
};

using TraitSet = std::set<std::unique_ptr<AbstractTrait>>;

template <typename T>
bool hasTrait(const TraitSet& traitSet)
{
    return std::any_of(traitSet.begin(), traitSet.end(),
                       [](const auto& trait) {
                           return dynamic_cast<T*>(trait.get()) != nullptr;
                       });
}

template <typename... TraitTypes>
bool hasTraits(const TraitSet& traitSet)
{
    return (hasTrait<TraitTypes>(traitSet) && ...);
}

/*
bool isSuperSet(const TraitSet& superset, const TraitSet& subset) {
    return std::all_of(subset.begin(), subset.end(), [&superset](const auto& subTrait) {
       return std::any_of(superset.begin(), superset.end(), [&subTrait](const auto& superTrait) {
          return *superTrait == *subTrait;
      });
   });
}
*/

}
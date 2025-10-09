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
#include <optional>
#include <ranges>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class TraitSet
{
public:
    explicit TraitSet() = default;

    template <IsTrait... TraitType>
    explicit TraitSet(TraitType&&... traits)
    {
        traitMap = std::unordered_map<std::type_index, Trait>{
            ((std::make_pair<std::type_index, Trait>(typeid(TraitType), std::forward<TraitType>(traits))), ...)};
    }

    template <std::ranges::input_range Range>
    requires std::is_same_v<std::ranges::range_value_t<Range>, Trait>
    explicit TraitSet(Range traits)
        : traitMap(
              traits | std::views::transform([](const Trait& trait) { return std::make_pair(std::type_index{trait.getTypeInfo()}, trait); })
              | std::ranges::to<std::unordered_map<std::type_index, Trait>>())
    {
    }

    template <IsTrait TraitType>
    [[nodiscard]] std::optional<TraitType> tryGet() const
    {
        if (const auto found = traitMap.find(typeid(TraitType)); found != traitMap.end())
        {
            return found->second.get<TraitType>();
        }
        return std::nullopt;
    }

    template <IsTrait TraitType>
    [[nodiscard]] TraitType get() const
    {
        const auto found = traitMap.find(typeid(TraitType));
        INVARIANT(found != traitMap.end(), "Trait {} not found", typeid(TraitType).name());
        return found->second.get<TraitType>();
    }

    template <IsTrait TraitType>
    [[nodiscard]] bool contains() const
    {
        return traitMap.contains(typeid(TraitType));
    }

    template <IsTrait TraitType>
    [[nodiscard]] bool tryInsert(TraitType trait)
    {
        const auto [iter, success] = traitMap.try_emplace(typeid(TraitType), std::move(trait));
        return success;
    }

    friend bool operator==(const TraitSet& lhs, const TraitSet& rhs);

    [[nodiscard]] auto begin() const { return traitMap.cbegin(); }

    [[nodiscard]] auto end() const { return traitMap.cend(); }

    [[nodiscard]] std::size_t size() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    std::unordered_map<std::type_index, Trait> traitMap;
};

static_assert(std::ranges::input_range<TraitSet>);

template <typename T>
std::optional<T> getTrait(const TraitSet& traitSet)
{
    return traitSet.tryGet<T>();
}

template <typename T>
bool hasTrait(const TraitSet& traitSet)
{
    return traitSet.contains<T>();
}

template <typename... TraitTypes>
bool hasTraits(const TraitSet& traitSet)
{
    return (hasTrait<TraitTypes>(traitSet) && ...);
}

template <IsTrait TraitType>
bool tryInsert(TraitSet& traitset, TraitType trait)
{
    return traitset.tryInsert(std::move(trait));
}

}

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

#include <tuple>
#include <vector>
#include <TraitSets/Trait.hpp>

template <Trait... T>
class RecursiveTupleTraitSet
{
    std::tuple<T...> underlying;
    std::vector<RecursiveTupleTraitSet<T...>> children;

public:
    explicit RecursiveTupleTraitSet(T... args) : underlying(args...) { }
    explicit RecursiveTupleTraitSet(std::vector<RecursiveTupleTraitSet<T...>>& children, T... args) : underlying(args...), children(children) { }

    bool operator==(const RecursiveTupleTraitSet& other) const { return this == &other; }

    template <Trait O>
    requires(std::same_as<O, T> || ...)
    O get()
    {
        return std::get<O>(underlying);
    }

    std::vector<RecursiveTupleTraitSet> getChildren()
    {
        return children;
    }
};
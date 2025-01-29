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
#include <set>

namespace NES::Optimizer
{

template <typename T, typename = std::void_t<>>
struct is_std_hashable : std::false_type
{
};

template <typename T>
struct is_std_hashable<T, std::void_t<decltype(std::declval<std::hash<T>>()(std::declval<T>()))>> : std::true_type
{
};

template <typename T>
constexpr bool is_std_hashable_v = is_std_hashable<T>::value;

template <typename T>
concept Trait = requires(T trait, T other) {
    //placeholder condition
    {
        T::atNode()
    } -> std::same_as<bool>;
    {
        trait == other
    };
    {
        T::getName()
    } -> std::same_as<std::string>;
};

template <typename T>
concept RecursiveTrait = requires()
{
    //Not going to be easy to make it work how I'd like it.
    //Ideally, we could just name a container type in the RecursiveTrait and use this as the container for the children.
    //But, the container type only gets fully specified when specifying a trait set with some parameters, because
    //the other traits need to be saved too.
    //There are two ways around this:
    //Always only have collections with pointers to TraitSets (I dislike this solution because now our plan parts are spread all across memory).
    //In my opinion, the better solution would be to allow for a finite set of options in this RecursiveTrait concept
    //and translate them into specific container types.
    //That way, all possible container types are known to the trait set concept.
    //
    //IN THEORY, it should be possible for a compiler to do the original idea, but it would require the compiler to
    //understand when to stop expanding.
    //Maybe this is possible by swapping in a "stop" type after one expansion with "using"
    //
    //In addition, we could also model other nested data structures as trait sets, like predicates.
    //These Traits could contain a definition of what traitset they contain, e.g. a predicate would only contain other predicates.
    //This would give us a unified representation of "relnodes and rexnodes" in calcite-lingo
    {T::recursive()};
};

//Traits with compile-time finite instances (enums or booleans like isGPU) could be added like
template <typename T>
concept FiniteTrait = requires() {
    { T::getInstances() } -> std::same_as<std::set<T>>;
}
    && Trait<T>;


class VirtualTrait
{
public:
    virtual std::string getName() = 0;
};

template <Trait T>
class VirtualTraitImpl : VirtualTrait
{
    T trait;
public:
    std::string getName() override
    {
        return trait.getName();
    }
};

}
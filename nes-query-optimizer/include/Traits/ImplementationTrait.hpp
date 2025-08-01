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

#include <Traits/Trait.hpp>
#include <SerializableTrait.pb.h>

namespace NES
{

/// Marks if a join operator should be translated/lowered to a netsed loop join
struct ImplementationTrait final : DefaultTrait<ImplementationTrait>
{
    static constexpr std::string_view Name = "Implementation";
    struct Data
    {
        std::string implementation;
    };

    Data data;

    const std::string& implementation() const { return data.implementation; }

    explicit ImplementationTrait(std::string implementation) : data(std::move(implementation)) { }
};
}

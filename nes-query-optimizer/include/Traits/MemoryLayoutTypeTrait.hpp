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
#include <string>
#include <string_view>
#include <typeinfo>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Struct that stores the memory layout type as traits. For now, we simply choose a row layout all the time
struct MemoryLayoutTypeTrait final
{
    static constexpr std::string_view NAME = "MemoryLayoutType";
    MemoryLayoutType memoryLayout;

    explicit MemoryLayoutTypeTrait(MemoryLayoutType memoryLayout);

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const MemoryLayoutTypeTrait& other) const;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<MemoryLayoutTypeTrait>;
};

template <>
struct Reflector<MemoryLayoutTypeTrait>
{
    Reflected operator()(const MemoryLayoutTypeTrait& trait) const;
};

template <>
struct Unreflector<MemoryLayoutTypeTrait>
{
    MemoryLayoutTypeTrait operator()(const Reflected& reflected) const;
};

static_assert(TraitConcept<MemoryLayoutTypeTrait>);

}

namespace NES::detail
{
struct ReflectedMemoryLayoutTypeTrait
{
    MemoryLayoutType memoryLayout;
};
}

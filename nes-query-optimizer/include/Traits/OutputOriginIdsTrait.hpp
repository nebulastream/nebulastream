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
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

class OutputOriginIdsTrait final
{
public:
    static constexpr std::string_view NAME = "OutputOriginIds";
    explicit OutputOriginIdsTrait(std::vector<OriginId> originIds);

    [[nodiscard]] const std::type_info& getType() const;
    [[nodiscard]] std::string_view getName() const;
    bool operator==(const OutputOriginIdsTrait& other) const;
    [[nodiscard]] size_t hash() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] auto begin() const -> decltype(std::declval<std::vector<OriginId>>().cbegin());
    [[nodiscard]] auto end() const -> decltype(std::declval<std::vector<OriginId>>().cend());

    OriginId& operator[](size_t index);
    const OriginId& operator[](size_t index) const;
    [[nodiscard]] size_t size() const;

private:
    std::vector<OriginId> originIds;

    friend Reflector<OutputOriginIdsTrait>;
};

template <>
struct Reflector<OutputOriginIdsTrait>
{
    Reflected operator()(const OutputOriginIdsTrait& trait) const;
};

template <>
struct Unreflector<OutputOriginIdsTrait>
{
    OutputOriginIdsTrait operator()(const Reflected& reflected) const;
};

static_assert(TraitConcept<OutputOriginIdsTrait>);

}

namespace NES::detail
{
struct ReflectedOutputOriginIdsTrait
{
    std::vector<uint64_t> originIds;
};
}

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
#include <string>
#include <string_view>
#include <typeinfo>
#include <unordered_map>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

class FieldMappingTrait final
{
public:
    static constexpr std::string_view NAME = "FieldMapping";

    explicit FieldMappingTrait(std::unordered_map<UnqualifiedUnboundField, IdentifierList> mapping);

    [[nodiscard]] std::optional<IdentifierList> getMapping(const UnqualifiedUnboundField& field) const;
    [[nodiscard]] const std::unordered_map<UnqualifiedUnboundField, IdentifierList>& getUnderlying() const;

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    bool operator==(const FieldMappingTrait& other) const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] size_t hash() const;

private:
    std::unordered_map<UnqualifiedUnboundField, IdentifierList> mapping;

    friend Reflector<FieldMappingTrait>;
};

template <>
struct Reflector<FieldMappingTrait>
{
    Reflected operator()(const FieldMappingTrait& trait) const;
};

template <>
struct Unreflector<FieldMappingTrait>
{
    FieldMappingTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<FieldMappingTrait>);

}

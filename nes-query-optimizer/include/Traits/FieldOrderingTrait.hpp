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


#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

class FieldOrderingTrait final
{
public:
    static constexpr std::string_view NAME = "FieldOrdering";

    explicit FieldOrderingTrait(Schema<UnqualifiedUnboundField, Ordered> orderedFields);

    const Schema<UnqualifiedUnboundField, Ordered>& getOrderedFields() const;

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    bool operator==(const FieldOrderingTrait& other) const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] size_t hash() const;

private:
    Schema<UnqualifiedUnboundField, Ordered> orderedFields;

    friend Reflector<FieldOrderingTrait>;
};

template <>
struct Reflector<FieldOrderingTrait>
{
    Reflected operator()(const FieldOrderingTrait& trait) const;
};

template <>
struct Unreflector<FieldOrderingTrait>
{
    FieldOrderingTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<FieldOrderingTrait>);

}

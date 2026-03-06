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

#include <Traits/FieldOrderingTrait.hpp>
#include <folly/Hash.h>
#include "Serialization/IdentifierSerializationUtil.hpp"
#include <Util/Reflection.hpp>

#include "TraitRegistry.hpp"

namespace NES
{
FieldOrderingTrait::FieldOrderingTrait(Schema<UnqualifiedUnboundField, Ordered> orderedFields) : orderedFields(std::move(orderedFields))
{
}

const Schema<UnqualifiedUnboundField, Ordered>& FieldOrderingTrait::getOrderedFields() const
{
    return orderedFields;
}

const std::type_info& FieldOrderingTrait::getType() const
{
    return typeid(FieldOrderingTrait);
}

std::string_view FieldOrderingTrait::getName() const
{
    return NAME;
}

Reflected Reflector<FieldOrderingTrait>::operator()(const FieldOrderingTrait& trait) const
{
    return reflect(trait.orderedFields);
}

FieldOrderingTrait Unreflector<FieldOrderingTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto orderedFields = context.unreflect<Schema<UnqualifiedUnboundField, Ordered>>(reflected);
    return FieldOrderingTrait{std::move(orderedFields)};
}

bool FieldOrderingTrait::operator==(const FieldOrderingTrait& other) const
{
    return this->orderedFields == other.orderedFields;
}

std::string FieldOrderingTrait::explain(ExplainVerbosity) const
{
    return fmt::format(
        "FieldOrderingTrait({})",
        fmt::join(orderedFields | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); }), ", "));
}

size_t FieldOrderingTrait::hash() const
{
    return folly::hash::hash_range(orderedFields.begin(), orderedFields.end());
}
}
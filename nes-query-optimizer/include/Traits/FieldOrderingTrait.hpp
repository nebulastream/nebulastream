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


#include "DataTypes/UnboundSchema.hpp"
#include "Schema/Field.hpp"


#include "Trait.hpp"

namespace NES
{

class FieldOrderingTrait : public TraitConcept
{
public:
    static constexpr std::string_view NAME = "FieldOrdering";

    explicit FieldOrderingTrait(SchemaBase<UnboundFieldBase<1>, true> orderedFields);

    const SchemaBase<UnboundFieldBase<1>, true>& getOrderedFields() const;

    [[nodiscard]] const std::type_info& getType() const override;
    [[nodiscard]] std::string_view getName() const override;
    [[nodiscard]] SerializableTrait serialize() const override;
    bool operator==(const TraitConcept& other) const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] size_t hash() const override;

private:
    SchemaBase<UnboundFieldBase<1>, true> orderedFields;
};

}
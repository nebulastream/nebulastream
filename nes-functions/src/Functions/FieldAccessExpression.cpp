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

#include <Functions/FieldAccessExpression.hpp>
#include <ErrorHandling.hpp>
#include <API/Schema.hpp>

namespace NES
{
FieldAccessExpression::FieldAccessExpression(Schema::Identifier field) : field(std::move(field))
{
}
FieldAccessExpression::FieldAccessExpression(std::string field) : field(Schema::Identifier{.name = std::move(field), .table = {}})
{
}
const Schema::Identifier& FieldAccessExpression::getFieldName() const
{
    return field;
}
std::shared_ptr<Expression> FieldAccessExpression::clone() const
{
    return std::make_shared<FieldAccessExpression>(*this);
}
void FieldAccessExpression::inferStamp(const Schema& schema, std::vector<ExpressionValue>&)
{
    if (auto fieldInSchema = schema.getFieldByName(field))
    {
        stamp = fieldInSchema->second;
        return;
    }

    throw CannotInferSchema("FieldAccess: Field `{}` does not exist in {}", field, schema);
}
bool FieldAccessExpression::equals(const NES::Expression& other) const
{
    if (const auto* otherFunction = dynamic_cast<const FieldAccessExpression*>(&other))
    {
        return otherFunction->field == field;
    }
    return false;
}
std::string FieldAccessExpression::toString() const
{
    return format_as(field);
}
}

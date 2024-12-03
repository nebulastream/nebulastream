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
#include <memory>
#include <ostream>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
NodeFunctionFieldRename::NodeFunctionFieldRename(const std::shared_ptr<NodeFunctionFieldAccess>& originalField, std::string newFieldName)
    : NodeFunction(originalField->getStamp(), "FieldRename"), originalField(originalField), newFieldName(std::move(newFieldName)) {};

NodeFunctionFieldRename::NodeFunctionFieldRename(const std::shared_ptr<NodeFunctionFieldRename>& other)
    : NodeFunctionFieldRename(other->getOriginalField(), other->getNewFieldName()) {};

std::shared_ptr<NodeFunction>
NodeFunctionFieldRename::create(const std::shared_ptr<NodeFunctionFieldAccess>& originalField, std::string newFieldName)
{
    return std::make_shared<NodeFunctionFieldRename>(NodeFunctionFieldRename(originalField, std::move(newFieldName)));
}

bool NodeFunctionFieldRename::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionFieldRename>(rhs))
    {
        auto otherFieldRead = NES::Util::as<NodeFunctionFieldRename>(rhs);
        return otherFieldRead->getOriginalField()->equal(getOriginalField()) && this->newFieldName == otherFieldRead->getNewFieldName();
    }
    return false;
}

std::shared_ptr<NodeFunctionFieldAccess> NodeFunctionFieldRename::getOriginalField() const
{
    return this->originalField;
}

std::string NodeFunctionFieldRename::getNewFieldName() const
{
    return newFieldName;
}

std::ostream& NodeFunctionFieldRename::toDebugString(std::ostream& os) const
{
    auto node = getOriginalField();
    return os << fmt::format("FieldRenameFunction({} => {} : {})", *node, newFieldName, stamp->toString());
}

std::ostream& NodeFunctionFieldRename::toQueryPlanString(std::ostream& os) const
{
    auto node = getOriginalField();
    return os << fmt::format("FieldRename({:q} => {})", *node, newFieldName);
}

void NodeFunctionFieldRename::inferStamp(const Schema& schema)
{
    auto originalFieldName = getOriginalField();
    originalFieldName->inferStamp(schema);
    auto fieldName = originalFieldName->getFieldName();
    auto fieldAttribute = schema.getFieldByName(fieldName);
    ///Detect if user has added attribute name separator
    if (!fieldAttribute)
    {
        throw FieldNotFound("Original field with name: {} does not exists in the schema: {}", fieldName, schema.toString());
    }
    if (newFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        newFieldName = fieldName.substr(0, fieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1) + newFieldName;
    }

    if (fieldName == newFieldName)
    {
        NES_WARNING("Both existing and new fields are same: existing: {} new field name: {}", fieldName, newFieldName);
    }
    else
    {
        auto newFieldAttribute = schema.getFieldByName(newFieldName);
        if (newFieldAttribute)
        {
            throw FieldAlreadyExists("New field with name " + newFieldName + " already exists in the schema " + schema.toString());
        }
    }
    /// assign the stamp of this field access with the type of this field.
    stamp = fieldAttribute.value()->getDataType();
}

std::shared_ptr<NodeFunction> NodeFunctionFieldRename::deepCopy()
{
    return NodeFunctionFieldRename::create(Util::as<NodeFunctionFieldAccess>(originalField->deepCopy()), newFieldName);
}

bool NodeFunctionFieldRename::validateBeforeLowering() const
{
    ///NodeFunction Currently, we do not have any validation for FieldRename before lowering
    return true;
}

}

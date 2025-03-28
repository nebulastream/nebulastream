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

#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <memory>
#include <Configurations/Descriptor.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Operators/UnaryLogicalOperator.hpp>
#include <Plans/Operator.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

EventTimeWatermarkAssignerLogicalOperator::EventTimeWatermarkAssignerLogicalOperator(
std::shared_ptr<LogicalFunction> onField, Windowing::TimeUnit unit)
: Operator()
, UnaryLogicalOperator()
, onField(std::move(onField))
, unit(unit)
{
}


std::string EventTimeWatermarkAssignerLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "EVENTTIMEWATERMARKASSIGNER(" << id << ")";
    return ss.str();
}

bool EventTimeWatermarkAssignerLogicalOperator::isIdentical(const Operator& rhs) const
{
    return (*this == rhs) && dynamic_cast<const EventTimeWatermarkAssignerLogicalOperator*>(&rhs)->id == id;
}

bool EventTimeWatermarkAssignerLogicalOperator::operator==(Operator const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const EventTimeWatermarkAssignerLogicalOperator*>(&rhs)) {
        bool onFieldEqual = false;
        if (onField && rhsOperator->onField) {
            // Compare based on string representation (or call a custom equals() method if available)
            onFieldEqual = (onField->toString() == rhsOperator->onField->toString());
        }
        else if (!onField && !rhsOperator->onField) {
            onFieldEqual = true;
        }
        return onFieldEqual && (rhsOperator->unit == this->unit);
    }
    return false;
}

std::shared_ptr<Operator> EventTimeWatermarkAssignerLogicalOperator::clone() const
{
    std::shared_ptr<LogicalFunction> clonedOnField;
    if (onField)
    {
        clonedOnField = onField->clone();
    }
    auto copy = std::make_shared<EventTimeWatermarkAssignerLogicalOperator>(clonedOnField, unit);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}


bool EventTimeWatermarkAssignerLogicalOperator::inferSchema()
{
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }
    return true;
}

// TODO
SerializableOperator EventTimeWatermarkAssignerLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(children[0]->id.getRawValue());

    NES::FunctionList list;
    auto* serializedFunction = list.add_functions();
    FunctionSerializationUtil::serializeFunction(this->onField, serializedFunction);

    Configurations::DescriptorConfig::ConfigType configVariant = list;
    SerializableVariantDescriptor variantDescriptor =
        Configurations::descriptorConfigTypeToProto(configVariant);
    (*opDesc->mutable_config())["functionList"] = variantDescriptor;

    auto* unaryOpDesc = new SerializableOperator_UnaryLogicalOperator();


    auto* inputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchema(), inputSchema);
    unaryOpDesc->set_allocated_inputschema(inputSchema);

    for (const auto& originId : this->getInputOriginIds()) {
        unaryOpDesc->add_originids(originId.getRawValue());
    }

    opDesc->set_allocated_unaryoperator(unaryOpDesc);

    auto* outputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->outputSchema, outputSchema);
    serializedOperator.set_allocated_outputschema(outputSchema);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterEventTimeWatermarkAssignerLogicalOperator(LogicalOperatorRegistryArguments)
{
    // TODO
    return nullptr;
}

}

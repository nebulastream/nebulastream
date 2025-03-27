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
#include <Operators/LogicalOperator.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

EventTimeWatermarkAssignerLogicalOperator::EventTimeWatermarkAssignerLogicalOperator(LogicalFunction onField, Windowing::TimeUnit unit)
: onField(std::move(onField)), unit(unit)
{
}

std::string_view EventTimeWatermarkAssignerLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string EventTimeWatermarkAssignerLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "EVENTTIMEWATERMARKASSIGNER(" << id << ")";
    return ss.str();
}

bool EventTimeWatermarkAssignerLogicalOperator::operator==(LogicalOperatorConcept const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const EventTimeWatermarkAssignerLogicalOperator*>(&rhs)) {
        bool onFieldEqual = (onField.toString() == rhsOperator->onField.toString());
        return onFieldEqual && (rhsOperator->unit == this->unit);
    }
    return false;
}


bool EventTimeWatermarkAssignerLogicalOperator::inferSchema(Schema)
{
    std::vector<Schema> distinctSchemas;

    /// Infer schema of all child operators
    for (auto& child : children)
    {
        if (!child.inferSchema(outputSchema))
        {
            throw CannotInferSchema("BinaryOperator: failed inferring the schema of the child operator");
        }
    }

    /// Identify different type of schemas from children operators
    for (const auto& child : children)
    {
        auto childOutputSchema = child.getInputSchemas()[0];
        auto found = std::find_if(
            distinctSchemas.begin(),
            distinctSchemas.end(),
            [&](const Schema& distinctSchema) { return (childOutputSchema == distinctSchema); });
        if (found == distinctSchemas.end())
        {
            distinctSchemas.push_back(childOutputSchema);
        }
    }

    ///validate that only two different type of schema were present
    INVARIANT(distinctSchemas.size() == 2, "BinaryOperator: this node should have exactly two distinct schemas");
    return true;
}


Optimizer::TraitSet EventTimeWatermarkAssignerLogicalOperator::getTraitSet() const
{
    return {};
}

void EventTimeWatermarkAssignerLogicalOperator::setChildren(std::vector<LogicalOperator> children)
{
    this->children = children;
}

std::vector<Schema> EventTimeWatermarkAssignerLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema EventTimeWatermarkAssignerLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> EventTimeWatermarkAssignerLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> EventTimeWatermarkAssignerLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

void EventTimeWatermarkAssignerLogicalOperator::setOutputOriginIds(std::vector<OriginId> ids)
{
    outputOriginIds = ids;
}

void EventTimeWatermarkAssignerLogicalOperator::setInputOriginIds(std::vector<std::vector<OriginId>> ids)
{
    inputOriginIds = ids;
}

std::vector<LogicalOperator> EventTimeWatermarkAssignerLogicalOperator::getChildren() const
{
    return children;
}

// TODO
SerializableOperator EventTimeWatermarkAssignerLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(getChildren()[0].getId().getRawValue());

    NES::FunctionList list;
    auto* serializedFunction = list.add_functions();
    serializedFunction->CopyFrom(onField.serialize());

    NES::Configurations::DescriptorConfig::ConfigType configVariant = list;
    SerializableVariantDescriptor variantDescriptor =
        Configurations::descriptorConfigTypeToProto(configVariant);
    (*opDesc->mutable_config())["functionList"] = variantDescriptor;

    auto* unaryOpDesc = new SerializableOperator_UnaryLogicalOperator();


    auto* inputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchemas()[0], inputSchema);
    unaryOpDesc->set_allocated_inputschema(inputSchema);

    for (const auto& originId : this->getInputOriginIds()[0]) {
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
LogicalOperatorGeneratedRegistrar::RegisterEventTimeWatermarkAssignerLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    // TODO
    ///return EventTimeWatermarkAssignerLogicalOperator();
    throw UnsupportedOperation();
}

}

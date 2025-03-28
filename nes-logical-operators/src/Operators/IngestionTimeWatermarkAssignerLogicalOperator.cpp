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
#include <sstream>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

IngestionTimeWatermarkAssignerLogicalOperator::IngestionTimeWatermarkAssignerLogicalOperator() = default;

std::string_view IngestionTimeWatermarkAssignerLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string IngestionTimeWatermarkAssignerLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "INGESTIONTIMEWATERMARKASSIGNER(" << id << ")";
    return ss.str();
}

bool IngestionTimeWatermarkAssignerLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const IngestionTimeWatermarkAssignerLogicalOperator*>(&rhs))
    {
    }
    return false;
}

bool IngestionTimeWatermarkAssignerLogicalOperator::inferSchema()
{
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }
    // If any additional schema inference is needed, add it here.
    return true;
}


Optimizer::TraitSet IngestionTimeWatermarkAssignerLogicalOperator::getTraitSet() const
{
    return {};
}

void IngestionTimeWatermarkAssignerLogicalOperator::setChildren(std::vector<LogicalOperator> children)
{
    this->children = children;
}

std::vector<Schema> IngestionTimeWatermarkAssignerLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema IngestionTimeWatermarkAssignerLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> IngestionTimeWatermarkAssignerLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> IngestionTimeWatermarkAssignerLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

void IngestionTimeWatermarkAssignerLogicalOperator::setOutputOriginIds(std::vector<OriginId> ids)
{
    outputOriginIds = ids;
}

void IngestionTimeWatermarkAssignerLogicalOperator::setInputOriginIds(std::vector<std::vector<OriginId>> ids)
{
    inputOriginIds = ids;
}

std::vector<LogicalOperator> IngestionTimeWatermarkAssignerLogicalOperator::getChildren() const
{
    return children;
}

SerializableOperator IngestionTimeWatermarkAssignerLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(getChildren()[0].getId().getRawValue());

    auto* unaryOpDesc = new SerializableOperator_UnaryLogicalOperator();
    auto* inputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchemas()[0], inputSchema);
    unaryOpDesc->set_allocated_inputschema(inputSchema);

    for (const auto& originId : getInputOriginIds()[0])
    {
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
LogicalOperatorGeneratedRegistrar::RegisterIngestionTimeWatermarkAssignerLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    // TODO
    throw UnknownLogicalOperator();
}


}

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

#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <memory>
#include <sstream>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Operators/UnaryLogicalOperator.hpp>
#include <Plans/Operator.hpp>
#include <SerializableOperator.pb.h>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

IngestionTimeWatermarkAssignerLogicalOperator::IngestionTimeWatermarkAssignerLogicalOperator()
: Operator(), UnaryLogicalOperator()
{
}

std::string IngestionTimeWatermarkAssignerLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "INGESTIONTIMEWATERMARKASSIGNER(" << id << ")";
    return ss.str();
}

bool IngestionTimeWatermarkAssignerLogicalOperator::isIdentical(const Operator& rhs) const
{
    return (*this == rhs) && dynamic_cast<const IngestionTimeWatermarkAssignerLogicalOperator*>(&rhs)->id == id;
}

bool IngestionTimeWatermarkAssignerLogicalOperator::operator==(Operator const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const IngestionTimeWatermarkAssignerLogicalOperator*>(&rhs)) {
    }
    return false;
}


std::shared_ptr<Operator> IngestionTimeWatermarkAssignerLogicalOperator::clone() const
{
    auto copy = std::make_shared<IngestionTimeWatermarkAssignerLogicalOperator>();
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
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

SerializableOperator IngestionTimeWatermarkAssignerLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(children[0]->id.getRawValue());

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

std::unique_ptr<LogicalOperator>
LogicalOperatorGeneratedRegistrar::RegisterIngestionTimeWatermarkAssignerLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    // TODO
    return nullptr;
}


}

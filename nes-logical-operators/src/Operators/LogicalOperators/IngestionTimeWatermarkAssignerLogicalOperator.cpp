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
#include <Operators/LogicalOperators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/UnaryLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <SerializableOperator.pb.h>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Configurations/Descriptor.hpp>

namespace NES
{

IngestionTimeWatermarkAssignerLogicalOperator::IngestionTimeWatermarkAssignerLogicalOperator(OperatorId id)
: Operator(id), UnaryLogicalOperator(id)
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
    return (*this == rhs) && dynamic_cast<const IngestionTimeWatermarkAssignerLogicalOperator*>(&rhs)->getId() == id;
}

bool IngestionTimeWatermarkAssignerLogicalOperator::operator==(Operator const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const IngestionTimeWatermarkAssignerLogicalOperator*>(&rhs)) {
    }
    return false;
}


std::shared_ptr<Operator> IngestionTimeWatermarkAssignerLogicalOperator::clone() const
{
    auto copy = std::make_shared<IngestionTimeWatermarkAssignerLogicalOperator>(id);
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
    serializedOperator.set_operatorid(this->getId().getRawValue());
    serializedOperator.add_childrenids(children[0]->getId().getRawValue());

    auto* unaryOpDesc = new SerializableOperator_UnaryLogicalOperator();
    auto* outputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getOutputSchema(), outputSchema);
    unaryOpDesc->set_allocated_outputschema(outputSchema);

    auto* inputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchema(), inputSchema);
    unaryOpDesc->set_allocated_inputschema(inputSchema);

    for (const auto& originId : this->getInputOriginIds()) {
        unaryOpDesc->add_originids(originId.getRawValue());
    }

    opDesc->set_allocated_unaryoperator(unaryOpDesc);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}


}

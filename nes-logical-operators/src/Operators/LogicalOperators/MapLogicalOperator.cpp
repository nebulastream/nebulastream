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
#include <string>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/MapLogicalOperator.hpp>
#include <Configurations/Descriptor.hpp>
#include <Operators/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableSchema.pb.h>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>

namespace NES
{

MapLogicalOperator::MapLogicalOperator(const std::shared_ptr<FieldAssignmentLogicalFunction>& mapFunction, OperatorId id)
    : Operator(id), UnaryLogicalOperator(id), mapFunction(mapFunction)
{
}

std::shared_ptr<FieldAssignmentLogicalFunction> MapLogicalOperator::getMapFunction() const
{
    return mapFunction;
}

bool MapLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const MapLogicalOperator*>(&rhs)->getId() == id;
}

bool MapLogicalOperator::operator==(Operator const& rhs) const
{
    if (auto rhsOperator = dynamic_cast<const MapLogicalOperator*>(&rhs))
    {
        return this->mapFunction == rhsOperator->mapFunction;
    }
    return false;
};

bool MapLogicalOperator::inferSchema()
{
    /// infer the default input and output schema
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }

    /// use the default input schema to calculate the out schema of this operator.
    mapFunction->inferStamp(*getInputSchema());

    const auto assignedField = mapFunction->getField();
    if (std::string fieldName = assignedField->getFieldName(); outputSchema->getFieldByName(fieldName))
    {
        /// The assigned field is part of the current schema.
        /// Thus we check if it has the correct type.
        NES_TRACE("MAP Logical Operator: the field {} is already in the schema, so we updated its type.", fieldName);
        outputSchema->replaceField(fieldName, assignedField->getStamp());
    }
    else
    {
        /// The assigned field is not part of the current schema.
        /// Thus we extend the schema by the new attribute.
        NES_TRACE("MAP Logical Operator: the field {} is not part of the schema, so we added it.", fieldName);
        outputSchema->addField(fieldName, assignedField->getStamp());
    }
    return true;
}

std::string MapLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "MAP(opId: " << id << ": predicate: " << *mapFunction << ")";
    return ss.str();
}

std::shared_ptr<Operator> MapLogicalOperator::clone() const
{
    auto copy = std::make_shared<MapLogicalOperator>(Util::as<FieldAssignmentLogicalFunction>(mapFunction->clone()), id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

SerializableOperator MapLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->getId().getRawValue());
    serializedOperator.add_childrenids(children[0]->getId().getRawValue());

    NES::FunctionList list;
    auto* serializedFunction = list.add_functions();
    FunctionSerializationUtil::serializeFunction(this->getMapFunction(), serializedFunction);

    Configurations::DescriptorConfig::ConfigType configVariant = list;
    SerializableVariantDescriptor variantDescriptor =
        Configurations::descriptorConfigTypeToProto(configVariant);
    (*opDesc->mutable_config())["functionList"] = variantDescriptor;

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

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
MapLogicalOperator::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<MapLogicalOperator::ConfigParameters>(std::move(config), NAME);
}

std::unique_ptr<LogicalOperator>
LogicalOperatorGeneratedRegistrar::RegisterMapLogicalOperator(NES::LogicalOperatorRegistryArguments config)
{
    auto functionVariant = config.config[MapLogicalOperator::ConfigParameters::MAP_FUNCTION_NAME];
    if (std::holds_alternative<NES::FunctionList>(functionVariant)) {
        const auto& functionList = std::get<NES::FunctionList>(functionVariant);
        const auto& functions = functionList.functions();
        INVARIANT(functions.size() == 1, "Expected exactly one function");
        auto fieldAssignmentFunction = FunctionSerializationUtil::deserializeFunction(functions[0]);
        return std::make_unique<MapLogicalOperator>(Util::as<FieldAssignmentLogicalFunction>(fieldAssignmentFunction), getNextOperatorId());

    }
    //throw CannotDeserialize("Error while deserializing MapLogicalOperator with config {}", config);
    return nullptr;
}

}

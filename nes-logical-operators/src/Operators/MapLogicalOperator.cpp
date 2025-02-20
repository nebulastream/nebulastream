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

#include <Operators/MapLogicalOperator.hpp>
#include <memory>
#include <string>
#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Plans/Operator.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableSchema.pb.h>
#include <ErrorHandling.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

MapLogicalOperator::MapLogicalOperator(const std::shared_ptr<FieldAssignmentLogicalFunction>& mapFunction)
    : Operator(), UnaryLogicalOperator(), mapFunction(mapFunction)
{
}

std::shared_ptr<FieldAssignmentLogicalFunction> MapLogicalOperator::getMapFunction() const
{
    return mapFunction;
}

bool MapLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const MapLogicalOperator*>(&rhs)->id == id;
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
    auto copy = std::make_shared<MapLogicalOperator>(Util::as<FieldAssignmentLogicalFunction>(mapFunction->clone()));
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
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(children[0]->id.getRawValue());

    NES::FunctionList list;
    auto* serializedFunction = list.add_functions();
    serializedFunction->CopyFrom(getMapFunction()->serialize());

    NES::Configurations::DescriptorConfig::ConfigType configVariant = list;
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

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
MapLogicalOperator::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return NES::Configurations::DescriptorConfig::validateAndFormat<MapLogicalOperator::ConfigParameters>(std::move(config), NAME);
}

std::unique_ptr<LogicalOperator>
LogicalOperatorGeneratedRegistrar::RegisterMapLogicalOperator(NES::LogicalOperatorRegistryArguments config)
{
    auto functionVariant = config.config[MapLogicalOperator::ConfigParameters::MAP_FUNCTION_NAME];
    if (std::holds_alternative<NES::FunctionList>(functionVariant)) {
        const auto& functionList = std::get<NES::FunctionList>(functionVariant);
        const auto& functions = functionList.functions();
        INVARIANT(functions.size() == 1, "Expected exactly one function");

        auto functionType = functions[0].functiontype();
        NES::Configurations::DescriptorConfig::Config functionDescriptorConfig{};
        for (const auto& [key, value] : functions[0].config())
        {
            functionDescriptorConfig[key] = Configurations::protoToDescriptorConfigType(value);
        }

        if (auto function = LogicalFunctionRegistry::instance().create(functionType, LogicalFunctionRegistryArguments(functionDescriptorConfig)))
        {
            std::shared_ptr<NES::LogicalFunction> sharedBase(function.value().get());
            std::shared_ptr<NES::FieldAssignmentLogicalFunction> derivedSharedPtr =
                std::dynamic_pointer_cast<NES::FieldAssignmentLogicalFunction>(sharedBase);
            return std::make_unique<MapLogicalOperator>(derivedSharedPtr);
        }

        return nullptr;
    }
    return nullptr;
}

}

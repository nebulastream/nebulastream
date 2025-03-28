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
#include <SerializableOperator.pb.h>
#include <SerializableSchema.pb.h>
#include <ErrorHandling.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

MapLogicalOperator::MapLogicalOperator(const FieldAssignmentLogicalFunction& mapFunction) : mapFunction(mapFunction)
{
}

std::string_view MapLogicalOperator::getName() const noexcept
{
    return NAME;
}

const FieldAssignmentLogicalFunction& MapLogicalOperator::getMapFunction() const
{
    return mapFunction.get<FieldAssignmentLogicalFunction>();
}

bool MapLogicalOperator::operator==(LogicalOperatorConcept const& rhs) const
{
    auto other = dynamic_cast<const MapLogicalOperator*>(&rhs);
    if (other)
    {
        return this->mapFunction == other->mapFunction;
    }
    return false;
};

/*
bool MapLogicalOperator::inferSchema()
{
    /// infer the default input and output schema
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }

    /// use the default input schema to calculate the out schema of this operator.
    mapFunction->inferStamp(getInputSchema());

    if (std::string fieldName = mapFunction->getField().getFieldName(); outputSchema.getFieldByName(fieldName))
    {
        /// The assigned field is part of the current schema.
        /// Thus we check if it has the correct type.
        NES_TRACE("MAP Logical Operator: the field {} is already in the schema, so we updated its type.", fieldName);
        outputSchema.replaceField(fieldName, mapFunction->getField().getStamp().clone());
    }
    else
    {
        /// The assigned field is not part of the current schema.
        /// Thus we extend the schema by the new attribute.
        NES_TRACE("MAP Logical Operator: the field {} is not part of the schema, so we added it.", fieldName);
        outputSchema.addField(fieldName, mapFunction->getField().getStamp().clone());
    }
    return true;
}
 */

Optimizer::TraitSet MapLogicalOperator::getTraitSet() const
{
    return {};
}

void MapLogicalOperator::setChildren(std::vector<LogicalOperator> children)
{
    this->children = children;
}

std::vector<Schema> MapLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema MapLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> MapLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> MapLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

void MapLogicalOperator::setOutputOriginIds(std::vector<OriginId> ids)
{
    outputOriginIds = ids;
}

void MapLogicalOperator::setInputOriginIds(std::vector<std::vector<OriginId>> ids)
{
    inputOriginIds = ids;
}

std::vector<LogicalOperator> MapLogicalOperator::getChildren() const
{
    return children;
}

std::string MapLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "MAP(opId: " << id << ": predicate: " << mapFunction << ")";
    return ss.str();
}

SerializableOperator MapLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(children[0].getId().getRawValue());

    NES::FunctionList list;
    auto* serializedFunction = list.add_functions();
    serializedFunction->CopyFrom(getMapFunction().serialize());

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
    SchemaSerializationUtil::serializeSchema(this->getOutputSchema(), outputSchema);
    serializedOperator.set_allocated_outputschema(outputSchema);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}

NES::Configurations::DescriptorConfig::Config
MapLogicalOperator::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return NES::Configurations::DescriptorConfig::validateAndFormat<MapLogicalOperator::ConfigParameters>(std::move(config), NAME);
}

LogicalOperatorRegistryReturnType
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
            return MapLogicalOperator(function.value().get<FieldAssignmentLogicalFunction>());
        }
        throw UnknownLogicalOperator();
    }
    throw UnknownLogicalOperator();
}

}

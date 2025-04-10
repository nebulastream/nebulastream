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
#include <utility>
#include <ErrorHandling.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableSchema.pb.h>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Common/DataTypes/Boolean.hpp>

namespace NES
{

SelectionLogicalOperator::SelectionLogicalOperator(LogicalFunction predicate) : predicate(predicate)
{
}

std::string_view SelectionLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalFunction SelectionLogicalOperator::getPredicate() const
{
    return predicate;
}

bool SelectionLogicalOperator::operator==(LogicalOperatorConcept const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SelectionLogicalOperator*>(&rhs)) {
        return predicate == rhsOperator->predicate;
    }
    return false;
};

std::string SelectionLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "SELECTION(opId: " << id << ": predicate: " << predicate << ")";
    return ss.str();
}

LogicalOperator SelectionLogicalOperator::withInferredSchema(Schema inputSchema) const
{
    auto copy = *this;
    copy.predicate = predicate.withInferredStamp(inputSchema);
    if (*copy.predicate.getStamp().get() != Boolean())
    {
        throw CannotInferSchema("the filter expression is not a valid predicate");
    }
    copy.inputSchema = inputSchema;
    copy.outputSchema = inputSchema;

    std::vector<LogicalOperator> newChildren;
    for (const auto& child : children)
    {
        newChildren.push_back(child.withInferredSchema(copy.outputSchema));
    }
    return copy.withChildren(newChildren);
}

Optimizer::TraitSet SelectionLogicalOperator::getTraitSet() const
{
    return {};
}

LogicalOperator SelectionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SelectionLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema SelectionLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> SelectionLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> SelectionLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

void SelectionLogicalOperator::setOutputOriginIds(std::vector<OriginId> ids)
{
    outputOriginIds = ids;
}

void SelectionLogicalOperator::setInputOriginIds(std::vector<std::vector<OriginId>> ids)
{
    inputOriginIds = ids;
}

std::vector<LogicalOperator> SelectionLogicalOperator::getChildren() const
{
    return children;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterSelectionLogicalOperator(NES::LogicalOperatorRegistryArguments config)
{
    auto functionVariant = config.config[SelectionLogicalOperator::ConfigParameters::SELECTION_FUNCTION_NAME];
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
            throw UnknownLogicalOperator();
            //return SelectionLogicalOperator(function.value());
        }
        throw UnknownLogicalOperator();
    }
    throw UnknownLogicalOperator();
}

SerializableOperator SelectionLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(children[0].getId().getRawValue());

    NES::FunctionList list;
    auto* serializedFunction = list.add_functions();
    serializedFunction->CopyFrom(getPredicate().serialize());

    NES::Configurations::DescriptorConfig::ConfigType configVariant = list;
    SerializableVariantDescriptor variantDescriptor =
        Configurations::descriptorConfigTypeToProto(configVariant);
    (*opDesc->mutable_config())["selectionFunctionName"] = variantDescriptor;

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
}

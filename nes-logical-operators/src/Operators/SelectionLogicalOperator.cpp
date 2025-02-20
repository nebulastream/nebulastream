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

namespace NES
{

SelectionLogicalOperator::SelectionLogicalOperator(std::shared_ptr<LogicalFunction> const& predicate)
    : Operator(), UnaryLogicalOperator(), predicate(std::move(predicate))
{
}

std::shared_ptr<LogicalFunction> SelectionLogicalOperator::getPredicate() const
{
    return predicate;
}

void SelectionLogicalOperator::setPredicate(std::shared_ptr<LogicalFunction> newPredicate)
{
    predicate = std::move(newPredicate);
}

bool SelectionLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const SelectionLogicalOperator*>(&rhs)->id == id;
}

bool SelectionLogicalOperator::operator==(Operator const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SelectionLogicalOperator*>(&rhs)) {
        return predicate == rhsOperator->predicate;
    }
    return false;
};

std::string SelectionLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "FILTER(opId: " << id << ": predicate: " << *predicate << ")";
    return ss.str();
}

bool SelectionLogicalOperator::inferSchema()
{
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }
    predicate->inferStamp(*inputSchema);
    if (!predicate->isPredicate())
    {
        throw CannotInferSchema("FilterLogicalOperator: the filter expression is not a valid predicate");
    }
    return true;
}

std::shared_ptr<Operator> SelectionLogicalOperator::clone() const
{
    auto copy = std::make_shared<SelectionLogicalOperator>(predicate->clone());
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

std::unique_ptr<LogicalOperator>
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
            std::shared_ptr<NES::LogicalFunction> sharedBase(function.value().get());
            return std::make_unique<SelectionLogicalOperator>(sharedBase);
        }
        return nullptr;
    }
    return nullptr;
}

SerializableOperator SelectionLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(children[0]->id.getRawValue());

    NES::FunctionList list;
    auto* serializedFunction = list.add_functions();
    serializedFunction->CopyFrom(getPredicate()->serialize());

    NES::Configurations::DescriptorConfig::ConfigType configVariant = list;
    SerializableVariantDescriptor variantDescriptor =
        Configurations::descriptorConfigTypeToProto(configVariant);
    (*opDesc->mutable_config())["selectionFunctionName"] = variantDescriptor;

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
}

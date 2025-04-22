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
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableSchema.pb.h>
#include <Common/DataTypes/Boolean.hpp>

namespace NES
{

SelectionLogicalOperator::SelectionLogicalOperator(const std::shared_ptr<LogicalFunction>& predicate, OperatorId id)
    : Operator(id), UnaryLogicalOperator(id), predicate(std::move(predicate))
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
    return *this == rhs && dynamic_cast<const SelectionLogicalOperator*>(&rhs)->getId() == id;
}

bool SelectionLogicalOperator::operator==(const Operator& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SelectionLogicalOperator*>(&rhs))
    {
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
    auto copy = std::make_shared<SelectionLogicalOperator>(predicate->clone(), id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

float SelectionLogicalOperator::getSelectivity() const
{
    return selectivity;
}
void SelectionLogicalOperator::setSelectivity(float newSelectivity)
{
    selectivity = newSelectivity;
}

std::vector<std::string> SelectionLogicalOperator::getFieldNamesUsedByFilterPredicate() const
{
    ///vector to save the names of all the fields that are used in this predicate
    std::vector<std::string> fieldsInPredicate;

    ///iterator to go over all the fields of the predicate
    for (auto itr : DFSRange<LogicalFunction>(predicate))
    {
        ///LogicalFunctionif it finds a fieldAccess this means that the predicate uses this specific field that comes from any source
        if (NES::Util::instanceOf<FieldAccessLogicalFunction>(itr))
        {
            const std::shared_ptr<FieldAccessLogicalFunction> accessLogicalFunction = NES::Util::as<FieldAccessLogicalFunction>(itr);
            fieldsInPredicate.push_back(accessLogicalFunction->getFieldName());
        }
    }

    return fieldsInPredicate;
}

std::unique_ptr<LogicalOperator>
LogicalOperatorGeneratedRegistrar::RegisterSelectionLogicalOperator(NES::LogicalOperatorRegistryArguments config)
{
    auto functionVariant = config.config[SelectionLogicalOperator::ConfigParameters::SELECTION_FUNCTION_NAME];
    if (std::holds_alternative<NES::FunctionList>(functionVariant))
    {
        const auto& functionList = std::get<NES::FunctionList>(functionVariant);
        const auto& functions = functionList.functions();
        INVARIANT(functions.size() == 1, "Expected exactly one function");
        auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);
        return std::make_unique<SelectionLogicalOperator>(function, getNextOperatorId());
    }
    //throw CannotDeserialize("Error while deserializing SelectionLogicalOperator with config {}", config);
    return nullptr;
}

SerializableOperator SelectionLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->getId().getRawValue());
    serializedOperator.add_childrenids(children[0]->getId().getRawValue());

    NES::FunctionList list;
    auto* serializedFunction = list.add_functions();
    FunctionSerializationUtil::serializeFunction(this->getPredicate(), serializedFunction);

    Configurations::DescriptorConfig::ConfigType configVariant = list;
    SerializableVariantDescriptor variantDescriptor = Configurations::descriptorConfigTypeToProto(configVariant);
    (*opDesc->mutable_config())["selectionFunctionName"] = variantDescriptor;

    auto* unaryOpDesc = new SerializableOperator_UnaryLogicalOperator();
    auto* outputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getOutputSchema(), outputSchema);
    unaryOpDesc->set_allocated_outputschema(outputSchema);

    auto* inputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchema(), inputSchema);
    unaryOpDesc->set_allocated_inputschema(inputSchema);

    for (const auto& originId : this->getInputOriginIds())
    {
        unaryOpDesc->add_originids(originId.getRawValue());
    }

    opDesc->set_allocated_unaryoperator(unaryOpDesc);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}
}

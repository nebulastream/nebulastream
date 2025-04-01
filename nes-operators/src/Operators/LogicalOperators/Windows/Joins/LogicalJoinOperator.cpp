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

#include <functional>
#include <initializer_list>
#include <memory>
#include <ranges>
#include <span>
#include <unordered_set>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
LogicalJoinOperator::LogicalJoinOperator(std::shared_ptr<Join::LogicalJoinDescriptor> joinDefinition, const OperatorId id)
    : LogicalJoinOperator(std::move(joinDefinition), id, INVALID_ORIGIN_ID)
{
}
LogicalJoinOperator::LogicalJoinOperator(
    std::shared_ptr<Join::LogicalJoinDescriptor> joinDefinition, const OperatorId id, const OriginId originId)
    : Operator(id), LogicalBinaryOperator(id), OriginIdAssignmentOperator(id, originId), joinDefinition(std::move(joinDefinition))
{
}

bool LogicalJoinOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalJoinOperator>(rhs)->getId() == id;
}

std::string LogicalJoinOperator::toString() const
{
    return fmt::format(
        "Join({}, windowType = {}, joinFunction = {})",
        id,
        joinDefinition->getWindowType()->toString(),
        *joinDefinition->getJoinFunction());
}

std::shared_ptr<Join::LogicalJoinDescriptor> LogicalJoinOperator::getJoinDefinition() const
{
    return joinDefinition;
}

bool LogicalJoinOperator::inferSchema()
{
    if (!LogicalBinaryOperator::inferSchema())
    {
        return false;
    }

    ///validate that only two different type of schema were present
    if (distinctSchemas.size() != 2)
    {
        throw CannotInferSchema("Found {} distinct schemas but expected 2 distinct schemas", distinctSchemas.size());
    }

    auto inputSchema = Schema{distinctSchemas};


    ///reset left and right schema
    // leftInputSchema = Schema{leftInputSchema.memoryLayoutType};
    // rightInputSchema = Schema{rightInputSchema.memoryLayoutType};

    /// Finds the join schema that contains the joinKey and copies the fields to the input schema, if found
    // auto findSchemaInDistinctSchemas = [&](NodeFunctionFieldAccess& joinKey, Schema& inputSchema)
    // {
    //     for (const auto& distinctSchema : distinctSchemas)
    //     {
    //         const auto& joinKeyName = joinKey.getFieldName();
    //         if (const auto attributeField = distinctSchema.getFieldByName(joinKeyName); attributeField.has_value())
    //         {
    //             /// If we have not copied the fields from the schema, copy them for the first time
    //             if (inputSchema.getSizeOfSchemaInBytes() == 0)
    //             {
    //                 inputSchema.assignToFields(distinctSchema);
    //             }
    //             joinKey.inferStamp(inputSchema);
    //             return true;
    //         }
    //     }
    //     return false;
    // };


    //Goal: Assert that somewhere in the join predicate fields from different relations are used.
    //If it wouldn't, then there would be no join, but rather just an cartesian product, which I don't believe
    //we should permit implicitly.
    //(see Neumann, Leis "A Critique of Modern SQL And A Proposal Towards A Simple and Expressive Query Language")
    //We do permit something like (s1.x = 1 AND s2.y = 2), because imo its selective enough to be safe.
    //We might want to add some extra warnings for the user in the future for such cases, but I'd consider them valid queries.

    std::unordered_set<std::span<const Identifier>, std::hash<std::span<const Identifier>>, IdentifierList::SpanEquals> foundSources{};
    auto bfsIterator = BreadthFirstNodeIterator(joinDefinition->getJoinFunction());
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr)
    {
        if (NES::Util::instanceOf<NodeFunctionFieldAccess>(*itr))
        {
            auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(*itr);
            auto fieldName = fieldAccess->getFieldName();
            foundSources.insert(
                std::span<const Identifier>{std::ranges::begin(fieldName), std::ranges::size(fieldAccess->getFieldName()) - 1});
        }
    }

    if (std::ranges::size(foundSources) == 0)
    {
        throw CannotInferSchema("Implicit cartesian product: no fields referenced");
    }
    if (std::ranges::size(foundSources) == 1)
    {
        throw CannotInferSchema("Implicit cartesian product: join predicate only uses fields from one source");
    }

    if (std::ranges::size(foundSources) > 2)
    {
        throw CannotInferSchema("Join predicate refers to fields from more then two streams");
    }


    /// Maintain a list of visited nodes as there are multiple root nodes
    // std::unordered_set<std::shared_ptr<NodeFunctionBinary>> visitedFunctions;
    // const auto bfsIterator = BreadthFirstNodeIterator(joinDefinition->getJoinFunction());
    // for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr)
    // {
    //     if (NES::Util::instanceOf<NodeFunctionBinary>(*itr))
    //     {
    //         auto visitingOp = NES::Util::as<NodeFunctionBinary>(*itr);
    //         if (not visitedFunctions.contains(visitingOp))
    //         {
    //             visitedFunctions.insert(visitingOp);
    //             if (!Util::instanceOf<NodeFunctionBinary>(Util::as<NodeFunctionBinary>(*itr)->getLeft()))
    //             {
    //                 ///Find the schema for left and right join key
    //                 const auto leftJoinKey = Util::as<NodeFunctionFieldAccess>(Util::as<NodeFunctionBinary>(*itr)->getLeft());
    //                 const auto leftJoinKeyName = leftJoinKey->getFieldName();
    //                 const auto foundLeftKey = findSchemaInDistinctSchemas(*leftJoinKey, leftInputSchema);
    //                 if (!foundLeftKey)
    //                 {
    //                     throw CannotInferSchema("unable to find left join key \"{}\" in schemas", leftJoinKeyName);
    //                 }
    //                 const auto rightJoinKey = Util::as<NodeFunctionFieldAccess>(Util::as<NodeFunctionBinary>(*itr)->getRight());
    //                 const auto rightJoinKeyName = rightJoinKey->getFieldName();
    //                 const auto foundRightKey = findSchemaInDistinctSchemas(*rightJoinKey, rightInputSchema);
    //                 if (!foundRightKey)
    //                 {
    //                     throw CannotInferSchema("unable to find right join key \"{}\" in schemas", rightJoinKeyName);
    //                 }
    //                 NES_DEBUG("LogicalJoinOperator: Inserting operator in collection of already visited node.");
    //                 visitedFunctions.insert(visitingOp);
    //             }
    //         }
    //     }
    // }
    /// Clearing now the distinct schemas
    //TODO why do we need to clear distinct schemas?
    distinctSchemas.clear();

    // /// Checking if left and right input schema are not empty and are not equal
    // if (leftInputSchema.getSizeOfSchemaInBytes() == 0)
    // {
    //     throw CannotInferSchema("left schema is empty");
    // }
    // if (rightInputSchema.getSizeOfSchemaInBytes() == 0)
    // {
    //     throw CannotInferSchema("right schema is empty");
    // }
    // if (rightInputSchema == leftInputSchema)
    // {
    //     throw CannotInferSchema("found both left and right input schema to be same.");
    // }

    ///Infer stamp of window definition
    const auto windowType = Util::as<Windowing::TimeBasedWindowType>(joinDefinition->getWindowType());
    windowType->inferStamp(inputSchema);

    ///Reset output schema and add fields from left and right input schema
    const auto& sourceNameLeft = leftInputSchema.getCommonPrefix();
    const auto& sourceNameRight = rightInputSchema.getCommonPrefix();
    INVARIANT(!std::ranges::empty(sourceNameLeft) && !std::ranges::empty(sourceNameRight), "All source names must have values.");
    const auto newQualifierForSystemField = zipIdentifierLists(sourceNameLeft, sourceNameRight);

    windowMetaData.windowStartFieldName = newQualifierForSystemField + Identifier{"start", false};
    windowMetaData.windowEndFieldName = newQualifierForSystemField + Identifier{"end", false};

    const auto fieldView = distinctSchemas | std::views::transform(&Schema::getFields) | std::views::join
        | std::views::transform([&](const Schema::Field& field)
                                { return Schema::Field{newQualifierForSystemField + field.name, field.dataType}; });
    outputSchema = Schema{fieldView};
    //we can add these fields to the view as soon as std::views::concat lands in c++ 16
    outputSchema.addField(windowMetaData.windowStartFieldName, DataType::Type::UINT64);
    outputSchema.addField(windowMetaData.windowEndFieldName, DataType::Type::UINT64);

    NES_DEBUG("Output schema for join={}", outputSchema);
    joinDefinition->updateOutputDefinition(outputSchema);
    joinDefinition->updateSourceTypes(leftInputSchema, rightInputSchema);
    return true;
}

std::shared_ptr<Operator> LogicalJoinOperator::copy()
{
    auto copy = std::make_shared<LogicalJoinOperator>(joinDefinition, id);
    copy->setLeftInputOriginIds(leftInputOriginIds);
    copy->setRightInputOriginIds(rightInputOriginIds);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setOriginId(originId);
    copy->windowMetaData = windowMetaData;
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalJoinOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalJoinOperator>(rhs))
    {
        const auto rhsJoin = NES::Util::as<LogicalJoinOperator>(rhs);
        return joinDefinition->getWindowType()->equal(rhsJoin->joinDefinition->getWindowType())
            && joinDefinition->getJoinFunction()->equal(rhsJoin->joinDefinition->getJoinFunction())
            && (joinDefinition->getOutputSchema() == rhsJoin->joinDefinition->getOutputSchema())
            && (joinDefinition->getRightSourceType() == rhsJoin->joinDefinition->getRightSourceType())
            && (joinDefinition->getLeftSourceType() == rhsJoin->joinDefinition->getLeftSourceType());
    }
    return false;
}

std::vector<OriginId> LogicalJoinOperator::getOutputOriginIds() const
{
    return OriginIdAssignmentOperator::getOutputOriginIds();
}

void LogicalJoinOperator::setOriginId(const OriginId originId)
{
    OriginIdAssignmentOperator::setOriginId(originId);
    joinDefinition->setOriginId(originId);
}

std::shared_ptr<NodeFunction> LogicalJoinOperator::getJoinFunction() const
{
    return joinDefinition->getJoinFunction();
}

}

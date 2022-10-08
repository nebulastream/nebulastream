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
#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Query.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalBatchJoinDefinition.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <iostream>
#include <numeric>

#include <API/WindowedQuery.hpp>
#include <API/Windowing.hpp>
#include <utility>

namespace NES {

ExpressionNodePtr getExpressionNodePtr(ExpressionItem& expressionItem) { return expressionItem.getExpressionNode(); }

JoinOperatorBuilder::Join Query::joinWith(const Query& subQueryRhs) { return JoinOperatorBuilder::Join(subQueryRhs, *this); }

NES::Experimental::BatchJoinOperatorBuilder::Join Query::batchJoinWith(const Query& subQueryRhs) {
    return NES::Experimental::BatchJoinOperatorBuilder::Join(subQueryRhs, *this);
}

CEPOperatorBuilder::And Query::andWith(const Query& subQueryRhs) { return CEPOperatorBuilder::And(subQueryRhs, *this); }

CEPOperatorBuilder::Seq Query::seqWith(const Query& subQueryRhs) { return CEPOperatorBuilder::Seq(subQueryRhs, *this); }

CEPOperatorBuilder::Times Query::times(const uint64_t minOccurrences, const uint64_t maxOccurrences) {
    return CEPOperatorBuilder::Times(minOccurrences, maxOccurrences, *this);
}

CEPOperatorBuilder::Times Query::times(const uint64_t maxOccurrences) { return CEPOperatorBuilder::Times(maxOccurrences, *this); }

CEPOperatorBuilder::Times Query::times() { return CEPOperatorBuilder::Times(*this); }

namespace JoinOperatorBuilder {

JoinWhere Join::where(const ExpressionItem& onLeftKey) const { return JoinWhere(subQueryRhs, originalQuery, onLeftKey); }

Join::Join(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(subQueryRhs), originalQuery(originalQuery) {}

JoinCondition JoinWhere::equalsTo(const ExpressionItem& onRightKey) const {
    return JoinCondition(subQueryRhs, originalQuery, onLeftKey, onRightKey);
}

JoinWhere::JoinWhere(const Query& subQueryRhs, Query& originalQuery, const ExpressionItem& onLeftKey)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), onLeftKey(onLeftKey.getExpressionNode()) {}

Query& JoinCondition::window(const Windowing::WindowTypePtr& windowType) const {
    return originalQuery.joinWith(subQueryRhs, onLeftKey, onRightKey, windowType);//call original joinWith() function
}

JoinCondition::JoinCondition(const Query& subQueryRhs,
                             Query& originalQuery,
                             const ExpressionItem& onLeftKey,
                             const ExpressionItem& onRightKey)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), onLeftKey(onLeftKey.getExpressionNode()),
      onRightKey(onRightKey.getExpressionNode()) {}

}// namespace JoinOperatorBuilder

namespace Experimental::BatchJoinOperatorBuilder {

Join::Join(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(subQueryRhs), originalQuery(originalQuery) {}

JoinWhere Join::where(const ExpressionItem& onProbeKey) const { return JoinWhere(subQueryRhs, originalQuery, onProbeKey); }

Query& JoinWhere::equalsTo(const ExpressionItem& onBuildKey) const {
    return originalQuery.batchJoinWith(subQueryRhs, onProbeKey, onBuildKey);
}

JoinWhere::JoinWhere(const Query& subQueryRhs, Query& originalQuery, const ExpressionItem& onProbeKey)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), onProbeKey(onProbeKey.getExpressionNode()) {}

}// namespace Experimental::BatchJoinOperatorBuilder

namespace CEPOperatorBuilder {

And::And(const Query& subQueryRhs, Query& originalQuery)
    : subQueryRhs(const_cast<Query&>(subQueryRhs)), originalQuery(originalQuery) {
    NES_DEBUG("Query: add map operator to andWith to add virtual key to originalQuery");
    //here, we add artificial key attributes to the sources in order to reuse the join-logic later
    std::string cepLeftKey = keyAssignmentLeft();
    std::string cepRightKey = keyAssignmentRight();
    //next: map the attributes with value 1 to the left and right source
    originalQuery.map(Attribute(cepLeftKey) = 1);
    this->subQueryRhs.map(Attribute(cepRightKey) = 1);
    //last, define the artificial attributes as key attributes
    NES_DEBUG("Query: add name cepLeftKey " << cepLeftKey);
    NES_DEBUG("Query: add name cepRightKey " << cepRightKey);
    onLeftKey = ExpressionItem(Attribute(cepLeftKey)).getExpressionNode();
    onRightKey = ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
}

Query& And::window(const Windowing::WindowTypePtr& windowType) const {
    return originalQuery.andWith(subQueryRhs, onLeftKey, onRightKey, windowType);//call original andWith() function
}

Seq::Seq(const Query& subQueryRhs, Query& originalQuery)
    : subQueryRhs(const_cast<Query&>(subQueryRhs)), originalQuery(originalQuery) {
    NES_DEBUG("Query: add map operator to seqWith to add virtual key to originalQuery");
    //here, we add artificial key attributes to the sources in order to reuse the join-logic later
    std::string cepLeftKey = keyAssignmentLeft();
    std::string cepRightKey = keyAssignmentRight();
    //next: map the attributes with value 1 to the left and right source
    originalQuery.map(Attribute(cepLeftKey) = 1);
    this->subQueryRhs.map(Attribute(cepRightKey) = 1);
    //last, define the artificial attributes as key attributes
    onLeftKey = ExpressionItem(Attribute(cepLeftKey)).getExpressionNode();
    onRightKey = ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
}

Query& Seq::window(const Windowing::WindowTypePtr& windowType) const {
    NES_DEBUG("Sequence enters window function");
    auto timestamp = windowType->getTimeCharacteristic()->getField()->getName();
    std::string sourceNameLeft = originalQuery.getQueryPlan()->getSourceConsumed();
    std::string sourceNameRight = subQueryRhs.getQueryPlan()->getSourceConsumed();
    // to guarantee a correct order of events by time (sequence) we need to identify the correct source and its timestamp
    // in case of composed streams on the right branch
    if (sourceNameRight.find("_") != std::string::npos) {
        // we find the most left source and use its timestamp for the filter constraint
        uint64_t posStart = sourceNameRight.find("_");
        uint64_t posEnd = sourceNameRight.find("_", posStart + 1);
        sourceNameRight = sourceNameRight.substr(posStart + 1, posEnd - 2) + "$" + timestamp;
    }// in case the right branch only contains 1 source we can just use it
    else {
        sourceNameRight = sourceNameRight + "$" + timestamp;
    }
    // in case of composed sources on the left branch
    if (sourceNameLeft.find("_") != std::string::npos) {
        // we find the most right source and use its timestamp for the filter constraint
        uint64_t posStart = sourceNameLeft.find_last_of("_");
        sourceNameLeft = sourceNameLeft.substr(posStart + 1) + "$" + timestamp;
    }// in case the left branch only contains 1 source we can just use it
    else {
        sourceNameLeft = sourceNameLeft + "$" + timestamp;
    }
    NES_DEBUG("ExpressionItem for Left Source " << sourceNameLeft);
    NES_DEBUG("ExpressionItem for Right Source " << sourceNameRight);
    return originalQuery.seqWith(subQueryRhs, onLeftKey, onRightKey, windowType)
        .filter(Attribute(sourceNameLeft) < Attribute(sourceNameRight));//call original seqWith() function
}

//TODO that is a quick fix to generate unique keys for andWith chains and should be removed after implementation of Cartesian Product (#2296)
std::string keyAssignmentRight() {
    //first, get unique ids for the key attributes
    auto cepRightId = Util::getNextOperatorId();
    //second, create a unique name for both key attributes
    std::string cepRightKey = "cep_rightkey" + std::to_string(cepRightId);
    return cepRightKey;
}

std::string keyAssignmentLeft() {
    //first, get unique ids for the key attributes
    auto cepLeftId = Util::getNextOperatorId();
    //second, create a unique name for both key attributes
    std::string cepLeftKey = "cep_leftkey" + std::to_string(cepLeftId);
    return cepLeftKey;
}

Times::Times(const uint64_t minOccurrences, const uint64_t maxOccurrences, Query& originalQuery)
    : originalQuery(originalQuery), minOccurrences(minOccurrences), maxOccurrences(maxOccurrences), bounded(true) {
    // add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Times::Times(const uint64_t occurrences, Query& originalQuery)
    : originalQuery(originalQuery), minOccurrences(0), maxOccurrences(occurrences), bounded(true) {
    // add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Times::Times(Query& originalQuery) : originalQuery(originalQuery), minOccurrences(0), maxOccurrences(0), bounded(false) {
    // add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Query& Times::window(const Windowing::WindowTypePtr& windowType) const {
    auto timestamp = windowType->getTimeCharacteristic()->getField()->getName();
    // if no min and max occurrence is defined, apply count without filter
    if (!bounded) {
        return originalQuery.window(windowType).apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)));
    } else {
        // if min and/or max occurrence are defined, apply count without filter
        if (maxOccurrences == 0) {
            return originalQuery.window(windowType)
                .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
                .filter(Attribute("Count") >= minOccurrences);
        }

        if (minOccurrences == 0) {
            return originalQuery.window(windowType)
                .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
                .filter(Attribute("Count") == maxOccurrences);
        }

        return originalQuery.window(windowType)
            .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
            .filter(Attribute("Count") >= minOccurrences && Attribute("Count") <= maxOccurrences);
    }

    return originalQuery;
}

}// namespace CEPOperatorBuilder

Query::Query(QueryPlanPtr queryPlan) : queryPlan(std::move(queryPlan)) {}

Query::Query(const Query& query) = default;

Query Query::from(const std::string& sourceName) {
    NES_DEBUG("Query: create query for input source " << sourceName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(sourceName));
    auto queryPlan = QueryPlan::create(sourceOperator);
    queryPlan->setSourceConsumed(sourceName);
    return Query(queryPlan);
}

Query& Query::project(std::vector<ExpressionNodePtr> expressions) {
    OperatorNodePtr op = LogicalOperatorFactory::createProjectionOperator(expressions);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::as(const std::string& newSourceName) {
    auto renameOperator = LogicalOperatorFactory::createRenameSourceOperator(newSourceName);
    queryPlan->appendOperatorAsNewRoot(renameOperator);
    return *this;
}

Query& Query::unionWith(const Query& subQuery) {
    NES_DEBUG("Query: unionWith the subQuery to current query");
    OperatorNodePtr op = LogicalOperatorFactory::createUnionOperator();
    const QueryPlanPtr& subQueryPlan = subQuery.queryPlan;
    queryPlan->addRootOperator(subQueryPlan->getRootOperators()[0]);
    queryPlan->appendOperatorAsNewRoot(op);
    //Update the Source names by sorting and then concatenating the source names from the sub query plan
    std::vector<std::string> sourceNames;
    sourceNames.emplace_back(subQueryPlan->getSourceConsumed());
    sourceNames.emplace_back(queryPlan->getSourceConsumed());
    std::sort(sourceNames.begin(), sourceNames.end());
    auto updatedSourceName = std::accumulate(sourceNames.begin(), sourceNames.end(), std::string("-"));
    queryPlan->setSourceConsumed(updatedSourceName);
    return *this;
}

Query& Query::join(const Query& subQueryRhs,
                   ExpressionItem onLeftKey,
                   ExpressionItem onRightKey,
                   const Windowing::WindowTypePtr& windowType,
                   Join::LogicalJoinDefinition::JoinType joinType) {
    NES_DEBUG("Query: joinWith the subQuery to current query");

    auto subQuery = const_cast<Query&>(subQueryRhs);

    auto leftKeyExpression = onLeftKey.getExpressionNode();
    if (!leftKeyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + leftKeyExpression->toString());
        NES_THROW_RUNTIME_ERROR("Query: window key has to be an FieldAccessExpression");
    }
    auto rightKeyExpression = onRightKey.getExpressionNode();
    if (!rightKeyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + rightKeyExpression->toString());
        NES_THROW_RUNTIME_ERROR("Query: window key has to be an FieldAccessExpression");
    }
    auto leftKeyFieldAccess = leftKeyExpression->as<FieldAccessExpressionNode>();
    auto rightKeyFieldAccess = rightKeyExpression->as<FieldAccessExpressionNode>();

    //we use a on time trigger as default that triggers on each change of the watermark
    auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
    //    auto triggerPolicy = OnTimeTriggerPolicyDescription::create(1000);

    //we use a lazy NL join because this is currently the only one that is implemented
    auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();

    // we use a complete window type as we currently do not have a distributed join
    auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();

    auto rightQueryPlan = subQuery.getQueryPlan();
    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid right query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = getQueryPlan()->getRootOperators()[0]->getOutputSchema();
    auto rightJoinType = rootOperatorRhs->getOutputSchema();

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
            queryPlan->appendOperatorAsNewRoot(LogicalOperatorFactory::createWatermarkAssignerOperator(
                Windowing::IngestionTimeWatermarkStrategyDescriptor::create()));
        } else if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::EventTime) {
            queryPlan->appendOperatorAsNewRoot(
                LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                    API::Milliseconds(0),
                    windowType->getTimeCharacteristic()->getTimeUnit())));
        }
    }

    if (rightQueryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>().empty()) {
        if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
            auto op = LogicalOperatorFactory::createWatermarkAssignerOperator(
                Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
            rightQueryPlan->appendOperatorAsNewRoot(op);
        } else if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::EventTime) {
            auto op =
                LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    Attribute(windowType->getTimeCharacteristic()->getField()->getName()),
                    API::Milliseconds(0),
                    windowType->getTimeCharacteristic()->getTimeUnit()));
            rightQueryPlan->appendOperatorAsNewRoot(op);
        }
    }

    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::LogicalJoinDefinition::create(leftKeyFieldAccess,
                                                              rightKeyFieldAccess,
                                                              windowType,
                                                              distrType,
                                                              triggerPolicy,
                                                              triggerAction,
                                                              1,
                                                              1,
                                                              joinType);

    auto op = LogicalOperatorFactory::createJoinOperator(joinDefinition);
    queryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
    queryPlan->appendOperatorAsNewRoot(op);
    //Update the Source names by sorting and then concatenating the source names from the sub query plan
    std::vector<std::string> sourceNames;
    sourceNames.emplace_back(rightQueryPlan->getSourceConsumed());
    sourceNames.emplace_back(queryPlan->getSourceConsumed());
    std::sort(sourceNames.begin(), sourceNames.end());
    // accumulating sourceNames with delimiters _ between all sourceNames to enable backtracking of origin
    auto updatedSourceName =
        std::accumulate(sourceNames.begin(), sourceNames.end(), std::string("-"), [](std::string a, std::string b) {
            return a + "_" + b;
        });
    queryPlan->setSourceConsumed(updatedSourceName);

    return *this;
}

Query& Query::batchJoin(const Query& subQueryRhs, ExpressionItem onProbeKey, ExpressionItem onBuildKey) {
    NES_DEBUG("Query: batchJoinWith the subQuery to current query");

    auto subQuery = const_cast<Query&>(subQueryRhs);

    auto probeKeyExpression = onProbeKey.getExpressionNode();
    if (!probeKeyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + probeKeyExpression->toString());
        NES_THROW_RUNTIME_ERROR("Query: window key has to be an FieldAccessExpression");
    }
    auto buildKeyExpression = onBuildKey.getExpressionNode();
    if (!buildKeyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + buildKeyExpression->toString());
        NES_THROW_RUNTIME_ERROR("Query: window key has to be an FieldAccessExpression");
    }
    auto probeKeyFieldAccess = probeKeyExpression->as<FieldAccessExpressionNode>();
    auto buildKeyFieldAccess = buildKeyExpression->as<FieldAccessExpressionNode>();

    auto rightQueryPlan = subQuery.getQueryPlan();
    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid right query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = getQueryPlan()->getRootOperators()[0]->getOutputSchema();
    auto rightJoinType = rootOperatorRhs->getOutputSchema();

    // todo here again we wan't to extend to distributed joins:
    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::Experimental::LogicalBatchJoinDefinition::create(buildKeyFieldAccess, probeKeyFieldAccess, 1, 1);

    auto op = LogicalOperatorFactory::createBatchJoinOperator(joinDefinition);
    queryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
    queryPlan->appendOperatorAsNewRoot(op);
    //Update the Source names by sorting and then concatenating the source names from the sub query plan
    std::vector<std::string> sourceNames;
    sourceNames.emplace_back(rightQueryPlan->getSourceConsumed());
    sourceNames.emplace_back(queryPlan->getSourceConsumed());
    std::sort(sourceNames.begin(), sourceNames.end());
    auto updatedSourceName = std::accumulate(sourceNames.begin(), sourceNames.end(), std::string("-"));
    queryPlan->setSourceConsumed(updatedSourceName);

    return *this;
}

Query& Query::joinWith(const Query& subQueryRhs,
                       ExpressionItem onLeftKey,
                       ExpressionItem onRightKey,
                       const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: add JoinType (INNER_JOIN) to Join Operator");

    Join::LogicalJoinDefinition::JoinType joinType = Join::LogicalJoinDefinition::INNER_JOIN;
    return Query::join(subQueryRhs, onLeftKey, onRightKey, windowType, joinType);
}

Query& Query::batchJoinWith(const Query& subQueryRhs, ExpressionItem onProbeKey, ExpressionItem onBuildKey) {
    NES_DEBUG("Query: add JoinType (INNER_JOIN) to Join Operator");

    return Query::batchJoin(subQueryRhs, onProbeKey, onBuildKey);
}

Query& Query::andWith(const Query& subQueryRhs,
                      ExpressionItem onLeftKey,
                      ExpressionItem onRightKey,
                      const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: add JoinType (CARTESIAN_PRODUCT) to AND Operator");
    Join::LogicalJoinDefinition::JoinType joinType = Join::LogicalJoinDefinition::CARTESIAN_PRODUCT;
    return Query::join(subQueryRhs, onLeftKey, onRightKey, windowType, joinType);
}

Query& Query::seqWith(const Query& subQueryRhs,
                      ExpressionItem onLeftKey,
                      ExpressionItem onRightKey,
                      const Windowing::WindowTypePtr& windowType) {
    NES_DEBUG("Query: add JoinType (CARTESIAN_PRODUCT) to SEQ Operator");
    Join::LogicalJoinDefinition::JoinType joinType = Join::LogicalJoinDefinition::CARTESIAN_PRODUCT;
    return Query::join(subQueryRhs, onLeftKey, onRightKey, windowType, joinType);
}

Query& Query::orWith(const Query& subQueryRhs) {
    NES_DEBUG("Query: finally we translate the OR into a union OP ");
    return Query::unionWith(subQueryRhs);
}

Query& Query::filter(const ExpressionNodePtr& filterExpression) {
    NES_DEBUG("Query: add filter operator to query");
    if (!filterExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("Query: Filter predicate cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(filterExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}


Query& Query::map(const FieldAssignmentExpressionNodePtr& mapExpression) {
    NES_DEBUG("Query: add map operator to query");
    if (!mapExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("Query: Map expression cannot have a FieldRenameExpression");
    }
    OperatorNodePtr op = LogicalOperatorFactory::createMapOperator(mapExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

#ifdef TFDEF
Query& Query::inferModel(const std::string model, const std::initializer_list<ExpressionItem> inputFields, const std::initializer_list<ExpressionItem> outputFields) {
    NES_DEBUG("Query: add map inferModel to query");
    auto inputFieldVector = std::vector(inputFields);
    auto outputFieldVector = std::vector(outputFields);
    std::vector<ExpressionItemPtr> inputFieldsPtr;
    std::vector<ExpressionItemPtr> outputFieldsPtr;
    for(auto f : inputFieldVector){
        ExpressionItemPtr fp = std::make_shared<ExpressionItem>(f);
        inputFieldsPtr.push_back(fp);
    }
    for(auto f : outputFieldVector){
        ExpressionItemPtr fp = std::make_shared<ExpressionItem>(f);
        outputFieldsPtr.push_back(fp);
    }

    OperatorNodePtr op = LogicalOperatorFactory::createInferModelOperator(model, inputFieldsPtr, outputFieldsPtr);
    std::cout << op->toString() << std::endl;
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}
#endif // TFDEF

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor) {
    NES_DEBUG("Query: add sink operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::assignWatermark(const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor) {
    NES_DEBUG("Query: add assignWatermark operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() const { return queryPlan; }

}// namespace NES

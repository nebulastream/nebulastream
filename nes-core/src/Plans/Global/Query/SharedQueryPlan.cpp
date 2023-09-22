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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/MatchedOperatorPair.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Plans/ChangeLog/ChangeLogEntry.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

SharedQueryPlan::SharedQueryPlan(const QueryPlanPtr& queryPlan)
    : sharedQueryId(PlanIdGenerator::getNextSharedQueryId()), sharedQueryPlanStatus(SharedQueryPlanStatus::Created) {

    //Create a new query plan
    this->queryPlan = queryPlan->copy();
    this->queryPlan->setQueryId(sharedQueryId);//overwrite the query id with shared query plan id
    //Compute sink operators
    std::set<LogicalOperatorNodePtr> sinkOperators;
    for (const auto& rootOperator : this->queryPlan->getRootOperators()) {
        sinkOperators.insert(rootOperator->as<LogicalOperatorNode>());
    }
    auto queryId = queryPlan->getQueryId();
    queryIdToSinkOperatorMap[queryId] = sinkOperators;
    //Initialize hash-based signature
    hashBasedSignatures = (*sinkOperators.begin())->as<LogicalOperatorNode>()->getHashBasedSignature();
    //The query id
    queryIds = {queryId};
    //Set the placement strategy used
    placementStrategy = queryPlan->getPlacementStrategy();
    //Initialize change log
    changeLog = Optimizer::Experimental::ChangeLog::create();

    //Compute first change log entry
    std::set<LogicalOperatorNodePtr> downstreamOperators;
    for (const auto& sinkOperator : sinkOperators) {
        downstreamOperators.insert(sinkOperator);
    }
    std::set<LogicalOperatorNodePtr> upstreamOperators;
    for (const auto& sourceOperator : this->queryPlan->getLeafOperators()) {
        upstreamOperators.insert(sourceOperator->as<LogicalOperatorNode>());
    }
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changeLog->addChangeLogEntry(now, Optimizer::Experimental::ChangeLogEntry::create(upstreamOperators, downstreamOperators));
}

SharedQueryPlanPtr SharedQueryPlan::create(const QueryPlanPtr& queryPlan) {
    return std::make_shared<SharedQueryPlan>(SharedQueryPlan(queryPlan));
}

void SharedQueryPlan::addQuery(QueryId queryId, const std::vector<Optimizer::MatchedOperatorPairPtr>& matchedOperatorPairs) {

    NES_DEBUG("SharedQueryPlan: Add the matched operators of query with id {} to the shared query plan.", queryId);
    // TODO Handling Fault-Tolerance in case of query merging [#2327]
    std::set<LogicalOperatorNodePtr> sinkOperators;

    //Iterate over matched operator pairs and
    for (const auto& matchedOperatorPair : matchedOperatorPairs) {

        auto hostOperator = matchedOperatorPair->hostOperator;
        auto targetOperator = matchedOperatorPair->targetOperator;

        //initialize sets for change log entry
        std::set<LogicalOperatorNodePtr> clEntryUpstreamOperators;
        std::set<LogicalOperatorNodePtr> clEntryDownstreamOperators;

        switch (matchedOperatorPair->containmentRelationship) {
            case Optimizer::ContainmentRelationship::EQUALITY:
                //If host and target operator are of sink type then connect the target sink to the upstream of the host sink.
                if (hostOperator->instanceOf<SinkLogicalOperatorNode>()
                    && targetOperator->instanceOf<SinkLogicalOperatorNode>()) {

                    //Make a copy of the target operator so that we do not have to perform additional operation to
                    // add it to the shared query plan.
                    // Note: we otherwise have to remove the upstream operator of the target to decouple it from the original target plan.
                    auto targetOperatorCopy = targetOperator->copy()->as<LogicalOperatorNode>();

                    //fetch all upstream operators of the host operator and add the target operator as their parent operator
                    for (const auto& hostUpstreamOperator : hostOperator->getChildren()) {

                        //add target operator as the parent to the host upstream operator
                        hostUpstreamOperator->addParent(targetOperatorCopy);
                        //add the host upstream operator to the change log entry
                        clEntryUpstreamOperators.insert(hostUpstreamOperator->as<LogicalOperatorNode>());
                        //add the new sink operators as root to the set
                        sinkOperators.insert(hostUpstreamOperator->as<LogicalOperatorNode>());
                    }

                    //set target operator as the downstream operator in the change log
                    clEntryDownstreamOperators.insert(targetOperatorCopy);

                    //If host operator is of sink type then connect the downstream operators of target operator to the upstream of the host operator.
                } else if (hostOperator->instanceOf<SinkLogicalOperatorNode>()) {

                    //Fetch all sink operators
                    auto targetSinkOperators = targetOperator->getAllRootNodes();

                    //Get the downstream operator of the target and add them as the downstream operator to the host operator
                    auto downstreamOperatorsOfTarget = targetOperator->getParents();
                    for (const auto& downstreamOperatorOfTarget : downstreamOperatorsOfTarget) {
                        //Clear as upstream the target operator
                        downstreamOperatorOfTarget->removeChild(targetOperator);
                        //fetch all upstream operators of the host operator and add the target operator as their parent operator
                        for (const auto& hostUpstreamOperator : hostOperator->getChildren()) {
                            //add target operator as the parent to the host upstream operator
                            hostUpstreamOperator->addParent(downstreamOperatorOfTarget);
                            //add the host upstream operator to the change log entry
                            clEntryUpstreamOperators.insert(hostUpstreamOperator->as<LogicalOperatorNode>());
                            //add the new sink operators as root to the set
                            sinkOperators.insert(hostUpstreamOperator->as<LogicalOperatorNode>());
                        }
                    }

                    //If target operator is of sink type then connect the target sink to the host operator.
                } else if (targetOperator->instanceOf<SinkLogicalOperatorNode>()) {

                    //Make a copy of the target operator so that we do not have to perform additional operation to
                    // add it to the shared query plan.
                    // Note: we otherwise have to remove the upstream operator of the target to decouple it from the original target plan.
                    auto targetOperatorCopy = targetOperator->copy()->as<LogicalOperatorNode>();
                    clEntryDownstreamOperators.insert(targetOperatorCopy);
                    //add the new sink operators as root to the set
                    sinkOperators.insert(targetOperatorCopy);

                    //add target operator as the downstream to the host operator
                    hostOperator->addParent(targetOperatorCopy);
                    //add the host upstream operator to the change log entry
                    clEntryUpstreamOperators.insert(hostOperator);

                    //If both host and target operator are not of sink type then connect the downstream operators of target operator to the host operator.
                } else {
                    //set host operator as the upstream operator in the change log
                    clEntryUpstreamOperators.insert(hostOperator);

                    //fetch all root operator of the target operator to compute downstream operator list for the change log entry
                    for (const auto& newRootOperator : targetOperator->getAllRootNodes()) {
                        clEntryDownstreamOperators.insert(newRootOperator->as<LogicalOperatorNode>());
                        //add the new sink operators as root to the set
                        sinkOperators.insert(newRootOperator->as<LogicalOperatorNode>());
                    }

                    //add all downstream operators of the target operator as downstream operator to the host operator
                    auto downstreamTargetOperators = targetOperator->getParents();
                    for (const auto& downstreamTargetOperator : downstreamTargetOperators) {
                        //Clear as upstream the target operator
                        downstreamTargetOperator->removeChild(targetOperator);
                        //add host operator as the upstream operator to the downstreamTargetOperator
                        hostOperator->addParent(downstreamTargetOperator);
                    }
                }
                break;
            case Optimizer::ContainmentRelationship::RIGHT_SIG_CONTAINED:
                clEntryUpstreamOperators.insert(hostOperator);
                for (const auto& newRootOperator : targetOperator->getAllRootNodes()) {
                    clEntryDownstreamOperators.insert(newRootOperator->as<LogicalOperatorNode>());
                }
                //add the new sink operators as root to the set
                for (const auto& targetSinkOperator : targetOperator->getAllRootNodes()) {
                    sinkOperators.insert(targetSinkOperator->as<LogicalOperatorNode>());
                }
                break;
            case Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED:
                // Here we need to re-deploy the whole shared query plan and therefore add all source operators to the change log
                for (const auto& sourceOperator : hostOperator->getAllLeafNodes()) {
                    clEntryUpstreamOperators.insert(sourceOperator->as<LogicalOperatorNode>());
                }
                // We also need to add all root operators from the existing shared query plan
                for (const auto& existingRootOperatorsToBeReDeployed : targetOperator->getAllRootNodes()) {
                    clEntryDownstreamOperators.insert(existingRootOperatorsToBeReDeployed->as<LogicalOperatorNode>());
                }
                // Finally, we need to add all root operators from the new query plan
                for (const auto& newRootOperator : hostOperator->getAllRootNodes()) {
                    clEntryDownstreamOperators.insert(newRootOperator->as<LogicalOperatorNode>());
                }
                //add the new sink operators as root to the set
                //in case of left signature containment, the host operator marks the newly added query plan
                sinkOperators.insert(hostOperator->getAllRootNodes().front()->as<LogicalOperatorNode>());
                break;
            // In case of no containment, do nothing
            case Optimizer::ContainmentRelationship::NO_CONTAINMENT: break;
        }

        //Add new hash based signatures to the shared query plan for newly added downstream operators
        for (const auto& newDownstreamOperator : clEntryDownstreamOperators) {
            auto hashBasedSignature = newDownstreamOperator->as<LogicalOperatorNode>()->getHashBasedSignature();
            for (const auto& signatureEntry : hashBasedSignature) {
                for (const auto& stringValue : signatureEntry.second) {
                    updateHashBasedSignature(signatureEntry.first, stringValue);
                }
            }
        }

        //add change log entry indicating the addition
        auto now =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        changeLog->addChangeLogEntry(
            now,
            Optimizer::Experimental::ChangeLogEntry::create(clEntryUpstreamOperators, clEntryDownstreamOperators));
    }

    //add the new sink operators as root to the query plan
    for (const auto& targetSinkOperator : sinkOperators) {
        queryPlan->addRootOperator(targetSinkOperator);
    }

    //Add sink list for the newly inserted query
    queryIdToSinkOperatorMap[queryId] = sinkOperators;

    //add the query id
    queryIds.emplace_back(queryId);
}

bool SharedQueryPlan::removeQuery(QueryId queryId) {
    NES_DEBUG("SharedQueryPlan: Remove the Query Id {} and associated Global Query Nodes with sink operators.", queryId);
    if (queryIdToSinkOperatorMap.find(queryId) == queryIdToSinkOperatorMap.end()) {
        NES_ERROR("SharedQueryPlan: query id {} is not present in metadata information.", queryId);
        return false;
    }

    NES_TRACE("SharedQueryPlan: Remove the Global Query Nodes with sink operators for query  {}", queryId);
    std::set<LogicalOperatorNodePtr> sinkOperatorsToRemove = queryIdToSinkOperatorMap[queryId];
    // Iterate over all sink global query nodes for the input query and remove the corresponding exclusive upstream operator chains
    for (const auto& sinkOperator : sinkOperatorsToRemove) {
        //Remove sink operator and associated operators from query plan

        auto upstreamOperators = removeOperator(sinkOperator);
        if (upstreamOperators.empty()) {
            NES_ERROR("SharedQueryPlan: unable to remove Root operator from the shared query plan {}", sharedQueryId);
            return false;
        }
        queryPlan->removeAsRootOperator(sinkOperator);

        //add change log entry indicating the addition
        auto now =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        changeLog->addChangeLogEntry(now, Optimizer::Experimental::ChangeLogEntry::create(upstreamOperators, {sinkOperator}));
    }

    queryIdToSinkOperatorMap.erase(queryId);
    return true;
}

bool SharedQueryPlan::isEmpty() {
    NES_TRACE("SharedQueryPlan: Check if Global Query Metadata is empty. Found :  {}", queryIdToSinkOperatorMap.empty());
    return queryIdToSinkOperatorMap.empty();
}

std::vector<LogicalOperatorNodePtr> SharedQueryPlan::getSinkOperators() {
    NES_TRACE("SharedQueryPlan: Get all Global Query Nodes with sink operators for the current Metadata");
    std::vector<LogicalOperatorNodePtr> sinkOperators;
    for (const auto& rootOperator : this->queryPlan->getRootOperators()) {
        sinkOperators.emplace_back(rootOperator->as<LogicalOperatorNode>());
    }
    return sinkOperators;
}

std::map<QueryId, std::set<LogicalOperatorNodePtr>> SharedQueryPlan::getQueryIdToSinkOperatorMap() {
    return queryIdToSinkOperatorMap;
}

SharedQueryId SharedQueryPlan::getId() const { return sharedQueryId; }

void SharedQueryPlan::clear() {
    NES_DEBUG("SharedQueryPlan: clearing all metadata information.");
    queryIdToSinkOperatorMap.clear();
    queryIds.clear();
}

std::vector<QueryId> SharedQueryPlan::getQueryIds() { return queryIds; }

QueryPlanPtr SharedQueryPlan::getQueryPlan() { return queryPlan; }

std::set<LogicalOperatorNodePtr> SharedQueryPlan::removeOperator(const LogicalOperatorNodePtr& operatorToRemove) {

    //Collect all upstream operators till which removal of operators occurred
    std::set<LogicalOperatorNodePtr> upstreamOperatorsToReturn;

    //Mark the operator for To-Be-Removed
    operatorToRemove->setOperatorState(OperatorState::TO_BE_REMOVED);

    //Iterate over all child operator
    auto upstreamOperators = operatorToRemove->getChildren();

    //If it is the most upstream operator then return this operator
    if (!operatorToRemove->getChildren().empty()) {
        upstreamOperatorsToReturn.insert(operatorToRemove);
        return upstreamOperatorsToReturn;
    }

    for (const auto& optr : upstreamOperators) {
        //If the upstream operator is shared by multiple downstream operators then remove the operator to remove and add this operator
        // to the operators to return.
        auto upstreamOperator = optr->as<LogicalOperatorNode>();
        if (upstreamOperator->getParents().size() > 1) {// If the upstream operator is connected to multiple downstream operator
                                                        // then remove the downstream operator to remove and terminate recursion.
            //Recursively call removal of this upstream operator
            upstreamOperator->removeParent(operatorToRemove);
            //add this upstream operator to operators to return
            upstreamOperatorsToReturn.insert(upstreamOperator);
        } else {// If the upstream operator is only connected to one downstream operator
            // then remove the downstream operator and recursively call operator removal
            // for this upstream operator.
            //Remove the parent and call remove operator for children
            upstreamOperator->removeParent(operatorToRemove);
            //Recursively call removal of this upstream operator
            auto lastUpstreamOperators = removeOperator(upstreamOperator);
            //add returned operators to operators to return
            upstreamOperatorsToReturn.insert(lastUpstreamOperators.begin(), lastUpstreamOperators.end());
        }
    }
    return upstreamOperatorsToReturn;
}

ChangeLogEntries SharedQueryPlan::getChangeLogEntries(Timestamp timestamp) {
    return changeLog->getCompactChangeLogEntriesBeforeTimestamp(timestamp);
}

std::map<size_t, std::set<std::string>> SharedQueryPlan::getHashBasedSignature() { return hashBasedSignatures; }

void SharedQueryPlan::updateHashBasedSignature(size_t hashValue, const std::string& stringSignature) {
    if (hashBasedSignatures.find(hashValue) != hashBasedSignatures.end()) {
        auto stringSignatures = hashBasedSignatures[hashValue];
        stringSignatures.emplace(stringSignature);
        hashBasedSignatures[hashValue] = stringSignatures;
    } else {
        hashBasedSignatures[hashValue] = {stringSignature};
    }
}

SharedQueryPlanStatus SharedQueryPlan::getStatus() const { return sharedQueryPlanStatus; }

void SharedQueryPlan::setStatus(SharedQueryPlanStatus newStatus) { this->sharedQueryPlanStatus = newStatus; }

Optimizer::PlacementStrategy SharedQueryPlan::getPlacementStrategy() const { return placementStrategy; }

void SharedQueryPlan::updateProcessedChangeLogTimestamp(Timestamp timestamp) {
    changeLog->updateProcessedChangeLogTimestamp(timestamp);
}

void SharedQueryPlan::performReOperatorPlacement(const std::set<OperatorId>& upstreamOperatorIds,
                                                 const std::set<OperatorId>& downstreamOperatorIds) {

    std::set<LogicalOperatorNodePtr> upstreamLogicalOperators;
    for (const auto& upstreamOperatorId : upstreamOperatorIds) {
        upstreamLogicalOperators.emplace(queryPlan->getOperatorWithId(upstreamOperatorId)->as<LogicalOperatorNode>());
    }

    std::set<LogicalOperatorNodePtr> downstreamLogicalOperators;
    for (const auto& downstreamOperatorId : downstreamOperatorIds) {
        downstreamLogicalOperators.emplace(queryPlan->getOperatorWithId(downstreamOperatorId)->as<LogicalOperatorNode>());
    }

    std::set<OperatorNodePtr> downstreamOperator{downstreamLogicalOperators.begin(), downstreamLogicalOperators.end()};
    std::set<OperatorNodePtr> upstreamOperator{upstreamLogicalOperators.begin(), upstreamLogicalOperators.end()};

    auto operatorsToBeRePlaced = queryPlan->findAllOperatorsBetween(downstreamOperator, upstreamOperator);

    for (const auto& operatorToRePlace : operatorsToBeRePlaced) {
        operatorToRePlace->as_if<LogicalOperatorNode>()->setOperatorState(OperatorState::TO_BE_REPLACED);
    }

    //add change log entry indicating the addition
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changeLog->addChangeLogEntry(
        now,
        Optimizer::Experimental::ChangeLogEntry::create(upstreamLogicalOperators, downstreamLogicalOperators));
}

void SharedQueryPlan::updateOperators(const std::set<LogicalOperatorNodePtr>& updatedOperators) {

    //Iterate over all updated operators and update the corresponding operator in the shared query plan with correct properties and state.
    for (const auto& placedOperator : updatedOperators) {
        auto topologyNodeId = std::any_cast<TopologyNodeId>(placedOperator->getProperty(PINNED_NODE_ID));
        auto operatorInQueryPlan = queryPlan->getOperatorWithId(placedOperator->getId());
        operatorInQueryPlan->addProperty(PINNED_NODE_ID, topologyNodeId);
        placedOperator->setOperatorState(OperatorState::PLACED);
    }
}

}// namespace NES

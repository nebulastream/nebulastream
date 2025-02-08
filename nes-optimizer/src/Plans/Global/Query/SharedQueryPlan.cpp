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

#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/QueryMerger/MatchedOperatorPair.hpp>
#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Plans/ChangeLog/ChangeLogEntry.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <Util/QuerySignatures/QuerySignature.hpp>

namespace NES {

SharedQueryPlan::SharedQueryPlan(const QueryPlanPtr& queryPlan)
    : sharedQueryId(PlanIdGenerator::getNextSharedQueryId()), sharedQueryPlanStatus(SharedQueryPlanStatus::CREATED) {

    //Create a new query plan
    this->queryPlan = queryPlan->copy();
    this->queryPlan->setQueryId(
        UNSURE_CONVERSION_TODO_4761(sharedQueryId, QueryId));//overwrite the query id with shared query plan id

    //Extract worker ids where the source operator is located
    std::vector<uint64_t> workerIds;
    for (auto sourceOperator : this->queryPlan->getSourceOperators()) {
        workerIds.emplace_back(std::any_cast<WorkerId>(sourceOperator->getProperty(Optimizer::PINNED_WORKER_ID)).getRawValue());
    }

    //Compute sink operators
    std::set<LogicalOperatorPtr> sinkOperators;
    for (const auto& rootOperator : this->queryPlan->getRootOperators()) {
        rootOperator->addProperty(Optimizer::LIST_OF_SOURCE_WORKER_ID, workerIds);
        sinkOperators.insert(rootOperator->as<LogicalOperator>());
    }
    auto queryId = queryPlan->getQueryId();
    queryIdToSinkOperatorMap[queryId] = sinkOperators;
    //Initialize hash-based signature
    hashBasedSignatures = (*sinkOperators.begin())->as<LogicalOperator>()->getHashBasedSignature();
    //The query id
    runningQueryIds = {queryId};
    //Set the placement strategy used
    placementStrategy = queryPlan->getPlacementStrategy();
    //Initialize change log
    changeLog = Optimizer::ChangeLog::create();

    //Compute first change log entry
    std::set<LogicalOperatorPtr> downstreamOperators;
    for (const auto& sinkOperator : sinkOperators) {
        downstreamOperators.insert(sinkOperator);
    }
    std::set<LogicalOperatorPtr> upstreamOperators;
    for (const auto& sourceOperator : this->queryPlan->getLeafOperators()) {
        upstreamOperators.insert(sourceOperator->as<LogicalOperator>());
    }
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changeLog->addChangeLogEntry(now, Optimizer::ChangeLogEntry::create(upstreamOperators, downstreamOperators));
}

SharedQueryPlanPtr SharedQueryPlan::create(const QueryPlanPtr& queryPlan) {
    return std::make_shared<SharedQueryPlan>(SharedQueryPlan(queryPlan));
}

void SharedQueryPlan::addQuery(QueryId queryId, const std::vector<Optimizer::MatchedOperatorPairPtr>& matchedOperatorPairs) {

    NES_DEBUG("SharedQueryPlan: Add the matched operators of query with id {} to the shared query plan.", queryId);
    // TODO Handling Fault-Tolerance in case of query merging [#2327]
    std::set<LogicalOperatorPtr> sinkOperators;

    //Iterate over matched operator pairs and
    for (const auto& matchedOperatorPair : matchedOperatorPairs) {

        auto hostOperator = matchedOperatorPair->hostOperator;
        auto targetOperator = matchedOperatorPair->targetOperator;

        //initialize sets for change log entry
        std::set<LogicalOperatorPtr> clEntryUpstreamOperators;
        std::set<LogicalOperatorPtr> clEntryDownstreamOperators;

        switch (matchedOperatorPair->containmentRelationship) {
            case Optimizer::ContainmentRelationship::EQUALITY:
                //If host and target operator are of sink type then connect the target sink to the upstream of the host sink.
                if (hostOperator->instanceOf<SinkLogicalOperator>() && targetOperator->instanceOf<SinkLogicalOperator>()) {

                    //Make a copy of the target operator so that we do not have to perform additional operation to
                    // add it to the shared query plan.
                    // Note: we otherwise have to remove the upstream operator of the target to decouple it from the original target plan.
                    auto targetOperatorCopy = targetOperator->copy()->as<LogicalOperator>();

                    //fetch all upstream operators of the host operator and add the target operator as their parent operator
                    for (const auto& hostUpstreamOperator : hostOperator->getChildren()) {

                        //add target operator as the parent to the host upstream operator
                        hostUpstreamOperator->addParent(targetOperatorCopy);
                        //add the host upstream operator to the change log entry
                        clEntryUpstreamOperators.insert(hostUpstreamOperator->as<LogicalOperator>());
                    }

                    //set target operator as the downstream operator in the change log
                    clEntryDownstreamOperators.insert(targetOperatorCopy);
                    //add the new sink operators as root to the set
                    sinkOperators.insert(targetOperatorCopy->as<LogicalOperator>());

                    //If host operator is of sink type then connect the downstream operators of target operator to the upstream of the host operator.
                } else if (hostOperator->instanceOf<SinkLogicalOperator>()) {

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
                            clEntryUpstreamOperators.insert(hostUpstreamOperator->as<LogicalOperator>());
                            //add the new sink operators as root to the set
                            sinkOperators.insert(hostUpstreamOperator->as<LogicalOperator>());
                        }
                    }

                    //If target operator is of sink type then connect the target sink to the host operator.
                } else if (targetOperator->instanceOf<SinkLogicalOperator>()) {

                    //Make a copy of the target operator so that we do not have to perform additional operation to
                    // add it to the shared query plan.
                    // Note: we otherwise have to remove the upstream operator of the target to decouple it from the original target plan.
                    auto targetOperatorCopy = targetOperator->copy()->as<LogicalOperator>();
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
                        clEntryDownstreamOperators.insert(newRootOperator->as<LogicalOperator>());
                        //add the new sink operators as root to the set
                        sinkOperators.insert(newRootOperator->as<LogicalOperator>());
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
                    clEntryDownstreamOperators.insert(newRootOperator->as<LogicalOperator>());
                    sinkOperators.insert(newRootOperator->as<LogicalOperator>());
                }
                break;
            case Optimizer::ContainmentRelationship::LEFT_SIG_CONTAINED:
                // Here we need to re-deploy the whole shared query plan and therefore add all source operators to the change log
                for (const auto& sourceOperator : hostOperator->getAllLeafNodes()) {
                    clEntryUpstreamOperators.insert(sourceOperator->as<LogicalOperator>());
                }
                // We also need to add all root operators from the existing shared query plan
                for (const auto& existingRootOperatorsToBeReDeployed : targetOperator->getAllRootNodes()) {
                    clEntryDownstreamOperators.insert(existingRootOperatorsToBeReDeployed->as<LogicalOperator>());
                }
                // Finally, we need to add all root operators from the new query plan
                for (const auto& newRootOperator : hostOperator->getAllRootNodes()) {
                    clEntryDownstreamOperators.insert(newRootOperator->as<LogicalOperator>());
                }
                //add the new sink operators as root to the set
                //in case of left signature containment, the host operator marks the newly added query plan
                sinkOperators.insert(hostOperator->getAllRootNodes().front()->as<LogicalOperator>());
                break;
            // In case of no containment, do nothing
            case Optimizer::ContainmentRelationship::NO_CONTAINMENT:
                NES_WARNING("NO_CONTAINMENT relationship was passed to SharedQueryPlan::addQuery(). This should not happen. "
                            "Please check the code.");
                break;
        }

        //Add new hash based signatures to the shared query plan for newly added downstream operators
        for (const auto& newDownstreamOperator : clEntryDownstreamOperators) {
            auto hashBasedSignature = newDownstreamOperator->as<LogicalOperator>()->getHashBasedSignature();
            for (const auto& signatureEntry : hashBasedSignature) {
                for (const auto& stringValue : signatureEntry.second) {
                    updateHashBasedSignature(signatureEntry.first, stringValue);
                }
            }
        }

        //add change log entry indicating the addition
        auto now =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        changeLog->addChangeLogEntry(now,
                                     Optimizer::ChangeLogEntry::create(clEntryUpstreamOperators, clEntryDownstreamOperators));
    }

    //add the new sink operators as root to the query plan
    for (const auto& targetSinkOperator : sinkOperators) {
        NES_TRACE("New sink operators: {}", targetSinkOperator->toString());
        queryPlan->addRootOperator(targetSinkOperator);
    }

    //Add sink list for the newly inserted query
    queryIdToSinkOperatorMap[queryId] = sinkOperators;

    NES_TRACE("Size of queryIdToSinkOperatorMap: {}", queryIdToSinkOperatorMap.size());

    //add the query id
    runningQueryIds.emplace_back(queryId);
}

bool SharedQueryPlan::markQueryForRemoval(QueryId queryId) {
    NES_DEBUG("SharedQueryPlan: Remove the Query Id {} and associated Global Query Nodes with sink operators.", queryId);
    if (queryIdToSinkOperatorMap.find(queryId) == queryIdToSinkOperatorMap.end()) {
        NES_ERROR("SharedQueryPlan: query id {} is not present in metadata information.", queryId);
        return false;
    }

    NES_TRACE("SharedQueryPlan: Remove the Global Query Nodes with sink operators for query  {}", queryId);
    std::set<LogicalOperatorPtr> sinkOperatorsToRemove = queryIdToSinkOperatorMap[queryId];
    // Iterate over all sink global query nodes for the input query and remove the corresponding exclusive upstream operator chains
    for (const auto& sinkOperator : sinkOperatorsToRemove) {

        //mark sink and associated operators for removal
        auto upstreamOperators = markOperatorsToBeRemoved(sinkOperator);
        if (upstreamOperators.empty()) {
            NES_ERROR("SharedQueryPlan: unable to remove Root operator from the shared query plan {}", sharedQueryId);
            return false;
        }

        //add change log entry indicating the addition
        auto now =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        changeLog->addChangeLogEntry(now, Optimizer::ChangeLogEntry::create(upstreamOperators, {sinkOperator}));
    }
    queriesMarkedForRemoval.emplace_back(queryId);
    if (containsAllQueryIds(runningQueryIds, queriesMarkedForRemoval)) {
        // Mark SQP as stopped if all queries are removed post stop
        setStatus(SharedQueryPlanStatus::STOPPED);
    } else {
        // Mark SQP as updated if after stop more queries are remaining
        setStatus(SharedQueryPlanStatus::UPDATED);
    }
    return true;
}

bool SharedQueryPlan::removeQueryMarkedForRemoval() {
    for (const auto& queryId : queriesMarkedForRemoval) {
        std::set<LogicalOperatorPtr> sinkOperatorsToRemove = queryIdToSinkOperatorMap[queryId];
        // Iterate over all sink global query nodes for the input query and remove the corresponding exclusive upstream operator chains
        for (const auto& sinkOperator : sinkOperatorsToRemove) {
            //mark sink and associated operators for removal
            removeOperator(sinkOperator);
            queryPlan->removeAsRootOperator(sinkOperator);
        }
        runningQueryIds.erase(std::remove(runningQueryIds.begin(), runningQueryIds.end(), queryId));
        queryIdToSinkOperatorMap.erase(queryId);
    }
    queriesMarkedForRemoval.clear();
    return true;
}

bool SharedQueryPlan::isEmpty() {
    NES_TRACE("SharedQueryPlan: Check if Global Query Metadata is empty. Found :  {}", queryIdToSinkOperatorMap.empty());
    return queryIdToSinkOperatorMap.empty();
}

std::vector<LogicalOperatorPtr> SharedQueryPlan::getSinkOperators() {
    NES_TRACE("SharedQueryPlan: Get all Global Query Nodes with sink operators for the current Metadata");
    std::vector<LogicalOperatorPtr> sinkOperators;
    for (const auto& rootOperator : this->queryPlan->getRootOperators()) {
        sinkOperators.emplace_back(rootOperator->as<LogicalOperator>());
    }
    return sinkOperators;
}

std::map<QueryId, std::set<LogicalOperatorPtr>> SharedQueryPlan::getQueryIdToSinkOperatorMap() {
    return queryIdToSinkOperatorMap;
}

SharedQueryId SharedQueryPlan::getId() const { return sharedQueryId; }

void SharedQueryPlan::clear() {
    NES_DEBUG("SharedQueryPlan: clearing all metadata information.");
    queryIdToSinkOperatorMap.clear();
    runningQueryIds.clear();
}

std::vector<QueryId> SharedQueryPlan::getQueryIds() { return runningQueryIds; }

QueryPlanPtr SharedQueryPlan::getQueryPlan() { return queryPlan; }

std::set<LogicalOperatorPtr> SharedQueryPlan::markOperatorsToBeRemoved(const NES::LogicalOperatorPtr& operatorToRemove) {
    //Collect all upstream operators till which removal of operators occurred
    std::set<LogicalOperatorPtr> upstreamOperatorsToReturn;

    //Mark the operator for To-Be-Removed
    operatorToRemove->setOperatorState(OperatorState::TO_BE_REMOVED);

    //Iterate over all child operator
    auto upstreamOperators = operatorToRemove->getChildren();

    //If it is the most upstream operator then return this operator
    if (upstreamOperators.empty()) {
        upstreamOperatorsToReturn.insert(operatorToRemove);
        return upstreamOperatorsToReturn;
    }

    for (const auto& upstreamOperator : upstreamOperators) {
        auto connectedDownstream = upstreamOperator->getParents();

        //If chas more than 1 downstream operators and not all downstream operators of this upstream operator is in to-be-removed state then add this operator
        // to the upstream operators to return.
        if (connectedDownstream.size() > 1) {
            bool allNotInToBeRemoved =
                std::any_of(connectedDownstream.begin(),
                            connectedDownstream.end(),
                            [](const NodePtr connectedDownStreamOperator) {
                                return connectedDownStreamOperator->as_if<LogicalOperator>()->getOperatorState()
                                    != OperatorState::TO_BE_REMOVED;
                            });

            if (allNotInToBeRemoved) {
                //add this upstream operator to operators to return
                upstreamOperatorsToReturn.insert(upstreamOperator->as<LogicalOperator>());
                //Skip remaining processing
                continue;
            }
        }

        //Recursively call removal of this upstream operator
        auto lastUpstreamOperators = markOperatorsToBeRemoved(upstreamOperator->as<LogicalOperator>());
        //add returned operators to operators to return
        upstreamOperatorsToReturn.insert(lastUpstreamOperators.begin(), lastUpstreamOperators.end());
    }
    return upstreamOperatorsToReturn;
}

void SharedQueryPlan::removeOperator(const LogicalOperatorPtr& operatorToRemove) {

    //Iterate over all child operator
    auto upstreamOperators = operatorToRemove->getChildren();

    //If it is the most upstream operator then return this operator
    if (upstreamOperators.empty()) {
        return;
    }

    //Remove all children
    operatorToRemove->removeChildren();

    for (const auto& upstreamOperator : upstreamOperators) {
        //If the upstream operator is in the state REMOVED then recursively process its upstream operators.
        if (upstreamOperator->as_if<LogicalOperator>()->getOperatorState() == OperatorState::REMOVED) {
            removeOperator(upstreamOperator->as<LogicalOperator>());
        }
    }
}

ChangeLogEntries SharedQueryPlan::getChangeLogEntries(Timestamp timestamp) {
    return changeLog->getCompactChangeLogEntriesBeforeTimestamp(timestamp);
}

void SharedQueryPlan::recordFailedChangeLogEntries(std::vector<Optimizer::ChangeLogEntryPtr> changeLogEntries) {
    for (const auto& changeLogEntry : changeLogEntries) {

        // Find the most downstream pinned operator
        std::set<LogicalOperatorPtr> newDownStreamOperators;

        std::queue<LogicalOperatorPtr> downStreamOperatorsToCheck;
        for (const auto& downstreamOperator : changeLogEntry->downstreamOperators) {
            downStreamOperatorsToCheck.emplace(downstreamOperator);
        }

        while (!downStreamOperatorsToCheck.empty()) {
            auto downstreamOperator = downStreamOperatorsToCheck.front();
            downStreamOperatorsToCheck.pop();
            auto upstreamOperators = downstreamOperator->getChildren();
            // Check if at least one the upstream operator is not in the state placed
            bool isDownStreamCandidate = false;
            for (const auto& upstreamOperator : upstreamOperators) {
                if (upstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::PLACED) {
                    newDownStreamOperators.insert(downstreamOperator);
                    isDownStreamCandidate = true;
                    break;
                }
            }

            if (!isDownStreamCandidate) {
                for (const auto& upstreamOperator : upstreamOperators) {
                    downStreamOperatorsToCheck.emplace(upstreamOperator->as<LogicalOperator>());
                }
            }
        }

        if (newDownStreamOperators.empty()) {
            NES_ERROR("Unable to find the most downstream operator. Skipping rest of the operation. The failed changelog will "
                      "not be processed.");
            continue;
        }

        // Find the most upstream pinned operator
        std::set<LogicalOperatorPtr> newUpstreamOperators;

        std::queue<LogicalOperatorPtr> upStreamOperatorsToCheck;
        for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
            upStreamOperatorsToCheck.emplace(upstreamOperator);
        }

        while (!upStreamOperatorsToCheck.empty()) {
            auto upstreamOperator = upStreamOperatorsToCheck.front();
            upStreamOperatorsToCheck.pop();
            auto downstreamOperators = upstreamOperator->getParents();
            // Check if at least one the upstream operator is not in the state placed
            bool isDownStreamCandidate = false;
            for (const auto& downstreamOperator : downstreamOperators) {
                if (downstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::PLACED) {
                    newUpstreamOperators.insert(upstreamOperator);
                    isDownStreamCandidate = true;
                    break;
                }
            }

            if (!isDownStreamCandidate) {
                for (const auto& downstreamOperator : downstreamOperators) {
                    upStreamOperatorsToCheck.emplace(downstreamOperator->as<LogicalOperator>());
                }
            }
        }

        if (newUpstreamOperators.empty()) {
            NES_ERROR("Unable to find the most upstream operator. Skipping rest of the operation. The failed changelog will "
                      "not be processed.");
            continue;
        }

        // Compute a new change log entry
        auto now =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        changeLog->addChangeLogEntry(now, Optimizer::ChangeLogEntry::create(newUpstreamOperators, newDownStreamOperators));
    }
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

    std::set<LogicalOperatorPtr> upstreamLogicalOperators;
    for (const auto& upstreamOperatorId : upstreamOperatorIds) {
        upstreamLogicalOperators.emplace(queryPlan->getOperatorWithOperatorId(upstreamOperatorId)->as<LogicalOperator>());
    }

    std::set<LogicalOperatorPtr> downstreamLogicalOperators;
    for (const auto& downstreamOperatorId : downstreamOperatorIds) {
        downstreamLogicalOperators.emplace(queryPlan->getOperatorWithOperatorId(downstreamOperatorId)->as<LogicalOperator>());
    }

    std::set<OperatorPtr> downstreamOperator{downstreamLogicalOperators.begin(), downstreamLogicalOperators.end()};
    std::set<OperatorPtr> upstreamOperator{upstreamLogicalOperators.begin(), upstreamLogicalOperators.end()};

    auto operatorsToBeRePlaced = queryPlan->findAllOperatorsBetween(downstreamOperator, upstreamOperator);

    for (const auto& operatorToRePlace : operatorsToBeRePlaced) {
        operatorToRePlace->as_if<LogicalOperator>()->setOperatorState(OperatorState::TO_BE_REPLACED);
    }

    //add change log entry indicating the addition
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changeLog->addChangeLogEntry(now, Optimizer::ChangeLogEntry::create(upstreamLogicalOperators, downstreamLogicalOperators));
}

void SharedQueryPlan::updateOperators(const std::set<LogicalOperatorPtr>& updatedOperators) {

    //Iterate over all updated operators and update the corresponding operator in the shared query plan with correct properties and state.
    for (const auto& placedOperator : updatedOperators) {
        auto topologyNodeId = std::any_cast<WorkerId>(placedOperator->getProperty(Optimizer::PINNED_WORKER_ID));
        auto operatorInQueryPlan = queryPlan->getOperatorWithOperatorId(placedOperator->getId());
        operatorInQueryPlan->addProperty(Optimizer::PINNED_WORKER_ID, topologyNodeId);
        placedOperator->setOperatorState(OperatorState::PLACED);
    }
}

bool SharedQueryPlan::containsAllQueryIds(std::vector<QueryId>& first, std::vector<QueryId>& second) {
    for (const auto& queryId : first) {
        // If a query id from first is not in the second
        if (std::find(second.begin(), second.end(), queryId) == second.end()) {
            return false;
        }
    }
    return true;
}

}// namespace NES

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

#include <algorithm>

#include <Statistics/Requests/StatCreateRequest.hpp>
#include <Statistics/Requests/StatDeleteRequest.hpp>
#include <Statistics/Requests/StatProbeRequest.hpp>
#include <Statistics/StatCoordinator.hpp>

#include <API/QueryAPI.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Services/QueryService.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>

#include <GRPC/WorkerRPCClient.hpp>

namespace NES {

namespace Experimental::Statistics {

StatCoordinator::StatCoordinator(QueryServicePtr queryService, Catalogs::Source::SourceCatalogPtr sourceCatalog)
    : queryService(queryService), sourceCatalog(sourceCatalog), workerClient(WorkerRPCClient::create()) {}

QueryId StatCoordinator::createStat(StatCreateRequest& createRequest) {

    auto statIdentifier = StatCollectorIdentifier(createRequest.getLogicalSourceName(),
                                                  createRequest.getFieldName(),
                                                  createRequest.getStatCollectorType());
    auto statQueryIdIt = trackedStatistics.find(statIdentifier);

    QueryId queryId;
    if (statQueryIdIt == trackedStatistics.end()) {

        NES_DEBUG("Statistic does not yet exist");
        // ToDo: add logic for different queries in later issue
        auto sourceName = statIdentifier.getLogicalSourceName();
        auto fieldName = statIdentifier.getFieldName();
        auto query = Query::from(sourceName).filter(Attribute(fieldName) < 42).sink(PrintSinkDescriptor::create());

        // ToDo: add actual statistic query. Issue: 4314
        //        NES_DEBUG("Submitting statistic query");
        //        QueryId queryId = queryService->addQueryRequest(query.getQueryPlan()->toString(),
        //                                                        query.getQueryPlan(),
        //                                                        Optimizer::PlacementStrategy::BottomUp,
        //                                                        FaultToleranceType::NONE,
        //                                                        LineageType::IN_MEMORY);
        queryId = 1;
        NES_DEBUG("Adding statistic to the unordered_map of tracked statistics");
        trackedStatistics[statIdentifier] = queryId;

        return queryId;

    } else {

        NES_DEBUG("Statistic already exists. QueryID of the generating query is returned")
        queryId = statQueryIdIt->second;
        return queryId;
    }
}

std::vector<double> StatCoordinator::probeStat(StatProbeRequest& probeRequest) {

    std::vector<double> stats;
    std::vector<double> erroneousResult{-1.0};

    auto sources = sourceCatalog->getSubsetOfPhysicalSources(probeRequest.getLogicalSourceName(), probeRequest.getPhysicalSourceNames());

    if (sources[0] == nullptr) {
        /*
         * if a single statistic cannot be found, then we return a vector with the singular error value -1
         * as we potentially otherwise run into undefined behavior about how to combine only the available
         * statistics
         */
        return erroneousResult;
    }

    // all the desired physicalSource exist
    auto statIdentifier = StatCollectorIdentifier(probeRequest.getLogicalSourceName(),
                                                  probeRequest.getFieldName(),
                                                  probeRequest.getStatCollectorType());

    auto statQueryIdIt = trackedStatistics.find(statIdentifier);

    if (statQueryIdIt == trackedStatistics.end()) {
        NES_DEBUG("Statistic cannot queried, as it is not being generated.");
        return erroneousResult;
    } else {
        NES_DEBUG("Statistic is being generated. Proceeding with probe operation.");
        for (auto source : sources) {
            auto node = source->getNode();
            std::string destAddress = node->getIpAddress() + ":" + std::to_string(node->getGrpcPort());
            stats.push_back(workerClient->probeStat(destAddress, probeRequest));
        }
    }
    if(probeRequest.getMerge()) {
        //ToDo: In future potentially combine fine grained stats to coarse grained ones
    }

    return stats;
}

bool StatCoordinator::deleteStat(StatDeleteRequest& deleteRequest) {

    // check if statistic(s) even exists
    auto statIdentifier = StatCollectorIdentifier(deleteRequest.getLogicalSourceName(),
                                                  deleteRequest.getFieldName(),
                                                  deleteRequest.getStatCollectorType());

    auto statQueryIdIt = trackedStatistics.find(statIdentifier);
    if (statQueryIdIt == trackedStatistics.end()) {
        // stat is not being generated, return with error value
        return -1;
    } else {
        auto allAddresses = addressesOfLogicalStream(deleteRequest.getLogicalSourceName());

        // Success equal to one for the moment
        int32_t success = 1;
        for (std::string destAddress : allAddresses) {
            success = workerClient->deleteStat(destAddress, deleteRequest);
            if (success == -1) {
                return success;
            }
        }
//        ToDo: Add Logic to stop statistic queries. Issue: 4315
//        NES_DEBUG("Trying to stop query!");
//        auto queryStopped = queryService->validateAndQueueStopQueryRequest(it->second);
//        if (!queryStopped) {
//            return -1;
//        }

        trackedStatistics.erase(statIdentifier);
        NES_DEBUG("StatCollectors successfully deleted and query stopped!");
        return success;
    }
}

std::vector<std::string> StatCoordinator::addressesOfLogicalStream(const std::string& logicalSourceName) {

    std::vector<std::string> addresses;
    auto physicalSources = sourceCatalog->getPhysicalSources(logicalSourceName);
    for (const auto& physicalSource : physicalSources) {
        TopologyNodePtr node = physicalSource->getNode();
        addresses.push_back(node->getIpAddress() + ":" + std::to_string(node->getGrpcPort()));
    }
    return addresses;
}
}// namespace Experimental::Statistics
}// namespace NES

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

#include <Statistics/Requests/StatisticCreateRequest.hpp>
#include <Statistics/Requests/StatisticDeleteRequest.hpp>
#include <Statistics/Requests/StatisticProbeRequest.hpp>
#include <Statistics/StatisticCoordinator/StatisticCoordinator.hpp>

#include <API/QueryAPI.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>

#include <GRPC/WorkerRPCClient.hpp>

namespace NES::Experimental::Statistics {

StatisticCoordinator::StatisticCoordinator(QueryServicePtr queryService, Catalogs::Source::SourceCatalogPtr sourceCatalog)
    : queryService(queryService), sourceCatalog(sourceCatalog), workerClient(WorkerRPCClient::create()) {}

QueryId StatisticCoordinator::createStatistic(StatisticCreateRequest& createRequest) {

    auto statisticQueryIdentifier = StatisticQueryIdentifier(createRequest.getLogicalSourceName(),
                                                             createRequest.getFieldName(),
                                                             createRequest.getStatisticCollectorType());
    auto statisticQueryIdIt = trackedStatistics.find(statisticQueryIdentifier);

    QueryId queryId;
    if (statisticQueryIdIt == trackedStatistics.end()) {

        NES_DEBUG("Statistic does not yet exist");
        // ToDo: add logic for different queries in later issue
        auto sourceName = statisticQueryIdentifier.getLogicalSourceName();
        auto fieldName = statisticQueryIdentifier.getFieldName();
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
        trackedStatistics[statisticQueryIdentifier] = queryId;

        return queryId;

    } else {

        NES_DEBUG("Statistic already exists. QueryID of the generating query is returned")
        queryId = statisticQueryIdIt->second;
        return queryId;
    }
}

std::vector<double> StatisticCoordinator::probeStatistic(StatisticProbeRequest& probeRequest) {

    std::vector<double> statistics;
    std::vector<double> erroneousResult{-1.0};
    auto queriedPhysicalSources = probeRequest.getPhysicalSourceNames();
    probeRequest.clearPhysicalSourceNames();

    // get all physicalSources and sort them. Sorting them according to the node allows us to combine requests to nodes that
    // have multiple physicalSource we want to query
    auto allPhysicalSources =
        sourceCatalog->getSubsetOfPhysicalSources(probeRequest.getLogicalSourceName(), queriedPhysicalSources);
    std::sort(allPhysicalSources.begin(),
              allPhysicalSources.end(),
              [](const Catalogs::Source::SourceCatalogEntryPtr& entry1, const Catalogs::Source::SourceCatalogEntryPtr& entry2) {
                  return entry1->getNode()->getId() < entry2->getNode()->getId();
              });

    if (allPhysicalSources[0] == nullptr) {
        /*
         * if a single statistic cannot be found, then we return a vector with the singular error value -1
         * as we potentially otherwise run into undefined behavior about how to combine only the available
         * statistics
         */
        return erroneousResult;
    }

    // all the desired physicalSource exist
    auto statisticQueryIdentifier = StatisticQueryIdentifier(probeRequest.getLogicalSourceName(),
                                                             probeRequest.getFieldName(),
                                                             probeRequest.getStatisticCollectorType());

    auto statisticQueryPairIt = trackedStatistics.find(statisticQueryIdentifier);

    if (statisticQueryPairIt == trackedStatistics.end()) {
        NES_DEBUG("Statistic cannot queried, as it is not being generated.");
        return erroneousResult;
    } else {
        NES_DEBUG("Statistic is being generated. Proceeding with probe operation.");
        for (uint64_t index = 0; index < allPhysicalSources.size(); index++) {
            auto sourceCatalogEntry = allPhysicalSources.at(index);
            auto physicalSourceName = sourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName();
            probeRequest.addPhysicalSourceName(physicalSourceName);
            auto node = sourceCatalogEntry->getNode();
            std::string destAddress = node->getIpAddress() + ":" + std::to_string(node->getGrpcPort());
            for (uint64_t followUpIndexes = index + 1; followUpIndexes < allPhysicalSources.size(); followUpIndexes++) {
                sourceCatalogEntry = allPhysicalSources.at(followUpIndexes);
                auto secondNode = sourceCatalogEntry->getNode();
                if (node == secondNode) {
                    physicalSourceName = sourceCatalogEntry->getPhysicalSource()->getPhysicalSourceName();
                    probeRequest.addPhysicalSourceName(physicalSourceName);
                } else {
                    break;
                }
            }
            auto localStatistics = workerClient->probeStatistic(destAddress, probeRequest);
            statistics.insert(statistics.end(), localStatistics.begin(), localStatistics.end());
        }
    }
    if (probeRequest.getMerge()) {
        //ToDo: In future potentially combine fine grained statistics to coarse grained ones
    }

    return statistics;
}

bool StatisticCoordinator::deleteStatistic(StatisticDeleteRequest& deleteRequest) {

    // check if statistic(s) even exists
    auto statisticQueryIdentifier = StatisticQueryIdentifier(deleteRequest.getLogicalSourceName(),
                                                             deleteRequest.getFieldName(),
                                                             deleteRequest.getStatisticCollectorType());

    auto statisticQueryIdIt = trackedStatistics.find(statisticQueryIdentifier);
    if (statisticQueryIdIt == trackedStatistics.end()) {
        // statistic is not being generated, return with error value
        return -1;
    } else {
        auto allAddresses = addressesOfLogicalStream(deleteRequest.getLogicalSourceName());

        // Success equal to one for the moment
        int32_t success = 1;
        for (std::string destAddress : allAddresses) {
            success = workerClient->deleteStatistic(destAddress, deleteRequest);
            if (success == -1) {
                return success;
            }
        }
        // ToDo: Add Logic to stop statistic queries. Issue: 4315
        //        NES_DEBUG("Trying to stop query!");
        //        auto queryStopped = queryService->validateAndQueueStopQueryRequest(statisticQueryIdIt->second);
        //        if (!queryStopped) {
        //            return -1;
        //        }

        trackedStatistics.erase(statisticQueryIdentifier);
        NES_DEBUG("StatisticCollectors successfully deleted!");
        NES_DEBUG("Statistic queries successfully stopped!");
        return success;
    }
}

std::vector<std::string> StatisticCoordinator::addressesOfLogicalStream(const std::string& logicalSourceName) {

    std::vector<std::string> addresses;
    auto physicalSources = sourceCatalog->getPhysicalSources(logicalSourceName);
    for (const auto& physicalSource : physicalSources) {
        TopologyNodePtr node = physicalSource->getNode();
        addresses.push_back(node->getIpAddress() + ":" + std::to_string(node->getGrpcPort()));
    }
    return addresses;
}
}// namespace NES::Experimental::Statistics

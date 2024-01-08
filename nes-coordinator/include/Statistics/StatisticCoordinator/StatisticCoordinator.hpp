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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICS_STATISTICCOORDINATOR_STATISTICCOORDINATOR_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICS_STATISTICCOORDINATOR_STATISTICCOORDINATOR_HPP_

#include <Identifiers.hpp>
#include <Statistics/StatisticCoordinator/StatisticQueryIdentifier.hpp>
#include <memory>
#include <unordered_map>
#include <vector>

namespace NES {

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source

namespace Experimental::Statistics {

class StatisticRequest;
class StatisticCreateRequest;
class StatisticProbeRequest;
class StatisticDeleteRequest;
class StatisticCollectorIdentifier;

/**
 * @brief the statisticCoordinator is a class that is a member of the coordinator node and that handles all
 * calls to create, query, and delete statistics. It forwards these to the local worker nodes and
 * combines local results to global ones if desired.
 */
class StatisticCoordinator {
  public:
    /**
     * @param queryService the query service gives the StatisticCoordinator the functionality to start and
     * stop statistic queries
     * @param sourceCatalog the source catalog allows the StatisticCoordinator to understand the relationship
     * between logical and physical sources and get the IPs of the nodes on which the physical sources
     * are located
     */
    StatisticCoordinator(QueryServicePtr queryService, Catalogs::Source::SourceCatalogPtr sourceCatalog);

    /**
     * @brief checks if the parametrized statistic query already exists and if not starts one accordingly
     * @param createRequest the parameter object specifying the data source (logicalSourceName and fieldName)
     * as well as the type of Statistic that is desired on the data source
     * @return the QueryId of the query that is now generating the statistic
     */
    QueryId createStatistic(StatisticCreateRequest& createRequest);

    /**
     * @brief checks if the desired statistic exists for all specified physicalSources and if so, queries
     * all StatisticCollectorFormats for the specified statistic. Depending on the probeQuery many local statistics
     * are returned or one globally merged statistic is returned
     * @param probeRequest the parameter object that specifies what statistic is desired. The user has to
     * specify the data source (logicalSource, physicalSource(s), and the field name),the time frame
     * (startTime and endTime), the statistic type and which specific statistic is desired (e.g. selectivity
     * (Count-Min) and x == 15), as well as whether the results are to be merged or not (merge).
     * @return a vector of one or more statistics that were queried
     */
    std::vector<double> probeStatistic(StatisticProbeRequest& probeRequest);

    /**
     * @brief attempts to stops a statistic query and deletes all associated StatisticCollectorFormats
     * @param deleteRequest specifies the data source (logicalSourceName and fieldName) for which to
     * delete the StatisticCollectorFormat of StatisticCollectorType.
     * @return true when successful and false otherwise
     */
    bool deleteStatistic(StatisticDeleteRequest& deleteRequest);

  private:
    /**
     * @brief gets the addresses of the nodes on which the physicalSources sit of the logicalSource
     * @param logicalSourceName the LogicalSourceName for which we want the addresses of the nodes
     * on which the associated physicalSource sit
     * @return a vector of addresses of the worker nodes whose physicalSources are associated
     * with the logicalSourceName
     */
    std::vector<std::string> addressesOfLogicalStream(const std::string& logicalSourceName);

    std::unordered_map<StatisticQueryIdentifier, QueryId, StatisticQueryIdentifier::Hash> trackedStatistics;
    QueryServicePtr queryService;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    WorkerRPCClientPtr workerClient;
};
}// namespace Experimental::Statistics
}// namespace NES

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICS_STATISTICCOORDINATOR_STATISTICCOORDINATOR_HPP_

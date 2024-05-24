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

#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>

namespace NES::RequestProcessor {
StorageDataStructures::StorageDataStructures(Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                             TopologyPtr topology,
                                             Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                                             GlobalQueryPlanPtr globalQueryPlan,
                                             Catalogs::Query::QueryCatalogPtr queryCatalog,
                                             Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                             Catalogs::UDF::UDFCatalogPtr udfCatalog,
                                             Optimizer::UMPMCAmendmentQueuePtr amendmentQueue,
                                             Statistic::StatisticProbeHandlerPtr statisticProbeHandler)
    : coordinatorConfiguration(std::move(coordinatorConfiguration)), topology(std::move(topology)),
      globalExecutionPlan(std::move(globalExecutionPlan)), globalQueryPlan(std::move(globalQueryPlan)),
      queryCatalog(std::move(queryCatalog)), sourceCatalog(std::move(sourceCatalog)), udfCatalog(std::move(udfCatalog)),
      amendmentQueue(std::move(amendmentQueue)), statisticProbeHandler(std::move(statisticProbeHandler)) {}
}// namespace NES::RequestProcessor

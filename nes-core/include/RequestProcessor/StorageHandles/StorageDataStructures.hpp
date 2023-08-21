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

#ifndef NES_STORAGEDATASTRUCTURES_HPP
#define NES_STORAGEDATASTRUCTURES_HPP

#include <memory>

namespace NES {

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

namespace Catalogs::UDF {
class UDFCatalog;
using UDFCatalogPtr = std::shared_ptr<UDFCatalog>;
}// namespace Catalogs::UDF

namespace Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace Configurations

namespace RequestProcessor::Experimental {
/**
 * @brief This struct contains smart pointers to the data structures which coordinator requests operate on.
 */
struct StorageDataStructures {
    StorageDataStructures(Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                          TopologyPtr topology,
                          GlobalExecutionPlanPtr globalExecutionPlan,
                          QueryCatalogServicePtr queryCatalogService,
                          GlobalQueryPlanPtr globalQueryPlan,
                          Catalogs::Source::SourceCatalogPtr sourceCatalog,
                          Catalogs::UDF::UDFCatalogPtr udfCatalog);
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    TopologyPtr topology;
    QueryCatalogServicePtr queryCatalogService;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    GlobalExecutionPlanPtr globalExecutionPlan;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
};
}// namespace RequestProcessor::Experimental
}// namespace NES
#endif//NES_STORAGEDATASTRUCTURES_HPP
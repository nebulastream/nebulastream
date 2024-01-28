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

#ifndef NES_EXPORTQUERYOPTIMIZER_H
#define NES_EXPORTQUERYOPTIMIZER_H

namespace NES {
class Topology;
class QueryPlan;
class GlobalExecutionPlan;
namespace Catalogs::Source {
class SourceCatalog;
}
namespace Configurations {
class CoordinatorConfiguration;
}
}// namespace NES

#include <memory>
class ExportQueryOptimizer {
  public:
    std::pair<std::shared_ptr<NES::QueryPlan>, std::shared_ptr<NES::GlobalExecutionPlan>>
    optimize(std::shared_ptr<NES::Topology> topology,
             std::shared_ptr<NES::Configurations::CoordinatorConfiguration> config,
             std::shared_ptr<NES::QueryPlan> query,
             std::shared_ptr<NES::Catalogs::Source::SourceCatalog> sourceCatalog);
};

#endif//NES_EXPORTQUERYOPTIMIZER_H

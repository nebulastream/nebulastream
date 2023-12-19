//
// Created by ls on 04.10.23.
//

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

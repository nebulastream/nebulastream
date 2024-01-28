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

#ifndef NES_YAMLEXPORT_H
#define NES_YAMLEXPORT_H
#include <CLIOptions.h>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <QueryPlanExport.hpp>
#include <Stage/QueryPipeliner.hpp>
#include <YAMLModel.h>
#include <string>
#include <vector>

struct WorkerSubQuery {
    NES::QueryPlanPtr subplan;
    NES::Unikernel::Export::QueryPipeliner::Result stages;
    NES::Unikernel::Export::QueryPlanExporter::Result sourcesAndSinks;
};

class YamlExport {
    Configuration configuration;
    std::optional<ExportKafkaConfiguration> exportToKafka;
    const NES::NodeId SINK_NODE = 1;

  public:
    explicit YamlExport(const std::optional<ExportKafkaConfiguration>& exportToKafka);
    void setQueryPlan(NES::QueryPlanPtr queryPlan, NES::GlobalExecutionPlanPtr gep, NES::Catalogs::Source::SourceCatalogPtr);

    void addWorker(const std::vector<WorkerSubQuery>& subqueries, const NES::ExecutionNodePtr& workerNode);

    void setSinkSchema(const NES::SchemaPtr& schema);
    void writeToOutputFile(std::string filepath) const;
};

#endif//NES_YAMLEXPORT_H

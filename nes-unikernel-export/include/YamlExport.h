//
// Created by ls on 01.10.23.
//

#ifndef NES_YAMLEXPORT_H
#define NES_YAMLEXPORT_H
#include "CLIOptions.h"
#include "Execution/Pipelines/CompiledExecutablePipelineStage.hpp"
#include "Util/Logger/Logger.hpp"
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <YAMLModel.h>
#include <fstream>
#include <map>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <yaml-cpp/yaml.h>

struct WorkerSubQueryStage {
    size_t stageId;
    std::vector<size_t> predecessors;
    std::optional<size_t> successor;
    size_t numberOfHandlers;
};

struct WorkerSubQuery {
    NES::QueryPlanPtr subplan;
    std::vector<WorkerSubQueryStage> stages;
    std::map<NES::PipelineId, NES::SourceDescriptorPtr> sources;
    std::map<NES::PipelineId, SinkStage> sinks;
};

class YamlExport {
    Configuration configuration;
    std::optional<ExportKafkaConfiguration> exportToKafka;
    const NES::NodeId SINK_NODE = 1;

  public:
    explicit YamlExport(const std::optional<ExportKafkaConfiguration>& exportToKafka);
    void setQueryPlan(NES::QueryPlanPtr queryPlan, NES::GlobalExecutionPlanPtr gep);

    void addWorker(const std::vector<WorkerSubQuery>& subqueries, const NES::ExecutionNodePtr& workerNode);

    void setSinkSchema(NES::SchemaPtr schema) {
        configuration.sink.schema.fields.clear();
        for (const auto& field : schema->fields) {
            configuration.sink.schema.fields.emplace_back(field->getName(),
                                                          std::string(magic_enum::enum_name(toBasicType(field->getDataType()))));
        }
    }
    void writeToOutputFile(std::string filepath) {
        std::ofstream f(filepath);
        YAML::Node yaml;
        yaml = this->configuration;
        NES_INFO("Writing Export YAML to {}", filepath);
        f << yaml;
    }
};

#endif//NES_YAMLEXPORT_H

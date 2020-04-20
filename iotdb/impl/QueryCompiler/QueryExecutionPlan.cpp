#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <iostream>
#include <Util/Logger.hpp>
#include <utility>

namespace NES {

QueryExecutionPlan::QueryExecutionPlan() : sources(), stages() {
}

QueryExecutionPlan::QueryExecutionPlan(std::vector<DataSourcePtr> sources,
                                       std::vector<PipelineStagePtr> stages,
                                       std::map<DataSource*, uint32_t> sourceToStage,
                                       std::map<uint32_t, uint32_t> stageToDest)
    : sources(std::move(sources)),
      stages(std::move(stages)),
      sourceToStage(std::move(sourceToStage)),
      stageToDest(std::move(stageToDest)) {
}

QueryExecutionPlan::~QueryExecutionPlan() {
    NES_DEBUG("destroy qep")
    sources.clear();
    stages.clear();
    sourceToStage.clear();
    stageToDest.clear();
}

bool QueryExecutionPlan::stop() {
    bool ret = true;
    NES_DEBUG("QueryExecutionPlan: stop");
    for (auto& stage: stages) {
        if (!stage->stop()) {
            NES_ERROR("QueryExecutionPlan: stop failed for stage " << stage);
            ret = false;
        }
    }
    return ret;
}

bool QueryExecutionPlan::setup() {
    NES_DEBUG("QueryExecutionPlan: setup");
    for (auto& stage: stages) {
        if (!stage->setup()) {
            NES_ERROR("QueryExecutionPlan: setup failed!");
            this->stop();
            return false;
        }
    }
    return true;
}

bool QueryExecutionPlan::start() {
    NES_DEBUG("QueryExecutionPlan: start");
    for (auto& stage: stages) {
        if (!stage->start()) {
            NES_ERROR("QueryExecutionPlan: start failed!");
            this->stop();
            return false;
        }
    }
    return true;
}

void QueryExecutionPlan::addDataSource(DataSourcePtr source) {
    sources.push_back(source);
}

const std::vector<DataSourcePtr> QueryExecutionPlan::getSources() const { return sources; }

void QueryExecutionPlan::addDataSink(DataSinkPtr sink) {
    sinks.push_back(sink);
}

const std::vector<DataSinkPtr> QueryExecutionPlan::getSinks() const { return sinks; }

void QueryExecutionPlan::appendsPipelineStage(PipelineStagePtr pipelineStage) {
    stages.push_back(pipelineStage);
}

void QueryExecutionPlan::print() {
    for (auto source : sources) {
        NES_INFO("Source:" << source)
        NES_INFO(
            "\t Generated Buffers=" << source->getNumberOfGeneratedBuffers())
        NES_INFO("\t Generated Tuples=" << source->getNumberOfGeneratedTuples())
        NES_INFO("\t Schema=" << source->getSourceSchemaAsString())
    }
    for (auto sink : sinks) {
        NES_INFO("Sink:" << sink)
        NES_INFO("\t Generated Buffers=" << sink->getNumberOfSentBuffers())
        NES_INFO("\t Generated Tuples=" << sink->getNumberOfSentTuples())
    }
}

bool QueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, TupleBuffer& buf) {
    return false;
}

uint32_t QueryExecutionPlan::stageIdFromSource(DataSource* source) { return sourceToStage[source]; };

} // namespace NES

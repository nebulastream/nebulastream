#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

QueryExecutionPlan::QueryExecutionPlan(std::string queryId,
                                       std::vector<DataSourcePtr> sources,
                                       std::vector<PipelineStagePtr> stages,
                                       QueryManagerPtr queryManager,
                                       BufferManagerPtr bufferManager)
    : queryId(std::move(queryId)),
      sources(std::move(sources)),
      stages(std::move(stages)),
      queryManager(std::move(queryManager)),
      bufferManager(std::move(bufferManager)),
      qepStatus(Created) {
}

QueryExecutionPlanId QueryExecutionPlan::getQueryId() {
    return queryId;
}

QueryExecutionPlan::~QueryExecutionPlan() {
    NES_DEBUG("destroy qep");
    NES_ASSERT(qepStatus.load() == Stopped || qepStatus.load() == ErrorState, "QueryPlan is created but not executing " << queryId);
    sources.clear();
    stages.clear();
}

bool QueryExecutionPlan::stop() {
    bool ret = true;
    NES_ASSERT(qepStatus == Running, "QueryPlan is created but not executing " << queryId);
    NES_DEBUG("QueryExecutionPlan: stop");
    for (auto& stage : stages) {
        if (!stage->stop()) {
            NES_ERROR("QueryExecutionPlan: stop failed for stage " << stage);
            ret = false;
        }
    }
    if (ret) {
        qepStatus.store(Stopped);
    } else {
        qepStatus.store(ErrorState);
    }
    return ret;
}

QueryExecutionPlan::QueryExecutionPlanStatus QueryExecutionPlan::getStatus() {
    return qepStatus.load();
}

bool QueryExecutionPlan::setup() {
    NES_DEBUG("QueryExecutionPlan: setup");
    auto expected = Created;
    if (qepStatus.compare_exchange_strong(expected, Deployed)) {
        for (auto& stage : stages) {
            if (!stage->setup()) {
                NES_ERROR("QueryExecutionPlan: setup failed!");
                this->stop();
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Created but was not");
    }
    return true;
}

bool QueryExecutionPlan::start() {
    NES_DEBUG("QueryExecutionPlan: start");
    auto expected = Deployed;
    if (qepStatus.compare_exchange_strong(expected, Running)) {
        for (auto& stage : stages) {
            if (!stage->start()) {
                NES_ERROR("QueryExecutionPlan: start failed!");
                this->stop();
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Deployed but was not");
    }
    return true;
}

QueryManagerPtr QueryExecutionPlan::getQueryManager() {
    return queryManager;
}

BufferManagerPtr QueryExecutionPlan::getBufferManager() {
    return bufferManager;
}

std::vector<DataSourcePtr> QueryExecutionPlan::getSources() const { return sources; }

std::vector<DataSinkPtr> QueryExecutionPlan::getSinks() const { return sinks; }

PipelineStagePtr QueryExecutionPlan::getStage(size_t index) const {
    return stages[index];
}

void QueryExecutionPlan::print() {
    for (auto source : sources) {
        NES_INFO("Source:" << source);
        NES_INFO("\t Generated Buffers=" << source->getNumberOfGeneratedBuffers());
        NES_INFO("\t Generated Tuples=" << source->getNumberOfGeneratedTuples());
        NES_INFO("\t Schema=" << source->getSourceSchemaAsString());
    }
    for (auto sink : sinks) {
        NES_INFO("Sink:" << sink);
        NES_INFO("\t Generated Buffers=" << sink->getNumberOfWrittenOutBuffers());
        NES_INFO("\t Generated Tuples=" << sink->getNumberOfWrittenOutTuples());
    }
}

}// namespace NES

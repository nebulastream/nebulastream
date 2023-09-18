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
#include <Runtime/BufferManager.hpp>
#include <Sinks/Mediums/MaterializedViewSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Views/MaterializedView.hpp>
namespace NES::Experimental::MaterializedView {

MaterializedViewSink::MaterializedViewSink(MaterializedViewPtr view,
                                           SinkFormatPtr format,
                                           Runtime::NodeEnginePtr nodeEngine,
                                           uint32_t numOfProducers,
                                           QueryId queryId,
                                           QuerySubPlanId parentPlanId,
                                           FaultToleranceType faultToleranceType,
                                           uint64_t numberOfOrigins)
    : SinkMedium(std::move(format),
                 std::move(nodeEngine),
                 numOfProducers,
                 queryId,
                 parentPlanId,
                 faultToleranceType,
                 numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)),
      view(std::move(view)) {}

// It is somehow required to clear the view at the sink shutdown due to NES's memory management
void MaterializedViewSink::shutdown() { view->clear(); }

void MaterializedViewSink::setup(){};

bool MaterializedViewSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    NES_INFO("MaterializedViewSink::writeData");
    bool ret = view->writeData(inputBuffer);
    ++sentBuffer;
    updateWatermarkCallback(inputBuffer);
    return ret;
}

std::string MaterializedViewSink::toString() const {
    std::stringstream ss;
    ss << "MATERIALIZED_VIEW_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

SinkMediumTypes MaterializedViewSink::getSinkMediumType() { return SinkMediumTypes::MATERIALIZED_VIEW_SINK; }

uint64_t MaterializedViewSink::getViewId() const { return view->getId(); }
}// namespace NES::Experimental::MaterializedView
/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Sinks/Mediums/SinkMedium.hpp>

namespace NES::Experimental {

MaterializedViewSink::MaterializedViewSink(SinkFormatPtr format,
                                           QuerySubPlanId parentPlanId,
                                           Runtime::BufferManagerPtr bufferManager,
                                           MaterializedViewPtr mView)
    : SinkMedium(std::move(format), parentPlanId), localBufferManager(std::move(bufferManager)), mView(mView) {}

MaterializedViewSink::~MaterializedViewSink() = default;

SinkMediumTypes MaterializedViewSink::getSinkMediumType() { return MATERIALIZED_VIEW_SINK; }

bool MaterializedViewSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    NES_DEBUG("MaterializedViewSink::getData: write data");
    auto dataBuffers = sinkFormat->getData(inputBuffer);
    auto bufferSize = localBufferManager.get()->getBufferSize();

    for (auto buffer : dataBuffers) {
        NES_DEBUG("MaterializedViewSink::getData: write buffer of size " << buffer.getNumberOfTuples());
        if (mView.get()->getCurrentWritePosInBytes() + bufferSize > mView.get()->getMemAreaSize()) {
            NES_DEBUG("MaterializedViewSource::receiveData: reset buffer to 0");
            mView.get()->setCurrentWritePosInBytes(0);
        }
        memcpy(mView.get()->getMemArea() + mView.get()->getCurrentWritePosInBytes(), buffer.getBuffer(), bufferSize);
        mView.get()->setCurrentWritePosInBytes(mView.get()->getCurrentWritePosInBytes() + bufferSize);
    }
    return true;
}

std::string MaterializedViewSink::toString() const {
    std::stringstream ss;
    ss << "MATERIALIZED_VIEW_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void MaterializedViewSink::setup() {
    // currently not required
}
void MaterializedViewSink::shutdown() {
    // currently not required
}
}// namespace NES::Experimental
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

#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/SenseSource.hpp>
#include <Util/Logger.hpp>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
using namespace std;

namespace NES {

SenseSource::SenseSource(SchemaPtr schema,
                         Runtime::BufferManagerPtr bufferManager,
                         Runtime::QueryManagerPtr queryManager,
                         std::string udfs,
                         OperatorId operatorId,
                         size_t numSourceLocalBuffers,
                         std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 numSourceLocalBuffers,
                 GatheringMode::FREQUENCY_MODE,
                 std::move(successors)),
      udfs(std::move(udfs)) {}

std::optional<Runtime::TupleBuffer> SenseSource::receiveData() {
    NES_DEBUG("SenseSource::receiveData called");
    auto buf = bufferManager->getBufferBlocking();
    fillBuffer(buf);
    NES_DEBUG("SenseSource::receiveData filled buffer with tuples=" << buf.getNumberOfTuples());
    return buf;
}

std::string SenseSource::toString() const {
    std::stringstream ss;
    ss << "SenseSource(SCHEMA(" << schema->toString() << "), UDFS=" << udfs << endl;
    return ss.str();
}

void SenseSource::fillBuffer(Runtime::TupleBuffer&) {}

SourceType SenseSource::getType() const { return SENSE_SOURCE; }

const string& SenseSource::getUdfs() const { return udfs; }

}// namespace NES

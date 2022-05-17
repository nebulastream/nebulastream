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

#include "Sources/TCPSource.hpp"

namespace NES {

TCPSource::TCPSource(SchemaPtr schema,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     const int& tcpSourceType,
                     OperatorId operatorId,
                     OriginId originId,
                     size_t numSourceLocalBuffers,
                     GatheringMode::Value gatheringMode,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors,
                     SourceDescriptor::InputFormat inputFormat)
    : DataSource(schema,
                 bufferManager,
                 queryManager,
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(executableSuccessors)) {
    NES_DEBUG("TCPSource::TCPSource " << this << ": Init TCPSource.");
}

std::string TCPSource::toString() const {
    std::stringstream ss;
    ss << "TCPSOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    return ss.str();
}

}// namespace NES

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

#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/BinarySource.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger.hpp>
#include <sstream>

namespace NES {

BinarySource::BinarySource(SchemaPtr schema,
                           NodeEngine::BufferManagerPtr bufferManager,
                           NodeEngine::QueryManagerPtr queryManager,
                           const std::string& file_path,
                           OperatorId operatorId,
                           OperatorId logicalSourceOperatorId,
                           size_t numSourceLocalBuffers,
                           GatheringMode gatheringMode,
                           std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(schema,
                 bufferManager,
                 queryManager,
                 operatorId,
                 logicalSourceOperatorId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 successors),
      input(std::ifstream(file_path.c_str())), file_path(file_path) {
    input.seekg(0, input.end);
    file_size = input.tellg();
    if (file_size == -1) {
        NES_FATAL_ERROR("ERROR: File " << file_path << " is corrupted");
    }
    input.seekg(0, input.beg);
    tuple_size = schema->getSchemaSizeInBytes();
}

std::optional<NodeEngine::TupleBuffer> BinarySource::receiveData() {
    auto buf = this->bufferManager->getBufferBlocking();
    fillBuffer(buf);
    return buf;
}

const std::string BinarySource::toString() const {
    std::stringstream ss;
    ss << "BINARY_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << file_path << ")";
    return ss.str();
}

void BinarySource::fillBuffer(NodeEngine::TupleBuffer& buf) {
    /** while(generated_tuples < num_tuples_to_process)
     * read <buf.buffer_size> bytes data from file into buffer
     * advance internal file pointer, if we reach the file end, set to file begin
     */

    if (input.tellg() == file_size) {
        input.seekg(0, input.beg);
    }
    uint64_t uint64_to_read = buf.getBufferSize() < (uint64_t) file_size ? buf.getBufferSize() : file_size;
    input.read(buf.getBuffer<char>(), uint64_to_read);
    uint64_t generated_tuples_this_pass = uint64_to_read / tuple_size;
    buf.setNumberOfTuples(generated_tuples_this_pass);

    generatedTuples += generated_tuples_this_pass;
    generatedBuffers++;
}
SourceType BinarySource::getType() const { return BINARY_SOURCE; }

const std::string& BinarySource::getFilePath() const { return file_path; }
}// namespace NES

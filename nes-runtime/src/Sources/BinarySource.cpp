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

#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/BinarySource.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
#include <utility>

namespace NES {

BinarySource::BinarySource(const SchemaPtr& schema,
                           Runtime::BufferManagerPtr bufferManager,
                           Runtime::QueryManagerPtr queryManager,
                           const std::string& pathToFile,
                           OperatorId operatorId,
                           OriginId originId,
                           StatisticId statisticId,
                           size_t numSourceLocalBuffers,
                           GatheringMode gatheringMode,
                           const std::string& physicalSourceName,
                           std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 statisticId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 physicalSourceName,
                 false,
                 false,
                 std::move(successors)),
      input(std::ifstream(pathToFile.c_str())), filePath(pathToFile) {
    if (!(input.is_open() && input.good())) {
        NES_THROW_RUNTIME_ERROR("Binary input file is not valid");
    }
    input.seekg(0, std::ifstream::end);
    fileSize = input.tellg();
    if (fileSize < 0) {
        NES_FATAL_ERROR("ERROR: File {} is corrupted", pathToFile);
    }
    input.seekg(0, std::ifstream::beg);
    tupleSize = schema->getSchemaSizeInBytes();
}

std::optional<Runtime::TupleBuffer> BinarySource::receiveData() {
    auto buf = this->bufferManager->getBufferBlocking();
    fillBuffer(buf);
    return buf;
}

std::string BinarySource::toString() const {
    std::stringstream ss;
    ss << "BINARY_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << ")";
    return ss.str();
}

void BinarySource::fillBuffer(Runtime::TupleBuffer& buf) {
    /** while(generated_tuples < num_tuples_to_process)
     * read <buf.buffer_size> bytes data from file into buffer
     * advance internal file pointer, if we reach the file end, set to file begin
     */

    // 'std::streamoff' (aka 'long') and 'size_t' (aka 'unsigned long')
    if (input.tellg() > 0 && (unsigned) input.tellg() == fileSize) {
        input.seekg(0, std::ifstream::beg);
    }
    uint64_t uint64_to_read = buf.getBufferSize() < (uint64_t) fileSize ? buf.getBufferSize() : fileSize;
    input.read(buf.getBuffer<char>(), uint64_to_read);
    uint64_t generated_tuples_this_pass = uint64_to_read / tupleSize;
    buf.setNumberOfTuples(generated_tuples_this_pass);

    generatedTuples += generated_tuples_this_pass;
    generatedBuffers++;
}
SourceType BinarySource::getType() const { return SourceType::BINARY_SOURCE; }

const std::string& BinarySource::getFilePath() const { return filePath; }
}// namespace NES

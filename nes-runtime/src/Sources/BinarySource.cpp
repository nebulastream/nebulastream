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
#include <filesystem>

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
                           std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                           bool shouldDelayEOS,
                           uint64_t)
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
                 shouldDelayEOS,
                 std::move(successors)), filePath(pathToFile) {
    this->numberOfBuffersToProduce = 0;
}

void BinarySource::openFile() {
    while (!std::filesystem::exists(filePath)) {
        // NES_DEBUG("File {} does not exist yet", filePath);
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }

    if (generatedBuffers > 0) {
        return;
    }

    if (filePath.find("recreation_file_node_0_completed") != std::string::npos) {
        auto startTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        NES_ERROR("started sending from {}", startTime)
    }

    input = std::ifstream(filePath.c_str());
    if (!(input.is_open() && input.good())) {
        NES_THROW_RUNTIME_ERROR("Binary input file is not valid");
    }
    input.seekg(0, std::ifstream::end);
    fileSize = input.tellg();
    if (fileSize < 0) {
        NES_FATAL_ERROR("ERROR: File {} is corrupted", filePath);
    }
    input.seekg(0, std::ifstream::beg);
    tupleSize = schema->getSchemaSizeInBytes();
}

std::optional<Runtime::TupleBuffer> BinarySource::receiveData() {
    openFile();
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
    uint64_t size;
    uint64_t numberOfTuples;
    uint64_t seqNumber;
    uint64_t watermark;
    input.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    input.read(reinterpret_cast<char*>(&numberOfTuples), sizeof(uint64_t));
    input.read(reinterpret_cast<char*>(&seqNumber), sizeof(uint64_t));
    input.read(reinterpret_cast<char*>(&watermark), sizeof(uint64_t));

    uint64_t uint64_to_read = buf.getBufferSize();
    input.read(buf.getBuffer<char>(), uint64_to_read);
    // uint64_t generated_tuples_this_pass = uint64_to_read / tupleSize;
    // buf.setNumberOfTuples(generated_tuples_this_pass);
    buf.setNumberOfTuples(numberOfTuples);
    buf.setSequenceNumber(seqNumber);
    buf.setWatermark(watermark);
    // generatedTuples += generated_tuples_this_pass;
    generatedBuffers++;
}
SourceType BinarySource::getType() const { return SourceType::BINARY_SOURCE; }

const std::string& BinarySource::getFilePath() const { return filePath; }
}// namespace NES

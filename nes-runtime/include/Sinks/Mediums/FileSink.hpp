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

#ifndef NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_FILESINK_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_FILESINK_HPP_

#include <Sinks/Mediums/SinkMedium.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <Sequencing/NonBlockingMonotonicSeqQueue.hpp>
#include <folly/Synchronized.h>

namespace NES {

struct Record {
    uint64_t id;
    uint64_t value;
    uint64_t ingestionTimestamp;
    uint64_t processingTimestamp;
    uint64_t outputTimestamp;
};

/**
 * @brief The file sink writes the stream result to a text file, in CSV or JSON format.
 */
class FileSink : public SinkMedium {
  public:
    /**
     * @brief Create a file sink.
     * @param nodeEngine The node engine of the worker.
     * @param numOfProducers ?
     * @param filePath Name of the file to which the stream is written.
     * @param append True, if the stream should be appended to an existing file. If false, an existing file is first removed.
     * @param sharedQueryId ?
     * @param decomposedQueryId ?
     * @param numberOfOrigins number of origins of a given query
     */
    explicit FileSink(SinkFormatPtr format,
                      Runtime::NodeEnginePtr nodeEngine,
                      uint32_t numOfProducers,
                      const std::string& filePath,
                      bool append,
                      SharedQueryId sharedQueryId,
                      DecomposedQueryId decomposedQueryId,
                      DecomposedQueryPlanVersion decomposedQueryVersion,
                      uint64_t numberOfOrigins = 1, bool timestampAndWriteToSocket = true);

    /**
     * @brief Setup the file sink.
     *
     * This method attempts to open the file. If the file exists, it is first removed, unless append is true.
     * If the file cannot be opened, subsequent calls to writeData will fail.
     */
    void setup() override;

    /**
     * @brief Clean up the file sink.
     *
     * This method closes the file.
     */
    void shutdown() override;

    /** 
     * @brief Write the contents of a tuple buffer to the file sink.
     * @param inputBuffer The tuple buffer that should be written to the file sink.
     * @return True, if the contents of the tuple buffer could be written completely to the file sink.
     */
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;

    /**
     * @brief Return a string representation of the file sink.
     * @return A string describing the file sink.
     */
    std::string toString() const override;

    std::string getFilePath() const;

    /**
    * @brief Return the type of the sink medium.
    * @return SinkMediumTypes::FILE_SINK indicating that this is a file sink.
    */
    SinkMediumTypes getSinkMediumType() override;

    /**
     * @brief method to write a TupleBuffer to a local file system file
     * @param a tuple buffers pointer
     * @return bool indicating if the write was complete
     */
    bool writeDataToFile(Runtime::TupleBuffer& inputBuffer);
    bool writeDataToTCP(std::vector<std::vector<Record>>& buffersToWrite);
  protected:

    bool writeDataToTCP(std::vector<Runtime::TupleBuffer>& buffersToWrite);

    /// The output file path of the file sink.
    std::string filePath;

    /// The output file stream.
    std::ofstream outputFile;

    /// Indicate if the output should be appended to an existing file.
    bool append{false};

    /// Indicate if the file could be opened during setup.
    bool isOpen{false};
    std::vector<std::basic_string<char>> receivedBuffers;
    std::vector<uint64_t> arrivalTimestamps;
//    int sockfd;
    bool timestampAndWriteToSocket;
    std::atomic<uint64_t> numberOfWrittenBuffers{0};
    std::atomic<uint64_t> numberOfReceivedBuffers{0};
    folly::Synchronized<std::map<uint64_t, Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>>> seqQueueMap;
    // keep unordered tuple buffers with sequence number as key
//    folly::Synchronized<std::map<uint64_t, std::map<uint64_t, Runtime::TupleBuffer>>> bufferStorage;
    folly::Synchronized<std::map<uint64_t, std::map<uint64_t, std::vector<Record>>>> bufferStorage;
};
using FileSinkPtr = std::shared_ptr<FileSink>;
}// namespace NES

#endif// NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_FILESINK_HPP_

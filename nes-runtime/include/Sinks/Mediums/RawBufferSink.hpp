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
#ifndef NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_RAWBUFFERSINK_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_RAWBUFFERSINK_HPP_

#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sequencing/NonBlockingMonotonicSeqQueue.hpp>
#include <cstdint>
#include <memory>
#include <string>

namespace NES {

/*
Sink to write raw tuple buffers to the file in format:
buffer size | number of tuples in buffer | buffer content
uint_64 | uint_64 | buffer content of buffer size
*/
class RawBufferSink : public SinkMedium {
  public:
    /**
     * @brief Create a file sink in migrate format.
     * @param nodeEngine The node engine of the worker.
     * @param numOfProducers number of producers
     * @param filePath Name of the file to which the stream is written.
     * @param append True, if the stream should be appended to an existing file. If false, an existing file is first removed.
     * @param sharedQueryId shared query plan id
     * @param decomposedQueryId decomposed query plan id
     * @param decomposedQueryVersion decomposed query plan version
     * @param numberOfOrigins number of origins of a given query
     */
    explicit RawBufferSink(Runtime::NodeEnginePtr nodeEngine,
                           uint32_t numOfProducers,
                           const std::string& filePath,
                           bool append,
                           SharedQueryId sharedQueryId,
                           DecomposedQueryId decomposedQueryId,
                           DecomposedQueryPlanVersion decomposedQueryVersion,
                           uint64_t numberOfOrigins = 1);

    ~RawBufferSink() override;

    /**
     * @brief Setup the file sink.
     * This method attempts to open the file. If the file exists, it is first removed, unless append is true.
     * If the file cannot be opened, subsequent calls to writeData will fail.
     */
    void setup() override;

    /**
     * @brief Clean up the file sink.
     * This method closes the file.
     */
    void shutdown() override;

    /**
     * @brief Write the contents of a tuple buffer to the file sink.
     * format: buffer size | number of tuples in buffer | sequence number | buffer content
     * uint64_t | uint64_t | uint64_t | buffer size bytes
     * @param inputBuffer The tuple buffer that should be written to the file sink.
     * @return True, if the contents of the tuple buffer could be written completely to the file sink.
     */
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;

    /**
     * @brief Return a string representation of the file sink.
     * @return A string describing the file sink.
     */
    std::string toString() const override;

    /**
    * @brief Return the type of the sink medium.
    * @return SinkMediumTypes::FILE_SINK indicating that this is a file sink.
    */
    SinkMediumTypes getSinkMediumType() override;

    bool writeToTheFile(Runtime::TupleBuffer& inputBuffer);

    bool writeBulkToFile(std::vector<Runtime::TupleBuffer>& buffers);

  protected:
    Sequencing::NonBlockingMonotonicSeqQueue<uint64_t> seqQueue;
    // keep unordered tuple buffers with sequence number as key
    folly::Synchronized<std::map<uint64_t, Runtime::TupleBuffer>> bufferStorage;

    // sequence number of last written tuple buffer
    std::atomic<uint64_t> lastWritten{0};
    std::mutex lastWrittenMtx;

    /// The output file path of the file sink.
    std::string filePath;

    /// The output file stream.
    std::ofstream outputFile;

    /// Indicate if the output should be appended to an existing file.
    bool append{false};

    /// Indicate if the file could be opened during setup.
    bool isOpen{false};

    uint64_t numberOfWrittenBuffers{0};
    std::atomic<uint64_t> numberOfReceivedBuffers{0};
    uint64_t isClosed{false};
};
}// namespace NES

#endif// NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_RAWBUFFERSINK_HPP_

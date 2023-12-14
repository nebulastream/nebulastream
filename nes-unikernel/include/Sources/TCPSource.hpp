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

#ifndef NES_CORE_INCLUDE_SOURCES_TCPSOURCE_HPP_
#define NES_CORE_INCLUDE_SOURCES_TCPSOURCE_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <SchemaBuffer.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sources/DataSource.hpp>
#include <Util/CircularBuffer.hpp>

namespace NES {

class Parser;
using ParserPtr = std::shared_ptr<Parser>;

/**
 * @brief source to receive data via TCP connection
 */
class TCPSource : public DataSource {

    template<typename Schema>
    using BufferType = SchemaBuffer<Schema, 8192>;

  public:
    /**
     * @brief constructor of a TCP Source
     * @param schema the schema of the data
     * @param bufferManager The BufferManager is responsible for: 1. Pooled Buffers: preallocated fixed-size buffers of memory that
     * must be reference counted 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly.
     * They are also subject to reference counting.
     * @param queryManager comes with functionality to manage the queries
     * @param tcpSourceType points at current TCPSourceType config object, look at same named file for info
     * @param operatorId represents a locally running query execution plan
     * @param originId represents an origin
     * @param numSourceLocalBuffers number of local source buffers
     * @param gatheringMode the gathering mode used
     * @param physicalSourceName the name and unique identifier of a physical source
     * @param executableSuccessors executable operators coming after this source
     */
    explicit TCPSource(TCPSourceTypePtr tcpSourceType,
                       OperatorId operatorId,
                       OriginId originId,
                       size_t numSourceLocalBuffers,
                       const std::string& physicalSourceName,
                       NES::Unikernel::UnikernelPipelineExecutionContext successor);

    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    template<typename Schema>
    std::optional<Runtime::TupleBuffer> receiveData() {
        NES_DEBUG("TCPSource  {}: receiveData ", this->toString());
        auto tupleBuffer = allocateBuffer<Schema>();
        NES_DEBUG("TCPSource buffer allocated ");
        try {
            do {
                if (!running) {
                    return std::nullopt;
                }
                fillBuffer(tupleBuffer);
            } while (tupleBuffer.getNumberOfTuples() == 0);
        } catch (const std::exception& e) {
            delete[] messageBuffer;
            NES_ERROR("TCPSource<Schema>::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
            throw e;
        }
        return tupleBuffer.getBuffer();
    }

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    template<typename Schema>
    bool fillBuffer(TCPSource::BufferType<Schema>& tupleBuffer) {

        // determine how many tuples fit into the buffer
        tuplesThisPass = tupleBuffer.getCapacity();
        NES_DEBUG("TCPSource<Schema>::fillBuffer: Fill buffer with #tuples= {}  of size= {}", tuplesThisPass, tupleSize);

        //init tuple count for buffer
        uint64_t tupleCount = 0;
        //init timer for flush interval
        auto flushIntervalTimerStart = std::chrono::system_clock::now();
        //init flush interval value
        bool flushIntervalPassed = false;
        //need this to indicate that the whole tuple was successfully received from the circular buffer
        bool popped = true;
        //init tuple size
        uint64_t inputTupleSize = 0;
        //init size of received data from socket with 0
        int64_t bufferSizeReceived = 0;
        //receive data until tupleBuffer capacity reached or flushIntervalPassed
        while (tupleCount < tuplesThisPass && !flushIntervalPassed) {
            //if circular buffer is not full obtain data from socket
            if (!circularBuffer.full()) {
                //create new buffer with size equal to free space in circular buffer
                messageBuffer = new char[circularBuffer.capacity() - circularBuffer.size()];
                //fill created buffer with data from socket. Socket returns the number of bytes it actually sent.
                //might send more than one tuple at a time, hence we need to extract one tuple below in switch case.
                //user needs to specify how to find out tuple size when creating TCPSource
                bufferSizeReceived = read(sockfd, messageBuffer, circularBuffer.capacity() - circularBuffer.size());
                //if read method returned -1 an error occurred during read.
                if (bufferSizeReceived == -1) {
                    delete[] messageBuffer;
                    NES_ERROR("TCPSource<Schema>::fillBuffer: an error occurred while reading from socket. Error: {}",
                              strerror(errno));
                    return false;
                }
                //if size of received data is not 0 (no data received), push received data to circular buffer
                else if (bufferSizeReceived != 0) {
                    NES_DEBUG("TCPSOURCE::fillBuffer: bytes send: {}.", bufferSizeReceived);
                    NES_DEBUG("TCPSOURCE::fillBuffer: print current buffer: {}.", messageBuffer);
                    //push the received data into the circularBuffer
                    circularBuffer.push(messageBuffer, bufferSizeReceived);
                }
                //delete allocated buffer
                delete[] messageBuffer;
            }

            if (!circularBuffer.empty()) {
                try {
                    // search the circularBuffer until Tuple seperator is found to obtain size of tuple
                    inputTupleSize = sizeUntilSearchToken(sourceConfig->getTupleSeparator()->getValue());
                    // allocate buffer with size of tuple
                    messageBuffer = new char[inputTupleSize];
                    NES_DEBUG("TCPSOURCE::fillBuffer: Pop Bytes from Circular Buffer to obtain Tuple of size: '{}'.",
                              inputTupleSize);
                    NES_DEBUG("TCPSOURCE::fillBuffer: current circular buffer size: '{}'.", circularBuffer.size());
                    //copy and delete tuple from circularBuffer, delete tuple separator
                    popped = popGivenNumberOfValues(inputTupleSize, true);
                } catch (const std::exception& e) {
                    NES_ERROR("Failed to obtain tupleSize searching for separator token. Error: {}", e.what());
                    throw e;
                }

                NES_TRACE("TCPSOURCE::fillBuffer: Successfully prepared tuples? '{}'", popped);
                //if we were able to obtain a complete tuple from the circular buffer, we are going to forward it ot the appropriate parser
                if (inputTupleSize != 0 && popped) {
                    std::string buf(messageBuffer, inputTupleSize);
                    NES_TRACE("TCPSOURCE::fillBuffer: Client consume message: '{}'.", buf);
                    parseCSVIntoBuffer(std::istringstream(buf), tupleBuffer);
                    tupleCount++;
                }
                delete[] messageBuffer;
            }
            // If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
            // and writing data exceeds the user defined limit (bufferFlushIntervalMs).
            // If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
            if ((sourceConfig->getFlushIntervalMS()->getValue() > 0
                 && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()
                                                                          - flushIntervalTimerStart)
                         .count()
                     >= sourceConfig->getFlushIntervalMS()->getValue())) {
                NES_DEBUG("TCPSource<Schema>::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current "
                          "TupleBuffer.");
                flushIntervalPassed = true;
            }
        }
        tupleBuffer.setNumberOfTuples(tupleCount);
        generatedTuples += tupleCount;
        generatedBuffers++;
        return true;
    }

    /**
     * @brief search from the back (first inputted item) to the front for the given search token
     * @token to search for
     * @return number of places until first occurrence of token (place of token not included)
     */
    uint64_t sizeUntilSearchToken(char token);

    /**
     * @brief pop number of values given and fill temp with popped values. If popTextDevider true, pop one more value and discard
     * @param numberOfValuesToPop number of values to pop and fill temp with
     * @param popTextDivider if true, pop one more value and discard, if false, only pop given number of values to pop
     * @return true if number of values to pop successfully popped, false otherwise
     */
    bool popGivenNumberOfValues(uint64_t numberOfValuesToPop, bool popTextDivider);

    /**
     * @brief override the toString method for the csv source
     * @return returns string describing the binary source
     */
    std::string toString() const override;

    /**
     * @brief Get source type
     * @return source type
     */
    SourceType getType() const override;

    /**
     * @brief getter for source config
     * @return tcpSourceType
     */
    const TCPSourceTypePtr& getSourceConfig() const;

    /**
     * @brief opens TCP connection
     */
    void open() override;

    /**
     * @brief closes TCP connection
     */
    void close() override;

  private:
    template<typename Schema>
    TCPSource::BufferType<Schema> allocateBuffer();
    int connection = -1;
    uint64_t tupleSize;
    uint64_t tuplesThisPass;
    TCPSourceTypePtr sourceConfig;
    int sockfd = -1;
    CircularBuffer<char> circularBuffer;
    char* messageBuffer;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_SOURCES_TCPSOURCE_HPP_

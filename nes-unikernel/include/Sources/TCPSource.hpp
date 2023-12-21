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
#include <arpa/inet.h>
#include <netinet/in.h>

namespace NES {

class Parser;
using ParserPtr = std::shared_ptr<Parser>;

/**
 * @brief source to receive data via TCP connection
 */
template<typename TCPConfig>
class TCPSource : public DataSource<TCPConfig> {

  public:
    constexpr static NES::SourceType SourceType = NES::SourceType::TCP_SOURCE;
    /**
     * @brief constructor of a TCP Source
     * must be reference counted 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly.
     * They are also subject to reference counting.
     * @param successor executable operators coming after this source
     */
    explicit TCPSource(NES::Unikernel::UnikernelPipelineExecutionContext successor)
        : DataSource<TCPConfig>(successor), circularBuffer(2048) {}

    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    std::optional<Runtime::TupleBuffer> receiveData() override {
        NES_DEBUG("TCPSource  {}: receiveData ", this->toString());
        auto tupleBuffer = DataSource<TCPConfig>::allocateBuffer();
        auto schemaBuffer = DataSource<TCPConfig>::BufferType::of(tupleBuffer);
        NES_DEBUG("TCPSource buffer allocated ");
        try {
            do {
                if (!DataSource<TCPConfig>::running) {
                    return std::nullopt;
                }
                fillBuffer(schemaBuffer);
            } while (schemaBuffer.getSize() == 0);
        } catch (const std::exception& e) {
            delete[] messageBuffer;
            NES_ERROR("TCPSource<Schema>::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
            throw e;
        }
        return tupleBuffer;
    }

    /**
     * @brief search from the back (first inputted item) to the front for the given search token
     * @token to search for
     * @return number of places until first occurrence of token (place of token not included)
     */
    uint64_t sizeUntilSearchToken(char token) {
        uint64_t places = 0;
        for (auto itr = circularBuffer.end() - 1; itr != circularBuffer.begin() - 1; --itr) {
            if (*itr == token) {
                return places;
            }
            ++places;
        }
        return places;
    }

    /**
     * @brief pop number of values given and fill temp with popped values. If popTextDevider true, pop one more value and discard
     * @param numberOfValuesToPop number of values to pop and fill temp with
     * @param popTextDivider if true, pop one more value and discard, if false, only pop given number of values to pop
     * @return true if number of values to pop successfully popped, false otherwise
     */
    bool popGivenNumberOfValues(uint64_t numberOfValuesToPop, bool popTextDivider) {
        if (circularBuffer.size() >= numberOfValuesToPop) {
            for (uint64_t i = 0; i < numberOfValuesToPop; ++i) {
                char popped = circularBuffer.pop();
                messageBuffer[i] = popped;
            }
            messageBuffer[numberOfValuesToPop] = '\0';
            if (popTextDivider) {
                circularBuffer.pop();
            }
            return true;
        }
        return false;
    }

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    bool fillBuffer(DataSource<TCPConfig>::BufferType& tupleBuffer) {

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
                    inputTupleSize = sizeUntilSearchToken('\n');
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

                NES_DEBUG("TCPSOURCE::fillBuffer: Successfully prepared tuples? '{}'", popped);
                //if we were able to obtain a complete tuple from the circular buffer, we are going to forward it ot the appropriate parser
                if (inputTupleSize != 0 && popped) {
                    std::string buf(messageBuffer, inputTupleSize);
                    auto iss = std::istringstream(buf);
                    NES_DEBUG("TCPSOURCE::fillBuffer: Client consume message: '{}'.", buf);
                    tupleCount += parseCSVIntoBuffer(iss, tupleBuffer);
                }
                delete[] messageBuffer;
            }
            // If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
            // and writing data exceeds the user defined limit (bufferFlushIntervalMs).
            // If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
            if (TCPConfig::BufferFlushInterval > 0ms
                && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()
                                                                         - flushIntervalTimerStart)
                    >= TCPConfig::BufferFlushInterval) {
                NES_DEBUG("TCPSource<Schema>::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current "
                          "TupleBuffer.");
                flushIntervalPassed = true;
            }
        }
        DataSource<TCPConfig>::generatedTuples += tupleCount;
        DataSource<TCPConfig>::generatedBuffers++;
        return true;
    }

    /**
     * @brief override the toString method for the csv source
     * @return returns string describing the binary source
     */
    std::string toString() const override {
        std::stringstream ss;
        ss << "TCPSOURCE(";
        ss << ")";
        return ss.str();
    }

    /**
     * @brief opens TCP connection
     */
    void open() override {
        DataSource<TCPConfig>::open();
        NES_TRACE("TCPSource::connected: Trying to create socket.");
        if (sockfd < 0) {
            sockfd = socket(AF_INET, SOCK_STREAM, 0);
            NES_TRACE("Socket created with  {}", sockfd);
        }
        if (sockfd < 0) {
            NES_ERROR("TCPSource::connected: Failed to create socket. Error: {}", strerror(errno));
            connection = -1;
            return;
        }
        NES_TRACE("Created socket");
        struct sockaddr_in servaddr;
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = inet_addr(TCPConfig::NodeIP);
        servaddr.sin_port = htons(TCPConfig::Port);// htons is necessary to convert a number to network byte order

        if (connection < 0) {
            NES_INFO("Try connecting to server: {}:{}", TCPConfig::NodeIP, TCPConfig::Port);
            connection = connect(sockfd, (struct sockaddr*) &servaddr, sizeof(servaddr));
        }
        if (connection < 0) {
            connection = -1;
            NES_THROW_RUNTIME_ERROR("TCPSource::connected: Connection with server failed. Error: " << strerror(errno));
        }
        NES_TRACE("TCPSource::connected: Connected to server.");
    }

    /**
     * @brief closes TCP connection
     */
    void close() override {
        NES_TRACE("TCPSource::close: trying to close connection.");
        DataSource<TCPConfig>::close();
        if (connection >= 0) {
            ::close(connection);
            ::close(sockfd);
            NES_TRACE("TCPSource::close: connection closed.");
        }
    }

  private:
    int connection = -1;
    uint64_t tupleSize = UINT_MAX;
    uint64_t tuplesThisPass;
    int sockfd = -1;
    CircularBuffer<char> circularBuffer;
    char* messageBuffer;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_SOURCES_TCPSOURCE_HPP_

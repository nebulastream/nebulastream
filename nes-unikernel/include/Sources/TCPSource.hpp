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
#include <SchemaBuffer.hpp>
#ifdef USE_MMAP_CIRCBUFFER
#include <Util/MMapCircularBuffer.hpp>
#else
#include <Util/CircularBuffer.hpp>
#endif
#include <arpa/inet.h>
#include <boost/iostreams/stream.hpp>
#include <netinet/in.h>

namespace NES {

class Parser;
using ParserPtr = std::shared_ptr<Parser>;

/**
 * @brief source to receive data via TCP connection
 */
template<typename TCPConfig>
class TCPSource {

  public:
    using BufferType = NES::Unikernel::SchemaBuffer<typename TCPConfig::Schema, 8192>;
    constexpr static NES::SourceType SourceType = NES::SourceType::TCP_SOURCE;
    constexpr static bool NeedsBuffer = true;

    /**
     * @brief constructor of a TCP Source
     * must be reference counted 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly.
     * They are also subject to reference counting.
     * @param successor executable operators coming after this source
     */
    explicit TCPSource() : circularBuffer(getpagesize() * 8) {}

    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    std::optional<NES::Runtime::TupleBuffer> receiveData(const std::stop_token& stoken, BufferType schemaBuffer) {
        try {
            if (stoken.stop_requested()) {
                return std::nullopt;
            }
            if (!fillBuffer(schemaBuffer, stoken)) {
                return std::nullopt;
            }
        } catch (const std::exception& e) {
            NES_ERROR("TCPSource<Schema>::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
            throw e;
        }
        return schemaBuffer.getBuffer();
    }

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    bool fillBuffer(BufferType& tupleBuffer, std::stop_token stoken) {

        // determine how many tuples fit into the buffer
        tuplesThisPass = tupleBuffer.getCapacity();
        NES_TRACE("TCPSource<Schema>::fillBuffer: Fill buffer with #tuples= {}  of size= {}", tuplesThisPass, tupleSize);
        //init timer for flush interval
        auto flushIntervalTimerStart = std::chrono::system_clock::now();
        //init flush interval value
        bool flushIntervalPassed = false;
        //need this to indicate that the whole tuple was successfully received from the circular buffer
        //init size of received data from socket with 0
        int64_t bufferSizeReceived = 0;
        //receive data until tupleBuffer capacity reached or flushIntervalPassed
        while (!tupleBuffer.isFull() && !(flushIntervalPassed || stoken.stop_requested())) {
            //if circular buffer is not full obtain data from socket
            if (!circularBuffer.full()) {
                //fill created buffer with data from socket. Socket returns the number of bytes it actually sent.
                //might send more than one tuple at a time, hence we need to extract one tuple below in switch case.
                //user needs to specify how to find out tuple size when creating TCPSource
                auto bufferReservation = circularBuffer.reserveDataForWrite(circularBuffer.capacity());
                bufferSizeReceived = read(sockfd, bufferReservation.data(), bufferReservation.size());
                //if read method returned -1 an error occurred during read.
                if (bufferSizeReceived == -1) {
                    NES_ERROR("TCPSource<Schema>::fillBuffer: an error occurred while reading from socket. Error: {}",
                              strerror(errno));
                    circularBuffer.returnMemoryForWrite(bufferReservation, 0);
                    return false;
                }

                if (bufferSizeReceived == 0) {
                    NES_ERROR("TCPSource<Schema>::fillBuffer: EOF");
                    circularBuffer.returnMemoryForWrite(bufferReservation, 0);
                    return false;
                }

                NES_TRACE("TCPSOURCE::fillBuffer: bytes send: {}.", bufferSizeReceived);
                circularBuffer.returnMemoryForWrite(bufferReservation, bufferSizeReceived);

                //if size of received data is not 0 (no data received), push received data to circular buffer
            }
#ifdef USE_MMAP_CIRCBUFFER
            auto csvData = circularBuffer.peekData(circularBuffer.capacity());
#else
            auto csvData = circularBuffer.peekData(std::span{backupBuffer.data(), backupBuffer.size()}, circularBuffer.capacity());
#endif
            auto dataLeft = Unikernel::parseCSVIntoBuffer(csvData, tupleBuffer);

#ifndef USE_MMAP_CIRCBUFFER
            // this means that the tuple did no fit into the backup buffer and the parser could not extract a complete tuple
            if (dataLeft.size() == backupBuffer.size()) {
                backupBuffer.resize(backupBuffer.size() * 2);
            }
#endif

            circularBuffer.popData(csvData.size() - dataLeft.size());

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
        return true;
    }

    /**
     * @brief override the toString method for the csv source
     * @return returns string describing the binary source
     */
    std::string toString() const {
        std::stringstream ss;
        ss << "TCPSOURCE(";
        ss << ")";
        return ss.str();
    }

    /**
     * @brief opens TCP connection
     */
    bool open() {
        NES_INFO("TCP source open");
        NES_TRACE("TCPSource::connected: Trying to create socket.");
        if (sockfd < 0) {
            sockfd = socket(AF_INET, SOCK_STREAM, 0);
            NES_TRACE("Socket created with  {}", sockfd);
        }
        if (sockfd < 0) {
            NES_ERROR("TCPSource::connected: Failed to create socket. Error: {}", strerror(errno));
            connection = -1;
            return false;
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
            NES_ERROR("TCPSource::connected: Connection with server failed. Error: ", strerror(errno));
            return false;
        }
        NES_TRACE("TCPSource::connected: Connected to server.");
        return true;
    }

    /**
     * @brief closes TCP connection
     */
    bool close(Runtime::QueryTerminationType /*type*/) {
        NES_TRACE("TCPSource::close: trying to close connection.");
        if (connection < 0) {
            NES_WARNING("Calling close on a non active tcp connection");
            return false;
        }

        if (::close(connection) < 0) {
            NES_WARNING("Could not close tcp connection");
        }

        if (::close(sockfd)) {
            NES_WARNING("Could not close tcp socket");
            return false;
        }

        NES_TRACE("TCPSource::close: connection closed.");
        return true;
    }

  private:
    int connection = -1;
    uint64_t tupleSize = UINT_MAX;
    uint64_t tuplesThisPass;
    int sockfd = -1;
#ifdef USE_MMAP_CIRCBUFFER
    MMapCircularBuffer circularBuffer;
#else
    CircularBuffer circularBuffer;
    std::vector<char> backupBuffer = std::vector<char>(100);
#endif
};
}// namespace NES
#endif// NES_CORE_INCLUDE_SOURCES_TCPSOURCE_HPP_

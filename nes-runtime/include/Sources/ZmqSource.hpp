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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_ZMQSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_ZMQSOURCE_HPP_

#include <Sources/DataSource.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>
namespace NES {
class TupleBuffer;
/**
 * @brief this class provide a zmq as data source
 */
class ZmqSource : public DataSource {

  public:
    /**
     * @brief constructor for the zmq source
     * @param schema schema of the data
     * @param bufferManager pointer to the buffer manager
     * @param queryManager pointer to the query manager
     * @param host host name of the source queue
     * @param port port of the source queue
     * @param operatorId current operator id
     * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
     * @param numSourceLocalBuffers the number of buffers allocated to a source
     * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
     * @param physicalSourceName the name and unique identifier of a physical source
     * @param successors the subsequent operators in the pipeline to which the data is pushed
     */
    explicit ZmqSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       const std::string& host,
                       uint16_t port,
                       OperatorId operatorId,
                       OriginId originId,
                       uint64_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       const std::string& physicalSourceName,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    /**
     * @brief destructor of zmq sink that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~ZmqSource() NES_NOEXCEPT(false) override;

    /**
     * @brief blocking method to receive a buffer from the zmq source
     * @return TupleBufferPtr containing thre received buffer
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the zmq source
     * @return returns string describing the zmq source
     */
    std::string toString() const override;

    /**
     * @brief The address address for the ZMQ
     * @return ZMQ address
     */
    std::string const& getHost() const;

    /**
     * @brief The port of the ZMQ
     * @return ZMQ port
     */
    uint16_t getPort() const;

    /**
     * @brief Get source type
     */
    SourceType getType() const override;

    bool stop(Runtime::QueryTerminationType graceful) override {
        disconnect();
        return DataSource::stop(graceful);
    }

  private:
    /**
     * @brief default constructor required for boost serialization
     */
    ZmqSource() = delete;

    /**
     * @brief method to connect zmq using the address and port specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connect();

    /**
     * @brief method to disconnect zmq
     * check if already disconnected, if not disconnected try to disconnect, if already disconnected return
     * @return bool indicating if connection could be established
     */
    bool disconnect();

    /**
     * @brief method for serialization, all listed variable below are added to the
     * serialization/deserialization process
     */
    friend class DataSource;
    std::string host;
    uint16_t port;
    bool connected;
    zmq::context_t context;
    zmq::socket_t socket;
};

using ZmqSourcePtr = std::shared_ptr<ZmqSource>;
}// namespace NES
#endif // NES_RUNTIME_INCLUDE_SOURCES_ZMQSOURCE_HPP_

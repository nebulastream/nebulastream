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

#ifndef NES_RUNTIME_INCLUDE_NETWORK_EXCHANGEPROTOCOL_HPP_
#define NES_RUNTIME_INCLUDE_NETWORK_EXCHANGEPROTOCOL_HPP_
#include <Network/NetworkMessage.hpp>
#include <Sequencing/NonBlockingMonotonicSeqQueue.hpp>
#include <folly/Synchronized.h>
#include <map>
#include <variant>

namespace NES::Runtime {
class BaseEvent;
}
namespace NES::Network {
class PartitionManager;
class ExchangeProtocolListener;
/**
 * @brief This class is used by the ZmqServer and defines the reaction for events onDataBuffer,
 * clientAnnouncement, endOfStream and exceptionHandling between all nodes of NES.
 */
class ExchangeProtocol {
  public:
    /**
     * @brief Create an exchange protocol object with a partition manager and a listener
     * @param partitionManager
     * @param listener
     */
    explicit ExchangeProtocol(uint64_t numbOfBuffers,
                              std::shared_ptr<PartitionManager> partitionManager,
                              std::shared_ptr<ExchangeProtocolListener> listener);

    /**
     * @brief Copy-Constructor for ExchangeProtocol
     * @param other
     */
    ExchangeProtocol(const ExchangeProtocol& other);

    /**
     * @brief Reaction of the zmqServer after a ClientAnnounceMessage is received.
     * @param clientAnnounceMessage
     * @return if successful, return ServerReadyMessage
     */
    std::variant<Messages::ServerReadyMessage, Messages::ErrorMessage> onClientAnnouncement(Messages::ClientAnnounceMessage msg);

    /**
     * @brief Reaction of the zmqServer after a buffer is received.
     * @param id of the buffer
     * @param buffer content
     * @param messageSequenceData
     */
    void onBuffer(NesPartition nesPartition, Runtime::TupleBuffer& buffer, SequenceData messageSequenceData);

    /**
     * @brief Reaction of the zmqServer after an error occurs.
     * @param the error message
     */
    void onServerError(Messages::ErrorMessage error);

    /**
     * @brief Reaction of the zmqServer after an error occurs.
     * @param the error message
     */
    void onChannelError(Messages::ErrorMessage error);

    /**
     * @brief Reaction of the zmqServer after an EndOfStream message is received.
     * @param endOfStreamMessage a message containing details about the type of EoS
     * @param marker in case of reconfiguration: a marker containing a set of reconfiguration events
     */
    void onEndOfStream(Messages::EndOfStreamMessage endOfStreamMessage,
                       std::optional<ReconfigurationMarkerPtr> marker = std::nullopt);

    /**
     * @brief This method is called when the server receives an event message.
     * @param nesPartition
     * @param event
     */
    void onEvent(NesPartition nesPartition, Runtime::BaseEvent& event);

    /**
     * @brief getter for the PartitionManager
     * @return the PartitionManager
     */
    [[nodiscard]] std::shared_ptr<PartitionManager> getPartitionManager() const;

  private:
    uint64_t numbOfBuffers{0};
    std::shared_ptr<PartitionManager> partitionManager{nullptr};
    std::shared_ptr<ExchangeProtocolListener> protocolListener{nullptr};
    folly::Synchronized<std::unordered_map<NesPartition, Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>>>
        maxSeqNumberPerNesPartition;
};

}// namespace NES::Network

#endif// NES_RUNTIME_INCLUDE_NETWORK_EXCHANGEPROTOCOL_HPP_

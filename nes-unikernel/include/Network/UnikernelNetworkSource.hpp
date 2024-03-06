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
#ifndef NES_NETWORKSOURCE_HPP
#define NES_NETWORKSOURCE_HPP

#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Runtime/Events.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/Execution/UnikernelPipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SchemaBuffer.hpp>
#include <Sources/DataSource.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <folly/MPMCQueue.h>
#include <variant>

extern NES::Network::NetworkManagerPtr TheNetworkManager;
namespace NES::Unikernel {
class UnikernelPipelineExecutionContext;
}

namespace NES::Runtime {
class BaseEvent;
}

namespace NES::Unikernel {

class NetworkSource;

class TupleBufferHolder {
  public:
    /**
         * @brief default constructor
    */
    TupleBufferHolder() = default;

    /**
         * @brief constructor via an reference to the buffer to be hold
         * @param ref
    */
    TupleBufferHolder(const Runtime::TupleBuffer& ref) : bufferToHold(ref) {}

    /**
     * @brief constructor via && reference to the buffer to be hold
     * @param ref
     */
    TupleBufferHolder(TupleBufferHolder&& rhs) noexcept : bufferToHold(std::move(rhs.bufferToHold)) {}

    /**
     * @brief equal sign operator via a reference
     * @param other
     * @return
     */
    TupleBufferHolder& operator=(const TupleBufferHolder& other) {
        bufferToHold = other.bufferToHold;
        return *this;
    }

    /**
     * @brief equal sign operator via a reference/reference
     * @param other
     * @return
     */
    TupleBufferHolder& operator=(TupleBufferHolder&& other) {
        bufferToHold = std::move(other.bufferToHold);
        return *this;
    }

    Runtime::TupleBuffer bufferToHold;
};

template<typename T>
concept NetworkSourceConfig = requires(T* t) {
    DataSourceConfig<T>;
    { T::QueueSize } -> std::same_as<const size_t&>;
    { T::QueryId } -> std::same_as<const size_t&>;
    { T::UpstreamPartitionId } -> std::same_as<const size_t&>;
    { T::UpstreamSubPartitionId } -> std::same_as<const size_t&>;
    { T::UpstreamNodeId } -> std::same_as<const size_t&>;
    { T::UpstreamNodeHostname } -> std::same_as<const char* const&>;
    { T::UpstreamNodePort } -> std::same_as<const size_t&>;
    requires(std::same_as<typename T::Schema, Schema<>>);
};

/**
 * @brief this class provide a zmq as data source
 */
template<NetworkSourceConfig Config>
class UnikernelNetworkSource : DataEmitter {
    // helper type for the visitor #4
    template<class... Ts>
    struct overloaded : Ts... {
        using Ts::operator()...;
    };
    // explicit deduction guide (not needed as of C++20)
    template<class... Ts>
    overloaded(Ts...) -> overloaded<Ts...>;

  public:
    constexpr static NES::SourceType SourceType = NES::SourceType::NETWORK_SOURCE;
    // Buffers are created at the ZMQServer
    constexpr static bool NeedsBuffer = false;
    using QueueItemType = std::variant<TupleBufferHolder, Runtime::EventPtr, Runtime::QueryTerminationType>;
    //MPSC Queue
    using Queue = typename folly::MPMCQueue<QueueItemType>;
    using BufferType = NES::Unikernel::SchemaBuffer<typename Config::Schema, 8192>;
    void emitWork(Runtime::TupleBuffer& buffer) override {
        NES_DEBUG("Network Source {} Received Data", Config::OperatorId);
        NES_DEBUG("Buffer origin: {}", buffer.getOriginId());
        queue.write(buffer);
    }
    void onEvent(Runtime::EventPtr event) override {
        NES_INFO("Network Source {} Received Event", Config::OperatorId);
        queue.write(event);
    }
    void onEndOfStream(Runtime::QueryTerminationType type) override {
        NES_INFO("Network Source {} Received EoS", Config::OperatorId);
        queue.write(type);
    }

    /**
     * @brief this method is just dummy and is replaced by the ZmqServer in the NetworkStack. Do not use!
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<NES::Runtime::TupleBuffer> receiveData(const std::stop_token& stoken, BufferType /*unused*/) {
        using namespace std::chrono_literals;
        QueueItemType bufferOrEvent;

        while (!(queue.tryReadUntil<std::chrono::high_resolution_clock>(std::chrono::high_resolution_clock::now() + 20ms,
                                                                        bufferOrEvent)
                 || stoken.stop_requested())) {
            //poll until a items arrives or stop was requested
        }

        if (stoken.stop_requested()) {
            return std::nullopt;
        }

        return std::visit(overloaded{[](TupleBufferHolder& buffer) -> std::optional<Runtime::TupleBuffer> {
                                         NES_TRACE("DataSource Thread: Buffer origin: {}", buffer.bufferToHold.getOriginId());
                                         return std::make_optional(buffer.bufferToHold);
                                     },
                                     [](Runtime::EventPtr& /*event*/) -> std::optional<Runtime::TupleBuffer> {
                                         NES_WARNING("Got Event can't really deal with it... Stopping Query");
                                         return std::nullopt;
                                     },
                                     [](Runtime::QueryTerminationType& type) -> std::optional<Runtime::TupleBuffer> {
                                         NES_INFO("Received Query Termination: {}", magic_enum::enum_name(type));
                                         return std::nullopt;
                                     }},
                          bufferOrEvent);
    }

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * It registers the source on the NetworkManager
     * @return true if registration on the network stack is successful
     */
    bool open() {
        auto channel = TheNetworkManager->registerSubpartitionEventProducer(sinkLocation,
                                                                            nesPartition,
                                                                            TheBufferManager,
                                                                            waitTime,
                                                                            retryTimes);
        if (!channel) {
            NES_ERROR("Could not register SubpartitionEventProducer!");
            return false;
        }

        TheWorkerContext->storeEventOnlyChannel(nesPartition.getOperatorId(), std::move(channel));

        if (!TheNetworkManager->registerSubpartitionConsumer(nesPartition, sinkLocation, this)) {
            NES_ERROR("Could not register SubpartitionConsumer!");
            return false;
        }

        return true;
    }

    /**
         * @brief This method is overridden here to prevent the NetworkSource to start a thread.
         * It de-registers the source on the NetworkManager
         * @return true if deregistration on the network stack is successful
         */
    bool close(Runtime::QueryTerminationType type) {
        return TheWorkerContext->releaseEventOnlyChannel(nesPartition.getOperatorId(), type);
    }

  private:
    Queue queue{Config::QueueSize};
    Network::NesPartition nesPartition{Config::QueryId,
                                       Config::OperatorId,
                                       Config::UpstreamPartitionId,
                                       Config::UpstreamSubPartitionId};
    Network::NodeLocation sinkLocation{Config::UpstreamNodeId, Config::UpstreamNodeHostname, Config::UpstreamNodePort};
    // for event channel
    std::chrono::milliseconds waitTime{2000};
    uint8_t retryTimes = 255;
};

}// namespace NES::Unikernel

#endif//NES_NETWORKSOURCE_HPP

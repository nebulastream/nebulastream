//
// Created by ls on 10.09.23.
//

#ifndef NES_NETWORKSOURCE_H
#define NES_NETWORKSOURCE_H


#include <Network/NetworkForwardRefs.hpp>
#include <Network/NodeLocation.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/TupleBuffer.hpp>
#include "DataSource.hpp"
#include <Util/VirtualEnableSharedFromThis.hpp>

namespace NES::Unikernel {
    class UnikernelPipelineExecutionContextBase;
}
namespace NES::Runtime {
    class BaseEvent;
}
namespace NES::Network {

/**
 * @brief this class provide a zmq as data source
 */
    class NetworkSource : public DataSource {

    public:
        /*
       * @param SchemaPtr
       * @param bufferManager
       * @param queryManager
       * @param networkManager
       * @param nesPartition
       * @param sinkLocation
       * @param numSourceLocalBuffers
       * @param waitTime
       * @param retryTimes
       * @param successors
       * @param physicalSourceName
       */
        NetworkSource(
                Runtime::BufferManagerPtr bufferManager,
                NES::Runtime::WorkerContext& workerContext,
                NetworkManagerPtr networkManager,
                NesPartition nesPartition,
                NodeLocation sinkLocation,
                size_t numSourceLocalBuffers,
                std::chrono::milliseconds waitTime,
                uint8_t retryTimes,
                NES::Unikernel::UnikernelPipelineExecutionContextBase *successors,
                const std::string &physicalSourceName = "defaultPhysicalSourceName");

        /**
         * @brief this method is just dummy and is replaced by the ZmqServer in the NetworkStack. Do not use!
         * @return TupleBufferPtr containing the received buffer
         */
        std::optional<Runtime::TupleBuffer> receiveData() override;

        /**
         * @brief override the toString method
         * @return returns string describing the network source
         */
        std::string toString() const override;

        /**
          * @brief This method is called once an event is triggered for the current source.
          * The event type is PropagateEpochEvent. Method passes epoch barrier further to network sink as a reconfiguration message.
          * @param event
          */
        void onEvent(Runtime::BaseEvent &event) override;

        /**
         * @brief Get source type
         */
        SourceType getType() const override;

        /**
         * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
         * It registers the source on the NetworkManager
         * @return true if registration on the network stack is successful
         */
        bool start() final;

        /**
         * @brief This method is overridden here to prevent the NetworkSource to start a thread.
         * It de-registers the source on the NetworkManager
         * @return true if deregistration on the network stack is successful
         */
        bool stop(Runtime::QueryTerminationType = Runtime::QueryTerminationType::Graceful) final;

        /**
         * @brief This method is overridden here to manage failures of NetworkSource.
         * It de-registers the source on the NetworkManager
         * @return true if deregistration on the network stack is successful
         */
        bool fail() final;

        /**
         * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
         * @param bufferManager
         * @param queryManager
         */
        static void runningRoutine(const Runtime::BufferManagerPtr &);

        /**
         * @brief This method is invoked when the source receives a reconfiguration message.
         * @param message the reconfiguration message
         * @param context thread context
         */
        void reconfigure(Runtime::ReconfigurationMessage &message, Runtime::WorkerContext &context);


        /**
         * @brief API method called upon receiving an event, send event further upstream via Network Channel.
         * @param event
         * @param workerContext
         */
        void onEvent(Runtime::BaseEvent &event, Runtime::WorkerContextRef workerContext) override;

        /**
         * @brief
         * @param terminationType
         */
        void onEndOfStream(Runtime::QueryTerminationType terminationType) override;

        bool bind();

        friend bool operator<(const NetworkSource &lhs, const NetworkSource &rhs) {
            return lhs.nesPartition < rhs.nesPartition;
        }

    private:
        NetworkManagerPtr networkManager;
        NesPartition nesPartition;
        NodeLocation sinkLocation;
        // for event channel
        const std::chrono::milliseconds waitTime;
        const uint8_t retryTimes;
    };

}// namespace NES::Network


#endif //NES_NETWORKSOURCE_H

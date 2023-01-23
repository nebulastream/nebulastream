//
// Created by Kasper Hjort Berthelsen on 10/11/2022.
//

#ifndef NES_LORAWANPROXYSOURCE_HPP
#define NES_LORAWANPROXYSOURCE_HPP
#include "Catalogs/Source/PhysicalSourceTypes/LoRaWANProxySourceType.hpp"
#include "EndDeviceProtocol.pb.h"
#include <Sources/DataSource.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <mqtt/async_client.h>
namespace NES {
class LoRaWANProxySource : public DataSource {
  public:
    LoRaWANProxySource(const SchemaPtr& schema,
                       const Runtime::BufferManagerPtr& bufferManager,
                       const Runtime::QueryManagerPtr& queryManager,
                       const LoRaWANProxySourceTypePtr& sourceConfig,
                       OperatorId operatorId,
                       OriginId originId,
                       size_t numSourceLocalBuffers,
                       GatheringMode::Value gatheringMode,
                       const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& executableSuccessors
                       );

    /**
     * @brief destructor of mqtt sink that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~LoRaWANProxySource() override;

    /**
     * @brief method to connect mqtt using the host and port specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connect();

    /**
     * @brief method to make sure mqtt is disconnected
     * check if already disconnected, if not disconnected try to disconnect, if already disconnected return
     * @return bool indicating if connection could be established
     */
    bool disconnect();

//    void open() override;
//    void close() override;
//    bool start() override;
//    bool stop(Runtime::QueryTerminationType graceful) override;
//    void runningRoutine() override;
    /**
     * @brief blocking method to receive a buffer from the LoRaWAN source
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;
    std::string toString() const override;
    SourceType getType() const override;

  private:
    LoRaWANProxySourceTypePtr sourceConfig;
    mqtt::async_client_ptr client;
    std::string user;
    std::string appId;
    std::vector<std::string> deviceEUIs;
    std::string topicBase;
    std::string topicAll;
    std::string topicReceive;
    std::string topicSend;

    std::unique_ptr<Parser> inputParser;

    bool sendQueries();

    //TODO: Should maybe be macro'ed in by DEBUG flag
    class debug_action_listener: virtual mqtt::iaction_listener{
        std::string name;
      public:
        explicit debug_action_listener(std::string  name);

      private:
        void on_failure(const mqtt::token& asyncActionToken) override;
        void on_success(const mqtt::token& asyncActionToken) override;
    };

//    class callback: virtual mqtt::callback {
//        void connected(const mqtt::string& string) override;
//        void connection_lost(const mqtt::string& string) override;
//        void message_arrived(mqtt::const_message_ptr ptr) override;
//        void delivery_complete(mqtt::delivery_token_ptr ptr) override;
//    };
};

}

#endif//NES_LORAWANPROXYSOURCE_HPP

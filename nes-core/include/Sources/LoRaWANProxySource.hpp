//
// Created by Kasper Hjort Berthelsen on 10/11/2022.
// Designed in a modular way to support multiple different network stacks.
// We define a base class of a network server and then two implementations - one for each network stack
// Network stacks are responsible for any communication with lorawan network servers. Often through mqtt, however this
// is not standard. Which is why the mqtt is hidden away in these implementation classes. This causes some code duplication.
// The only thing we assume applies to all network stacks is that they have an address, a user and a password. They also
// need a list of deviceids and and application id. device and app ids are from the lorawan spec. I dont believe it requires
// usernames and passwords.
//

#ifndef NES_LORAWANPROXYSOURCE_HPP
#define NES_LORAWANPROXYSOURCE_HPP
#include "EndDeviceProtocol.pb.h"
#include <Catalogs/Source/PhysicalSourceTypes/LoRaWANProxySourceType.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <mqtt/client.h>
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
                       GatheringMode gatheringMode,
                       const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& executableSuccessors);

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
    const LoRaWANProxySourceTypePtr& getSourceConfig() const;

  private:
    //Classes to handle network communication
    class NetworkServer {
      public:
        NetworkServer(const std::string& url,
                      const std::string& user,
                      const std::string& password,
                      const std::string& appId,
                      const std::vector<std::string>& deviceEUIs);
        virtual ~NetworkServer() = default;
        virtual bool connect() = 0;
        virtual bool isConnected() = 0;
        virtual bool disconnect() = 0;
        virtual EndDeviceProtocol::Output receiveData() = 0;
        virtual bool sendMessage(EndDeviceProtocol::Message) = 0;
        virtual std::string toString() = 0;

      protected:
        const std::string url;
        const std::string user;
        const std::string password;
        const std::string appId;
        const std::vector<std::string> deviceEUIs;
    };
    class ChirpStackServer : public NetworkServer {
      private:
        mqtt::client client;

        std::string topicBase;
        std::string topicAll;
        std::string topicDevice;
        std::string topicReceiveSuffix;
        std::string topicSendSuffix;
        std::string topicAllDevicesReceive;
        enum class ChirpStackEvent { UP, STATUS, JOIN, ACK, TXACK, LOG, LOCATION, INTEGRATION };
        ChirpStackEvent strToEvent(const std::string& s);

      public:
        ChirpStackServer(const std::string& url,
                         const std::string& user,
                         const std::string& password,
                         const std::string& appId,
                         const std::vector<std::string>& deviceEUIs);
        ~ChirpStackServer() override;
        bool connect() override;
        bool isConnected() override;
        bool disconnect() override;
        EndDeviceProtocol::Output receiveData() override;
        bool sendMessage(EndDeviceProtocol::Message) override;
        std::string toString() override;
    };

    class TheThingsNetworkServer : public NetworkServer {
        mqtt::client client;

        std::string topicBase;
        std::string topicAll;
        std::string topicDevice;
        std::string topicReceiveSuffix;
        std::string topicSendSuffix;
        std::string topicAllDevicesReceive;

      public:
        TheThingsNetworkServer(const std::string& url,
                               const std::string& user,
                               const std::string& password,
                               const std::string& appId,
                               const std::vector<std::string>& deviceEUIs);
        ~TheThingsNetworkServer() override;

        bool connect() override;
        bool isConnected() override;
        bool disconnect() override;
        EndDeviceProtocol::Output receiveData() override;
        bool sendMessage(EndDeviceProtocol::Message) override;
        std::string toString() override;
    };

    LoRaWANProxySourceTypePtr sourceConfig;
    //    std::string url;
    //    std::string user;
    //    std::string appId;
    //    std::vector<std::string> deviceEUIs;
    //    std::string topicBase;
    //    std::string topicAll;
    //    std::string topicDevice;
    //    std::string topicReceiveSuffix;
    //    std::string topicSendSuffix;
    //    std::string topicAllDevicesReceive;
    //    std::string capath;
    //    std::string certpath;
    //    std::string keypath;

    NetworkServer& server;

    std::unique_ptr<Parser> inputParser;
    std::vector<QueryId> runningQueries;
    bool sendQueries();

    //TODO: Should maybe be macro'ed in by DEBUG flag
    class debug_action_listener : virtual mqtt::iaction_listener {
        std::string name;

      public:
        explicit debug_action_listener(std::string name);

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

}// namespace NES

#endif//NES_LORAWANPROXYSOURCE_HPP

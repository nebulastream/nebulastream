//#ifndef OPCSOURCE_HPP
//#define OPCSOURCE_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>

#include <Sources/DataSource.hpp>

namespace NES {
class TupleBuffer;
/**
 * @brief this class provide a zmq as data source
 */
class OPCSource : public DataSource {

  public:
    /**
     * @brief constructor for the zmq source
     * @param schema of the input buffer
     * @param host name of the source queue
     * @param port of the source queue
     */

    OPCSource(SchemaPtr schema,
        BufferManagerPtr bufferManager,
        QueryManagerPtr queryManager,
        const std::string & url,
        UA_UInt16 nsIndex,
        const std::string& nsId,
        const std::string& user,
        const std::string& password);
    
    //ToDo: Implement historical read.
    
    /*OPCSource(SchemaPtr schema,
        BufferManagerPtr bufferManager,
        QueryManagerPtr queryManager,
        const std::string& url,
        UA_Int64 startTime);

    OPCSource(SchemaPtr schema,
        BufferManagerPtr bufferManager,
        QueryManagerPtr queryManager,
        const std::string& url,
        const std::string& user,
        const std::string& password,
        UA_Int64 startTime);*/
    /**
     * @brief destructor of OPC sink that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~OPCSource();

    /**
     * @brief blocking method to receive a buffer from the OPC source
     * @return TupleBufferPtr containing thre received buffer
     */
    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the zmq source
     * @return returns string describing the zmq source
     */
    const std::string toString() const override;


    /**
     * @brief The port of the ZMQ
     * @return ZMQ port
     */
    const std::string& getUrl() const;

    UA_UInt16 getNsIndex() const;

    const std::string& getNsId() const;

    const std::string& getUser() const;

    const std::string& getPassword() const;


    /**
     * @brief Get source type
     */
    SourceType getType() const override;

  private:
    /**
     * @brief default constructor required for boost serialization
     */
    OPCSource() = default;

    /**
     * @brief method to connect zmq using the host and port specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connect();

    bool disconnect();

    /**
     * @brief method for serialization, all listed variable below are added to the
     * serialization/deserialization process
     */
    friend class DataSource;
    const std::string& url;
    UA_UInt16 nsIndex;
    const std::string& nsId;
    const std::string& user;
    const std::string& password;
    UA_StatusCode retval;
    UA_Client *client;
    bool connected;

};

typedef std::shared_ptr<OPCSource> OPCSourcePtr;
}// namespace NES

//#endif// OPCSOURCE_HPP

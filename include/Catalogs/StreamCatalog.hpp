/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef INCLUDE_CATALOGS_STREAMCATALOG_HPP_
#define INCLUDE_CATALOGS_STREAMCATALOG_HPP_

#include <API/Schema.hpp>
#include <Sources/DataSource.hpp>
#include <deque>
#include <map>
#include <string>
#include <vector>
//#include <Topology/NESTopologyEntry.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Persistence/StreamCatalogPersistence.hpp>
namespace NES {

class LogicalStream;
typedef std::shared_ptr<LogicalStream> LogicalStreamPtr;

/**
 * @brief the stream catalog handles the mapping of logical to physical streams
 * @Limitations
 *    - TODO: do we really want to identify streams by name or would it be better to introduce an id?
 *    - TODO: add mutex to make it secure
 *    - TODO: delete methods only delete catalog entries not the entries in the topology
 */
class StreamCatalog {
  public:
    /**
   * @brief method to add a logical stream
   * @param logical stream name
   * @param schema of logical stream as object
   * TODO: what to do if logical stream exists but the new one has a different schema
   * @return bool indicating if insert was successful
   */
    bool addLogicalStream(std::string logicalStreamName, SchemaPtr schemaPtr);

    /**
   * @brief method to add a logical stream
   * @param logical stream name
   * @param schema of logical stream as string
   * @return bool indicating if insert was successful
     */
    bool addLogicalStream(const std::string& streamName, const std::string& streamSchema);

    /**
   * @brief method to add a logical stream which was present in mismappedStreams
   * @param logical stream name
   * @param schema of logical stream as string
     */
    void addLogicalStreamFromMismappedStreams(std::string logicalStreamName, SchemaPtr schemaPtr);

    /**
       * @brief method to delete a logical stream
       * @caution this method only remove the entry from the catalog not from the topology
       * @param name of logical stream to delete
       * @param bool indicating the success of the removal
       */
    bool removeLogicalStream(std::string logicalStreamName);

    /**
     * @brief method to add a physical stream without a logical stream leading to a misconfigured physicalStream
     * @param newEntry
     * @return
     */
    bool addPhysicalStreamWithoutLogicalStreams(StreamCatalogEntryPtr newEntry);


    /**
     * @brief method to add to a logicalStream a physical stream
     * @param logicalStreamName name of the logical stream
     * @param newEntry (physicalstream)
     * @return true if the physical stream was successfully added to the logicalStream, false otherwise
     */
    bool addPhysicalStreamToLogicalStream(std::string logicalStreamName, StreamCatalogEntryPtr newEntry);

        /**
       * @brief method to add a physical stream
       * @caution combination of node and name has to be unique
       * @return true if the physical stream was added successfully to all logicalStreams named (also true im physical stream was added to mismapped
         * logical streams), false otherwise
       */
    bool addPhysicalStream(std::vector<std::string> logicalStreamNames, StreamCatalogEntryPtr entry);

    /**
   * @brief method to add a physical stream to a logical stream
   * @param physicalStreamName and logicalStreamName
   * @return bool indicating success of insert stream
   */
    bool addPhysicalStreamToLogicalStream(std::string physicalStreamName, std::string logicalStreamName);

    /**
     * @brief method to remove a physical stream to logical stream mapping
     * @param physicalStreamName
     * @param logicalStreamName
     * @return true if the physical stream was removed from the logical stream successfully, false otherwise
     */
    bool removePhysicalStreamFromLogicalStream(std::string physicalStreamName, std::string logicalStreamName);

    /**
   * @brief method to remove a physical stream from its logical streams and deleting it
   * @param name of the physical stream
   * @param hashId of the actor
   * @return bool indicating success of remove stream
   */
    bool unregisterPhysicalStream(std::string physicalStreamName, std::uint64_t hashId);

    /**
     * @brief method to remove a physical stream from its logical streams
     * @param hasId of the leaving node
     * @return bool indicating success of remove of physical stream
     */
    bool unregisterPhysicalStreamByHashId(uint64_t hashId);

    /**
    * @brief method to remove a physical stream from its logical stream in mismappedStreams mapping. If there are no elements left for that logicalStreamName that is also deleted from the mapping.
    * @param name of the physical stream, name of logical Stream
    * @return bool indicating success of remove of physical stream
    */
    bool removePhysicalToLogicalMappingFromMismappedStreams(std::string logicalStreamName, std::string physicalStreamName);

    /**
     * @brief method to delete all mismappings for a logical stream
     * @param logicalStreamName from which all mismappings to physical streams are to be deleted
     * @return bool indicating success of remove of physical stream
     */
    bool removeAllMismapped(std::string logicalStreamName);

    /**
   * @brief method to remove a physical stream from all logical streams
   * @param param of the node to be deleted
   * @return bool indicating success of remove stream
   */
   bool unregisterPhysicalStreamFromAllLogicalStreams(std::string physicalStreamName);

    /**
    * @brief method to return the stream for an existing logical stream
    * @param name of logical stream
    * @return smart pointer to a newly created stream
    * @note the stream will also contain the schema
    */
    SchemaPtr getSchemaForLogicalStream(std::string logicalStreamName);


    /**
    * @brief method to return the stream for an existing logical stream
    * @param name of logical stream
    * @return smart pointer to a newly created stream
    * @note the stream will also contain the schema
    */
    LogicalStreamPtr getStreamForLogicalStream(std::string logicalStreamName);

    /**
   * @brief method to return the stream for an existing logical stream or throw exception
   * @param name of logical stream
   * @return smart pointer to a newly created stream
   * @note the stream will also contain the schema
   */
    LogicalStreamPtr getStreamForLogicalStreamOrThrowException(std::string logicalStreamName);

    /**
     * @brief method to check which logical streams with given names exist in the log to schema mapping
     * @param logicalStreamNames names of the logical streams to test
     * @return a tuple of vectors, the first vector contains the logical streams included in the mapping and the second one the rest
     */
    std::tuple<std::vector<std::string>, std::vector<std::string>> testIfLogicalStreamVecExistsInSchemaMapping(std::vector<std::string> &logicalStreamNames);

    /**
   * @brief test if logical stream with this name exists in the log to schema mapping
   * @param name of the logical stream to test
   * @return bool indicating if stream exists
   */
    bool testIfLogicalStreamExistsInSchemaMapping(std::string logicalStreamName);

    /**
   * @brief test if logical stream with this name exists in the log to phy mapping
   * @param name of the logical stream to test
   * @return bool indicating if stream exists
   */
    bool testIfLogicalStreamExistsInLogicalToPhysicalMapping(std::string logicalStreamName);

    /**
    * @brief test if physical stream with this name exists in nameToPhysicalMapping
    * @param name of the physical stream to test
    * @return bool indicating if stream exists
    */
    bool testIfPhysicalStreamWithNameExists(std::string physicalStreamName);

    /**
    * @brief test if logical stream exists in mismappedStreams
    * @param name of the logical stream to test
    * @return bool indicating if stream exists
    */
    bool testIfLogicalStreamExistsInMismappedStreams(std::string logicalStreamName);

    /**
    * @brief test if in mismappedStreams there is a mapping of a specific physical to a specific logical stream.
    * @param name of the logical and physical streams to test.
    * @return bool indicating if mapping exists
    */
    bool testIfLogicalStreamToPhysicalStreamMappingExistsInMismappedStreams(std::string logicalStreamName, std::string physicalStreamName);

    /**
    * @brief return all physical nodes that contribute to this logical stream
    * @param name of logical stream
    * @return list of physical nodes as pointers into the topology
    */
    std::vector<TopologyNodePtr> getSourceNodesForLogicalStream(std::string logicalStreamName);

    /**
   * @brief reset the catalog and recreate the default_logical stream
   * @return bool indicating success
   */
    bool reset();

    /**
   * @brief Return a list of logical stream names registered at catalog
   * @return map containing stream name as key and schema object as value
   */
    std::map<std::string, SchemaPtr> getAllLogicalStream();

    /**
    * @brief Return a list of logical stream names registered at catalog for a specific physicalStream
    * @return map containing stream name as key and schema object as value
    */
    std::map<std::string, SchemaPtr> getAllLogicalStreamForPhysicalStream(std::string physicalStreamName);

    /**
    * @brief Return a list of logical stream names registered at catalog for a specific physicalStream as strings
    * @return map containing stream name as key and schema as string (empty if physical stream does not exists)
    */
    std::map<std::string, std::string> getAllLogicalStreamForPhysicalStreamAsString(std::string physicalStreamName);


    std::map<std::string, std::string> getAllLogicalStreamAsString();

    /**
       * @brief method to return the physical stream and the associated schemas
       * @return string containing the content of the catalog
       */
    std::string getPhysicalStreamAndSchemaAsString();

    /**
     * @brief get all physical streams
        * @return std::vector containing all StreamCatalogEntryPtr
    */
    std::vector<StreamCatalogEntryPtr> getPhysicalStreams();

    /**
     * @brief get all pyhsical streams for a logical stream
     * @param logicalStreamName
     * @return
     */
    std::vector<StreamCatalogEntryPtr> getPhysicalStreams(std::string logicalStreamName);

    /**
    * @brief gets the full mismappedStreams mapping
    * @return mapping
    */
    std::map<std::string, std::vector<std::string>> getMismappedPhysicalStreams();
    /**
    * @brief get all mismapped physical streams for a logical stream
    * @param logicalStreamName
    * @return vector of mismapped physical streams
    */
    std::vector<std::string> getMismappedPhysicalStreams(std::string logicalStreamName);

    /**
    * @brief get all StreamCatalogEntryPtr where the state is misconfigured
     * @return vector of misconfigured StreamCatalogEntryPtr
    */
    std::vector<StreamCatalogEntryPtr> getAllMisconfiguredPhysicalStreams();

    /**
     * @brief update an existing stream
     * @param streamName
     * @param streamSchema
     * @return
     */
    bool updatedLogicalStream(std::string& streamName, std::string& streamSchema);

    /**
     *
     * @param physicalStreamName
     * @return
     */
    std::string getSourceConfig(const std::string& physicalStreamName);

    /**
     *
     * @param physicalStreamName
     * @param sourceConfig
     * @return
     */
    std::string setSourceConfig(const std::string& physicalStreamName, const std::string& sourceConfig);


    /**
     * @brief removes from a misconfigured stream with a formerly duplicated name, the misconfigured flag and allows querying if possible
     * @param physicalStreamName the current name
     * @return true whether the validation was succesful
     */
    bool validatePhysicalStreamName(std::string physicalStreamName);

    StreamCatalog();
    StreamCatalog(StreamCatalogPersistencePtr persistence, WorkerRPCClientPtr workerRpcClient);
    ~StreamCatalog();

  private:
    std::recursive_mutex catalogMutex;

    WorkerRPCClientPtr workerRpcClient;
    StreamCatalogPersistencePtr persistence;

    //map logical stream to schema
    std::map<std::string, SchemaPtr> logicalStreamToSchemaMapping;

    //map logicalStreamName to vector of physicalStreamName
    std::map<std::string, std::vector<std::string>> logicalToPhysicalStreamMapping;

    // map physicalStreamName to StreamCatalogEntryPtr
    std::map<std::string, StreamCatalogEntryPtr> nameToPhysicalStream;

    // map logicalStreamName to vector of physicalStreamName
    std::map<std::string, std::vector<std::string>> mismappedStreams;

    void addDefaultStreams();
    bool setLogicalStreamsOnWorker(const std::string& physicalStreamName, const std::vector<std::string>& logicalStreamNames);
};
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;
}// namespace NES
#endif /* INCLUDE_CATALOGS_STREAMCATALOG_HPP_ */

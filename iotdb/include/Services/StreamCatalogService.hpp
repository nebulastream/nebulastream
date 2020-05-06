#ifndef NES_INCLUDE_SERVICES_STREAMCATALOGSERVICE_HPP_
#define NES_INCLUDE_SERVICES_STREAMCATALOGSERVICE_HPP_

#include <Catalogs/StreamCatalog.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

class StreamCatalogService;
typedef std::shared_ptr<StreamCatalogService> StreamCatalogServicePtr;

class StreamCatalogService {

  private:
    StreamCatalogService() = default;

  public:
    ~StreamCatalogService() = default;

    static StreamCatalogServicePtr getInstance() {
        static StreamCatalogServicePtr instance{new StreamCatalogService};
        return instance;
    }

    /**
     * @brief For adding new logical stream to the catalog
     */
    bool addNewLogicalStream(const std::string& streamName, const std::string& streamSchema);

    /**
     * @brief For updating the logical stream
     */
    bool updatedLogicalStream(std::string& streamName, std::string& streamSchema);

    /**
     * @brief Remove logical stream from the catalog
     */
    bool removeLogicalStream(const std::string& streamName);

    /**
     * @brief Returns a map containing name of all logical streams and corresponding schemas
     */
    std::map<std::string, SchemaPtr> getAllLogicalStream();

    /**
     * @brief get all physical streams registered for the given logical stream
     * @param logicalStreamName
     */
    vector<StreamCatalogEntryPtr> getAllPhysicalStream(const std::string logicalStreamName);

    /**
     * @brief Returns a map containing name of all logical streams and corresponding schemas as string
     * @return
     */
    std::map<std::string, std::string> getAllLogicalStreamAsString();
};

}// namespace NES

#endif//NES_INCLUDE_SERVICES_STREAMCATALOGSERVICE_HPP_

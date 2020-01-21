#ifndef NES_INCLUDE_SERVICES_STREAMCATALOGSERVICE_HPP_
#define NES_INCLUDE_SERVICES_STREAMCATALOGSERVICE_HPP_

#include <Catalogs/StreamCatalog.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

class StreamCatalogService {

  public:

    /**
     * @brief For adding new logical stream to the catalog
     */
    bool addNewLogicalStream(std::string &streamName, std::string &streamSchema);

    /**
     * @brief For updating the logical stream
     */
    void updatedLogicalStream(std::string &streamName, std::string &streamSchema);

    /**
     * @brief Remove logical stream from the catalog
     */
    bool removeLogicalStream(std::string &streamName);

    /**
     * @brief Returns a vector containing name of logical streams
     */
    std::vector<string> getAllLogicalStream();

};

}

#endif //NES_INCLUDE_SERVICES_STREAMCATALOGSERVICE_HPP_

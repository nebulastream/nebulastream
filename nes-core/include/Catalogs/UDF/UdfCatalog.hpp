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

#ifndef NES_INCLUDE_CATALOGS_UDF_UDFCATALOG_HPP_
#define NES_INCLUDE_CATALOGS_UDF_UDFCATALOG_HPP_

#include <string>
#include <unordered_map>
#include <vector>

#include <Catalogs/UDF/JavaUdfDescriptor.hpp>

namespace NES::Catalogs {

/**
 * @brief The UDF catalog stores all the data required to execute a Java UDF inside an embedded JVM.
 *
 * It provides an API to register and remove UDFs from a client,
 * to retrieve the implementation data during query rewrite,
 * and to retrieve a list of registered UDFs for visualization.
 */
class UdfCatalog {
  public:
    /**
     * @brief Create a UdfCatalog instance.
     * @return UdfCatalog instance.
     */
    static std::unique_ptr<UdfCatalog> create();

    /**
     * @brief Register the descriptor data of a Java UDF.
     * @param name The name of the UDF as it is used in queries.
     * @param descriptor The implementation data of the UDF.
     * @throws UdfException If descriptor is nullptr or if a UDF under the name is already registered.
     */
    // TODO #2079 use unique_ptr here to make sure that UdfCatalog takes ownership of the descriptor object
    void registerJavaUdf(const std::string& name, JavaUdfDescriptorPtr descriptor);

    /**
     * @brief Retrieve the implementation data for a Java UDF.
     * @param name The name of the UDF as it is used in queries.
     * @return The implementation data of the UDF, or nullptr if the UDF is not registered.
     */
    JavaUdfDescriptorPtr getUdfDescriptor(const std::string& name);

    /**
     * @brief Remove the UDF from the catalog.
     * @param name The name of the UDF as it is used in queries.
     * @return True, if the UDF was registered in the catalog; otherwise, False.
     *
     * Removing an unregistered UDF is not an error condition because it could have been removed by another user.
     * In this case, the user should just be notified.
     */
    bool removeUdf(const std::string& name);

    /**
     * @brief Retrieve a list of registered UDFs.
     * @return A STL container containing the names of the registered UDFs.
     */
    // TODO Not sure if this is the correct API.
    // I'd rather return an iterator over the keys. Or maybe we don't need this function at all.
    // It's not really used to support UDFs. It could be used by a visualization tool.
    // It would be nice to show more information about the UDFs (signature, size of dependencies),
    // but that's not needed right now.
    const std::vector<std::string> listUdfs() const;

  private:
    std::unordered_map<std::string, JavaUdfDescriptorPtr> udfStore;
};

}// namespace NES::Catalogs
#endif  // NES_INCLUDE_CATALOGS_UDF_UDFCATALOG_HPP_

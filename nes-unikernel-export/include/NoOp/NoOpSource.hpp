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

#ifndef NES_NOOPSOURCE_HPP
#define NES_NOOPSOURCE_HPP

#include <NoOp/NoOpSourceDescriptor.hpp>
#include <Sources/DataSource.hpp>
namespace NES {

/**
 * @brief this class implement the NoOp as an input source
 */
class NoOpSource : public DataSource {
  public:
    /**
   * @brief constructor of the NoOp source
   * @param schema of the source
   * @param bufferManager pointer to the buffer manager
   * @param queryManager pointer to the query manager
   * @param operatorId current operator id
   * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
     * @param statisticId represents the unique identifier of components that we can track statistics for
   * @param numSourceLocalBuffers number of local source buffers
   * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
   * @param physicalSourceName the name and unique identifier of a physical source
   * @return a DataSourcePtr pointing to the data source
   */
    explicit NoOpSource(SchemaPtr schema,
                        Runtime::BufferManagerPtr bufferManager,
                        Runtime::QueryManagerPtr queryManager,
                        OperatorId operatorId,
                        OriginId originId,
                        StatisticId statisticId,
                        size_t numSourceLocalBuffers,
                        GatheringMode gatheringMode,
                        const std::string& physicalSourceName);

    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the csv source
     * @return returns string describing the binary source
     */
    std::string toString() const override;

    /**
     * @brief Get source type
     * @return source type
     */
    SourceType getType() const override;
};

using NoOpSourcePtr = std::shared_ptr<NoOpSource>;
}// namespace NES
#endif//NES_NOOPSOURCE_HPP

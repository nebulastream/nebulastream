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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MATERIALIZEDVIEWSOURCDESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MATERIALIZEDVIEWSOURCDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES::Experimental {

/**
 * @brief Descriptor defining properties used for creating a materialized view source
 */
class MaterializedViewSourceDescriptor : public SourceDescriptor {
  public:
    /**
     * @brief **
     */
    explicit MaterializedViewSourceDescriptor(SchemaPtr schema,
                                              std::shared_ptr<uint8_t> memoryArea,
                                              size_t memoryAreaSize,
                                              uint64_t numBuffersToProcess,//TODO: Really needed?
                                              uint64_t mViewId);

    /**
     * @brief Factory method ***
     */
    static std::shared_ptr<MaterializedViewSourceDescriptor> create(const SchemaPtr& schema,
                                                                    const std::shared_ptr<uint8_t>& memoryArea,
                                                                    size_t memoryAreaSize,
                                                                    uint64_t numBuffersToProcess,
                                                                    uint64_t mViewId);

    /**
     * @brief Provides the string representation of the materialized view source
     * @return the string representation of the materialized view source
     */
    std::string toString() override;

    /**
     * @brief Equality method to compare two source descriptors stored as shared_ptr
     * @param other the source descriptor to compare against
     * @return true if type, schema, and memory area are equal
     */
    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) override;

    /**
     * @brief returns the shared ptr to the memory area
     * @return the shared ptr to the memory area
     */
    std::shared_ptr<uint8_t> getMemArea();

    /**
     * @brief returns the size of the stored memory area
     * @return the size of the stored memory area
     */
    size_t getMemAreaSize() const;

    /**
     * @brief returns number of buffer to process
     * @return
     */
    uint64_t getNumBuffersToProcess() const;

    /**
    * @brief returns the materialized view id
    * @return
    */
    uint64_t getMViewId() const;

  private:
    std::shared_ptr<uint8_t> memArea;
    size_t memAreaSize;
    uint64_t numBuffersToProcess;
    uint64_t mViewId;
};
}// namespace NES::Experimental
#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MATERIALIZEDVIEWSOURCDESCRIPTOR_HPP_

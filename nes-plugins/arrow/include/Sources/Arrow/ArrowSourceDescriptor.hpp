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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_ARROWSOURCEDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_ARROWSOURCEDESCRIPTOR_HPP_

#include <Sources/Arrow/ArrowSourceType.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical Arrow IPC file source
 */
class ArrowSourceDescriptor : public SourceDescriptor {
  public:
    static SourceDescriptorPtr create(SchemaPtr schema,
                                      ArrowSourceTypePtr arrowSourceType,
                                      const std::string logicalSourceName,
                                      const std::string physicalSourceName);

    static SourceDescriptorPtr create(SchemaPtr schema, ArrowSourceTypePtr arrowSourceType);

    /**
     * @brief get source config ptr with all configurations for Arrow source
     */
    ArrowSourceTypePtr getSourceConfig() const;

    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) const override;

    std::string toString() const override;

    SourceDescriptorPtr copy() override;

  private:
    explicit ArrowSourceDescriptor(SchemaPtr schema,
                                   ArrowSourceTypePtr sourceConfig,
                                   std::string logicalSourceName,
                                   std::string physicalSourceName);

    ArrowSourceTypePtr arrowSourceType;
};

using ArrowSourceDescriptorPtr = std::shared_ptr<ArrowSourceDescriptor>;

}// namespace NES

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_ARROWSOURCEDESCRIPTOR_HPP_

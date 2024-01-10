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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_STATICDATASOURCEDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_STATICDATASOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES::Experimental {
/**
 * @brief Descriptor defining properties used for creating physical table source
 */
class StaticDataSourceDescriptor : public SourceDescriptor {
  public:
    /**
     * @brief Ctor of a StaticDataSourceDescriptor
     * @param schema the schema of the source
     * @param lateStart indicates if the static data source should start sending data at deployment or only when receiving a "start" message
     */
    explicit StaticDataSourceDescriptor(SchemaPtr schema, std::string pathTableFile, bool lateStart);

    /**
     * @brief Factory method to create a StaticDataSourceDescriptor object
     * @param schema the schema of the source
     * @param lateStart indicates if the static data source should start sending data at deployment or only when receiving a "start" message
     * @return a correctly initialized shared ptr to StaticDataSourceDescriptor
     */
    static std::shared_ptr<StaticDataSourceDescriptor> create(const SchemaPtr& schema, std::string pathTableFile, bool lateStart);

    /**
     * @brief Provides the string representation of the table source
     * @return the string representation of the table source
     */
    std::string toString() const override;

    /**
     * @brief Equality method to compare two source descriptors stored as shared_ptr
     * @param other the source descriptor to compare against
     * @return true if type, schema, and table area are equal
     */
    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) const override;

    /**
     * @brief return the schema
     * @return
     */
    SchemaPtr getSchema() const;

    /**
     * @brief return the path to the table file to be loaded.
     * @return
     */
    std::string getPathTableFile() const;

    /**
     * @brief Getter for lateStart.
     * @returns lateStart indicates if the static data source should start sending data at deployment or only when receiving a "start" message
     */
    bool getLateStart();

    SourceDescriptorPtr copy() override;

  private:
    std::string pathTableFile;
    bool lateStart;
};
}// namespace NES::Experimental
#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_STATICDATASOURCEDESCRIPTOR_HPP_

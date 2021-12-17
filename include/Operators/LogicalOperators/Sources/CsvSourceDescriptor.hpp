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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SOURCES_CSV_SOURCE_DESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SOURCES_CSV_SOURCE_DESCRIPTOR_HPP_

#include <Configurations/ConfigOption.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <chrono>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical CSV source
 */
class CsvSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, Configurations::CSVSourceConfigPtr sourceConfigPtr);

    /**
     * @brief get source config ptr with all configurations for csv source
     */
    Configurations::CSVSourceConfigPtr getSourceConfig() const;

    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) override;
    std::string toString() override;

  private:
    explicit CsvSourceDescriptor(SchemaPtr schema, Configurations::CSVSourceConfigPtr sourceConfig);

    Configurations::CSVSourceConfigPtr sourceConfig;
};

using CsvSourceDescriptorPtr = std::shared_ptr<CsvSourceDescriptor>;

}// namespace NES

#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SOURCES_CSV_SOURCE_DESCRIPTOR_HPP_

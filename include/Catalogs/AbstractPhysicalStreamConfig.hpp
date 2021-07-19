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

#ifndef NES_INCLUDE_CATALOGS_ABSTRACTPHYSICALSTREAMCONFIG_HPP_
#define NES_INCLUDE_CATALOGS_ABSTRACTPHYSICALSTREAMCONFIG_HPP_

#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief An abstract physical stream config class to build source descriptor based on some configuration
 */
class AbstractPhysicalStreamConfig {
  public:
    /**
     * @brief Build a source descriptor using a given schema and the related logicalStreamName
     * @return the source descriptor matched with a given schema
     */
    virtual SourceDescriptorPtr build(SchemaPtr, std::string) = 0;

    /**
     * @brief The string representation of the object
     * @return the string representation of the object
     */
    virtual const std::string toString() = 0;

    /**
     * @brief The source type as a string
     * @return The source type as a string
     */
    virtual const std::string getSourceType() = 0;

    /**
     * @brief Provides the physical stream name of the source
     * @return the physical stream name of the source
     */
    virtual const std::string getPhysicalStreamName() = 0;

    /**
     * @brief Provides the vector of logical stream names of the source
     * @return the logical stream names of the source
     */
    virtual const std::vector<std::string> getLogicalStreamName() = 0;

    /**
     * @brief Provides a SourceConfig from this AbstractPhysicalStreamConfig
     * @return the source config
     */
    virtual SourceConfigPtr toSourceConfig() = 0;
};

typedef std::shared_ptr<AbstractPhysicalStreamConfig> AbstractPhysicalStreamConfigPtr;

}// namespace NES
#endif//NES_INCLUDE_CATALOGS_ABSTRACTPHYSICALSTREAMCONFIG_HPP_

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

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

class AbstractPhysicalStreamConfig {
  public:
    virtual SourceDescriptorPtr build(SchemaPtr) = 0;

    virtual const std::string toString() = 0;

    virtual const std::string getSourceType() = 0;

    virtual const std::string getPhysicalStreamName() = 0;

    virtual const std::string getLogicalStreamName() = 0;
};

typedef std::shared_ptr<AbstractPhysicalStreamConfig> AbstractPhysicalStreamConfigPtr;

}
#endif//NES_INCLUDE_CATALOGS_ABSTRACTPHYSICALSTREAMCONFIG_HPP_

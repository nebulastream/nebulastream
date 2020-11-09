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

#ifndef NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTPHYSICALTOLOGICALSINK_HPP_
#define NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTPHYSICALTOLOGICALSINK_HPP_

#include <memory>

namespace NES {

class SinkDescriptor;
typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

class SinkMedium;
typedef std::shared_ptr<SinkMedium> DataSinkPtr;

/**
 * @brief This class is responsible for creating the Logical sink from physical sink
 */
class ConvertPhysicalToLogicalSink {

  public:
    /**
     * @brief This method takes input as one of the several physical sink defined in the system and output the corresponding
     * logical sink descriptor
     * @param dataSink the input data sink object defining the physical sink
     * @return the logical sink descriptor
     */
    static SinkDescriptorPtr createSinkDescriptor(DataSinkPtr dataSink);

  private:
    ConvertPhysicalToLogicalSink() = default;
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTPHYSICALTOLOGICALSINK_HPP_

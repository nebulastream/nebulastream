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

#ifndef NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_
#define NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_

#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Sources/DataSource.hpp>

namespace NES {

/**
 * @brief This class is responsible for creating logical source descriptor to physical source.
 */
class ConvertLogicalToPhysicalSource {

  public:
    /**
     * @brief This method produces corresponding physical source for an input logical source descriptor
     * @param sourceDescriptor : the logical source desciptor
     * @return Data source pointer for the physical source
     */
    static DataSourcePtr createDataSource(SourceDescriptorPtr sourceDescriptor, NodeEngine::NodeEnginePtr nodeEngine);

  private:
    ConvertLogicalToPhysicalSource() = default;
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_

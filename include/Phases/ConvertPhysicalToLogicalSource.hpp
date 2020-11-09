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

#ifndef NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_
#define NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_

namespace NES {

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

/**
 * @brief This class is responsible for creating the Logical source from physical sources
 */
class ConvertPhysicalToLogicalSource {

  public:
    /**
     * @brief This method takes input as one of the several physical source defined in the system and output the corresponding
     * logical source descriptor
     * @param dataSource the input data source object defining the physical source
     * @return the logical source descriptor
     */
    static SourceDescriptorPtr createSourceDescriptor(DataSourcePtr dataSource);

  private:
    ConvertPhysicalToLogicalSource() = default;
};

}// namespace NES

#endif//NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_

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
#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTORPLUGIN_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTORPLUGIN_HPP_
#include <Execution/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Util/PluginRegistry.hpp>
namespace NES {

class SourceDescriptor;
using SourceDescriptorPtr = std::shared_ptr<SourceDescriptor>;

class SourceDescriptorPlugin {
  public:
    SourceDescriptorPlugin() = default;
    virtual SourceDescriptorPtr create(SchemaPtr schema, PhysicalSourceTypePtr physicalSource) = 0;

    virtual ~SourceDescriptorPlugin() = default;
};

using SourceDescriptorPluginRegistry = Util::PluginRegistry<SourceDescriptorPlugin>;

}// namespace NES

#endif  // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTORPLUGIN_HPP_

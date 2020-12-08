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

#ifndef NES_INCLUDE_COMMON_FORWARDDECLARATION_HPP_
#define NES_INCLUDE_COMMON_FORWARDDECLARATION_HPP_
#include <memory>
// TODO ALL: use this file instead of declaring types manually in every single file!
// TODO ALL: this is only for runtime components
namespace NES {

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

class QueryCompiler;
typedef std::shared_ptr<QueryCompiler> QueryCompilerPtr;

class SinkMedium;
typedef std::shared_ptr<SinkMedium> DataSinkPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

}// namespace NES

#endif//NES_INCLUDE_COMMON_FORWARDDECLARATION_HPP_

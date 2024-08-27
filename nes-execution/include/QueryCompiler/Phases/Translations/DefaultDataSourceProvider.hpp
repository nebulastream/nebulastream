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
#pragma once

#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::QueryCompilation
{

/// Transform a source descriptor to a SourceHandle that handles a 'DataSource' and a 'Source'
/// The DataSource spawns an independent thread for data ingestion and it manages the pipeline and task logic.
/// The Source is owned by the DataSource. The Source ingests bytes from an interface (TCP, CSV, ..) and writes the bytes to a TupleBuffer.
class DefaultDataSourceProvider
{
public:
    explicit DefaultDataSourceProvider(QueryCompilerOptionsPtr compilerOptions);
    static DataSourceProviderPtr create(const QueryCompilerOptionsPtr& compilerOptions);

    virtual SourceHandlPtr lower(
        OriginId originId,
        const SourceDescriptorPtr& sourceDescriptor,
        const Runtime::NodeEnginePtr& nodeEngine,
        const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

    virtual ~DefaultDataSourceProvider() = default;

private:
    QueryCompilerOptionsPtr compilerOptions;
};
} /// namespace NES::QueryCompilation

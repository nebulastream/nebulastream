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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_SOURCEPROVIDER_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_SOURCEPROVIDER_HPP_

#include <Identifiers/Identifiers.hpp>
#include <SourceReturnType.hpp>
#include <SourceHandle.hpp>

namespace NES
{

/// Transform a source descriptor to a SourceHandle that handles a 'DataSource' and a 'Source'
/// The DataSource spawns an independent thread for data ingestion and it manages the pipeline and task logic.
/// The Source is owned by the DataSource. The Source ingests bytes from an interface (TCP, CSV, ..) and writes the bytes to a TupleBuffer.
class SourceProvider
{
public:
    explicit SourceProvider();
    static std::shared_ptr<SourceProvider> create();

    /// Returning a shared pointer, because sources may be shared by multiple executable query plans (qeps).
    SourceHandlePtr lower(
        OriginId originId,
        const SourceDescriptorPtr& sourceDescriptor,
        Runtime::BufferManagerPtr bufferManager,
        SourceReturnType::EmitFunction&& emitFunction);

    ~SourceProvider() = default;
};
using DataSourceProviderPtr = std::shared_ptr<SourceProvider>;

} /// namespace NES::QueryCompilation

#endif /// NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_SOURCEPROVIDER_HPP_

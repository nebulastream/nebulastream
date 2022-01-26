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
#ifndef NES_INCLUDE_SOURCES_MATERIALIZED_VIEW_SOURCE_HPP_
#define NES_INCLUDE_SOURCES_MATERIALIZED_VIEW_SOURCE_HPP_

#include <Sources/DataSource.hpp>

namespace NES::Experimental::MaterializedView {

// Forward decl.
class MaterializedView;
using MaterializedViewPtr = std::shared_ptr<MaterializedView>;
class MaterializedViewSource;
using MaterializedViewSourcePtr = std::shared_ptr<MaterializedViewSource>;

/**
 * @brief this class provides a materialized view as a data source
 */
class MaterializedViewSource : public DataSource {

public:
    /// @brief constructor
    MaterializedViewSource(SchemaPtr schema,
                           Runtime::BufferManagerPtr bufferManager,
                           Runtime::QueryManagerPtr queryManager,
                           OperatorId operatorId,
                           size_t numSourceLocalBuffers,
                           GatheringMode::Value gatheringMode,
                           std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                           MaterializedViewPtr view);

    /**
     * @brief method to receive a buffer from the materialized view source
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief Provides a string representation of the source
     * @return The string representation of the source
     */
    std::string toString() const override;

    /**
     * @brief Provides the type of the source
     * @return the type of the source
     */
    SourceType getType() const override;

    /**
     *  @brief Provides the id of the used materialized view
     *  @return materialized view id
     */
    uint64_t getViewId() const;

private:
    MaterializedViewPtr view;

}; // class MaterializedViewSource
} // namespace NES::Experimental::MaterializedView
#endif// NES_INCLUDE_SOURCES_MATERIALIZED_VIEW_SOURCE_HPP_
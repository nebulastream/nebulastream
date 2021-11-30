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

#include <MaterializedView/MaterializedView.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/GeneratorSource.hpp>

namespace NES::Experimental {
/**
 * @brief ....
 */
class MaterializedViewSource : public GeneratorSource {
  public:
    /** TODO
     * @brief The constructor of a materialized view source
     * @param schema the schema of the source
     * @param mView
     * @param bufferManager valid pointer to the buffer manager
     * @param queryManager valid pointer to the query manager
     * @param
     * @param operatorId the valid id of the source
     */
    explicit MaterializedViewSource(SchemaPtr schema,
                                    MaterializedViewPtr mView,
                                    Runtime::BufferManagerPtr bufferManager,
                                    Runtime::QueryManagerPtr queryManager,
                                    uint64_t numBuffersToProcess,
                                    OperatorId operatorId,
                                    size_t numSourceLocalBuffers,
                                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);
    /**
     * @brief This method is implemented only to comply with the API: it will crash the system if called.
     * @return a nullopt
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

  private:
    MaterializedViewPtr mView;
    uint64_t numberOfTuplesToProduce;
    uint64_t currentPositionInBytes;
    uint64_t schemaSize;
    uint64_t bufferSize;
};
using MaterializedViewSourcePtr = std::shared_ptr<MaterializedViewSource>;
}// namespace NES::Experimental
#endif// NES_INCLUDE_SOURCES_MATERIALIZED_VIEW_SOURCE_HPP_
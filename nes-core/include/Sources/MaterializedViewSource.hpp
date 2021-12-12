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

#include <Views/MaterializedView.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/GeneratorSource.hpp>

namespace NES::Experimental::MaterializedView {

/**
 * @brief ....
 */
class MaterializedViewSource : public DataSource {
  public:

    /// @brief default constructor
    MaterializedViewSource(SchemaPtr schema,
                           Runtime::BufferManagerPtr bufferManager,
                           Runtime::QueryManagerPtr queryManager,
                           OperatorId operatorId,
                           size_t numSourceLocalBuffers,
                           GatheringMode gatheringMode,
                           std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                           MaterializedViewPtr view) : DataSource(std::move(schema),
                                                                  std::move(bufferManager),
                                                                  std::move(queryManager),
                                                                  operatorId,
                                                                  numSourceLocalBuffers,
                                                                  gatheringMode,
                                                                  std::move(successors)),
                                                                  view(view) {};

    /**
     * @brief This method is implemented only to comply with the API: it will crash the system if called.
     * @return a nullopt
     */
    std::optional<Runtime::TupleBuffer> receiveData() override {
        return view->receiveData();
    };

    /**
     * @brief Provides a string representation of the source
     * @return The string representation of the source
     */
    std::string toString() const override {
        return "MaterializedViewSource";
    };

    /**
     * @brief Provides the type of the source
     * @return the type of the source
     */
    SourceType getType() const override {
        return MATERIALIZED_VIEW_SOURCE;
    };

    /// @brief
    size_t getViewId() {
        return view->getId();
    }

  private:
    MaterializedViewPtr view;
};
using MaterializedViewSourcePtr = std::shared_ptr<MaterializedViewSource>;
}// namespace NES::Experimental
#endif// NES_INCLUDE_SOURCES_MATERIALIZED_VIEW_SOURCE_HPP_
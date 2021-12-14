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

#ifndef NES_INCLUDE_SINKS_MEDIUMS_MATERIALIZED_VIEW_SINK_HPP_
#define NES_INCLUDE_SINKS_MEDIUMS_MATERIALIZED_VIEW_SINK_HPP_

#include <Sinks/Mediums/SinkMedium.hpp>

namespace NES::Experimental::MaterializedView {

// forward decl.
class MaterializedView;
using MaterializedViewPtr = std::shared_ptr<MaterializedView>;
class MaterializedViewSink;
using MaterializedViewSinkPtr = std::shared_ptr<MaterializedViewSink>;

/**
 * @brief this class provides a materialized view as a data sink
 */
class MaterializedViewSink : public SinkMedium {

  public:
    /// @brief constructor
    MaterializedViewSink(MaterializedViewPtr view, SinkFormatPtr format, QuerySubPlanId parentPlanId);

    /**
     * @brief setup method for materialized view sink
     * @Note required by sinkmedium but does nothing
     */
    void setup() override {};

    /**
     * @brief shutdown method for materialized view sink
     * @Note this will clear the views content
     */
    void shutdown() override;

    /**
     * @brief method to write the content of a tuple buffer to the materialized view
     * @param tuple buffer to write
     * @param worker context currently not used
     * @return bool indicating success of the write
     */
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;

    /**
     * @brief override the toString method for the materialized view sink
     * @return returns string describing the materialized view sink
     */
    std::string toString() const override;

    /**
     * @brief method to return the type of medium
     * @return type of medium
     */
    SinkMediumTypes getSinkMediumType() override;

    /**
      *  @brief Provides the id of the used materialized view
      *  @return materialized view id
      */
    uint64_t getViewId() const;

  private:
    MaterializedViewPtr view;

}; // class MaterializedViewSink
} // namespace NES::Experimental::MaterializedView

#endif//NES_INCLUDE_SINKS_MEDIUMS_MATERIALIZED_VIEW_SINK_HPP_

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

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include <MaterializedView/MaterializedView.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>

namespace NES::Experimental::MaterializedView {

/**
 * @brief this class provides a materialized view sink
 */
class MaterializedViewSink : public SinkMedium {
  public:
    /**
     * @brief Default constructor
     */
    MaterializedViewSink(MaterializedViewPtr view,
                         SinkFormatPtr format,
                         QuerySubPlanId parentPlanId);

    /**
     * @brief Default destructor
     */
    ~MaterializedViewSink() override;

    /**
     * @brief setup method for mv sink
     * @Note required due to derivation but does nothing
     */
    void setup() override;

    /**
     * @brief shutdown method for mv sink
     * @Note required due to derivation but does nothing
     */
    void shutdown() override;

    /**
     * @brief method to write the content of a tuple buffer to mv
     * @param tuple buffer to write
     * @return bool indicating success of the write
     */
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;

    /**
     * @brief override the toString method for the mv sink
     * @return returns string describing the print sink
     */
    std::string toString() const override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

    /**
     * TODO
     */
     size_t getViewId();

     /**
      * TODO
      */
     Runtime::BufferManagerPtr getBufferManager();

  private:
    Runtime::BufferManagerPtr localBufferManager;
    MaterializedViewPtr view;
};
using MaterializedViewSinkPtr = std::shared_ptr<MaterializedViewSink>;
}// namespace NES::Experimental::MaterializedView

#endif//NES_INCLUDE_SINKS_MEDIUMS_MATERIALIZED_VIEW_SINK_HPP_

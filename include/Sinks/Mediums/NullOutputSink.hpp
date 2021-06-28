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

#ifndef NullOutputSink_HPP
#define NullOutputSink_HPP

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include <Sinks/Mediums/SinkMedium.hpp>
#include <iostream>

namespace NES {

/**
 * @brief this class provides a print sink
 */
class NullOutputSink : public SinkMedium {
  public:
    /**
     * @brief Default constructor
     * @Note the default output will be written to cout
     */
    explicit NullOutputSink(OperatorId logicalOperatorId, QuerySubPlanId parentPlanId);

    /**
     * @brief destructor
     * @Note this is required by some tests
     * TODO: find out why this is required
     */
    ~NullOutputSink() override;

    /**
     * @brief setup method for print sink
     * @Note required due to derivation but does nothing
     */
    void setup() override;

    /**
     * @brief shutdown method for print sink
     * @Note required due to derivation but does nothing
     */
    void shutdown() override;

    /**
     * @brief method to write the content of a tuple buffer to output console
     * @param tuple buffer to write
     * @return bool indicating success of the write
     */
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;

    /**
     * @brief override the toString method for the print sink
     * @return returns string describing the print sink
     */
    std::string toString() const override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

  private:
};
using NullOutputSinkPtr = std::shared_ptr<NullOutputSink>;
}// namespace NES

#endif// NullOutputSink_HPP

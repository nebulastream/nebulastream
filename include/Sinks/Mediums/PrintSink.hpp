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

#ifndef PRINTSINK_HPP
#define PRINTSINK_HPP

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
class PrintSink : public SinkMedium {
  public:
    /**
     * @brief Default constructor
     * @Note the default output will be written to cout
     */
    PrintSink(SinkFormatPtr format, QuerySubPlanId parentPlanId, std::ostream& pOutputStream = std::cout);

    /**
     * @brief destructor
     * @Note this is required by some tests
     * TODO: find out why this is required
     */
    ~PrintSink();

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
    bool writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContextRef) override;

    /**
     * @brief override the toString method for the print sink
     * @return returns string describing the print sink
     */
    const std::string toString() const override;

    /**
     * @brief Get sink type
     */
    std::string toString() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();

  private:
    std::ostream& outputStream;
};
typedef std::shared_ptr<PrintSink> PrintSinkPtr;
}// namespace NES

#endif// PRINTSINK_HPP

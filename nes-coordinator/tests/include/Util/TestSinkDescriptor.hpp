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
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/TestPhaseProvider.hpp>

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSINKDESCRIPTOR_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSINKDESCRIPTOR_HPP_

namespace NES::TestUtils {

/**
 * @brief This class is used for representing the description of a test sink operator
 */
class TestSinkDescriptor : public SinkDescriptor {
  public:
    /**
     * @brief Constructor for a TestSinkDescriptor
     * @param dataSink
     */
    explicit TestSinkDescriptor(DataSinkPtr dataSink);

    /**
     * @brief Getter for the DataSink
     * @return DataSink
     */
    DataSinkPtr getSink();

    /**
     * @brief Deconstructor for a TestSinkDescriptor
     */
    ~TestSinkDescriptor() override = default;

    /**
     * @brief Creates a string representation
     * @return String representation
     */
    std::string toString() const override;

    /**
     * @brief Checks if two SinkDescriptors are equal
     * @return True, if equal otherwise false
     */
    bool equal(SinkDescriptorPtr const&) override;

  private:
    DataSinkPtr sink;
};
}// namespace NES::TestUtils

#endif  // NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTSINKDESCRIPTOR_HPP_

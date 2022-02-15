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

#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_QUERYCOMPILERCONFIGURATION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_QUERYCOMPILERCONFIGURATION_HPP_

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <utility>

namespace NES {
namespace Configurations {

/**
 * @brief Configuration for the query compiler
 */
class QueryCompilerConfiguration : public BaseConfiguration {
  public:
    QueryCompilerConfiguration() : BaseConfiguration(){};
    QueryCompilerConfiguration(std::string name, std::string description) : BaseConfiguration(name, description){};

    /**
     * @brief Sets the compilation strategy. We differentiate between FAST, DEBUG, and OPTIMIZED compilation.
     */
    EnumOption<QueryCompilation::QueryCompilerOptions::CompilationStrategy> compilationStrategy = {
        QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG,
        QueryCompilation::QueryCompilerOptions::CompilationStrategy::OPTIMIZE,
        "Indicates the optimization strategy for the query compiler [FAST|DEBUG|OPTIMIZE]."};

    /**
     * @brief Sets the pipelining strategy. We differentiate between an OPERATOR_FUSION and OPERATOR_AT_A_TIME strategy.
     */
    EnumOption<QueryCompilation::QueryCompilerOptions::PipeliningStrategy> pipeliningStrategy = {
        QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG,
        QueryCompilation::QueryCompilerOptions::OPERATOR_FUSION,
        "Indicates the pipelining strategy for the query compiler [OPERATOR_FUSION|OPERATOR_AT_A_TIME]."};

    /**
     * @brief Sets the output buffer optimization level.
     */
    EnumOption<QueryCompilation::QueryCompilerOptions::OutputBufferOptimizationLevel> outputBufferOptimizationLevel = {
        QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG,
        QueryCompilation::QueryCompilerOptions::ALL,
        "Indicates the OutputBufferAllocationStrategy "
        "[ALL|NO|ONLY_INPLACE_OPERATIONS_NO_FALLBACK,"
        "|REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK,|"
        "REUSE_INPUT_BUFFER_NO_FALLBACK|OMIT_OVERFLOW_CHECK_NO_FALLBACK]. "};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&compilationStrategy, &pipeliningStrategy, &outputBufferOptimizationLevel};
    }
};

}// namespace Configurations
}// namespace NES

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_QUERYCOMPILERCONFIGURATION_HPP_

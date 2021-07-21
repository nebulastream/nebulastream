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
#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <QueryCompiler/Phases/BufferOptimizationStrategies.hpp>
#include <cstdint>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Set of common options for the query compiler
 */
class QueryCompilerOptions {
  public:
    /**
     * @brief Creates the default options.
     * @return QueryCompilerOptionsPtr
     */
    static QueryCompilerOptionsPtr createDefaultOptions();

    /**
     * @brief Enables operator fusion.
     */
    void enableOperatorFusion();

    /**
     * @brief Disables operator fusion.
     */
    void disableOperatorFusion();

    /**
     * @brief Returns if operator fusion is enabled.
     */
    bool isOperatorFusionEnabled() const;

    /**
     * @brief Sets desired buffer optimization strategy.
     */
    void setBufferOptimizationStrategy(BufferOptimizationStrategy strategy);

    /**
     * @brief Returns desired buffer optimization strategy.
     */
    BufferOptimizationStrategy getBufferOptimizationStrategy() const;

    /**
     * @brief Sets the number of local buffers per source.
     * @param num of buffers
     */
    void setNumSourceLocalBuffers(uint64_t num);
    /**
     * @brief Returns the number of local source buffers.
     * @return uint64_t
     */
    uint64_t getNumSourceLocalBuffers() const;

  protected:
    bool operatorFusion;
    uint64_t numSourceLocalBuffers;
    BufferOptimizationStrategy desiredBufferStrategy;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_

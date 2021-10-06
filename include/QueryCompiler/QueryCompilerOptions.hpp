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
#include <QueryCompiler/Phases/OutputBufferAllocationStrategies.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <cstdint>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Set of common options for the query compiler
 */
class QueryCompilerOptions {
  public:
    enum FilterProcessingStrategy {
        // Uses a branches to process filter expressions
        BRANCHED,
        // Uses predication for filter expressions if possible
        PREDICATION
    };

    enum CompilationStrategy {
        // Use fast compilation strategy, i.e., dose not apply any optimizations and omits debug output.
        FAST,
        // Creates debug output i.e., source code files and applies formatting. No code optimizations.
        DEBUG,
        // Applies all compiler optimizations.
        OPTIMIZE
    };

    enum PipeliningStrategy {
        // Applies operator fusion.
        OPERATOR_FUSION,
        // Places each operator in an individual pipeline.
        OPERATOR_AT_A_TIME
    };

    enum OutputBufferOptimizationLevel {
        // Use highest optimization.
        ALL,
        // create separate result buffer and copy everything over after all operations are applied.
        // Check size after every written tuple.
        NO,
        // If all records and all fields match up in input and result buffer we can simply emit the input buffer.
        // For this no filter can be applied and no new fields can be added.
        // The only typical operations possible are inplace-maps, e.g. "id = id + 1".
        ONLY_INPLACE_OPERATIONS_NO_FALLBACK,
        // Output schema is smaller or equal (bytes) than input schema.
        // We can reuse the buffer and omit size checks.
        REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK,
        // enable the two optimizations individually (benchmarking only)
        REUSE_INPUT_BUFFER_NO_FALLBACK,
        OMIT_OVERFLOW_CHECK_NO_FALLBACK,
        BITMASK
    };

    /**
     * @brief Creates the default options.
     * @return QueryCompilerOptionsPtr
     */
    static QueryCompilerOptionsPtr createDefaultOptions();

    [[nodiscard]] PipeliningStrategy getPipeliningStrategy() const;

    void setPipeliningStrategy(PipeliningStrategy pipeliningStrategy);

    [[nodiscard]] CompilationStrategy getCompilationStrategy() const;

    void setCompilationStrategy(CompilationStrategy compilationStrategy);

    void setFilterProcessingStrategy(FilterProcessingStrategy filterProcessingStrategy);
    [[nodiscard]] QueryCompilerOptions::FilterProcessingStrategy getFilterProcessingStrategy() const;

    /**
     * @brief Sets desired buffer optimization strategy.
     */
    void setOutputBufferOptimizationLevel(QueryCompilerOptions::OutputBufferOptimizationLevel level);

    /**
     * @brief Returns desired buffer optimization strategy.
     */
    [[nodiscard]] QueryCompilerOptions::OutputBufferOptimizationLevel getOutputBufferOptimizationLevel() const;

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
    uint64_t numSourceLocalBuffers;
    OutputBufferOptimizationLevel outputBufferOptimizationLevel;
    PipeliningStrategy pipeliningStrategy;
    CompilationStrategy compilationStrategy;
    FilterProcessingStrategy filterProcessingStrategy;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_

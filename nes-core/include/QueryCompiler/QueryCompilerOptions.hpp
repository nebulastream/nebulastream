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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_
#include <QueryCompiler/Phases/OutputBufferAllocationStrategies.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Util/Common.hpp>
#include <cstdint>
#include <string>
namespace NES::QueryCompilation {

/**
 * @brief Set of common options for the query compiler
 */
class QueryCompilerOptions {
  public:
    enum class QueryCompiler : uint8_t {
        // Uses the default query compiler
        DEFAULT_QUERY_COMPILER,
        // Uses the nautilus query compiler
        NAUTILUS_QUERY_COMPILER
    };

    enum class DumpMode : uint8_t {
        // Disables all dumping
        NONE,
        // Dumps intermediate representations to console, std:out
        CONSOLE,
        // Dumps intermediate representations to file
        FILE,
        // Dumps intermediate representations to console and file
        FILE_AND_CONSOLE
    };

    enum class NautilusBackend : uint8_t {
        // Uses the interpretation based nautilus backend.
        INTERPRETER,
        // Uses the mlir based nautilus backend.
        MLIR_COMPILER,
        // Uses the byte code interpretation based nautilus backend.
        BC_INTERPRETER,
        // Uses the flounder based nautilus backend.
        FLOUNDER_COMPILER,
        // Uses the cpp based nautilus backend.
        CPP_COMPILER
    };

    enum class FilterProcessingStrategy : uint8_t {
        // Uses a branches to process filter expressions
        BRANCHED,
        // Uses predication for filter expressions if possible
        PREDICATION
    };

    enum class CompilationStrategy : uint8_t {
        // Use fast compilation strategy, i.e., does not apply any optimizations and omits debug output.
        FAST,
        // Creates debug output i.e., source code files and applies formatting. No code optimizations.
        DEBUG,
        // Applies all compiler optimizations.
        OPTIMIZE,
        // Applies all compiler optimizations and inlines proxy functions.
        PROXY_INLINING
    };

    enum class PipeliningStrategy : uint8_t {
        // Applies operator fusion.
        OPERATOR_FUSION,
        // Places each operator in an individual pipeline.
        OPERATOR_AT_A_TIME
    };

    enum class WindowingStrategy : uint8_t {
        // Applies default windowing strategy.
        DEFAULT,
        // Applies an experimental thread local implementation for window aggregations
        THREAD_LOCAL,
        BUCKET
    };

    enum class OutputBufferOptimizationLevel : uint8_t {
        // Use highest optimization available.
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

    class StreamHashJoinOptions {
      public:
        /**
         * @brief getter for max hash table size
         * @return
         */
        uint64_t getTotalSizeForDataStructures() const;

        /**
         * @brief setter for max hash table size
         * @param total_size_for_data_structures
         */
        void setTotalSizeForDataStructures(uint64_t totalSizeForDataStructures);

        /**
         * @brief get the number of partitions for the hash join
         * @return number of partitions
         */
        uint64_t getNumberOfPartitions() const;

        /**
         * @brief get the number of partitions for the hash join
         * @param num
         */
        void setNumberOfPartitions(uint64_t num);

        /**
         * @brief get the size of each page in the hash table
         * @return page size
         */
        uint64_t getPageSize() const;

        /**
         * @brief set the size of each page in the hash table
         * @param size
         */
        void setPageSize(uint64_t size);

        /**
         * @brief get the number of pre-allocated pages in the hash table per bucket
         * @return number of pages
         */
        uint64_t getPreAllocPageCnt() const;

        /**
        * @brief  get the number of pre-allocated pages in the hash table per bucket
        * @param cnt
        */
        void setPreAllocPageCnt(uint64_t cnt);

      private:
        uint64_t numberOfPartitions;
        uint64_t pageSize;
        uint64_t preAllocPageCnt;
        uint64_t totalSizeForDataStructures;
    };

    using StreamHashJoinOptionsPtr = std::shared_ptr<StreamHashJoinOptions>;

    class VectorizationOptions {
    public:
        /**
         * @return Whether the query compiler should use compiler passes for enabling vectorized execution
         */
        bool isUsingVectorization() const;

        /**
         * @brief Specify if the query compiler should use vectorization
         * @param enable
         */
        void useVectorization(bool enable);

        /**
         * @return The stage buffer size used for materializing tuples
         */
        uint64_t getStageBufferSize() const;

        /**
         * @brief Set the stage buffer size.
         * @param size
         */
        void setStageBufferSize(uint64_t size);

    private:
        bool enabled;
        uint64_t stageBufferSize;
    };

    using VectorizationOptionsPtr = std::shared_ptr<VectorizationOptions>;

    /**
     * @brief Creates the default options.
     * @return QueryCompilerOptionsPtr
     */
    static QueryCompilerOptionsPtr createDefaultOptions();

    [[nodiscard]] PipeliningStrategy getPipeliningStrategy() const;

    void setPipeliningStrategy(PipeliningStrategy pipeliningStrategy);

    [[nodiscard]] QueryCompiler getQueryCompiler() const;

    void setQueryCompiler(QueryCompiler pipeliningStrategy);

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

    WindowingStrategy getWindowingStrategy() const;

    /**
     * @brief Sets the strategy for the stream join
     * @param strategy
     */
    void setStreamJoinStratgy(QueryCompilation::StreamJoinStrategy strategy);

    /**
     * @brief gets the stream join strategy.
     * @return
     */
    [[nodiscard]] QueryCompilation::StreamJoinStrategy getStreamJoinStratgy() const;

    /**
     * @brief Return hash join options
     * @return
     */
    StreamHashJoinOptionsPtr getHashJoinOptions();

    /**
     * @brief Set compiler options
     * @param streamHashJoinOptions
     */
    void setHashJoinOptions(StreamHashJoinOptionsPtr streamHashJoinOptions);

    void setWindowingStrategy(WindowingStrategy windowingStrategy);

    [[nodiscard]] NautilusBackend getNautilusBackend() const;
    void setNautilusBackend(const NautilusBackend nautilusBackend);

    [[nodiscard]] const DumpMode& getDumpMode() const;
    void setDumpMode(DumpMode dumpMode);

    /**
     * @brief Set the path to the CUDA SDK
     * @param cudaSdkPath the CUDA SDK path
     */
    void setCUDASdkPath(const std::string& cudaSdkPath);

    /**
     * @brief Get the path to the CUDA SDK
     */
    const std::string getCUDASdkPath() const;

    /**
     * @brief Return vectorization options
     */
    VectorizationOptionsPtr getVectorizationOptions() const;

    /**
     * @brief Set vectorization options
     * @param options the vectorization options
     */
    void setVectorizationOptions(const VectorizationOptionsPtr& options);

  protected:
    uint64_t numSourceLocalBuffers;
    OutputBufferOptimizationLevel outputBufferOptimizationLevel;
    PipeliningStrategy pipeliningStrategy;
    CompilationStrategy compilationStrategy;
    FilterProcessingStrategy filterProcessingStrategy;
    WindowingStrategy windowingStrategy;
    QueryCompiler queryCompiler;
    NautilusBackend nautilusBackend;
    DumpMode dumpMode;
    StreamHashJoinOptionsPtr hashJoinOptions;
    std::string cudaSdkPath;
    StreamJoinStrategy joinStrategy;
    VectorizationOptionsPtr vectorizationOptions;
};
}// namespace NES::QueryCompilation

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_

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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_

#include <Configurations/Enums/CompilationStrategy.hpp>
#include <Configurations/Enums/DumpMode.hpp>
#include <Configurations/Enums/NautilusBackend.hpp>
#include <Configurations/Enums/OutputBufferOptimizationLevel.hpp>
#include <Configurations/Enums/PipeliningStrategy.hpp>
#include <Configurations/Enums/QueryCompilerType.hpp>
#include <Configurations/Enums/WindowingStrategy.hpp>
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
    enum class FilterProcessingStrategy : uint8_t {
        // Uses a branches to process filter expressions
        BRANCHED,
        // Uses predication for filter expressions if possible
        PREDICATION
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

    /**
     * @brief Creates the default options.
     * @return QueryCompilerOptionsPtr
     */
    static QueryCompilerOptionsPtr createDefaultOptions();

    [[nodiscard]] PipeliningStrategy getPipeliningStrategy() const;

    void setPipeliningStrategy(PipeliningStrategy pipeliningStrategy);

    [[nodiscard]] QueryCompilerType getQueryCompiler() const;

    void setQueryCompiler(QueryCompilerType queryCompilerType);

    [[nodiscard]] CompilationStrategy getCompilationStrategy() const;

    void setCompilationStrategy(CompilationStrategy compilationStrategy);

    void setFilterProcessingStrategy(FilterProcessingStrategy filterProcessingStrategy);

    [[nodiscard]] QueryCompilerOptions::FilterProcessingStrategy getFilterProcessingStrategy() const;

    /**
     * @brief Sets desired buffer optimization strategy.
     */
    void setOutputBufferOptimizationLevel(OutputBufferOptimizationLevel level);

    /**
     * @brief Returns desired buffer optimization strategy.
     */
    [[nodiscard]] OutputBufferOptimizationLevel getOutputBufferOptimizationLevel() const;

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
    void setStreamJoinStrategy(QueryCompilation::StreamJoinStrategy strategy);

    /**
     * @brief gets the stream join strategy.
     * @return
     */
    [[nodiscard]] QueryCompilation::StreamJoinStrategy getStreamJoinStrategy() const;

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

    uint64_t numOfBuffersToProduce {0};

  protected:
    uint64_t numSourceLocalBuffers;
    OutputBufferOptimizationLevel outputBufferOptimizationLevel;
    PipeliningStrategy pipeliningStrategy;
    CompilationStrategy compilationStrategy;
    FilterProcessingStrategy filterProcessingStrategy;
    WindowingStrategy windowingStrategy;
    QueryCompilerType queryCompiler;
    NautilusBackend nautilusBackend;
    DumpMode dumpMode;
    StreamHashJoinOptionsPtr hashJoinOptions;
    std::string cudaSdkPath;
    StreamJoinStrategy joinStrategy;
};
}// namespace NES::QueryCompilation

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_

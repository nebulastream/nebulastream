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
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>

namespace NES::QueryCompilation {
OutputBufferOptimizationLevel QueryCompilerOptions::getOutputBufferOptimizationLevel() const {
    return outputBufferOptimizationLevel;
};

void QueryCompilerOptions::setOutputBufferOptimizationLevel(OutputBufferOptimizationLevel level) {
    this->outputBufferOptimizationLevel = level;
};

void QueryCompilerOptions::setNumSourceLocalBuffers(uint64_t num) { this->numSourceLocalBuffers = num; }

uint64_t QueryCompilerOptions::getNumSourceLocalBuffers() const { return numSourceLocalBuffers; }

QueryCompilerOptionsPtr QueryCompilerOptions::createDefaultOptions() {
    auto options = QueryCompilerOptions();
    options.setCompilationStrategy(CompilationStrategy::OPTIMIZE);
    options.setPipeliningStrategy(PipeliningStrategy::OPERATOR_FUSION);
    options.setFilterProcessingStrategy(FilterProcessingStrategy::BRANCHED);
    options.setNumSourceLocalBuffers(64);
    options.setNumSourceLocalBuffers(10000);
    options.setOutputBufferOptimizationLevel(OutputBufferOptimizationLevel::ALL);
    options.setWindowingStrategy(WindowingStrategy::SLICING);
    options.setQueryCompiler(QueryCompilerType::NAUTILUS_QUERY_COMPILER);
    options.setDumpMode(DumpMode::FILE_AND_CONSOLE);
    options.setNautilusBackend(NautilusBackend::MLIR_COMPILER_BACKEND);

    QueryCompilerOptions::StreamHashJoinOptionsPtr hashOptions = std::make_shared<QueryCompilerOptions::StreamHashJoinOptions>();
    hashOptions->setNumberOfPartitions(NES::Configurations::DEFAULT_HASH_NUM_PARTITIONS);
    hashOptions->setPageSize(NES::Configurations::DEFAULT_HASH_PAGE_SIZE);
    hashOptions->setPreAllocPageCnt(NES::Configurations::DEFAULT_HASH_PREALLOC_PAGE_COUNT);
    hashOptions->setTotalSizeForDataStructures(NES::Configurations::DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE);
    options.setStreamJoinStrategy(QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN);
    options.setHashJoinOptions(hashOptions);
    return std::make_shared<QueryCompilerOptions>(options);
}

PipeliningStrategy QueryCompilerOptions::getPipeliningStrategy() const { return pipeliningStrategy; }

void QueryCompilerOptions::setHashJoinOptions(QueryCompilerOptions::StreamHashJoinOptionsPtr streamHashJoinOptions) {
    this->hashJoinOptions = streamHashJoinOptions;
}

void QueryCompilerOptions::setPipeliningStrategy(PipeliningStrategy pipeliningStrategy) {
    this->pipeliningStrategy = pipeliningStrategy;
}

void QueryCompilerOptions::setQueryCompiler(NES::QueryCompilation::QueryCompilerType queryCompiler) {
    this->queryCompiler = queryCompiler;
}

QueryCompilerType QueryCompilerOptions::getQueryCompiler() const { return queryCompiler; }

CompilationStrategy QueryCompilerOptions::getCompilationStrategy() const { return compilationStrategy; }

void QueryCompilerOptions::setCompilationStrategy(CompilationStrategy compilationStrategy) {
    this->compilationStrategy = compilationStrategy;
}

void QueryCompilerOptions::setFilterProcessingStrategy(FilterProcessingStrategy filterProcessingStrategy) {
    this->filterProcessingStrategy = filterProcessingStrategy;
}

QueryCompilerOptions::FilterProcessingStrategy QueryCompilerOptions::getFilterProcessingStrategy() const {
    return filterProcessingStrategy;
}

WindowingStrategy QueryCompilerOptions::getWindowingStrategy() const { return windowingStrategy; }

void QueryCompilerOptions::setWindowingStrategy(WindowingStrategy windowingStrategy) {
    QueryCompilerOptions::windowingStrategy = windowingStrategy;
}

NautilusBackend QueryCompilerOptions::getNautilusBackend() const { return nautilusBackend; }

void QueryCompilerOptions::setNautilusBackend(const NautilusBackend nautilusBackend) {
    QueryCompilerOptions::nautilusBackend = nautilusBackend;
}

void QueryCompilerOptions::setStreamJoinStrategy(QueryCompilation::StreamJoinStrategy strategy) { this->joinStrategy = strategy; }

QueryCompilation::StreamJoinStrategy QueryCompilerOptions::getStreamJoinStrategy() const { return joinStrategy; }

QueryCompilerOptions::StreamHashJoinOptionsPtr QueryCompilerOptions::getHashJoinOptions() { return hashJoinOptions; }

const DumpMode& QueryCompilerOptions::getDumpMode() const { return dumpMode; }

void QueryCompilerOptions::setDumpMode(DumpMode dumpMode) { QueryCompilerOptions::dumpMode = dumpMode; }

uint64_t QueryCompilerOptions::StreamHashJoinOptions::getNumberOfPartitions() const { return numberOfPartitions; }

void QueryCompilerOptions::StreamHashJoinOptions::setNumberOfPartitions(uint64_t num) { numberOfPartitions = num; }

uint64_t QueryCompilerOptions::StreamHashJoinOptions::getPageSize() const { return pageSize; }

void QueryCompilerOptions::StreamHashJoinOptions::setPageSize(uint64_t size) { pageSize = size; }

uint64_t QueryCompilerOptions::StreamHashJoinOptions::getPreAllocPageCnt() const { return preAllocPageCnt; }

void QueryCompilerOptions::StreamHashJoinOptions::setPreAllocPageCnt(uint64_t cnt) { preAllocPageCnt = cnt; }

uint64_t QueryCompilerOptions::StreamHashJoinOptions::getTotalSizeForDataStructures() const { return totalSizeForDataStructures; }

void QueryCompilerOptions::StreamHashJoinOptions::setTotalSizeForDataStructures(uint64_t totalSizeForDataStructures) {
    this->totalSizeForDataStructures = totalSizeForDataStructures;
}

void QueryCompilerOptions::setCUDASdkPath(const std::string& cudaSdkPath) { QueryCompilerOptions::cudaSdkPath = cudaSdkPath; }
const std::string QueryCompilerOptions::getCUDASdkPath() const { return cudaSdkPath; }

}// namespace NES::QueryCompilation

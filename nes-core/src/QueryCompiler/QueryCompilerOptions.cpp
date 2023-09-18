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
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Experimental/Vectorization/DefaultQueryCompilerOptions.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>

namespace NES::QueryCompilation {
QueryCompilerOptions::OutputBufferOptimizationLevel QueryCompilerOptions::getOutputBufferOptimizationLevel() const {
    return outputBufferOptimizationLevel;
};

void QueryCompilerOptions::setOutputBufferOptimizationLevel(QueryCompilerOptions::OutputBufferOptimizationLevel level) {
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
    options.setOutputBufferOptimizationLevel(OutputBufferOptimizationLevel::ALL);
    options.setWindowingStrategy(WindowingStrategy::LEGACY);
    options.setQueryCompiler(QueryCompiler::DEFAULT_QUERY_COMPILER);
    options.setDumpMode(DumpMode::FILE_AND_CONSOLE);
    options.setNautilusBackend(NautilusBackend::MLIR_COMPILER);

    QueryCompilerOptions::StreamHashJoinOptionsPtr hashOptions = std::make_shared<QueryCompilerOptions::StreamHashJoinOptions>();
    hashOptions->setNumberOfPartitions(NES::Runtime::Execution::DEFAULT_HASH_NUM_PARTITIONS);
    hashOptions->setPageSize(NES::Runtime::Execution::DEFAULT_HASH_PAGE_SIZE);
    hashOptions->setPreAllocPageCnt(NES::Runtime::Execution::DEFAULT_HASH_PREALLOC_PAGE_COUNT);
    hashOptions->setTotalSizeForDataStructures(NES::Runtime::Execution::DEFAULT_HASH_TOTAL_HASH_TABLE_SIZE);
    options.setStreamJoinStratgy(QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN);
    options.setHashJoinOptions(hashOptions);

    QueryCompilerOptions::VectorizationOptionsPtr vectorizationOptions = std::make_shared<QueryCompilerOptions::VectorizationOptions>();
    vectorizationOptions->useVectorization(false);
    vectorizationOptions->setStageBufferSize(NES::Runtime::Execution::Experimental::Vectorization::STAGE_BUFFER_SIZE);
    vectorizationOptions->useCUDA(false);
    vectorizationOptions->setCUDASdkPath(NES::Runtime::Execution::Experimental::Vectorization::CUDA_SDK_PATH);
    options.setVectorizationOptions(vectorizationOptions);
    return std::make_shared<QueryCompilerOptions>(options);
}
QueryCompilerOptions::PipeliningStrategy QueryCompilerOptions::getPipeliningStrategy() const { return pipeliningStrategy; }

void QueryCompilerOptions::setHashJoinOptions(QueryCompilerOptions::StreamHashJoinOptionsPtr streamHashJoinOptions) {
    this->hashJoinOptions = streamHashJoinOptions;
}

void QueryCompilerOptions::setPipeliningStrategy(QueryCompilerOptions::PipeliningStrategy pipeliningStrategy) {
    this->pipeliningStrategy = pipeliningStrategy;
}

void QueryCompilerOptions::setQueryCompiler(NES::QueryCompilation::QueryCompilerOptions::QueryCompiler queryCompiler) {
    this->queryCompiler = queryCompiler;
}

QueryCompilerOptions::QueryCompiler QueryCompilerOptions::getQueryCompiler() const { return queryCompiler; }

QueryCompilerOptions::CompilationStrategy QueryCompilerOptions::getCompilationStrategy() const { return compilationStrategy; }

void QueryCompilerOptions::setCompilationStrategy(QueryCompilerOptions::CompilationStrategy compilationStrategy) {
    this->compilationStrategy = compilationStrategy;
}

void QueryCompilerOptions::setFilterProcessingStrategy(FilterProcessingStrategy filterProcessingStrategy) {
    this->filterProcessingStrategy = filterProcessingStrategy;
}

QueryCompilerOptions::FilterProcessingStrategy QueryCompilerOptions::getFilterProcessingStrategy() const {
    return filterProcessingStrategy;
}
QueryCompilerOptions::WindowingStrategy QueryCompilerOptions::getWindowingStrategy() const { return windowingStrategy; }
void QueryCompilerOptions::setWindowingStrategy(QueryCompilerOptions::WindowingStrategy windowingStrategy) {
    QueryCompilerOptions::windowingStrategy = windowingStrategy;
}
QueryCompilerOptions::NautilusBackend QueryCompilerOptions::getNautilusBackend() const { return nautilusBackend; }
void QueryCompilerOptions::setNautilusBackend(const NautilusBackend nautilusBackend) {
    QueryCompilerOptions::nautilusBackend = nautilusBackend;
}

void QueryCompilerOptions::setStreamJoinStratgy(QueryCompilation::StreamJoinStrategy strategy) { this->joinStrategy = strategy; }

QueryCompilation::StreamJoinStrategy QueryCompilerOptions::getStreamJoinStratgy() const { return joinStrategy; }

QueryCompilerOptions::StreamHashJoinOptionsPtr QueryCompilerOptions::getHashJoinOptions() { return hashJoinOptions; }

const QueryCompilerOptions::DumpMode& QueryCompilerOptions::getDumpMode() const { return dumpMode; }
void QueryCompilerOptions::setDumpMode(QueryCompilerOptions::DumpMode dumpMode) { QueryCompilerOptions::dumpMode = dumpMode; }
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

QueryCompilerOptions::VectorizationOptionsPtr QueryCompilerOptions::getVectorizationOptions() const {
    return vectorizationOptions;
}

void QueryCompilerOptions::setVectorizationOptions(const QueryCompilerOptions::VectorizationOptionsPtr& options) {
    this->vectorizationOptions = options;
}

bool QueryCompilerOptions::VectorizationOptions::isUsingVectorization() const {
    return enabled;
}

void QueryCompilerOptions::VectorizationOptions::useVectorization(bool enable) {
    this->enabled = enable;
}

uint64_t QueryCompilerOptions::VectorizationOptions::getStageBufferSize() const {
    return stageBufferSize;
}

void QueryCompilerOptions::VectorizationOptions::setStageBufferSize(uint64_t size) {
    this->stageBufferSize = size;
}

bool QueryCompilerOptions::VectorizationOptions::isUsingCUDA() const {
    return cudaEnabled;
}

void QueryCompilerOptions::VectorizationOptions::useCUDA(bool enable) {
    this->cudaEnabled = enable;
}

void QueryCompilerOptions::VectorizationOptions::setCUDASdkPath(const std::string& cudaSdkPath) {
    QueryCompilerOptions::VectorizationOptions::cudaSdkPath = cudaSdkPath;
}

const std::string QueryCompilerOptions::VectorizationOptions::getCUDASdkPath() const {
    return cudaSdkPath;
}

void QueryCompilerOptions::VectorizationOptions::setCUDAThreadsPerBlock(uint32_t threadsPerBlock) {
    QueryCompilerOptions::VectorizationOptions::cudaThreadsPerBlock = threadsPerBlock;
}

uint32_t QueryCompilerOptions::VectorizationOptions::getCUDAThreadsPerBlock() const {
    return cudaThreadsPerBlock;
}

}// namespace NES::QueryCompilation
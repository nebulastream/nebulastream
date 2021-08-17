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
#include <QueryCompiler/QueryCompilerOptions.hpp>

namespace NES::QueryCompilation {

bool QueryCompilerOptions::isOperatorFusionEnabled() const { return operatorFusion; }

void QueryCompilerOptions::enableOperatorFusion() { this->operatorFusion = true; }

void QueryCompilerOptions::disableOperatorFusion() { this->operatorFusion = false; }

bool QueryCompilerOptions::isPredicationEnabled() const { return predication; }

void QueryCompilerOptions::enablePredication() { this->predication = true; }

void QueryCompilerOptions::disablePredication() { this->predication = false; }

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
    options.enableOperatorFusion();
    options.enablePredication(); // todo: disable by default
    options.setNumSourceLocalBuffers(64);
    options.setOutputBufferOptimizationLevel(ALL);
    return std::make_shared<QueryCompilerOptions>(options);
}

}// namespace NES::QueryCompilation
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
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/SourceCode.hpp>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <utility>

namespace NES::Compiler {

CompilationRequest::CompilationRequest(std::unique_ptr<SourceCode> sourceCode,
                                       std::string name,
                                       bool profileCompilation,
                                       bool profileExecution,
                                       bool optimizeCompilation,
                                       bool debug,
                                       std::vector<std::shared_ptr<ExternalAPI>> externalApis)
    : sourceCode(std::move(sourceCode)), name(std::move(name)), profileCompilation(profileCompilation),
      profileExecution(profileExecution), optimizeCompilation(optimizeCompilation), debug(debug),
      externalApis(std::move(externalApis)) {}

std::shared_ptr<CompilationRequest> CompilationRequest::create(std::unique_ptr<SourceCode> sourceCode,
                                                               std::string identifier,
                                                               bool profileCompilation,
                                                               bool profileExecution,
                                                               bool optimizeCompilation,
                                                               bool debug,
                                                               std::vector<std::shared_ptr<ExternalAPI>> externalApis) {

    // creates a unique name for a compilation request.
    auto time = std::time(nullptr);
    auto localtime = *std::localtime(&time);

    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(1, 10000000);

    long long microseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
    std::stringstream requestName;
    requestName << identifier << "_" << std::put_time(&localtime, "%d-%m-%Y_%H-%M-%S") << "_" << dist(rng) << "_" << microseconds;

    return std::make_shared<CompilationRequest>(std::move(sourceCode),
                                                requestName.str(),
                                                profileCompilation,
                                                profileExecution,
                                                optimizeCompilation,
                                                debug,
                                                externalApis);
};

bool CompilationRequest::enableOptimizations() const { return optimizeCompilation; }

bool CompilationRequest::enableDebugging() const { return debug; }

bool CompilationRequest::enableCompilationProfiling() const { return profileCompilation; }

bool CompilationRequest::enableExecutionProfiling() const { return profileExecution; }

std::string CompilationRequest::getName() const { return name; }

std::vector<std::shared_ptr<ExternalAPI>> CompilationRequest::getExternalAPIs() const { return externalApis; }

const std::shared_ptr<SourceCode> CompilationRequest::getSourceCode() const { return sourceCode; }
bool CompilationRequest::operator==(const CompilationRequest& rhs) const {
    return sourceCode == rhs.sourceCode && name == rhs.name && profileCompilation == rhs.profileCompilation
        && profileExecution == rhs.profileExecution && optimizeCompilation == rhs.optimizeCompilation && debug == rhs.debug;
}
bool CompilationRequest::operator!=(const CompilationRequest& rhs) const { return !(rhs == *this); }

}// namespace NES::Compiler
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
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/OperatorId.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <QueryCompiler/Phases/Translations/DataSourceProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <utility>

namespace NES::QueryCompilation {

DataSourceProvider::DataSourceProvider(QueryCompilerOptionsPtr compilerOptions) : compilerOptions(std::move(compilerOptions)) {}

DataSourceProviderPtr QueryCompilation::DataSourceProvider::create(const QueryCompilerOptionsPtr& compilerOptions) {
    return std::make_shared<DataSourceProvider>(compilerOptions);
}

DataSourcePtr DataSourceProvider::lower(OperatorId operatorId,
                                        SourceDescriptorPtr sourceDescriptor,
                                        Runtime::NodeEnginePtr nodeEngine,
                                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) {

    if (sourceDescriptor->instanceOf<LambdaSourceDescriptor>()) {
        for (auto& entry : physicalStreamNameToSourceMap) {
            if (entry.first == sourceDescriptor->getPhysicalSourceName()) {
                NES_DEBUG("Reuse already existing physical stream " << sourceDescriptor->getPhysicalSourceName());
                for (auto& successor : successors) {
                    entry.second->getExecutableSuccessors().push_back(successor);
                }
                return entry.second;
            }
        }
    }

    DataSourcePtr newPtr = ConvertLogicalToPhysicalSource::createDataSource(operatorId,
                                                                            std::move(sourceDescriptor),
                                                                            std::move(nodeEngine),
                                                                            compilerOptions->getNumSourceLocalBuffers(),
                                                                            std::move(successors));
    if (sourceDescriptor->instanceOf<LambdaSourceDescriptor>()) {
        physicalStreamNameToSourceMap[sourceDescriptor->getPhysicalSourceName()] = newPtr;
    }

    return newPtr;
}

}// namespace NES::QueryCompilation
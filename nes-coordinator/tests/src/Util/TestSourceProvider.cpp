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
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/TestSourceProvider.hpp>

namespace NES::TestUtils {

TestSourceProvider::TestSourceProvider(QueryCompilation::QueryCompilerOptionsPtr options)
    : QueryCompilation::DefaultDataSourceProvider(std::move(options)) {}

DataSourcePtr TestSourceProvider::lower(OperatorId operatorId,
                                        OriginId originId,
                                        StatisticId statisticId,
                                        SourceDescriptorPtr sourceDescriptor,
                                        Runtime::NodeEnginePtr nodeEngine,
                                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) {
    if (sourceDescriptor->instanceOf<TestSourceDescriptor>()) {
        auto testSourceDescriptor = sourceDescriptor->as<TestSourceDescriptor>();
        return testSourceDescriptor
            ->create(operatorId, originId, statisticId, sourceDescriptor, nodeEngine, compilerOptions->getNumSourceLocalBuffers(), successors);
    }
    return DefaultDataSourceProvider::lower(operatorId, originId, statisticId, sourceDescriptor, nodeEngine, successors);
}

}// namespace NES::TestUtils
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/TestPhaseProvider.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/TestSourceProvider.hpp>

namespace NES::TestUtils {

TestSourceProvider::TestSourceProvider(QueryCompilation::QueryCompilerOptionsPtr options)
    : QueryCompilation::DefaultDataSourceProvider(std::move(std::move(options))) {}

DataSourcePtr TestSourceProvider::lower(OperatorId operatorId,
                                        OriginId originId,
                                        SourceDescriptorPtr sourceDescriptor,
                                        Runtime::NodeEnginePtr nodeEngine,
                                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) {
    if (sourceDescriptor->instanceOf<TestSourceDescriptor>()) {
        auto testSourceDescriptor = sourceDescriptor->as<TestSourceDescriptor>();
        return testSourceDescriptor
            ->create(operatorId, originId, sourceDescriptor, nodeEngine, compilerOptions->getNumSourceLocalBuffers(), successors);
    }
    return DefaultDataSourceProvider::lower(operatorId, originId, sourceDescriptor, nodeEngine, successors);
}

}// namespace NES::TestUtils
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/TestPhaseProvider.hpp>

#ifndef NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSOURCEDESCRIPTOR_HPP_
#define NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSOURCEDESCRIPTOR_HPP_

namespace NES::TestUtils {

class TestSourceDescriptor : public SourceDescriptor {
  public:
    TestSourceDescriptor(
        SchemaPtr schema,
        std::function<DataSourcePtr(OperatorId,
                                    OriginId,
                                    SourceDescriptorPtr,
                                    Runtime::NodeEnginePtr,
                                    size_t,
                                    std::vector<Runtime::Execution::SuccessorExecutablePipeline>)> createSourceFunction);

    DataSourcePtr create(OperatorId operatorId,
                         OriginId originId,
                         SourceDescriptorPtr sourceDescriptor,
                         Runtime::NodeEnginePtr nodeEngine,
                         size_t numSourceLocalBuffers,
                         std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    [[nodiscard]] std::string toString() override;
    [[nodiscard]] bool equal(SourceDescriptorPtr const&) override;
    SourceDescriptorPtr copy() override;

  private:
    std::function<DataSourcePtr(OperatorId,
                                OriginId,
                                SourceDescriptorPtr,
                                Runtime::NodeEnginePtr,
                                size_t,
                                std::vector<Runtime::Execution::SuccessorExecutablePipeline>)>
        createSourceFunction;
};

}// namespace NES::TestUtils

#endif//NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSOURCEDESCRIPTOR_HPP_

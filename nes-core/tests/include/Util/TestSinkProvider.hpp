
#ifndef NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSINKPROVIDER_HPP_
#define NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSINKPROVIDER_HPP_

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

namespace NES::TestUtils {

class TestSinkProvider : public QueryCompilation::DataSinkProvider {
  public:
    DataSinkPtr lower(OperatorId sinkId,
                      SinkDescriptorPtr sinkDescriptor,
                      SchemaPtr schema,
                      Runtime::NodeEnginePtr nodeEngine,
                      const QueryCompilation::PipelineQueryPlanPtr& querySubPlan,
                      size_t numOfProducers) override;
};
}
#endif//NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSINKPROVIDER_HPP_

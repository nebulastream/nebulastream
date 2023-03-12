#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/TestPhaseProvider.hpp>

#ifndef NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSINKDESCRIPTOR_HPP_
#define NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSINKDESCRIPTOR_HPP_

namespace NES::TestUtils {
class TestSinkDescriptor : public SinkDescriptor {
  public:
    explicit TestSinkDescriptor(DataSinkPtr dataSink);
    DataSinkPtr getSink();
    ~TestSinkDescriptor() override = default;
    std::string toString() override;
    bool equal(SinkDescriptorPtr const&) override;

  private:
    DataSinkPtr sink;
};
}// namespace NES::TestUtils

#endif//NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSINKDESCRIPTOR_HPP_

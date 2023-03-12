
#ifndef NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSOURCEPROVIDER_HPP_
#define NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSOURCEPROVIDER_HPP_

#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
namespace NES::TestUtils {

class TestSourceProvider : public QueryCompilation::DefaultDataSourceProvider {
  public:
    explicit TestSourceProvider(QueryCompilation::QueryCompilerOptionsPtr options);
    DataSourcePtr lower(OperatorId operatorId,
                        OriginId originId,
                        SourceDescriptorPtr sourceDescriptor,
                        Runtime::NodeEnginePtr nodeEngine,
                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) override;
};
}// namespace NES::TestUtils
#endif//NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTSOURCEPROVIDER_HPP_

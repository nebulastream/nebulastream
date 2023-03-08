//
// Created by pgrulich on 08.03.23.
//

#ifndef NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTPHASEPROVIDER_HPP_
#define NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTPHASEPROVIDER_HPP_


#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompiler.hpp>

namespace NES {
class TestPhaseProvider : public QueryCompilation::Phases::DefaultPhaseFactory {
  public:
    QueryCompilation::LowerToExecutableQueryPlanPhasePtr
    createLowerToExecutableQueryPlanPhase(QueryCompilation::QueryCompilerOptionsPtr options, bool) override {
        auto sinkProvider = std::make_shared<TestUtils::TestSinkProvider>();
        auto sourceProvider = std::make_shared<TestUtils::TestSourceProvider>(options);
        return QueryCompilation::LowerToExecutableQueryPlanPhase::create(sinkProvider, sourceProvider);
    }
};
}// namespace NES

#endif//NES_NES_CORE_TESTS_INCLUDE_UTIL_TESTPHASEPROVIDER_HPP_

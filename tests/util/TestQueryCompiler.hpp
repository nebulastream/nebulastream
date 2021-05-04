#ifndef NES_TESTS_UTIL_TESTQUERYCOMPILER_HPP_
#define NES_TESTS_UTIL_TESTQUERYCOMPILER_HPP_
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
namespace NES {



namespace TestUtils {

class TestSourceDescriptor : public SourceDescriptor {
  public:
    TestSourceDescriptor(SchemaPtr schema,
                         std::function<DataSourcePtr(OperatorId, SourceDescriptorPtr, NodeEngine::NodeEnginePtr, size_t,
                                                     std::vector<NodeEngine::Execution::SuccessorExecutablePipeline>)>
                         createSourceFunction)
        : SourceDescriptor(schema), createSourceFunction(createSourceFunction) {}
    DataSourcePtr create(OperatorId operatorId, SourceDescriptorPtr sourceDescriptor, NodeEngine::NodeEnginePtr nodeEngine,
                         size_t numSourceLocalBuffers,
                         std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors) {
        return createSourceFunction(operatorId, sourceDescriptor, nodeEngine, numSourceLocalBuffers, successors);
    }

    std::string toString() override { return std::string(); }
    bool equal(SourceDescriptorPtr) override { return false; }

  private:
    std::function<DataSourcePtr(OperatorId, SourceDescriptorPtr, NodeEngine::NodeEnginePtr, size_t,
                                std::vector<NodeEngine::Execution::SuccessorExecutablePipeline>)>
        createSourceFunction;
};


class TestSinkDescriptor : public SinkDescriptor {
  public:
    TestSinkDescriptor(DataSinkPtr dataSink) : sink(dataSink) {}
    DataSinkPtr getSink() { return sink; }
    ~TestSinkDescriptor() override = default;
    std::string toString() override { return std::string(); }
    bool equal(SinkDescriptorPtr) override { return false; }

  private:
    DataSinkPtr sink;
};


class TestSinkProvider : public QueryCompilation::DataSinkProvider {
  public:
    DataSinkPtr lower(OperatorId operatorId, SinkDescriptorPtr sinkDescriptor, SchemaPtr schema,
                      NodeEngine::NodeEnginePtr nodeEngine, QuerySubPlanId querySubPlanId) override {
        if (sinkDescriptor->instanceOf<TestSinkDescriptor>()) {
            auto testSinkDescriptor = sinkDescriptor->as<TestSinkDescriptor>();
            return testSinkDescriptor->getSink();
        } else {
            return DataSinkProvider::lower(operatorId, sinkDescriptor, schema, nodeEngine, querySubPlanId);
        }
    }
};

class TestSourceProvider : public QueryCompilation::DataSourceProvider {
  public:
    DataSourcePtr lower(OperatorId operatorId, SourceDescriptorPtr sourceDescriptor, NodeEngine::NodeEnginePtr nodeEngine,
                        size_t numSourceLocalBuffers,
                        std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors) override {
        if (sourceDescriptor->instanceOf<TestSourceDescriptor>()) {
            auto testSourceDescriptor = sourceDescriptor->as<TestSourceDescriptor>();
            return testSourceDescriptor->create(operatorId, sourceDescriptor, nodeEngine, numSourceLocalBuffers, successors);
        } else {
            return DataSourceProvider::lower(operatorId, sourceDescriptor, nodeEngine, numSourceLocalBuffers, successors);
        }
    }
};

class TestPhaseProvider : public QueryCompilation::Phases::DefaultPhaseFactory {
  public:
    const QueryCompilation::LowerToExecutableQueryPlanPhasePtr
    createLowerToExecutableQueryPlanPhase(QueryCompilation::QueryCompilerOptionsPtr) override {
        auto sinkProvider = std::make_shared<TestSinkProvider>();
        auto sourceProvider = std::make_shared<TestSourceProvider>();
        return QueryCompilation::LowerToExecutableQueryPlanPhase::create(sinkProvider, sourceProvider);
    }
};

QueryCompilation::QueryCompilerPtr createTestQueryCompiler() {
    auto options = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
    auto phaseProvider = std::make_shared<TestPhaseProvider>();
    return QueryCompilation::DefaultQueryCompiler::create(options, phaseProvider);
}

}// namespace QueryCompilation
}// namespace NES

#endif//NES_TESTS_UTIL_TESTQUERYCOMPILER_HPP_

/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_TESTS_UTIL_TESTQUERYCOMPILER_HPP_
#define NES_TESTS_UTIL_TESTQUERYCOMPILER_HPP_
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
namespace NES {

namespace TestUtils {

class TestSourceDescriptor : public SourceDescriptor {
  public:
    TestSourceDescriptor(
        SchemaPtr schema,
        std::function<DataSourcePtr(OperatorId,
                                    SourceDescriptorPtr,
                                    NodeEngine::NodeEnginePtr,
                                    size_t,
                                    std::vector<NodeEngine::Execution::SuccessorExecutablePipeline>)> createSourceFunction)
        : SourceDescriptor(schema), createSourceFunction(createSourceFunction) {}
    DataSourcePtr create(OperatorId operatorId,
                         SourceDescriptorPtr sourceDescriptor,
                         NodeEngine::NodeEnginePtr nodeEngine,
                         size_t numSourceLocalBuffers,
                         std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors) {
        return createSourceFunction(operatorId, sourceDescriptor, nodeEngine, numSourceLocalBuffers, successors);
    }

    std::string toString() override { return std::string(); }
    bool equal(SourceDescriptorPtr) override { return false; }

  private:
    std::function<DataSourcePtr(OperatorId,
                                SourceDescriptorPtr,
                                NodeEngine::NodeEnginePtr,
                                size_t,
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
    DataSinkPtr lower(OperatorId operatorId,
                      SinkDescriptorPtr sinkDescriptor,
                      SchemaPtr schema,
                      NodeEngine::NodeEnginePtr nodeEngine,
                      QuerySubPlanId querySubPlanId) override {
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
    TestSourceProvider(QueryCompilation::QueryCompilerOptionsPtr options) : QueryCompilation::DataSourceProvider(options){};
    DataSourcePtr lower(OperatorId operatorId,
                        OperatorId logicalSourceOperatorId,
                        SourceDescriptorPtr sourceDescriptor,
                        NodeEngine::NodeEnginePtr nodeEngine,
                        std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors) override {
        if (sourceDescriptor->instanceOf<TestSourceDescriptor>()) {
            auto testSourceDescriptor = sourceDescriptor->as<TestSourceDescriptor>();
            return testSourceDescriptor->create(operatorId,
                                                sourceDescriptor,
                                                nodeEngine,
                                                compilerOptions->getNumSourceLocalBuffers(),
                                                successors);
        } else {
            return DataSourceProvider::lower(operatorId, logicalSourceOperatorId, sourceDescriptor, nodeEngine, successors);
        }
    }
};

class TestPhaseProvider : public QueryCompilation::Phases::DefaultPhaseFactory {
  public:
    const QueryCompilation::LowerToExecutableQueryPlanPhasePtr
    createLowerToExecutableQueryPlanPhase(QueryCompilation::QueryCompilerOptionsPtr options) override {
        auto sinkProvider = std::make_shared<TestSinkProvider>();
        auto sourceProvider = std::make_shared<TestSourceProvider>(options);
        return QueryCompilation::LowerToExecutableQueryPlanPhase::create(sinkProvider, sourceProvider);
    }
};

QueryCompilation::QueryCompilerPtr createTestQueryCompiler() {
    auto options = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
    auto phaseProvider = std::make_shared<TestPhaseProvider>();
    return QueryCompilation::DefaultQueryCompiler::create(options, phaseProvider);
}

}// namespace TestUtils
}// namespace NES

#endif//NES_TESTS_UTIL_TESTQUERYCOMPILER_HPP_

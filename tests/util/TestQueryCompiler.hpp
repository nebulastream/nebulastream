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
#ifndef NES_TESTS_UTIL_TEST_QUERY_COMPILER_HPP_
#define NES_TESTS_UTIL_TEST_QUERY_COMPILER_HPP_
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <utility>
namespace NES {

namespace TestUtils {

class TestSourceDescriptor : public SourceDescriptor {
  public:
    TestSourceDescriptor(
        SchemaPtr schema,
        std::function<DataSourcePtr(OperatorId,
                                    SourceDescriptorPtr,
                                    Runtime::NodeEnginePtr,
                                    size_t,
                                    std::vector<Runtime::Execution::SuccessorExecutablePipeline>)> createSourceFunction)
        : SourceDescriptor(std::move(std::move(schema))), createSourceFunction(std::move(std::move(createSourceFunction))) {}
    DataSourcePtr create(OperatorId operatorId,
                         SourceDescriptorPtr sourceDescriptor,
                         Runtime::NodeEnginePtr nodeEngine,
                         size_t numSourceLocalBuffers,
                         std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) {
        return createSourceFunction(operatorId,
                                    std::move(std::move(sourceDescriptor)),
                                    std::move(std::move(nodeEngine)),
                                    numSourceLocalBuffers,
                                    std::move(std::move(successors)));
    }

    [[nodiscard]] std::string toString() override { return std::string(); }
    [[nodiscard]] bool equal(SourceDescriptorPtr const&) override { return false; }

  private:
    std::function<DataSourcePtr(OperatorId,
                                SourceDescriptorPtr,
                                Runtime::NodeEnginePtr,
                                size_t,
                                std::vector<Runtime::Execution::SuccessorExecutablePipeline>)>
        createSourceFunction;
};

class TestSinkDescriptor : public SinkDescriptor {
  public:
    explicit TestSinkDescriptor(DataSinkPtr dataSink) : sink(std::move(std::move(dataSink))) {}
    DataSinkPtr getSink() { return sink; }
    ~TestSinkDescriptor() override = default;
    std::string toString() override { return std::string(); }
    bool equal(SinkDescriptorPtr const&) override { return false; }

  private:
    DataSinkPtr sink;
};

class TestSinkProvider : public QueryCompilation::DataSinkProvider {
  public:
    DataSinkPtr lower(OperatorId operatorId,
                      SinkDescriptorPtr sinkDescriptor,
                      SchemaPtr schema,
                      Runtime::NodeEnginePtr nodeEngine,
                      QuerySubPlanId querySubPlanId) override {
        if (sinkDescriptor->instanceOf<TestSinkDescriptor>()) {
            auto testSinkDescriptor = sinkDescriptor->as<TestSinkDescriptor>();
            return testSinkDescriptor->getSink();
        }
        return DataSinkProvider::lower(operatorId, sinkDescriptor, schema, nodeEngine, querySubPlanId);
    }
};

class TestSourceProvider : public QueryCompilation::DataSourceProvider {
  public:
    explicit TestSourceProvider(QueryCompilation::QueryCompilerOptionsPtr options)
        : QueryCompilation::DataSourceProvider(std::move(std::move(options))){};
    DataSourcePtr lower(OperatorId operatorId,
                        SourceDescriptorPtr sourceDescriptor,
                        Runtime::NodeEnginePtr nodeEngine,
                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) override {
        if (sourceDescriptor->instanceOf<TestSourceDescriptor>()) {
            auto testSourceDescriptor = sourceDescriptor->as<TestSourceDescriptor>();
            return testSourceDescriptor->create(operatorId,
                                                sourceDescriptor,
                                                nodeEngine,
                                                compilerOptions->getNumSourceLocalBuffers(),
                                                successors);
        }
        return DataSourceProvider::lower(operatorId, sourceDescriptor, nodeEngine, successors);
    }
};

class TestPhaseProvider : public QueryCompilation::Phases::DefaultPhaseFactory {
  public:
    QueryCompilation::LowerToExecutableQueryPlanPhasePtr
    createLowerToExecutableQueryPlanPhase(QueryCompilation::QueryCompilerOptionsPtr options) override {
        auto sinkProvider = std::make_shared<TestSinkProvider>();
        auto sourceProvider = std::make_shared<TestSourceProvider>(options);
        return QueryCompilation::LowerToExecutableQueryPlanPhase::create(sinkProvider, sourceProvider);
    }
};

inline QueryCompilation::QueryCompilerPtr createTestQueryCompiler() {
    auto options = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
    auto phaseProvider = std::make_shared<TestPhaseProvider>();
    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    return QueryCompilation::DefaultQueryCompiler::create(options, phaseProvider, jitCompiler);
}

inline QueryCompilation::QueryCompilerPtr createTestQueryCompiler(QueryCompilation::QueryCompilerOptionsPtr options) {
    auto phaseProvider = std::make_shared<TestPhaseProvider>();
    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    return QueryCompilation::DefaultQueryCompiler::create(options, phaseProvider, jitCompiler);
}

}// namespace TestUtils
}// namespace NES

#endif// NES_TESTS_UTIL_TEST_QUERY_COMPILER_HPP_

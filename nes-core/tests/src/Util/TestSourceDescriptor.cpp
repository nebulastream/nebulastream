#include <Util/TestSourceDescriptor.hpp>
namespace NES::TestUtils {

TestSourceDescriptor::TestSourceDescriptor(
    SchemaPtr schema,
    std::function<DataSourcePtr(OperatorId,
                                OriginId,
                                SourceDescriptorPtr,
                                Runtime::NodeEnginePtr,
                                size_t,
                                std::vector<Runtime::Execution::SuccessorExecutablePipeline>)> createSourceFunction)
    : SourceDescriptor(std::move(std::move(schema))), createSourceFunction(std::move(std::move(createSourceFunction))) {}

DataSourcePtr TestSourceDescriptor::create(OperatorId operatorId,
                                           OriginId originId,
                                           SourceDescriptorPtr sourceDescriptor,
                                           Runtime::NodeEnginePtr nodeEngine,
                                           size_t numSourceLocalBuffers,
                                           std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) {
    return createSourceFunction(operatorId,
                                originId,
                                std::move(std::move(sourceDescriptor)),
                                std::move(std::move(nodeEngine)),
                                numSourceLocalBuffers,
                                std::move(std::move(successors)));
}

std::string TestSourceDescriptor::toString() { return std::string(); }

bool TestSourceDescriptor::equal(const SourceDescriptorPtr&) { return false; }

SourceDescriptorPtr TestSourceDescriptor::copy() { return NES::SourceDescriptorPtr(); }

}// namespace NES::TestUtils
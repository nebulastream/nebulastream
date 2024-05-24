/*
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
#include <Util/TestSourceDescriptor.hpp>
namespace NES::TestUtils {

TestSourceDescriptor::TestSourceDescriptor(
    SchemaPtr schema,
    std::function<DataSourcePtr(SchemaPtr schema,
                                OperatorId,
                                OriginId,
                                StatisticId,
                                SourceDescriptorPtr,
                                Runtime::NodeEnginePtr,
                                size_t,
                                std::vector<Runtime::Execution::SuccessorExecutablePipeline>)> createSourceFunction)
    : SourceDescriptor(std::move(schema)), createSourceFunction(std::move(createSourceFunction)) {}

DataSourcePtr TestSourceDescriptor::create(OperatorId operatorId,
                                           OriginId originId,
                                           StatisticId statisticId,
                                           SourceDescriptorPtr sourceDescriptor,
                                           Runtime::NodeEnginePtr nodeEngine,
                                           size_t numSourceLocalBuffers,
                                           std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) {
    return createSourceFunction(schema,
                                operatorId,
                                originId,
                                statisticId,
                                std::move(sourceDescriptor),
                                std::move(nodeEngine),
                                numSourceLocalBuffers,
                                std::move(successors));
}

std::string TestSourceDescriptor::toString() const { return std::string(); }

bool TestSourceDescriptor::equal(const SourceDescriptorPtr&) const { return false; }

SourceDescriptorPtr TestSourceDescriptor::copy() { return NES::SourceDescriptorPtr(); }

}// namespace NES::TestUtils

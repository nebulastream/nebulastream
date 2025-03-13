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

#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <ExecutablePipelineStage.hpp>
#include <Pipeline.hpp>

namespace NES
{

namespace
{
PipelineId getNextPipelineId()
{
    static std::atomic_uint64_t id = INITIAL_PIPELINE_ID.getRawValue();
    return PipelineId(id++);
}

std::string providerTypeToString(Pipeline::ProviderType providerType) {
    switch(providerType) {
        case Pipeline::ProviderType::Interpreter:
            return "Interpreter";
        case Pipeline::ProviderType::Compiler:
            return "Compiler";
        default:
            return "Unknown";
    }
}
}

std::string Pipeline::getProviderType() const
{
    return providerTypeToString(this->providerType);
}

std::string SourcePipeline::toString() const {
    std::ostringstream oss;
    oss << "SourcePipeline(id: " << pipelineId
        << ", providerType: " << providerTypeToString(providerType) << ", ";

    oss << "sourceOperator: ";
    if (sourceOperator)
        oss << sourceOperator->toString();
    else
        oss << "null";

    oss << ")";
    return oss.str();
}

std::string SinkPipeline::toString() const {
    std::ostringstream oss;
    oss << "SinkPipeline(id: " << pipelineId
        << ", providerType: " << providerTypeToString(providerType) << ", ";

    oss << "sinkOperator: ";
    if (sinkOperator)
        oss << sinkOperator->toString();
    else
        oss << "null";

    oss << ")";
    return oss.str();
}

std::string OperatorPipeline::toString() const {
    std::ostringstream oss;
    oss << "OperatorPipeline(id: " << pipelineId
        << ", providerType: " << getProviderType() << ", ";
    if (rootOperator)
        oss << "rootOperator: " << rootOperator->toString();
    else
        oss << "rootOperator: null";
    oss << ")";
    return oss.str();
}

Pipeline::Pipeline() : pipelineId(getNextPipelineId()), providerType(ProviderType::Compiler) {};


std::ostream& operator<<(std::ostream& os, const Pipeline& t)
{
    return os << t.toString();
}

}
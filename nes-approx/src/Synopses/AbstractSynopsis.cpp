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

#include <Benchmarking/Parsing/YamlAggregation.hpp>
#include <Synopses/AbstractSynopsis.hpp>
#include <Synopses/Samples/SampleRandomWithoutReplacement.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::ASP {

AbstractSynopsesPtr AbstractSynopsis::create(SynopsisArguments arguments) {

    if (arguments.getType() == SynopsisArguments::Type::SRSWR) {
        return std::make_shared<SampleRandomWithoutReplacement>(arguments.getWidth());
    } else {
        NES_NOT_IMPLEMENTED();
    }

}

void AbstractSynopsis::setAggregationFunction(
    const Runtime::Execution::Aggregation::AggregationFunctionPtr& aggregationFunction) {
    AbstractSynopsis::aggregationFunction = aggregationFunction;
}

void AbstractSynopsis::setFieldNameAggregation(const std::string& fieldNameAggregation) {
    AbstractSynopsis::fieldNameAggregation = fieldNameAggregation;
}

void AbstractSynopsis::setFieldNameApproximate(const std::string& fieldNameApproximate) {
    AbstractSynopsis::fieldNameApproximate = fieldNameApproximate;
}

void AbstractSynopsis::setInputSchema(const SchemaPtr& inputSchema) {
    AbstractSynopsis::inputSchema = inputSchema;
}

void AbstractSynopsis::setOutputSchema(const SchemaPtr& outputSchema) {
    AbstractSynopsis::outputSchema = outputSchema;
}

void AbstractSynopsis::setAggregationValue(Benchmarking::AggregationValuePtr aggregationValue) {
    AbstractSynopsis::aggregationValue = std::move(aggregationValue);
}

void AbstractSynopsis::setBufferManager(const Runtime::BufferManagerPtr& bufferManager) {
    AbstractSynopsis::bufferManager = bufferManager;
}

} // namespace NES::ASP
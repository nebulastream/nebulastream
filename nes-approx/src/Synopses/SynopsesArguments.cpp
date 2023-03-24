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

#include <Synopses/SynopsesArguments.hpp>

namespace NES::ASP {

SynopsesArguments::SynopsesArguments(Type type, size_t width, size_t height, size_t windowSize, std::string fieldName,
                                     Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction)
    : type(type), width(width), height(height), windowSize(windowSize), fieldNameAggregation(fieldName),
      aggregationFunction(std::move(aggregationFunction)) {}

SynopsesArguments SynopsesArguments::createArguments(Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction,
                                                     std::string fieldName, Type type, size_t width, size_t height,
                                                     size_t windowSize) {
    return SynopsesArguments(type, width, height, windowSize, fieldName, aggregationFunction);
}

size_t SynopsesArguments::getWidth() const { return width; }

size_t SynopsesArguments::getHeight() const { return height; }

size_t SynopsesArguments::getWindowSize() const { return windowSize; }

Runtime::Execution::Aggregation::AggregationFunctionPtr SynopsesArguments::getAggregationFunction() const {
    return aggregationFunction;
}

SynopsesArguments::Type SynopsesArguments::getType() const { return type; }

const std::string& SynopsesArguments::getFieldName() const { return fieldNameAggregation; }

std::vector<SynopsesArguments> SynopsesArguments::parseArgumentsFromYamlFile(const std::string& fileName) {



}

} // namespace NES::ASP
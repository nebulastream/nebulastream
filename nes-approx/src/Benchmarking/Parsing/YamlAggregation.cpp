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

#include <API/AttributeField.hpp>
#include <Benchmarking/Parsing/MicroBenchmarkSchemas.hpp>
#include <Benchmarking/Parsing/YamlAggregation.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>

namespace NES::ASP::Benchmarking {

YamlAggregation::YamlAggregation(const AGGREGATION_TYPE& type,
                                 const std::string& fieldNameAggregation,
                                 const std::string& fieldNameApproximate,
                                 const std::string& timeStampFieldName,
                                 const std::string& inputFile,
                                 const SchemaPtr& inputSchema,
                                 const SchemaPtr& outputSchema)
    : type(type), fieldNameAggregation(fieldNameAggregation), fieldNameApproximate(fieldNameApproximate),
      timeStampFieldName(timeStampFieldName), inputFile(inputFile), inputSchema(inputSchema), outputSchema(outputSchema) {}

std::string YamlAggregation::toString() {
    std::stringstream stringStream;
    stringStream << std::endl << " - type: " << magic_enum::enum_name(type)
                 << std::endl << " - fieldNameAggregation:" << fieldNameAggregation
                 << std::endl << " - fieldNameAccuracy:" << fieldNameApproximate
                 << std::endl << " - timeStampFieldName: " << timeStampFieldName
                 << std::endl << " - inputFile:" << inputFile
                 << std::endl << " - inputSchema:" << inputSchema->toString()
                 << std::endl << " - outputSchema:" << outputSchema->toString();


    return stringStream.str();
}

std::string YamlAggregation::getHeaderAsCsv() {
    return "type,fieldNameAggregation,fieldNameApproximate,timeStampFieldName,inputFile,inputSchema,outputSchema";
}

std::string YamlAggregation::getValuesAsCsv() {
    std::stringstream stringStream;
    stringStream << magic_enum::enum_name(type) << ","
                 << fieldNameAggregation << ","
                 << fieldNameApproximate << ","
                 << timeStampFieldName << ","
                 << inputFile << ","
                 << inputSchema->toString() << ","
                 << outputSchema->toString();

    return stringStream.str();
}

YamlAggregation YamlAggregation::createAggregationFromYamlNode(Yaml::Node& aggregationNode) {
    auto type = magic_enum::enum_cast<AGGREGATION_TYPE>(aggregationNode["type"].As<std::string>()).value();
    auto fieldNameAggregation = aggregationNode["fieldNameAgg"].As<std::string>();
    auto fieldNameAccuracy = aggregationNode["fieldNameAcc"].As<std::string>();
    auto inputFile = aggregationNode["inputFile"].As<std::string>();
    auto timeStampFieldName = aggregationNode["timestamp"].As<std::string>();

    auto inputSchema = inputFileSchemas[inputFile];
    auto outputSchema = accuracyFileSchemas[type];

    NES_ASSERT(outputSchema->get(fieldNameAggregation)->getDataType() == DataTypeFactory::createDouble(),
               "Currently we only support double as the aggregation field data type!");

    return YamlAggregation(type, fieldNameAggregation, fieldNameAccuracy, timeStampFieldName,
                           inputFile, inputSchema, outputSchema);
}

Runtime::Execution::Aggregation::AggregationFunctionPtr YamlAggregation::createAggregationFunction() {
    // Converting the DataType into a PhysicalDataType
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    auto inputType = defaultPhysicalTypeFactory.getPhysicalType(inputSchema->get(fieldNameAggregation)->getDataType());
    auto finalType = defaultPhysicalTypeFactory.getPhysicalType(outputSchema->get(fieldNameApproximate)->getDataType());

    switch (type) {
        case AGGREGATION_TYPE::MIN: return std::make_shared<Runtime::Execution::Aggregation::MinAggregationFunction>(inputType, finalType);
        case AGGREGATION_TYPE::MAX: return std::make_shared<Runtime::Execution::Aggregation::MaxAggregationFunction>(inputType, finalType);;
        case AGGREGATION_TYPE::SUM: return std::make_shared<Runtime::Execution::Aggregation::SumAggregationFunction>(inputType, finalType);;
        case AGGREGATION_TYPE::AVERAGE: return std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(inputType, finalType);;
        case AGGREGATION_TYPE::COUNT: return std::make_shared<Runtime::Execution::Aggregation::CountAggregationFunction>(inputType, finalType);;
        case AGGREGATION_TYPE::NONE: NES_NOT_IMPLEMENTED();
    }
}

} // namespace NES::ASP::Benchmarking

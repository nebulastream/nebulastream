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
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkSchemas.hpp>
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <filesystem>
#include <sstream>

namespace NES::ASP::Parsing {

SynopsisAggregationConfig::SynopsisAggregationConfig(const Aggregation_Type& type,
                                                     const std::string& fieldNameKey,
                                                     const std::string& fieldNameAggregation,
                                                     const std::string& fieldNameApproximate,
                                                     const std::string& timeStampFieldName,
                                                     const SchemaPtr& inputSchema,
                                                     const SchemaPtr& outputSchema)
    : type(type), fieldNameKey(fieldNameKey), fieldNameAggregation(fieldNameAggregation),
    fieldNameApproximate(fieldNameApproximate), timeStampFieldName(timeStampFieldName), inputSchema(inputSchema),
    outputSchema(outputSchema) {}

const std::string SynopsisAggregationConfig::toString() const {
    std::stringstream stringStream;
    stringStream << "type (" << magic_enum::enum_name(type) << ") "
                 << "fieldNameAggregation (" << fieldNameAggregation << ") "
                 << "fieldNameAccuracy (" << fieldNameApproximate << ") "
                 << "timeStampFieldName (" << timeStampFieldName << ") "
                 << "inputSchema (" << inputSchema->toString() << ") "
                 << "outputSchema (" << outputSchema->toString() << ")";

    return stringStream.str();
}

const std::string SynopsisAggregationConfig::getHeaderAsCsv() const {
    return "aggregation_type,aggregation_fieldNameAggregation,aggregation_fieldNameApproximate,aggregation_timeStampFieldName"
           ",aggregation_inputSchema,aggregation_outputSchema";
}

const std::string SynopsisAggregationConfig::getValuesAsCsv() const {
    std::stringstream stringStream;
    stringStream << magic_enum::enum_name(type) << ","
                 << fieldNameAggregation << ","
                 << fieldNameApproximate << ","
                 << timeStampFieldName << ","
                 << inputSchema->toString() << ","
                 << outputSchema->toString();

    return stringStream.str();
}

std::pair<SynopsisAggregationConfig, std::string>
SynopsisAggregationConfig::createAggregationFromYamlNode(Yaml::Node& aggregationNode,
                                                               const std::filesystem::path& data) {
    auto type = magic_enum::enum_cast<Aggregation_Type>(aggregationNode["type"].As<std::string>()).value();
    auto fieldNameKey = aggregationNode["fieldNameKey"].As<std::string>();
    auto fieldNameAggregation = aggregationNode["fieldNameAgg"].As<std::string>();
    auto fieldNameApprox = aggregationNode["fieldNameApprox"].As<std::string>();
    auto inputFile = data / std::filesystem::path(aggregationNode["inputFile"].As<std::string>());
    auto timeStampFieldName = aggregationNode["timestamp"].As<std::string>();

    auto inputSchema = Benchmarking::inputFileSchemas[inputFile.filename()];
    auto outputSchema = Benchmarking::getOutputSchemaFromTypeAndInputSchema(type, *inputSchema,
                                                                            fieldNameKey, fieldNameAggregation,
                                                                            fieldNameKey, fieldNameApprox);

    NES_ASSERT(inputSchema->get(timeStampFieldName)->getDataType()->isEquals(DataTypeFactory::createUInt64()),
               "The timestamp has to be a UINT64!");

    return {SynopsisAggregationConfig(type, fieldNameKey, fieldNameAggregation, fieldNameApprox, timeStampFieldName,
                           inputSchema, outputSchema), inputFile};
}

Runtime::Execution::Aggregation::AggregationFunctionPtr SynopsisAggregationConfig::createAggregationFunction() {
    auto inputType = getInputType();
    auto finalType = getFinalType();
    auto readFieldExpression = getReadFieldAggregationExpression();
    auto resultFieldIdentifier = fieldNameApproximate;


    switch (type) {
        case Aggregation_Type::MIN: return std::make_shared<Runtime::Execution::Aggregation::MinAggregationFunction>(inputType,
                                                                                                                     finalType,
                                                                                                                     readFieldExpression,
                                                                                                                     resultFieldIdentifier);
        case Aggregation_Type::MAX: return std::make_shared<Runtime::Execution::Aggregation::MaxAggregationFunction>(inputType,
                                                                                                                     finalType,
                                                                                                                     readFieldExpression,
                                                                                                                     resultFieldIdentifier);
        case Aggregation_Type::SUM: return std::make_shared<Runtime::Execution::Aggregation::SumAggregationFunction>(inputType,
                                                                                                                     finalType,
                                                                                                                     readFieldExpression,
                                                                                                                     resultFieldIdentifier);
        case Aggregation_Type::AVERAGE: return std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(inputType,
                                                                                                                         finalType,
                                                                                                                         readFieldExpression,
                                                                                                                         resultFieldIdentifier);
        case Aggregation_Type::COUNT: return std::make_shared<Runtime::Execution::Aggregation::CountAggregationFunction>(inputType,
                                                                                                                         finalType,
                                                                                                                         readFieldExpression,
                                                                                                                         resultFieldIdentifier);
        case Aggregation_Type::NONE: NES_NOT_IMPLEMENTED();
    }
}

SynopsisAggregationConfig::SynopsisAggregationConfig(const SynopsisAggregationConfig& other) {
    type = other.type;
    fieldNameApproximate = other.fieldNameApproximate;
    fieldNameAggregation = other.fieldNameAggregation;
    timeStampFieldName = other.timeStampFieldName;
    inputSchema = other.inputSchema;
    outputSchema = other.outputSchema;
}

SynopsisAggregationConfig& SynopsisAggregationConfig::operator=(const SynopsisAggregationConfig& other) {
    // If this is the same object, then return it
    if (this == &other) {
        return *this;
    }

    type = other.type;
    fieldNameApproximate = other.fieldNameApproximate;
    fieldNameAggregation = other.fieldNameAggregation;
    timeStampFieldName = other.timeStampFieldName;
    inputSchema = other.inputSchema;
    outputSchema = other.outputSchema;

    return *this;
}

SynopsisAggregationConfig SynopsisAggregationConfig::create(const Aggregation_Type& type,
                                                            const std::string& fieldNameKey,
                                                            const std::string& fieldNameAggregation,
                                                            const std::string& fieldNameApproximate,
                                                            const std::string& timestampFieldName,
                                                            const SchemaPtr& inputSchema,
                                                            const SchemaPtr& outputSchema) {
    return SynopsisAggregationConfig(type, fieldNameKey, fieldNameAggregation, fieldNameApproximate, timestampFieldName,
                                     inputSchema, outputSchema);
}

PhysicalTypePtr SynopsisAggregationConfig::getFinalType() const {
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    return defaultPhysicalTypeFactory.getPhysicalType(outputSchema->get(fieldNameApproximate)->getDataType());
}

PhysicalTypePtr SynopsisAggregationConfig::getInputType() const {
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    return defaultPhysicalTypeFactory.getPhysicalType(inputSchema->get(fieldNameAggregation)->getDataType());
}

std::shared_ptr<Runtime::Execution::Expressions::ReadFieldExpression>
SynopsisAggregationConfig::getReadFieldAggregationExpression() const {
    return std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(fieldNameAggregation);
}
std::shared_ptr<Runtime::Execution::Expressions::ReadFieldExpression>
SynopsisAggregationConfig::getReadFieldKeyExpression() const {
    return std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(fieldNameKey);
}

} // namespace NES::ASP::Parsing

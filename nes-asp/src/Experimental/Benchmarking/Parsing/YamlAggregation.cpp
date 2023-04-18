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
#include <Experimental/Benchmarking/Parsing/MicroBenchmarkSchemas.hpp>
#include <Experimental/Benchmarking/Parsing/YamlAggregation.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <filesystem>
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
    stringStream << " type (" << magic_enum::enum_name(type) << ") "
                 << "fieldNameAggregation (" << fieldNameAggregation << ") "
                 << "fieldNameAccuracy (" << fieldNameApproximate << ") "
                 << "timeStampFieldName (" << timeStampFieldName << ") "
                 << "inputFile (" << inputFile << ") "
                 << "inputSchema (" << inputSchema->toString() << ") "
                 << "outputSchema (" << outputSchema->toString() << ")";


    return stringStream.str();
}

std::string YamlAggregation::getHeaderAsCsv() {
    return "aggregation_type,aggregation_fieldNameAggregation,aggregation_fieldNameApproximate,aggregation_timeStampFieldName"
           ",aggregation_inputFile,aggregation_inputSchema,aggregation_outputSchema";
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

YamlAggregation YamlAggregation::createAggregationFromYamlNode(Yaml::Node& aggregationNode,
                                                               const std::filesystem::path& data) {
    auto type = magic_enum::enum_cast<AGGREGATION_TYPE>(aggregationNode["type"].As<std::string>()).value();
    auto fieldNameAggregation = aggregationNode["fieldNameAgg"].As<std::string>();
    auto fieldNameApprox = aggregationNode["fieldNameApprox"].As<std::string>();
    auto inputFile = data / std::filesystem::path(aggregationNode["inputFile"].As<std::string>());
    auto timeStampFieldName = aggregationNode["timestamp"].As<std::string>();

    auto inputSchema = inputFileSchemas[inputFile.filename()];
    auto outputSchema = getOutputSchemaFromTypeAndInputSchema(type, *inputSchema, fieldNameAggregation);

    NES_ASSERT(inputSchema->get(timeStampFieldName)->getDataType()->isEquals(DataTypeFactory::createUInt64()),
               "The timestamp has to be a UINT64!");

    return YamlAggregation(type, fieldNameAggregation, fieldNameApprox, timeStampFieldName,
                           inputFile, inputSchema, outputSchema);
}

Runtime::Execution::Aggregation::AggregationFunctionPtr YamlAggregation::createAggregationFunction() {
    // Converting the DataType into a PhysicalDataType
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    auto inputType = defaultPhysicalTypeFactory.getPhysicalType(inputSchema->get(fieldNameAggregation)->getDataType());
    auto finalType = defaultPhysicalTypeFactory.getPhysicalType(outputSchema->get(fieldNameApproximate)->getDataType());

    switch (type) {
        case AGGREGATION_TYPE::MIN: return std::make_shared<Runtime::Execution::Aggregation::MinAggregationFunction>(inputType, finalType);
        case AGGREGATION_TYPE::MAX: return std::make_shared<Runtime::Execution::Aggregation::MaxAggregationFunction>(inputType, finalType);
        case AGGREGATION_TYPE::SUM: return std::make_shared<Runtime::Execution::Aggregation::SumAggregationFunction>(inputType, finalType);
        case AGGREGATION_TYPE::AVERAGE: return std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(inputType, finalType);
        case AGGREGATION_TYPE::COUNT: return std::make_shared<Runtime::Execution::Aggregation::CountAggregationFunction>(inputType, finalType);
        case AGGREGATION_TYPE::NONE: NES_NOT_IMPLEMENTED();
    }
}

AggregationValuePtr YamlAggregation::createAggregationValue() {
    switch(type) {
        case AGGREGATION_TYPE::NONE: NES_THROW_RUNTIME_ERROR("Can not create aggregation value for the AGGREGATION_TYPE::NONE!");
        case AGGREGATION_TYPE::MIN: return createAggregationValueMin();
        case AGGREGATION_TYPE::MAX: return createAggregationValueMax();
        case AGGREGATION_TYPE::SUM: return createAggregationValueSum();
        case AGGREGATION_TYPE::AVERAGE: return createAggregationValueAverage();
        case AGGREGATION_TYPE::COUNT: return createAggregationValueCount();
    }
}
AggregationValuePtr YamlAggregation::createAggregationValueMin() {
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(outputSchema->get(fieldNameApproximate)->getDataType());
    auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalField);

    switch (basicType->nativeType) {
        case BasicPhysicalType::NativeType::UINT_8:
            return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint8_t>>();
        case BasicPhysicalType::NativeType::UINT_16:
            return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint16_t>>();
        case BasicPhysicalType::NativeType::UINT_32:
            return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint32_t>>();
        case BasicPhysicalType::NativeType::UINT_64:
            return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint64_t>>();
        case BasicPhysicalType::NativeType::INT_8:
            return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int8_t>>();
        case BasicPhysicalType::NativeType::INT_16:
            return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int16_t>>();
        case BasicPhysicalType::NativeType::INT_32:
            return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int32_t>>();
        case BasicPhysicalType::NativeType::INT_64:
            return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int64_t>>();
        case BasicPhysicalType::NativeType::FLOAT:
            return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<float_t>>();
        case BasicPhysicalType::NativeType::DOUBLE:
            return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<double_t>>();
        default: NES_THROW_RUNTIME_ERROR("Unsupported data type!");
    }
}

AggregationValuePtr YamlAggregation::createAggregationValueMax() {
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(outputSchema->get(fieldNameApproximate)->getDataType());
    auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalField);

    switch (basicType->nativeType) {
        case BasicPhysicalType::NativeType::UINT_8:
            return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint8_t>>();
        case BasicPhysicalType::NativeType::UINT_16:
            return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint16_t>>();
        case BasicPhysicalType::NativeType::UINT_32:
            return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint32_t>>();
        case BasicPhysicalType::NativeType::UINT_64:
            return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint64_t>>();
        case BasicPhysicalType::NativeType::INT_8:
            return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int8_t>>();
        case BasicPhysicalType::NativeType::INT_16:
            return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int16_t>>();
        case BasicPhysicalType::NativeType::INT_32:
            return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int32_t>>();
        case BasicPhysicalType::NativeType::INT_64:
            return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int64_t>>();
        case BasicPhysicalType::NativeType::FLOAT:
            return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<float_t>>();
        case BasicPhysicalType::NativeType::DOUBLE:
            return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<double_t>>();
        default: NES_THROW_RUNTIME_ERROR("Unsupported data type!");
    }
}

AggregationValuePtr YamlAggregation::createAggregationValueCount() {
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(outputSchema->get(fieldNameApproximate)->getDataType());
    auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalField);

    switch (basicType->nativeType) {
        case BasicPhysicalType::NativeType::UINT_8:
            return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint8_t>>();
        case BasicPhysicalType::NativeType::UINT_16:
            return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint16_t>>();
        case BasicPhysicalType::NativeType::UINT_32:
            return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint32_t>>();
        case BasicPhysicalType::NativeType::UINT_64:
            return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint64_t>>();
        case BasicPhysicalType::NativeType::INT_8:
            return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int8_t>>();
        case BasicPhysicalType::NativeType::INT_16:
            return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int16_t>>();
        case BasicPhysicalType::NativeType::INT_32:
            return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int32_t>>();
        case BasicPhysicalType::NativeType::INT_64:
            return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int64_t>>();
        case BasicPhysicalType::NativeType::FLOAT:
            return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<float_t>>();
        case BasicPhysicalType::NativeType::DOUBLE:
            return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<double_t>>();
        default: NES_THROW_RUNTIME_ERROR("Unsupported data type!");
    }
}

AggregationValuePtr YamlAggregation::createAggregationValueAverage() {
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(outputSchema->get(fieldNameApproximate)->getDataType());
    auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalField);

    switch (basicType->nativeType) {
        case BasicPhysicalType::NativeType::UINT_8:
            return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint8_t>>();
        case BasicPhysicalType::NativeType::UINT_16:
            return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint16_t>>();
        case BasicPhysicalType::NativeType::UINT_32:
            return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint32_t>>();
        case BasicPhysicalType::NativeType::UINT_64:
            return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint64_t>>();
        case BasicPhysicalType::NativeType::INT_8:
            return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int8_t>>();
        case BasicPhysicalType::NativeType::INT_16:
            return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int16_t>>();
        case BasicPhysicalType::NativeType::INT_32:
            return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int32_t>>();
        case BasicPhysicalType::NativeType::INT_64:
            return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int64_t>>();
        case BasicPhysicalType::NativeType::FLOAT:
            return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<float_t>>();
        case BasicPhysicalType::NativeType::DOUBLE:
            return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<double_t>>();
        default: NES_THROW_RUNTIME_ERROR("Unsupported data type!");
    }
}

AggregationValuePtr YamlAggregation::createAggregationValueSum() {
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(outputSchema->get(fieldNameApproximate)->getDataType());
    auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalField);

    switch (basicType->nativeType) {
        case BasicPhysicalType::NativeType::UINT_8:
            return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint8_t>>();
        case BasicPhysicalType::NativeType::UINT_16:
            return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint16_t>>();
        case BasicPhysicalType::NativeType::UINT_32:
            return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint32_t>>();
        case BasicPhysicalType::NativeType::UINT_64:
            return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint64_t>>();
        case BasicPhysicalType::NativeType::INT_8:
            return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int8_t>>();
        case BasicPhysicalType::NativeType::INT_16:
            return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int16_t>>();
        case BasicPhysicalType::NativeType::INT_32:
            return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int32_t>>();
        case BasicPhysicalType::NativeType::INT_64:
            return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int64_t>>();
        case BasicPhysicalType::NativeType::FLOAT:
            return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<float_t>>();
        case BasicPhysicalType::NativeType::DOUBLE:
            return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<double_t>>();
        default: NES_THROW_RUNTIME_ERROR("Unsupported data type!");
    }
}

YamlAggregation::YamlAggregation(const YamlAggregation& other) {
    type = other.type;
    fieldNameApproximate = other.fieldNameApproximate;
    fieldNameAggregation = other.fieldNameAggregation;
    timeStampFieldName = other.timeStampFieldName;
    inputFile = other.inputFile;
    inputSchema = other.inputSchema;
    outputSchema = other.outputSchema;
}

YamlAggregation& YamlAggregation::operator=(const YamlAggregation& other) {
    // If this is the same object, then return it
    if (this == &other) {
        return *this;
    }

    type = other.type;
    fieldNameApproximate = other.fieldNameApproximate;
    fieldNameAggregation = other.fieldNameAggregation;
    timeStampFieldName = other.timeStampFieldName;
    inputFile = other.inputFile;
    inputSchema = other.inputSchema;
    outputSchema = other.outputSchema;

    return *this;
}

} // namespace NES::ASP::Benchmarking

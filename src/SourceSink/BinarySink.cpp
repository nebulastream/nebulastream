#include <NodeEngine/TupleBuffer.hpp>
#include <SourceSink/BinarySink.hpp>
#include <DataTypes/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <DataTypes/PhysicalTypes/PhysicalType.hpp>
#include <SourceSink/BinarySink.hpp>
#include <Util/Logger.hpp>
#include <fstream>
#include <iostream>
#include <string>

namespace NES {
BinarySink::BinarySink(std::string filePath)
    : FileOutputSink(filePath) {
}

BinarySink::BinarySink(SchemaPtr schema, std::string filePath)
    : FileOutputSink(schema, filePath) {
}

bool BinarySink::writeData(TupleBuffer& input_buffer) {
    NES_DEBUG("BinarySink::writeData: write buffer tuples " << input_buffer.getNumberOfTuples());

    std::fstream outputFile(filePath, std::fstream::in | std::fstream::out | std::fstream::app);
    outputFile << this->outputBufferWithSchema(input_buffer, this->getSchema());
    outputFile.close();

    return true;
}

std::string BinarySink::outputBufferWithSchema(TupleBuffer& tupleBuffer, SchemaPtr schema) const {
    if (!tupleBuffer.isValid()) {
        return "INVALID_BUFFER_PTR";
    }

    std::stringstream str;
    std::vector<uint32_t> offsets;
    std::vector<PhysicalTypePtr> types;
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (uint32_t i = 0; i < schema->getSize(); ++i) {
        offsets.push_back(schema->get(i)->getFieldSize());
        NES_DEBUG("CodeGenerator: " + std::string("Field Size ") + schema->get(i)->toString() + std::string(": ") + std::to_string(schema->get(i)->getFieldSize()));
        types.push_back(physicalDataTypeFactory.getPhysicalType(schema->get(i)->getDataType()));
    }

    uint32_t prefix_sum = 0;
    for (uint32_t i = 0; i < offsets.size(); ++i) {
        uint32_t val = offsets[i];
        offsets[i] = prefix_sum;
        prefix_sum += val;
        NES_DEBUG("CodeGenerator: " + std::string("Prefix Sum: ") + schema->get(i)->toString() + std::string(": ") + std::to_string(offsets[i]));
    }

    str << "+----------------------------------------------------+" << std::endl;
    str << "|";
    for (uint32_t i = 0; i < schema->getSize(); ++i) {
        str << schema->get(i)->toString() << "|";
    }
    str << std::endl;
    str << "+----------------------------------------------------+" << std::endl;

    auto buf = tupleBuffer.getBufferAs<char>();
    for (uint32_t i = 0; i < tupleBuffer.getNumberOfTuples() * schema->getSchemaSizeInBytes();
         i += schema->getSchemaSizeInBytes()) {
        str << "|";
        for (uint32_t s = 0; s < offsets.size(); ++s) {
            void* value = &buf[i + offsets[s]];
            std::string tmp = types[s]->convertRawToString(value);
            str << tmp << "|";
        }
        str << std::endl;
    }
    str << "+----------------------------------------------------+";
    return str.str();
}
}// namespace NES

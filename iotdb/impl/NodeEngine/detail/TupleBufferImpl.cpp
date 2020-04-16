#include <NodeEngine/TupleBuffer.hpp>
#include <bitset>
#include <exception>
#include <boost/endian/buffers.hpp>  // see Synopsis below
#include <cstring>
#include <iostream>
#include <sstream>
#include <Util/Logger.hpp>

namespace NES {

namespace detail {

std::string printTupleBuffer(TupleBuffer& tbuffer, SchemaPtr schema) {
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto buffer = tbuffer.getBufferAs<char>();
    for (size_t i = 0; i < numberOfTuples; i++) {
        size_t offset = 0;
        for (size_t j = 0; j < schema->getSize(); j++) {
            auto field = schema->get(j);
            size_t fieldSize = field->getFieldSize();
            DataTypePtr ptr = field->getDataType();
            std::string str = ptr->convertRawToString(
                buffer + offset + i*schema->getSchemaSizeInBytes());
            ss << str.c_str();
            if (j < schema->getSize() - 1) {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << std::endl;
    }
    return ss.str();
}

/** possible types are:
 *  INT8,UINT8,INT16,UINT16,INT32,UINT32,INT64,FLOAT32,UINT64,FLOAT64,BOOLEAN,CHAR,DATE,VOID_TYPE
 *  Code for debugging:
 *  std::bitset<16> x(*orgVal);
 *  std::cout << "16-BEFORE biseq=" << x << " orgVal=" << *orgVal << endl;
 *  u_int16_t val = boost::endian::endian_reverse(*orgVal);
 *  std::bitset<16> x2(val);
 *  std::cout << "16-After biseq=" << x2 << " val=" << val << endl;
 *
 *  cout << "buffer=" << buffer << " offset=" << offset << " i=" << i
 *            << " tupleSize=" << tupleSize << " fieldSize=" << fieldSize
 *            << " res=" << (char*)buffer + offset + i * tupleSize << endl;
 */
void revertEndianness(TupleBuffer& tbuffer, SchemaPtr schema) {
    auto tupleSize = schema->getSchemaSizeInBytes();
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto buffer = tbuffer.getBufferAs<char>();
    for (size_t i = 0; i < numberOfTuples; i++) {
        size_t offset = 0;
        for (size_t j = 0; j < schema->getSize(); j++) {
            auto field = schema->get(j);
            size_t fieldSize = field->getFieldSize();
            //TODO: add enum with switch for performance reasons
            if (field->getDataType()->toString() == "UINT8") {
                u_int8_t* orgVal = (u_int8_t*) buffer + offset + i*tupleSize;
                memcpy((char*) buffer + offset + i*tupleSize, orgVal, fieldSize);
            } else if (field->getDataType()->toString() == "UINT16") {
                u_int16_t* orgVal = (u_int16_t*) ((char*) buffer + offset
                    + i*tupleSize);
                u_int16_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i*tupleSize, &val, fieldSize);
            } else if (field->getDataType()->toString() == "UINT32") {
                uint32_t* orgVal = (uint32_t*) ((char*) buffer + offset + i*tupleSize);
                uint32_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i*tupleSize, &val, fieldSize);
            } else if (field->getDataType()->toString() == "UINT64") {
                uint64_t* orgVal = (uint64_t*) ((char*) buffer + offset + i*tupleSize);
                uint64_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i*tupleSize, &val, fieldSize);
            } else if (field->getDataType()->toString() == "INT8") {
                int8_t* orgVal = (int8_t*) buffer + offset + i*tupleSize;
                int8_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i*tupleSize, &val, fieldSize);
            } else if (field->getDataType()->toString() == "INT16") {
                int16_t* orgVal = (int16_t*) ((char*) buffer + offset + i*tupleSize);
                int16_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i*tupleSize, &val, fieldSize);
            } else if (field->getDataType()->toString() == "INT32") {
                int32_t* orgVal = (int32_t*) ((char*) buffer + offset + i*tupleSize);
                int32_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i*tupleSize, &val, fieldSize);
            } else if (field->getDataType()->toString() == "INT64") {
                int64_t* orgVal = (int64_t*) ((char*) buffer + offset + i*tupleSize);
                int64_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i*tupleSize, &val, fieldSize);
            } else if (field->getDataType()->toString() == "FLOAT32") {
                NES_WARNING(
                    "TupleBuffer::revertEndianness: float conversation is not totally supported, please check results")
                uint32_t* orgVal = (uint32_t*) ((char*) buffer + offset + i*tupleSize);
                uint32_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i*tupleSize, &val, fieldSize);
            } else if (field->getDataType()->toString() == "FLOAT64") {
                NES_WARNING(
                    "TupleBuffer::revertEndianness: double conversation is not totally supported, please check results")
                uint64_t* orgVal = (uint64_t*) ((char*) buffer + offset + i*tupleSize);
                uint64_t val = boost::endian::endian_reverse(*orgVal);
                memcpy((char*) buffer + offset + i*tupleSize, &val, fieldSize);
            } else if (field->getDataType()->toString() == "CHAR") {
                //TODO: I am not sure if we have to convert char at all because it is one byte only
                throw new Exception(
                    "Data type float is currently not supported for endian conversation");
            } else {
                throw new Exception(
                    "Data type " + field->getDataType()->toString()
                        + " is currently not supported for endian conversation");
            }
            offset += fieldSize;
        }
    }
}

}

}
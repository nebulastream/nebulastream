#include <bitset>
#include <NodeEngine/TupleBuffer.hpp>
#include <exception>
#include <boost/endian/buffers.hpp>  // see Synopsis below
#include <cstring>
#include <iostream>
#include <sstream>
#include <Util/Logger.hpp>
using namespace std;

namespace NES {

TupleBuffer::TupleBuffer(void *_buffer, const size_t _buffer_size_bytes,
                         const uint32_t _tupleSizeBytes,
                         const uint32_t _numTuples)
    :
    buffer(_buffer),
    bufferSizeInBytes(_buffer_size_bytes),
    tupleSizeInBytes(_tupleSizeBytes),
    numberOfTuples(_numTuples),
    useCnt(0) {
}
TupleBuffer::~TupleBuffer()
{
  NES_DEBUG("TupleBuffer deconstruct " << this);
}
void TupleBuffer::copyInto(const TupleBufferPtr other) {
  if (other && other.get() != this) {
    bufferSizeInBytes = other->bufferSizeInBytes.load();
    tupleSizeInBytes = other->tupleSizeInBytes.load();
    numberOfTuples = other->numberOfTuples.load();
    std::memcpy(this->buffer, other->buffer, other->bufferSizeInBytes);
  }
}

TupleBuffer& TupleBuffer::operator=(const TupleBuffer &other) {
  NES_WARNING("TupleBuffer::operator=: " << this)
  if (this != &other) {
    bufferSizeInBytes = other.bufferSizeInBytes.load();
    tupleSizeInBytes = other.tupleSizeInBytes.load();
    numberOfTuples = other.numberOfTuples.load();
    std::memcpy(this->buffer, other.buffer, other.bufferSizeInBytes);
  }
  return *this;
}

void TupleBuffer::print() {
  std::cout << "buffer address=" << buffer << std::endl;
  std::cout << "buffer size=" << bufferSizeInBytes << std::endl;
  std::cout << "buffer tuple_size_bytes=" << tupleSizeInBytes << std::endl;
  std::cout << "buffer num_tuples=" << numberOfTuples << std::endl;
}

size_t TupleBuffer::getNumberOfTuples() {
  return numberOfTuples;
}

void TupleBuffer::setNumberOfTuples(size_t number) {
  numberOfTuples = number;
}

void* TupleBuffer::getBuffer() {
  return buffer;
}

size_t TupleBuffer::getBufferSizeInBytes() {
  return bufferSizeInBytes;
}

void TupleBuffer::setBufferSizeInBytes(size_t size) {
  bufferSizeInBytes = size;
}

size_t TupleBuffer::getTupleSizeInBytes() {
  return tupleSizeInBytes;
}

void TupleBuffer::setTupleSizeInBytes(size_t size) {
  tupleSizeInBytes = size;
}

void TupleBuffer::setUseCnt(size_t size) {
  useCnt = size;
}

size_t TupleBuffer::getUseCnt() {
  return useCnt;
}

bool TupleBuffer::decrementUseCntAndTestForZero() {
  if (useCnt >= 1) {
    useCnt--;
  } else
  {
    //TODO: I am not sure if we should break here, however we will fix this when we remove shared_ptr from this
    NES_WARNING("TupleBuffer::decrementUseCntAndTestForZero: decrease a buffer which has already 0 count")
  }


  return useCnt == 0;
}

void TupleBuffer::incrementUseCnt() {
  useCnt++;
}

std::string TupleBuffer::printTupleBuffer(Schema schema) {
  std::stringstream ss;
  for (size_t i = 0; i < numberOfTuples; i++) {
    size_t offset = 0;
    for (size_t j = 0; j < schema.getSize(); j++) {
      auto field = schema[j];
      size_t fieldSize = field->getFieldSize();
      DataTypePtr ptr = field->getDataType();
      std::string str = ptr->convertRawToString(
              reinterpret_cast<uint8_t*>(buffer) + offset + i * schema.getSchemaSize());
      ss << str.c_str();
      if (j < schema.getSize() - 1) {
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
void TupleBuffer::revertEndianness(Schema schema) {
  size_t tupleSize = schema.getSchemaSize();
  for (size_t i = 0; i < numberOfTuples; i++) {
    size_t offset = 0;
    for (size_t j = 0; j < schema.getSize(); j++) {
      auto field = schema[j];
      size_t fieldSize = field->getFieldSize();
      //TODO: add enum with switch for performance reasons
      if (field->getDataType()->toString() == "UINT8") {
        u_int8_t* orgVal = (u_int8_t*) buffer + offset + i * tupleSize;
        memcpy((char*) buffer + offset + i * tupleSize, orgVal, fieldSize);
      } else if (field->getDataType()->toString() == "UINT16") {
        u_int16_t* orgVal = (u_int16_t*) ((char*) buffer + offset
            + i * tupleSize);
        u_int16_t val = boost::endian::endian_reverse(*orgVal);
        memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
      } else if (field->getDataType()->toString() == "UINT32") {
        uint32_t* orgVal = (uint32_t*) ((char*) buffer + offset + i * tupleSize);
        uint32_t val = boost::endian::endian_reverse(*orgVal);
        memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
      } else if (field->getDataType()->toString() == "UINT64") {
        uint64_t* orgVal = (uint64_t*) ((char*) buffer + offset + i * tupleSize);
        uint64_t val = boost::endian::endian_reverse(*orgVal);
        memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
      } else if (field->getDataType()->toString() == "INT8") {
        int8_t* orgVal = (int8_t*) buffer + offset + i * tupleSize;
        int8_t val = boost::endian::endian_reverse(*orgVal);
        memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
      } else if (field->getDataType()->toString() == "INT16") {
        int16_t* orgVal = (int16_t*) ((char*) buffer + offset + i * tupleSize);
        int16_t val = boost::endian::endian_reverse(*orgVal);
        memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
      } else if (field->getDataType()->toString() == "INT32") {
        int32_t* orgVal = (int32_t*) ((char*) buffer + offset + i * tupleSize);
        int32_t val = boost::endian::endian_reverse(*orgVal);
        memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
      } else if (field->getDataType()->toString() == "INT64") {
        int64_t* orgVal = (int64_t*) ((char*) buffer + offset + i * tupleSize);
        int64_t val = boost::endian::endian_reverse(*orgVal);
        memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
      } else if (field->getDataType()->toString() == "FLOAT32") {
        NES_WARNING("TupleBuffer::revertEndianness: float conversation is not totally supported, please check results")
        uint32_t* orgVal = (uint32_t*) ((char*) buffer + offset + i * tupleSize);
        uint32_t val = boost::endian::endian_reverse(*orgVal);
        memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
      } else if (field->getDataType()->toString() == "FLOAT64") {
        NES_WARNING("TupleBuffer::revertEndianness: double conversation is not totally supported, please check results")
        uint64_t* orgVal = (uint64_t*) ((char*) buffer + offset + i * tupleSize);
        uint64_t val = boost::endian::endian_reverse(*orgVal);
        memcpy((char*) buffer + offset + i * tupleSize, &val, fieldSize);
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

// namespace NES


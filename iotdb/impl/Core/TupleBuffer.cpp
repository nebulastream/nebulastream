#include <NodeEngine/TupleBuffer.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::TupleBuffer);
#include <cstring>
#include <iostream>
#include <sstream>
namespace NES {

TupleBuffer::TupleBuffer(void *_buffer, const size_t _buffer_size_bytes,
                         const uint32_t _tuple_size_bytes,
                         const uint32_t _num_tuples)
    :
    buffer(_buffer),
    bufferSizeInBytes(_buffer_size_bytes),
    tupleSizeInBytes(_tuple_size_bytes),
    numberOfTuples(_num_tuples),
    useCnt(0){
}

void TupleBuffer::copyInto(const TupleBufferPtr other) {
  if (other && other.get() != this) {
    this->bufferSizeInBytes = other->bufferSizeInBytes;
    this->tupleSizeInBytes = other->tupleSizeInBytes;
    this->numberOfTuples = other->numberOfTuples;
    std::memcpy(this->buffer, other->buffer, other->bufferSizeInBytes);
  }
}

TupleBuffer& TupleBuffer::operator=(const TupleBuffer &other) {
  if (this != &other) {
    this->bufferSizeInBytes = other.bufferSizeInBytes;
    this->tupleSizeInBytes = other.tupleSizeInBytes;
    this->numberOfTuples = other.numberOfTuples;
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
  if(useCnt >= 1)
  {
    useCnt--;
  }
  else
    assert(0);

  return useCnt == 0;
}

bool TupleBuffer::incrementUseCnt() {
  //TODO: should this be thread save?
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
          buffer + offset + i * schema.getSchemaSize());
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
}

// namespace NES

#ifndef INCLUDE_TUPLEBUFFER_H_
#define INCLUDE_TUPLEBUFFER_H_
#include <cstdint>
#include <memory>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

namespace iotdb {

//forward declaration
class TupleBuffer;
typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;

/**
 * @brief this class handles the abstraction of tuples in byte buffer
 * @limitations
 */
class TupleBuffer {
 public:
  TupleBuffer(void* buffer, const uint64_t buffer_size,
              const uint32_t tuple_size_bytes, const uint32_t num_tuples);
  TupleBuffer& operator=(const TupleBuffer&);
  void copyInto(const TupleBufferPtr&);

  void print();

  size_t getNumberOfTuples();
  void* getBuffer();

 private:
  TupleBuffer()
      : buffer(nullptr),
        buffer_size(0),
        tuple_size_bytes(0),
        num_tuples(0) {
  }
  ;
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive & ar, const unsigned int version) {
//        ar & char(*)buffer;
    ar & buffer_size;
    ar & tuple_size_bytes;
    ar & num_tuples;
  }

  void* buffer;
  size_t buffer_size;
  size_t tuple_size_bytes;
  size_t num_tuples;
};

class Schema;
std::string toString(const TupleBuffer& buffer, const Schema& schema);
std::string toString(const TupleBuffer* buffer, const Schema& schema);

}  // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::TupleBuffer)
#endif /* INCLUDE_TUPLEBUFFER_H_ */

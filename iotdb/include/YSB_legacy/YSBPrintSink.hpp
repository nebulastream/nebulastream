#ifndef YSB_PRINTSINK_HPP
#define YSB_PRINTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <SourceSink/PrintSink.hpp>

namespace iotdb {

class YSBPrintSink : public PrintSink {
 public:
  YSBPrintSink();
  ~YSBPrintSink();

  bool writeData(const TupleBufferPtr input_buffer);
  void setup() override {};
  void shutdown() override {};
  const std::string toString() const override;

 protected:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & boost::serialization::base_object<PrintSink>(*this);
  }
};

} // namespace iotdb

#endif // YSB_PRINTSINK_HPP

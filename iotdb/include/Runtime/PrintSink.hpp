#ifndef PRINTSINK_HPP
#define PRINTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Runtime/DataSink.hpp>

namespace iotdb {

class PrintSink : public DataSink {

public:
  PrintSink(const Schema& schema);
  ~PrintSink();
  virtual void setup(){}
  virtual void shutdown(){}

  bool writeData(const TupleBuffer* input_buffer) override;

  const std::string toString() const override;

  template<class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
      ar & boost::serialization::base_object<DataSink>(*this);

  }
protected:
  friend class boost::serialization::access;
  PrintSink(){};

};
} // namespace iotdb
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::PrintSink)


#endif // ZMQSINK_HPP

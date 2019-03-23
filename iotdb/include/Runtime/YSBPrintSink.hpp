#ifndef YSB_PRINTSINK_HPP
#define YSB_PRINTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Runtime/PrintSink.hpp>

namespace iotdb {

class YSBPrintSink : public PrintSink {
public:

	~YSBPrintSink();
	YSBPrintSink();
  bool writeData(const TupleBuffer* input_buffer) override;

  void setup(){};
  void shutdown(){};

  const std::string toString() const override;

protected:
    friend class boost::serialization::access;
    template<class Archive>
	void serialize(Archive & ar, const unsigned int version)
	{
		ar & boost::serialization::base_object<PrintSink>(*this);
	}


};

} // namespace iotdb
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::YSBPrintSink)


#endif // ZMQSINK_HPP

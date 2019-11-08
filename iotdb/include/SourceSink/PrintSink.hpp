#ifndef PRINTSINK_HPP
#define PRINTSINK_HPP

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include "../SourceSink/DataSink.hpp"

namespace iotdb {

class PrintSink : public DataSink {

  public:
    PrintSink(std::ostream& pOutputStream = std::cout);
    PrintSink(const Schema& pSchema, std::ostream& pOutputStream = std::cout);
    ~PrintSink();
    virtual void setup() override {}
    virtual void shutdown() override {}

    bool writeData(const TupleBufferPtr input_buffer);
    const std::string toString() const override;

  protected:
    friend class boost::serialization::access;

    template <class Archive> void serialize(Archive& ar, const unsigned int version)
    {
        ar& boost::serialization::base_object<DataSink>(*this);
    }

  private:
    std::ostream& outputStream;
};

} // namespace iotdb

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::PrintSink)

#endif // PRINTSINK_HPP

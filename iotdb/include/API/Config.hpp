#pragma once

#include <string>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>

namespace iotdb {

class Config {
  public:
    static Config create();

    Config& setBufferCount(size_t bufferCount);
    size_t getBufferCount();

    Config& setBufferSizeInByte(size_t bufferSizeInByte);
    size_t getBufferSizeInByte();

    Config& setNumberOfWorker(size_t numberOfWorker);
    size_t getNumberOfWorker();

    void print();
    Config();

  private:
    friend class boost::serialization::access;
    template <class Archive> void serialize(Archive& ar, const unsigned int version)
    {
        ar& numberOfWorker;
        ar& bufferCount;
        ar& bufferSizeInByte;
    }

    size_t numberOfWorker;
    size_t bufferCount;
    size_t bufferSizeInByte;

};
} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::Config)


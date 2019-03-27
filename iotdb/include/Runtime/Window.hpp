#ifndef INCLUDE_RUNTIME_WINDOW_HPP_
#define INCLUDE_RUNTIME_WINDOW_HPP_
#include <Util/Logger.hpp>
#include <atomic>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <memory>

namespace iotdb {
class Window;
typedef std::shared_ptr<Window> WindowPtr;

class Window {
  public:
    virtual ~Window();
    virtual void setup() = 0;
    virtual void print() = 0;
    virtual void shutdown() = 0;
    virtual size_t getNumberOfEntries() = 0;

    template <class Archive> void serialize(Archive& ar, const unsigned int version) {}

  private:
    friend class boost::serialization::access;
};
const WindowPtr createTestWindow(size_t pCampaingCnt);

} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::Window)
#endif /* INCLUDE_RUNTIME_WINDOW_HPP_ */

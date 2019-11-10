#ifndef INCLUDE_WINDOW_LEGACY_WINDOW_HPP_
#define INCLUDE_WINDOW_LEGACY_WINDOW_HPP_

#include <atomic>
#include <iostream>
#include <memory>

#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>

#include <Util/Logger.hpp>

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
const WindowPtr createTestWindow(size_t pCampaingCnt, size_t windowSizeInSec);

} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::Window)
#endif /* INCLUDE_WINDOW_LEGACY_WINDOW_HPP_ */

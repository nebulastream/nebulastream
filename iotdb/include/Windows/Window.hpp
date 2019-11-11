#ifndef INCLUDE_WINDOWS_WINDOW_HPP_
#define INCLUDE_WINDOWS_WINDOW_HPP_

#include <atomic>
#include <iostream>
#include <memory>

#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <API/Window/WindowDefinition.hpp>

#include <Util/Logger.hpp>
#include <thread>

namespace iotdb {
class Window;
typedef std::shared_ptr<Window> WindowPtr;

class Window {
  public:
    virtual ~Window();
    virtual void setup() = 0;
    virtual bool start();
    virtual bool stop();
    virtual void trigger();

    virtual void print() = 0;

    virtual size_t getNumberOfEntries() = 0;

    template <class Archive> void serialize(Archive& ar, const unsigned int version) {}

  private:
    friend class boost::serialization::access;
    bool running;
    WindowDefinitionPtr window_difinition;
    std::thread thread;
};
//just for test compability
const WindowPtr createTestWindow(size_t campainCnt, size_t windowSizeInSec);

} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::Window)
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */

#ifndef NES_ZMQCLIENTCACHE_HPP
#define NES_ZMQCLIENTCACHE_HPP

#include "NetworkCommon.hpp"
#include <boost/thread/tss.hpp>

namespace NES {
namespace Network {

class OutputChannel;

class ZmqClientCache {
public:
    OutputChannel createOutputChannel();
private:

};


}

}



#endif //NES_ZMQCLIENTCACHE_HPP

#ifndef NESTOPOLOGYSERVICE_HPP
#define NESTOPOLOGYSERVICE_HPP

#include <cpprest/json.h>

namespace NES {

class NESTopologyService;
typedef std::shared_ptr<NESTopologyService> NESTopologyServicePtr;

/**\brief:
*          This class is used for serving different requests related to nes topology.
*
* Note: Please extend the header if new topology related functionality needed to be added.
*
*/
class NESTopologyService {

  private:
    NESTopologyService() = default;

  public:
    ~NESTopologyService() = default;

    static NESTopologyServicePtr getInstance() {
        static NESTopologyServicePtr instance{new NESTopologyService};
        return instance;
    }

    /**
     * This method is used for getting the nes topology as json string.
     *
     * @return a json object representing the nes topology
     */
    web::json::value getNESTopologyAsJson();
};
}// namespace NES

#endif//NESTOPOLOGYSERVICE_HPP

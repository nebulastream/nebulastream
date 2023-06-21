#ifndef TOPOLOGYPREDICTION_PREDICTEDVIEWS_TOPOLOGYCHANGE_HPP_
#define TOPOLOGYPREDICTION_PREDICTEDVIEWS_TOPOLOGYCHANGE_HPP_

#include <set>
#include <cstdint>
#include <memory>
#include <unordered_set>
#include <Topology/Predictions/Edge.hpp>
namespace NES::Experimental {
namespace TopologyPrediction {
struct TopologyDelta;
class TopologyChangeLog;
using TopologyChangeLogPtr = std::shared_ptr<TopologyChangeLog>;
using Timestamp = uint64_t;
class AggregatedTopologyChangeLog;
class TopologyChangeLog {
  public:
    explicit TopologyChangeLog(Timestamp time);

    void insert(const TopologyDelta& newDelta);

    void erase(const TopologyDelta& delta);

    std::string toString();

    AggregatedTopologyChangeLog getChangeLog();

    //TopologyChangeLogPtr getPreviousChange();

    //void setPreviousChange(TopologyChangeLogPtr change);

    bool empty();

    Timestamp getTime();

    //todo: implement getPath() function

  protected:
    TopologyChangeLogPtr previousChange;
    Timestamp time;

  private:
    std::unordered_set<Edge> changelogAdded;
    std::unordered_set<Edge> changelogRemoved;
};
}
}
#endif //TOPOLOGYPREDICTION_PREDICTEDVIEWS_TOPOLOGYCHANGE_HPP_

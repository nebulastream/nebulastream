#ifndef TOPOLOGYPREDICTION__TOPOLOGYVERSIONDELTALIST_HPP_
#define TOPOLOGYPREDICTION__TOPOLOGYVERSIONDELTALIST_HPP_
#include "AggregatedTopologyChangeLog.hpp"
#include <absl/container/btree_map.h>
#include <memory>

namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Experimental::TopologyPrediction {
using Timestamp = uint64_t;
struct TopologyDelta;
class TopologyChangeLog;
using TopologyChangeLogPtr = std::shared_ptr<TopologyChangeLog>;

class TopologyVersionTimeline {
  public:
    explicit TopologyVersionTimeline(TopologyPtr originalTopology);

    void addTopologyDelta(Timestamp predictedTime, const TopologyDelta& delta);

    TopologyPtr getTopologyVersion(Timestamp time);

    std::string predictionsToString();

    bool removeTopologyDelta(Timestamp predictedTime, const TopologyDelta& delta);

    static TopologyPtr copyTopology(const TopologyPtr&);

  private:
    TopologyPtr originalTopology;
    //Timestamp time;
    //absl::btree_map<Timestamp, TopologyChangeLogPtr> changeMap;
    absl::btree_map<Timestamp, AggregatedTopologyChangeLog> changeMap;
    TopologyPtr createTopologyVersion(TopologyPtr originalTopology, AggregatedTopologyChangeLog changeLog);
    std::optional<AggregatedTopologyChangeLog> getTopologyChangeLogBefore(Timestamp time);
    AggregatedTopologyChangeLog getOrCreateTopologyChangeLogAt(Timestamp predictedTime);
    //std::optional<AggregatedTopologyChangeLog&> getTopologyChangeLogAt(Timestamp time);
    bool removeTopologyChangeLogAt(Timestamp time);
    AggregatedTopologyChangeLog createAggregatedChangeLog(Timestamp time);
};
}// namespace Experimental::TopologyPrediction
}// namespace NES
#endif//TOPOLOGYPREDICTION__TOPOLOGYVERSIONDELTALIST_HPP_

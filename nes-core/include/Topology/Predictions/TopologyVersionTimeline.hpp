#ifndef TOPOLOGYPREDICTION__TOPOLOGYVERSIONDELTALIST_HPP_
#define TOPOLOGYPREDICTION__TOPOLOGYVERSIONDELTALIST_HPP_

#include "AggregatedTopologyChangeLog.hpp"
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

  void addTopologyChange(Timestamp predictedTime, const TopologyDelta &delta);

  TopologyPtr getTopologyVersion(Timestamp time);

  std::string toString(Timestamp viewTime);

  std::string predictionsToString();

  Timestamp getTime();

  bool removeTopologyChange(Timestamp predictedTime, const TopologyDelta &delta);

  [[maybe_unused]] void incrementTime(Timestamp increment);

  static TopologyPtr copyTopology(const TopologyPtr&);

 protected:
  TopologyPtr originalTopology;
  Timestamp time;

 private:
  TopologyChangeLogPtr latestChange;
   static void applyAggregatedChangeLog(TopologyPtr topology,
                                 NES::Experimental::TopologyPrediction::AggregatedTopologyChangeLog changeLog);
  TopologyPtr createTopologyVersion(TopologyPtr originalTopology, AggregatedTopologyChangeLog changeLog);
};
}
}
#endif //TOPOLOGYPREDICTION__TOPOLOGYVERSIONDELTALIST_HPP_

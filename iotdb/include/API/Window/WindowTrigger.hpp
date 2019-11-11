#ifndef IOTDB_INCLUDE_API_WINDOW_WINDOWTRIGGER_HPP_
#define IOTDB_INCLUDE_API_WINDOW_WINDOWTRIGGER_HPP_

#include <API/Window/WindowDefinition.hpp>
#include <QueryLib/WindowManagerLib.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <thread>
namespace iotdb {

class WindowTrigger;
typedef std::shared_ptr<WindowTrigger> WindowTriggerPtr;

class WindowTrigger {
  ~WindowTrigger();
  void setup();
  void start();
  void print();
  void shutdown();
  void trigger();
  size_t getNumberOfEntries();
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {}
  static WindowTriggerPtr createWindowTrigger(WindowDefinitionPtr window_definition_ptr, void* window_state);

 private:
  friend class boost::serialization::access;
  bool running;
  WindowDefinitionPtr window_definition_ptr;
  WindowManagerPtr window_manager_ptr_;
  void* window_state;
  uint64_t last_watermark;
  std::thread thread;

  template<class FinalAggregateType, class PartialAggregateType>
  std::vector<FinalAggregateType> aggregateWindows(uint64_t watermark,
                                                         WindowSliceStore<PartialAggregateType> *store){

    auto windows = std::make_shared<std::vector<WindowState>>();
    this->window_manager_ptr_->GetWindowDefinition()->windowType->triggerWindows(windows, 0, watermark);
    auto finalAggregates = std::vector<FinalAggregateType>();
    auto slices = store->getSliceMetadata();
    auto partialAggregates = store->getPartialAggregates();
    for (int s_id = 0; s_id < slices.size(); s_id++) {
      for (int w_id = 0; w_id < windows->size(); w_id++) {
        auto window = (*windows)[w_id];
        // A slice is contained in a window if the window starts before the slice and ends after the slice
        if (window.getStartTs() <= slices[s_id].getStartTs() &&
            window.getEndTs() >= slices[s_id].getEndTs()) {

          if (Sum *sumAggregation = dynamic_cast<Sum *>(this->window_manager_ptr_->GetWindowDefinition()->windowAggregation.get())) {
            auto partialAggregate = partialAggregates[s_id];
            finalAggregates[s_id] =
                sumAggregation->combine<PartialAggregateType>(finalAggregates[s_id], partialAggregate);
          }
          //auto combined = store.combine<PartialAggregateType>(partial, partial2);
        }
      }
    }
    return finalAggregates;
  };
};
};
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::WindowTrigger)

#endif //IOTDB_INCLUDE_API_WINDOW_WINDOWTRIGGER_HPP_

#include "WorkQueues/RequestTypes/TwoPhaseLockingStorageManager.hpp"

/**
 * manage locks for all coordinator data structures needed by the requests
 * https://stackoverflow.com/questions/23610561/return-locked-resource-from-class-with-automatic-unlocking#comment36245473_23610561
 */

public:
  TopologyHandle getTopology();
  /*
   * ...
   */

private:
  /**
   * either keep one mutex for each structure here or expose the existing mutexes in the respective structures
   *
   */



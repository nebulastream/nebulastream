#ifndef INCLUDE_RUNTIME_WINDOW_HPP_
#define INCLUDE_RUNTIME_WINDOW_HPP_
#include <Util/Logger.hpp>
#include <atomic>
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

private:
};
const WindowPtr createTestWindow(size_t pWindowSizeInSec = 2, size_t pCampaignCnt = 10);

class YSBWindow : public Window {
public:
  YSBWindow(size_t pWindowSizeInSec, size_t pCampaignCnt)
      : windowSizeInSec(pWindowSizeInSec), campaignCnt(pCampaignCnt), currentWindow(0), lastTimestamp(time(nullptr)){};
  ~YSBWindow();

  void setup() {
    hashTable = new std::atomic<size_t> *[2];
    hashTable[0] = new std::atomic<size_t>[campaignCnt + 1];
    for (size_t i = 0; i < campaignCnt + 1; i++)
      std::atomic_init(&hashTable[0][i], std::size_t(0));

    hashTable[1] = new std::atomic<size_t>[campaignCnt + 1];
    for (size_t i = 0; i < campaignCnt + 1; i++)
      std::atomic_init(&hashTable[1][i], std::size_t(0));
  }

  void print() {
    std::cout << "print" << std::endl;
    IOTDB_INFO("Hash Table Content with window 1:")
    for (size_t i = 0; i < campaignCnt; i++) {

      IOTDB_INFO("id=" << i << " cnt=" << hashTable[0][i])
    }

    IOTDB_INFO("Hash Table Content with window 2:")
    for (size_t i = 0; i < campaignCnt; i++) {

      IOTDB_INFO("id=" << i << " cnt=" << hashTable[1][i])
    }
  }

  void shutdown() {
    IOTDB_INFO("Final Window Result:");
    print();
    // maybe also delete the entries?
    delete[] hashTable[0];
    delete[] hashTable[1];
  }

  std::atomic<size_t> **getHashTable() { return hashTable; };

  const size_t getWindowSizeSec() const { return windowSizeInSec; }

  const size_t getCampaignCount() const { return campaignCnt; }

  const size_t getCurrentWindow() const { return currentWindow; }

  void setCurrentWindow(size_t pCurrentWindow) { currentWindow = pCurrentWindow; }

  const size_t getLastTimestamp() const { return lastTimestamp; }

  void setLastTimestamp(size_t pLastTimestamp) { lastTimestamp = pLastTimestamp; }

private:
  std::atomic<size_t> **hashTable;
  size_t windowSizeInSec;
  size_t campaignCnt;
  size_t currentWindow;
  size_t lastTimestamp;
};

} // namespace iotdb
#endif /* INCLUDE_RUNTIME_WINDOW_HPP_ */

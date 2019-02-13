#ifndef API_TIME_H
#define API_TIME_H

#include <cstdio>

namespace iotdb {

class Time {
public:
  static Time seconds(size_t seconds);
  static Time minutes(size_t minutes);
  size_t time;

private:
  Time(size_t seconds) : time(seconds) {}
};
}
#endif // API_TIME_H

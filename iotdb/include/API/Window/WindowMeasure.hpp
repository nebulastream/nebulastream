#ifndef IOTDB_INCLUDE_API_WINDOW_WINDOWMEASURE_HPP_
#define IOTDB_INCLUDE_API_WINDOW_WINDOWMEASURE_HPP_

namespace iotdb {

/**
 * Defines the measure of a window, common measures are time and count.
 */
class WindowMeasure {

};

/**
 * A time based window measure.
 */
class TimeMeasure : public WindowMeasure {
 public:
  u_int64_t getTime() const;

 protected:
  TimeMeasure(const uint64_t ms);

 private:
  const uint64_t _ms;
};

class Milliseconds : public TimeMeasure {
 public:
  Milliseconds(uint64_t milliseconds);
};

class Seconds : public Milliseconds {
 public:
  Seconds(uint64_t seconds);
};

class Minutes : public Seconds {
 public:
  Minutes(uint64_t milliseconds);
};

class Hours : public Minutes {
 public:
  Hours(uint64_t hours);
};

}

#endif //IOTDB_INCLUDE_API_WINDOW_WINDOWMEASURE_HPP_

//
// Created by pgrulich on 16.07.19.
//

#ifndef IOTDB_WINDOWDEFINITION_H
#define IOTDB_WINDOWDEFINITION_H

class WindowMeasure {

};

class TimeMeasure : public WindowMeasure {
private:
    uint64_t ms;
};

class Milliseconds : public TimeMeasure {
public:
    Milliseconds(uint64_t milliseconds);
};

class Seconds : public TimeMeasure {
public:
    Seconds(uint64_t seconds);
};

class Minutes : public TimeMeasure {
public:
    Minutes(uint64_t milliseconds);
};

class Hours : public TimeMeasure {
public:
    Hours(uint64_t hours);
};


class WindowType {

};

class TumblingWindow : public WindowType {
public:
    static TumblingWindow &of(WindowMeasure size);
};

class SlidingWindow : public WindowType {
public:
    static TumblingWindow &of(WindowMeasure size, WindowMeasure slide);
};

class SessionWindow : public WindowType {
public:
    static TumblingWindow &of(TimeMeasure gap);
};


class WindowAggregation {
public:
    WindowAggregation(Field onField);
    WindowAggregation& as(Field asField);
private:
    const Field& onField;
    const Field& asField;
};

class Sum : public WindowAggregation {
public:
    Sum(Field onField);
};

#endif //IOTDB_WINDOWDEFINITION_H

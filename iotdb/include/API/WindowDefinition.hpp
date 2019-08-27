//
// Created by pgrulich on 16.07.19.
//


#ifndef IOTDB_WINDOWDEFINITION_HPP
#define IOTDB_WINDOWDEFINITION_HPP

#include <API/ParameterTypes.hpp>

namespace iotdb {
    class WindowMeasure {

    };

    class TimeMeasure : public WindowMeasure {
    protected:
        TimeMeasure(uint64_t ms);
    private:
        uint64_t ms;
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


    class WindowType {

    };
    typedef std::shared_ptr<WindowType> WindowTypePtr;

    class TumblingWindow : public WindowType {
    public:
        static WindowTypePtr of(WindowMeasure size);

    private:
        TumblingWindow(WindowMeasure size);
        const WindowMeasure _size;
    };


    class SlidingWindow : public WindowType {
    public:
        static WindowTypePtr of(WindowMeasure size, WindowMeasure slide);
    private:
        SlidingWindow(WindowMeasure size, WindowMeasure slide);
        const WindowMeasure _size;
        const WindowMeasure _slide;
    };

    class SessionWindow : public WindowType {
    public:
        static WindowTypePtr of(TimeMeasure gap);
    private:
        SessionWindow(TimeMeasure gap);
        const TimeMeasure _gap;
    };


    class WindowAggregation {
    public:
        WindowAggregation(const Field& onField);

        WindowAggregation &as(const Field& asField);

    private:
        const Field &_onField;
        Field& _asField;
    };

    class Sum : public WindowAggregation {
    public:
        Sum(Field onField);
    };
}
#endif //IOTDB_WINDOWDEFINITION_HPP

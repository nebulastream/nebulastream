#include <cstddef>
#include <iomanip>
#include <iostream>

#include <API/WindowDefinition.hpp>
#include <API/UserAPIExpression.hpp>
#include <Operators/Operator.hpp>
#include <Runtime/DataSink.hpp>

#include <CodeGen/C_CodeGen/CodeCompiler.hpp>

namespace iotdb {

    TimeMeasure::TimeMeasure(uint64_t ms) : ms(ms) {}

    Milliseconds::Milliseconds(uint64_t milliseconds) : TimeMeasure(milliseconds) {}

    Seconds::Seconds(uint64_t seconds) : Milliseconds(seconds * 1000) {}

    Minutes::Minutes(uint64_t minutes) : Seconds(minutes * 60) {}

    Hours::Hours(uint64_t hours) : Minutes(hours * 60) {}

    TumblingWindow::TumblingWindow(iotdb::WindowMeasure size) : _size(size){}

    WindowTypePtr TumblingWindow::of(iotdb::WindowMeasure size) {
        return std::make_shared<TumblingWindow>(TumblingWindow(size));
    }

    SlidingWindow::SlidingWindow(iotdb::WindowMeasure size, iotdb::WindowMeasure slide) : _size(size), _slide(slide){}

    WindowTypePtr SlidingWindow::of(iotdb::WindowMeasure size, WindowMeasure slide) {
        return std::make_shared<SlidingWindow>(SlidingWindow(size, slide));
    }

    SessionWindow::SessionWindow(iotdb::TimeMeasure gap) : _gap(gap){}

    WindowTypePtr SessionWindow::of(iotdb::TimeMeasure gap) {
        return std::make_shared<SessionWindow>(SessionWindow(gap));
    }


    WindowAggregation::WindowAggregation(const iotdb::Field &onField) : _onField(onField), _asField(
            const_cast<Field &>(onField)) {

    }

    WindowAggregation& WindowAggregation::as(const iotdb::Field &asField) {
        this->_asField = asField;
    }

    Sum::Sum(iotdb::Field onField) : WindowAggregation(onField) {}
}
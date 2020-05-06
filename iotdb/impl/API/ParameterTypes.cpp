
#include <API/ParameterTypes.hpp>
#include <string>

namespace NES {

AggregationSpec& AggregationSpec::operator=(const AggregationSpec& other) {
    if (this != &other) {
        this->aggr_spec = other.aggr_spec;
        // this->grouping_fields=std::move(other.grouping_fields);
    }
    return *this;
}

Sort::Sort(SortAttr field1) : param() { param.push_back(field1); }

Sort::Sort(SortAttr field1, SortAttr field2) : param() {
    param.push_back(field1);
    param.push_back(field2);
}

Sort::Sort(SortAttr field1, SortAttr field2, SortAttr field3) : param() {
    param.push_back(field1);
    param.push_back(field2);
    param.push_back(field3);
}

// const DataSourcePtr copy(const DataSourcePtr& param){ return param->copy();}
// const DataSinkPtr copy(const DataSinkPtr& param){ return param->copy();}
// const PredicatePtr copy(const PredicatePtr& param){ return param->copy();}
// const Sort copy(const Sort& param){ return Sort(param);}
// const WindowPtr copy(const WindowPtr& param){ return param->copy();}
// const Attributes copy(const Attributes& param){ return Attributes(param);}
// const MapperPtr copy(const MapperPtr& param){ return param->copy();}
// const AggregationSpec copy(const AggregationSpec& param){ return AggregationSpec(param);}
// const JoinPredicatePtr copy(const JoinPredicatePtr& param){ return param->copy();}

const DataSourcePtr copy(const DataSourcePtr param) { return param; }
const DataSinkPtr copy(const DataSinkPtr param) { return param; }
const PredicatePtr copy(const PredicatePtr param) { return param; }
const Sort copy(const Sort& param) { return param; }
//const WindowPtr copy(const WindowPtr& param) { return param; }
//const WindowTypePtr copy(const WindowTypePtr& param) { return param; }
const Attributes copy(const Attributes& param) { return param; }
const MapperPtr copy(const MapperPtr param) { return param; }
const AggregationSpec copy(const AggregationSpec& param) { return param; }
const JoinPredicatePtr copy(const JoinPredicatePtr param) { return param; }

const std::string toString(const DataSourcePtr param) { return "***"; }
const std::string toString(const DataSinkPtr param) { return "***"; }
const std::string toString(const PredicatePtr param) { return "***"; }
const std::string toString(const Sort& param) { return "***"; }
//const std::string toString(const WindowPtr& param) { return "***"; }
const std::string toString(const Attributes& param) { return "***"; }
const std::string toString(const MapperPtr param) { return "***"; }
const std::string toString(const AggregationSpec& param) { return "***"; }
const std::string toString(const JoinPredicatePtr param) { return "***"; }

}// namespace NES

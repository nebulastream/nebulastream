#include <ProxyFunctions.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-label"
#pragma GCC diagnostic ignored "-Wtautological-compare"
static inline auto setup(uint8_t* var_0_121 ,uint8_t* var_0_122 ){
//variable declarations
uint64_t var_0_0;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
uint64_t var_0_4;
uint8_t* var_0_5;
uint64_t var_0_6;
uint64_t var_0_7;
uint64_t var_0_8;
uint8_t* var_0_11;
uint64_t var_0_12;
uint64_t var_0_14;
uint8_t* var_0_15;
uint64_t var_0_17;
uint8_t* var_0_18;
uint64_t var_0_19;
uint64_t var_0_20;
//basic blocks
Block_0:
var_0_0 = 0;
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 0;
var_0_5 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_121,var_0_4));
var_0_6 = 0;
var_0_7 = 8;
var_0_8 = var_0_6+var_0_7;
setupSliceMergingHandler(var_0_5,var_0_121,var_0_8);
var_0_11 = static_cast<uint8_t*>(getDefaultMergingState(var_0_5));
var_0_12 = 0;
*reinterpret_cast<uint64_t*>(var_0_11) = var_0_12;
var_0_14 = 8;
var_0_15 = var_0_11+var_0_14;
var_0_17 = 1;
var_0_18 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_121,var_0_17));
var_0_19 = 3;
var_0_20 = 1;
setNumberOfWorkerThreadsProxy(var_0_18,var_0_121,var_0_19,var_0_20);
return;

}

static inline auto execute(uint8_t* var_0_131 ,uint8_t* var_0_132 ,uint8_t* var_0_133 ){
//variable declarations
uint64_t var_0_0;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
uint64_t var_0_4;
uint8_t* var_0_5;
uint64_t var_0_6;
uint8_t* var_0_7;
uint64_t var_0_8;
uint8_t* var_0_9;
uint64_t var_0_10;
uint64_t var_0_11;
uint64_t var_0_12;
uint64_t var_0_14;
uint64_t var_0_16;
uint8_t* var_0_17;
uint8_t* var_0_18;
uint64_t var_0_19;
uint8_t* var_0_20;
uint64_t var_0_21;
uint64_t var_0_22;
uint8_t* var_0_23;
uint64_t var_0_24;
uint64_t var_0_25;
uint8_t* var_0_26;
uint64_t var_0_27;
uint8_t* var_0_28;
uint8_t* var_0_29;
uint64_t var_0_30;
uint64_t var_0_31;
uint8_t* var_3_102;
uint8_t* var_3_103;
uint8_t* var_3_104;
uint64_t var_3_105;
uint64_t var_3_106;
uint8_t* var_3_107;
uint64_t var_3_108;
uint64_t var_3_109;
uint64_t var_3_110;
uint8_t* var_3_111;
uint64_t var_3_112;
uint64_t var_3_113;
uint8_t* var_3_114;
uint8_t* var_3_115;
bool var_0_32;
uint8_t* var_1_112;
uint8_t* var_1_113;
uint8_t* var_1_114;
uint64_t var_1_115;
uint64_t var_1_116;
uint8_t* var_1_117;
uint64_t var_1_118;
uint64_t var_1_119;
uint64_t var_1_120;
uint8_t* var_1_121;
uint64_t var_1_122;
uint64_t var_1_123;
uint8_t* var_1_124;
uint8_t* var_1_125;
uint8_t* var_1_0;
uint64_t var_1_1;
uint8_t* var_1_2;
uint64_t var_1_3;
uint8_t* var_1_4;
uint64_t var_1_5;
uint64_t var_1_6;
uint64_t var_1_7;
uint64_t var_1_9;
uint64_t var_1_10;
uint8_t* var_2_113;
uint8_t* var_2_114;
uint8_t* var_2_115;
uint64_t var_2_116;
uint64_t var_2_117;
uint8_t* var_2_118;
uint64_t var_2_119;
uint64_t var_2_120;
uint64_t var_2_121;
uint8_t* var_2_122;
uint8_t* var_2_123;
uint64_t var_2_2;
uint8_t* var_2_5;
uint64_t var_2_6;
uint8_t* var_2_7;
uint64_t var_2_8;
bool var_2_10;
bool var_2_11;
bool var_2_12;
bool var_2_13;
bool var_2_14;
bool var_2_15;
uint8_t* var_4_107;
uint8_t* var_4_108;
uint64_t var_4_109;
uint64_t var_4_110;
uint64_t var_4_111;
uint8_t* var_4_112;
uint64_t var_4_113;
uint64_t var_4_114;
uint64_t var_4_115;
uint8_t* var_4_116;
uint64_t var_4_0;
uint8_t* var_4_1;
uint64_t var_4_3;
uint8_t* var_4_4;
uint64_t var_4_6;
uint64_t var_4_8;
uint8_t* var_8_110;
uint8_t* var_8_111;
uint64_t var_8_112;
uint64_t var_8_113;
uint64_t var_8_114;
uint8_t* var_8_115;
uint64_t var_8_116;
uint64_t var_8_117;
uint64_t var_8_118;
uint8_t* var_8_119;
uint64_t var_4_10;
uint8_t* var_4_11;
uint64_t var_4_12;
uint64_t var_4_13;
uint8_t* var_4_14;
uint64_t var_4_15;
bool var_4_16;
bool var_4_17;
bool var_4_18;
uint8_t* var_6_102;
uint8_t* var_6_103;
uint64_t var_6_104;
uint64_t var_6_105;
uint64_t var_6_106;
uint8_t* var_6_107;
uint64_t var_6_108;
uint64_t var_6_109;
uint64_t var_6_110;
uint8_t* var_6_111;
uint8_t* var_9_141;
uint8_t* var_9_142;
uint64_t var_9_143;
uint64_t var_9_144;
uint64_t var_9_145;
uint8_t* var_9_146;
uint64_t var_9_147;
uint64_t var_9_148;
uint64_t var_9_149;
uint8_t* var_9_150;
uint64_t var_6_1;
uint8_t* var_6_2;
uint8_t* var_6_3;
uint64_t var_6_4;
uint8_t* var_6_5;
uint64_t var_6_6;
uint64_t var_6_7;
uint64_t var_6_8;
uint8_t* var_6_9;
uint64_t var_6_10;
uint8_t* var_6_11;
uint64_t var_6_12;
uint64_t var_6_13;
uint64_t var_6_14;
uint64_t var_6_15;
uint8_t* var_6_16;
uint64_t var_6_18;
uint8_t* var_6_19;
uint64_t var_6_20;
uint64_t var_6_21;
uint64_t var_6_22;
uint64_t var_6_23;
uint8_t* var_6_24;
uint64_t var_6_27;
uint8_t* var_6_28;
uint64_t var_6_31;
uint8_t* var_6_32;
uint64_t var_6_35;
uint8_t* var_6_36;
uint64_t var_6_39;
uint8_t* var_6_40;
uint64_t var_6_41;
uint64_t var_6_42;
uint8_t* var_7_101;
uint8_t* var_7_102;
uint64_t var_7_103;
uint64_t var_7_104;
uint64_t var_7_105;
uint8_t* var_7_106;
uint64_t var_7_107;
uint64_t var_7_108;
uint64_t var_7_109;
uint8_t* var_7_110;
uint8_t* var_5_101;
uint8_t* var_5_102;
uint64_t var_5_103;
uint64_t var_5_104;
uint64_t var_5_105;
uint8_t* var_5_106;
uint64_t var_5_107;
uint64_t var_5_108;
uint64_t var_5_109;
uint8_t* var_5_110;
//basic blocks
Block_0:
var_0_0 = 0;
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 1;
var_0_5 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_131,var_0_4));
var_0_6 = getWorkerIdProxy(var_0_132);
var_0_7 = static_cast<uint8_t*>(getCurrentWindowProxy(var_0_5));
var_0_8 = 0;
var_0_9 = static_cast<uint8_t*>(getNLJPagedVectorProxy(var_0_7,var_0_6,var_0_8));
var_0_10 = 0;
var_0_11 = 0;
var_0_12 = getNLJSliceStartProxy(var_0_7);
var_0_14 = getNLJSliceEndProxy(var_0_7);
var_0_16 = 0;
var_0_17 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_131,var_0_16));
var_0_18 = static_cast<uint8_t*>(NES__Runtime__TupleBuffer__getBuffer(var_0_133));
var_0_19 = 8;
var_0_20 = var_0_18+var_0_19;
var_0_21 = *reinterpret_cast<uint64_t*>(var_0_20);
var_0_22 = 16;
var_0_23 = var_0_18+var_0_22;
var_0_24 = *reinterpret_cast<uint64_t*>(var_0_23);
var_0_25 = 0;
var_0_26 = var_0_18+var_0_25;
var_0_27 = *reinterpret_cast<uint64_t*>(var_0_26);
var_0_28 = static_cast<uint8_t*>(createGlobalState(var_0_17,var_0_18));
var_0_29 = static_cast<uint8_t*>(getGlobalSliceState(var_0_28));
var_0_30 = getNonKeyedNumberOfSlices(var_0_18);
var_0_31 = 0;
// prepare block arguments
var_3_102 = var_0_131;
var_3_103 = var_0_132;
var_3_104 = var_0_28;
var_3_105 = var_0_24;
var_3_106 = var_0_21;
var_3_107 = var_0_5;
var_3_108 = var_0_14;
var_3_109 = var_0_12;
var_3_110 = var_0_27;
var_3_111 = var_0_18;
var_3_112 = var_0_31;
var_3_113 = var_0_30;
var_3_114 = var_0_29;
var_3_115 = var_0_9;
goto Block_3;

Block_3:
var_0_32 = var_3_112 < var_3_113;
if (var_0_32){
// prepare block arguments
var_1_112 = var_3_102;
var_1_113 = var_3_103;
var_1_114 = var_3_104;
var_1_115 = var_3_105;
var_1_116 = var_3_106;
var_1_117 = var_3_107;
var_1_118 = var_3_108;
var_1_119 = var_3_109;
var_1_120 = var_3_110;
var_1_121 = var_3_111;
var_1_122 = var_3_113;
var_1_123 = var_3_112;
var_1_124 = var_3_114;
var_1_125 = var_3_115;
goto Block_1;
}else{
// prepare block arguments
var_2_113 = var_3_102;
var_2_114 = var_3_103;
var_2_115 = var_3_104;
var_2_116 = var_3_105;
var_2_117 = var_3_106;
var_2_118 = var_3_107;
var_2_119 = var_3_108;
var_2_120 = var_3_109;
var_2_121 = var_3_110;
var_2_122 = var_3_111;
var_2_123 = var_3_115;
goto Block_2;}

Block_1:
var_1_0 = static_cast<uint8_t*>(getNonKeyedSliceState(var_1_121,var_1_123));
var_1_1 = 0;
var_1_2 = var_1_124+var_1_1;
var_1_3 = 0;
var_1_4 = var_1_0+var_1_3;
var_1_5 = *reinterpret_cast<uint64_t*>(var_1_2);
var_1_6 = *reinterpret_cast<uint64_t*>(var_1_4);
var_1_7 = max(var_1_5,var_1_6);
*reinterpret_cast<uint64_t*>(var_1_2) = var_1_7;
var_1_9 = 1;
var_1_10 = var_1_123+var_1_9;
// prepare block arguments
var_3_102 = var_1_112;
var_3_103 = var_1_113;
var_3_104 = var_1_114;
var_3_105 = var_1_115;
var_3_106 = var_1_116;
var_3_107 = var_1_117;
var_3_108 = var_1_118;
var_3_109 = var_1_119;
var_3_110 = var_1_120;
var_3_111 = var_1_121;
var_3_112 = var_1_10;
var_3_113 = var_1_122;
var_3_114 = var_1_124;
var_3_115 = var_1_125;
goto Block_3;

Block_2:
freeNonKeyedSliceMergeTask(var_2_122);
var_2_2 = 3;
var_2_5 = static_cast<uint8_t*>(getGlobalSliceState(var_2_115));
var_2_6 = 0;
var_2_7 = var_2_5+var_2_6;
var_2_8 = *reinterpret_cast<uint64_t*>(var_2_7);
var_2_10 = var_2_120 < var_2_117;
var_2_11 = var_2_120 == var_2_117;
var_2_12 = var_2_10||var_2_11;
var_2_13 = var_2_117 < var_2_119;
var_2_14 = var_2_12&&var_2_13;
var_2_15= !var_2_14;
if (var_2_15){
// prepare block arguments
var_4_107 = var_2_113;
var_4_108 = var_2_114;
var_4_109 = var_2_116;
var_4_110 = var_2_121;
var_4_111 = var_2_2;
var_4_112 = var_2_115;
var_4_113 = var_2_8;
var_4_114 = var_2_116;
var_4_115 = var_2_117;
var_4_116 = var_2_118;
goto Block_4;
}else{
// prepare block arguments
var_5_101 = var_2_113;
var_5_102 = var_2_114;
var_5_103 = var_2_116;
var_5_104 = var_2_121;
var_5_105 = var_2_2;
var_5_106 = var_2_115;
var_5_107 = var_2_8;
var_5_108 = var_2_116;
var_5_109 = var_2_117;
var_5_110 = var_2_123;
goto Block_5;}

Block_4:
var_4_0 = getWorkerIdProxy(var_4_108);
var_4_1 = static_cast<uint8_t*>(getNLJSliceRefProxy(var_4_116,var_4_115));
var_4_3 = 0;
var_4_4 = static_cast<uint8_t*>(getNLJPagedVectorProxy(var_4_1,var_4_0,var_4_3));
var_4_6 = getNLJSliceStartProxy(var_4_1);
var_4_8 = getNLJSliceEndProxy(var_4_1);
// prepare block arguments
var_8_110 = var_4_107;
var_8_111 = var_4_108;
var_8_112 = var_4_109;
var_8_113 = var_4_110;
var_8_114 = var_4_111;
var_8_115 = var_4_112;
var_8_116 = var_4_113;
var_8_117 = var_4_114;
var_8_118 = var_4_115;
var_8_119 = var_4_4;
goto Block_8;

Block_8:
var_4_10 = 64;
var_4_11 = var_8_119+var_4_10;
var_4_12 = *reinterpret_cast<uint64_t*>(var_4_11);
var_4_13 = 24;
var_4_14 = var_8_119+var_4_13;
var_4_15 = *reinterpret_cast<uint64_t*>(var_4_14);
var_4_16 = var_4_12 > var_4_15;
var_4_17 = var_4_12 == var_4_15;
var_4_18 = var_4_16||var_4_17;
if (var_4_18){
// prepare block arguments
var_6_102 = var_8_110;
var_6_103 = var_8_111;
var_6_104 = var_8_112;
var_6_105 = var_8_113;
var_6_106 = var_8_114;
var_6_107 = var_8_115;
var_6_108 = var_8_116;
var_6_109 = var_8_117;
var_6_110 = var_8_118;
var_6_111 = var_8_119;
goto Block_6;
}else{
// prepare block arguments
var_7_101 = var_8_110;
var_7_102 = var_8_111;
var_7_103 = var_8_112;
var_7_104 = var_8_113;
var_7_105 = var_8_114;
var_7_106 = var_8_115;
var_7_107 = var_8_116;
var_7_108 = var_8_117;
var_7_109 = var_8_118;
var_7_110 = var_8_119;
goto Block_7;}

Block_6:
allocateNewPageProxy(var_6_111);
// prepare block arguments
var_9_141 = var_6_102;
var_9_142 = var_6_103;
var_9_143 = var_6_104;
var_9_144 = var_6_105;
var_9_145 = var_6_106;
var_9_146 = var_6_107;
var_9_147 = var_6_108;
var_9_148 = var_6_109;
var_9_149 = var_6_110;
var_9_150 = var_6_111;
goto Block_9;

Block_9:
var_6_1 = 56;
var_6_2 = var_9_150+var_6_1;
var_6_3 = *reinterpret_cast<uint8_t**>(var_6_2);
var_6_4 = 64;
var_6_5 = var_9_150+var_6_4;
var_6_6 = *reinterpret_cast<uint64_t*>(var_6_5);
var_6_7 = 24;
var_6_8 = var_6_6*var_6_7;
var_6_9 = var_6_3+var_6_8;
var_6_10 = 64;
var_6_11 = var_9_150+var_6_10;
var_6_12 = *reinterpret_cast<uint64_t*>(var_6_11);
var_6_13 = 1;
var_6_14 = var_6_12+var_6_13;
var_6_15 = 64;
var_6_16 = var_9_150+var_6_15;
*reinterpret_cast<uint64_t*>(var_6_16) = var_6_14;
var_6_18 = 72;
var_6_19 = var_9_150+var_6_18;
var_6_20 = *reinterpret_cast<uint64_t*>(var_6_19);
var_6_21 = 1;
var_6_22 = var_6_20+var_6_21;
var_6_23 = 72;
var_6_24 = var_9_150+var_6_23;
*reinterpret_cast<uint64_t*>(var_6_24) = var_6_22;
*reinterpret_cast<uint64_t*>(var_6_9) = var_9_149;
var_6_27 = 8;
var_6_28 = var_6_9+var_6_27;
*reinterpret_cast<uint64_t*>(var_6_28) = var_9_148;
var_6_31 = 8;
var_6_32 = var_6_28+var_6_31;
*reinterpret_cast<uint64_t*>(var_6_32) = var_9_147;
var_6_35 = 8;
var_6_36 = var_6_32+var_6_35;
deleteNonKeyedSlice(var_9_146);
var_6_39 = 1;
var_6_40 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_9_141,var_6_39));
var_6_41 = 3;
var_6_42 = 1;
checkWindowsTriggerProxy(var_6_40,var_9_141,var_9_142,var_9_143,var_9_144,var_9_145,var_6_41,var_6_42);
return;

Block_7:
// prepare block arguments
var_9_141 = var_7_101;
var_9_142 = var_7_102;
var_9_143 = var_7_103;
var_9_144 = var_7_104;
var_9_145 = var_7_105;
var_9_146 = var_7_106;
var_9_147 = var_7_107;
var_9_148 = var_7_108;
var_9_149 = var_7_109;
var_9_150 = var_7_110;
goto Block_9;

Block_5:
// prepare block arguments
var_8_110 = var_5_101;
var_8_111 = var_5_102;
var_8_112 = var_5_103;
var_8_113 = var_5_104;
var_8_114 = var_5_105;
var_8_115 = var_5_106;
var_8_116 = var_5_107;
var_8_117 = var_5_108;
var_8_118 = var_5_109;
var_8_119 = var_5_110;
goto Block_8;

}

static inline auto terminate(uint8_t* var_0_110 ,uint8_t* var_0_111 ){
//variable declarations
uint64_t var_0_0;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
uint64_t var_0_4;
uint8_t* var_0_5;
uint64_t var_0_6;
uint64_t var_0_7;
//basic blocks
Block_0:
var_0_0 = 0;
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 1;
var_0_5 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_110,var_0_4));
var_0_6 = 3;
var_0_7 = 1;
triggerAllWindowsProxy(var_0_5,var_0_110,var_0_6,var_0_7);
return;

}

#pragma GCC diagnostic pop
template<>
void NES::Unikernel::Stage<7>::terminate(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext) {
           ::terminate(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));
}
template<>
void NES::Unikernel::Stage<7>::setup(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext) {
           ::setup(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));
}
template<>
void NES::Unikernel::Stage<7>::execute(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext, NES::Runtime::TupleBuffer& buffer) {
           ::execute(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext), reinterpret_cast<uint8_t*>(&buffer));
}
template<size_t SubQueryPlanId>
extern NES::Runtime::Execution::OperatorHandler* getSharedOperatorHandler(int index);
template<>
NES::Runtime::Execution::OperatorHandler* NES::Unikernel::Stage<7>::getOperatorHandler(int index) {
static NES::Runtime::Execution::Operators::NonKeyedSliceMergingHandler handler0;
switch(index) {
case 0:
	return &handler0;
case 1:
	return getSharedOperatorHandler<3>(0);default:
	assert(false && "Bad OperatorHandler index");
	return nullptr;}}
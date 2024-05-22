#include <ProxyFunctions.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-label"
#pragma GCC diagnostic ignored "-Wtautological-compare"
static inline auto setup(uint8_t* var_0_129 ,uint8_t* var_0_130 ){
//variable declarations
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_0_0 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
uint64_t var_0_4;
uint64_t var_0_5;
bool var_0_6;
uint64_t var_0_7;
uint8_t* var_0_8;
uint64_t var_0_9;
uint64_t var_0_10;
uint64_t var_0_11;
uint8_t* var_0_14;
uint64_t var_0_15;
uint64_t var_0_17;
uint8_t* var_0_18;
uint64_t var_0_20;
uint8_t* var_0_21;
uint64_t var_0_22;
uint64_t var_0_23;
uint64_t var_0_25;
uint8_t* var_0_26;
uint64_t var_0_27;
uint64_t var_0_28;
//basic blocks
Block_0:
var_0_0 = NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>(0);
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 0;
var_0_5 = 0;
var_0_6 = 1;
var_0_7 = 0;
var_0_8 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_129,var_0_7));
var_0_9 = 0;
var_0_10 = 8;
var_0_11 = var_0_9+var_0_10;
setupSliceMergingHandler(var_0_8,var_0_129,var_0_11);
var_0_14 = static_cast<uint8_t*>(getDefaultMergingState(var_0_8));
var_0_15 = 0;
*reinterpret_cast<uint64_t*>(var_0_14) = var_0_15;
var_0_17 = 8;
var_0_18 = var_0_14+var_0_17;
var_0_20 = 1;
var_0_21 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_129,var_0_20));
var_0_22 = 3;
var_0_23 = 1;
setNumberOfWorkerThreadsProxy(var_0_21,var_0_129,var_0_22,var_0_23);
var_0_25 = 1;
var_0_26 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_129,var_0_25));
var_0_27 = 3;
var_0_28 = 1;
setBufferManagerProxy(var_0_26,var_0_129,var_0_27,var_0_28);
return;

}

static inline auto execute(uint8_t* var_0_140 ,uint8_t* var_0_141 ,uint8_t* var_0_142 ){
//variable declarations
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_0_0 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
uint64_t var_0_4;
uint64_t var_0_5;
bool var_0_6;
uint64_t var_0_7;
uint8_t* var_0_8;
NES::NESStrongType<unsigned long, NES::WorkerThreadId_, 0, 1> var_0_9 = NES::INVALID<NES::NESStrongType<unsigned long, NES::WorkerThreadId_, 0, 1>>;
uint8_t* var_0_10;
uint64_t var_0_11;
uint8_t* var_0_12;
uint64_t var_0_13;
uint64_t var_0_14;
uint64_t var_0_15;
uint64_t var_0_17;
uint64_t var_0_19;
uint8_t* var_0_20;
uint8_t* var_0_21;
uint64_t var_0_22;
uint8_t* var_0_23;
uint64_t var_0_24;
uint64_t var_0_25;
uint8_t* var_0_26;
uint64_t var_0_27;
uint64_t var_0_28;
uint8_t* var_0_29;
uint64_t var_0_30;
uint64_t var_0_31;
uint8_t* var_0_32;
uint64_t var_0_33;
uint64_t var_0_34;
uint8_t* var_0_35;
bool var_0_36;
uint8_t* var_0_37;
uint8_t* var_0_38;
uint64_t var_0_39;
uint64_t var_0_40;
uint8_t* var_3_102;
uint8_t* var_3_103;
uint8_t* var_3_104;
uint64_t var_3_105;
uint64_t var_3_106;
uint8_t* var_3_107;
uint64_t var_3_108;
uint64_t var_3_109;
bool var_3_110;
uint64_t var_3_111;
uint64_t var_3_112;
uint8_t* var_3_113;
uint64_t var_3_114;
uint64_t var_3_115;
uint8_t* var_3_116;
uint8_t* var_3_117;
bool var_0_41;
uint8_t* var_1_112;
uint8_t* var_1_113;
uint8_t* var_1_114;
uint64_t var_1_115;
uint64_t var_1_116;
uint8_t* var_1_117;
uint64_t var_1_118;
uint64_t var_1_119;
bool var_1_120;
uint64_t var_1_121;
uint64_t var_1_122;
uint8_t* var_1_123;
uint64_t var_1_124;
uint64_t var_1_125;
uint8_t* var_1_126;
uint8_t* var_1_127;
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
uint8_t* var_2_115;
uint8_t* var_2_116;
uint8_t* var_2_117;
uint64_t var_2_118;
uint64_t var_2_119;
uint8_t* var_2_120;
uint64_t var_2_121;
uint64_t var_2_122;
bool var_2_123;
uint64_t var_2_124;
uint64_t var_2_125;
uint8_t* var_2_126;
uint8_t* var_2_127;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_2_2 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint8_t* var_2_7;
uint64_t var_2_8;
uint8_t* var_2_9;
uint64_t var_2_10;
uint64_t var_2_11;
uint64_t var_2_12;
bool var_2_14;
bool var_2_15;
bool var_2_16;
bool var_2_17;
bool var_2_18;
bool var_2_19;
uint8_t* var_4_105;
uint8_t* var_4_106;
uint64_t var_4_107;
uint64_t var_4_108;
uint64_t var_4_109;
bool var_4_110;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_4_111 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint8_t* var_4_112;
uint64_t var_4_113;
uint64_t var_4_114;
uint64_t var_4_115;
uint8_t* var_4_116;
uint64_t var_4_117;
NES::NESStrongType<unsigned long, NES::WorkerThreadId_, 0, 1> var_4_0 = NES::INVALID<NES::NESStrongType<unsigned long, NES::WorkerThreadId_, 0, 1>>;
uint8_t* var_4_1;
uint64_t var_4_3;
uint64_t var_4_5;
uint8_t* var_6_121;
uint8_t* var_6_122;
uint64_t var_6_123;
uint64_t var_6_124;
uint64_t var_6_125;
bool var_6_126;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_6_127 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint8_t* var_6_128;
uint64_t var_6_129;
uint64_t var_6_130;
uint64_t var_6_131;
uint8_t* var_6_132;
NES::NESStrongType<unsigned long, NES::WorkerThreadId_, 0, 1> var_4_7 = NES::INVALID<NES::NESStrongType<unsigned long, NES::WorkerThreadId_, 0, 1>>;
uint64_t var_4_8;
uint8_t* var_4_9;
uint64_t var_4_10;
uint8_t* var_4_11;
uint64_t var_4_13;
uint8_t* var_4_14;
uint64_t var_4_17;
uint8_t* var_4_18;
uint64_t var_4_21;
uint8_t* var_4_22;
uint64_t var_4_25;
uint8_t* var_4_26;
uint64_t var_4_27;
uint64_t var_4_28;
uint8_t* var_5_101;
uint8_t* var_5_102;
uint64_t var_5_103;
uint64_t var_5_104;
uint64_t var_5_105;
bool var_5_106;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_5_107 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint8_t* var_5_108;
uint64_t var_5_109;
uint64_t var_5_110;
uint64_t var_5_111;
uint8_t* var_5_112;
//basic blocks
Block_0:
var_0_0 = NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>(0);
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 0;
var_0_5 = 0;
var_0_6 = 1;
var_0_7 = 1;
var_0_8 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_140,var_0_7));
var_0_9 = getWorkerIdProxy(var_0_141);
var_0_10 = static_cast<uint8_t*>(getCurrentWindowProxy(var_0_8));
var_0_11 = 0;
var_0_12 = static_cast<uint8_t*>(getNLJPagedVectorProxy(var_0_10,var_0_9,var_0_11));
var_0_13 = 0;
var_0_14 = 0;
var_0_15 = getNLJSliceStartProxy(var_0_10);
var_0_17 = getNLJSliceEndProxy(var_0_10);
var_0_19 = 0;
var_0_20 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_140,var_0_19));
var_0_21 = static_cast<uint8_t*>(NES__Runtime__TupleBuffer__getBuffer(var_0_142));
var_0_22 = 24;
var_0_23 = var_0_21+var_0_22;
var_0_24 = *reinterpret_cast<uint64_t*>(var_0_23);
var_0_25 = 32;
var_0_26 = var_0_21+var_0_25;
var_0_27 = *reinterpret_cast<uint64_t*>(var_0_26);
var_0_28 = 0;
var_0_29 = var_0_21+var_0_28;
var_0_30 = *reinterpret_cast<uint64_t*>(var_0_29);
var_0_31 = 8;
var_0_32 = var_0_21+var_0_31;
var_0_33 = *reinterpret_cast<uint64_t*>(var_0_32);
var_0_34 = 16;
var_0_35 = var_0_21+var_0_34;
var_0_36 = *reinterpret_cast<bool*>(var_0_35);
var_0_37 = static_cast<uint8_t*>(createGlobalState(var_0_20,var_0_21));
var_0_38 = static_cast<uint8_t*>(getGlobalSliceState(var_0_37));
var_0_39 = getNonKeyedNumberOfSlices(var_0_21);
var_0_40 = 0;
// prepare block arguments
var_3_102 = var_0_140;
var_3_103 = var_0_141;
var_3_104 = var_0_37;
var_3_105 = var_0_27;
var_3_106 = var_0_24;
var_3_107 = var_0_8;
var_3_108 = var_0_17;
var_3_109 = var_0_15;
var_3_110 = var_0_36;
var_3_111 = var_0_33;
var_3_112 = var_0_30;
var_3_113 = var_0_21;
var_3_114 = var_0_40;
var_3_115 = var_0_39;
var_3_116 = var_0_38;
var_3_117 = var_0_10;
goto Block_3;

Block_3:
var_0_41 = var_3_114 < var_3_115;
if (var_0_41){
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
var_1_122 = var_3_112;
var_1_123 = var_3_113;
var_1_124 = var_3_115;
var_1_125 = var_3_114;
var_1_126 = var_3_116;
var_1_127 = var_3_117;
goto Block_1;
}else{
// prepare block arguments
var_2_115 = var_3_102;
var_2_116 = var_3_103;
var_2_117 = var_3_104;
var_2_118 = var_3_105;
var_2_119 = var_3_106;
var_2_120 = var_3_107;
var_2_121 = var_3_108;
var_2_122 = var_3_109;
var_2_123 = var_3_110;
var_2_124 = var_3_111;
var_2_125 = var_3_112;
var_2_126 = var_3_113;
var_2_127 = var_3_117;
goto Block_2;}

Block_1:
var_1_0 = static_cast<uint8_t*>(getNonKeyedSliceState(var_1_123,var_1_125));
var_1_1 = 0;
var_1_2 = var_1_126+var_1_1;
var_1_3 = 0;
var_1_4 = var_1_0+var_1_3;
var_1_5 = *reinterpret_cast<uint64_t*>(var_1_2);
var_1_6 = *reinterpret_cast<uint64_t*>(var_1_4);
var_1_7 = max(var_1_5,var_1_6);
*reinterpret_cast<uint64_t*>(var_1_2) = var_1_7;
var_1_9 = 1;
var_1_10 = var_1_125+var_1_9;
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
var_3_112 = var_1_122;
var_3_113 = var_1_123;
var_3_114 = var_1_10;
var_3_115 = var_1_124;
var_3_116 = var_1_126;
var_3_117 = var_1_127;
goto Block_3;

Block_2:
freeNonKeyedSliceMergeTask(var_2_126);
var_2_2 = NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>(3);
var_2_7 = static_cast<uint8_t*>(getGlobalSliceState(var_2_117));
var_2_8 = 0;
var_2_9 = var_2_7+var_2_8;
var_2_10 = *reinterpret_cast<uint64_t*>(var_2_9);
var_2_11 = 1;
var_2_12 = var_2_119*var_2_11;
var_2_14 = var_2_122 < var_2_12;
var_2_15 = var_2_122 == var_2_12;
var_2_16 = var_2_14||var_2_15;
var_2_17 = var_2_12 < var_2_121;
var_2_18 = var_2_16&&var_2_17;
var_2_19= !var_2_18;
if (var_2_19){
// prepare block arguments
var_4_105 = var_2_115;
var_4_106 = var_2_116;
var_4_107 = var_2_119;
var_4_108 = var_2_125;
var_4_109 = var_2_124;
var_4_110 = var_2_123;
var_4_111 = var_2_2;
var_4_112 = var_2_117;
var_4_113 = var_2_10;
var_4_114 = var_2_118;
var_4_115 = var_2_119;
var_4_116 = var_2_120;
var_4_117 = var_2_12;
goto Block_4;
}else{
// prepare block arguments
var_5_101 = var_2_115;
var_5_102 = var_2_116;
var_5_103 = var_2_119;
var_5_104 = var_2_125;
var_5_105 = var_2_124;
var_5_106 = var_2_123;
var_5_107 = var_2_2;
var_5_108 = var_2_117;
var_5_109 = var_2_10;
var_5_110 = var_2_118;
var_5_111 = var_2_119;
var_5_112 = var_2_127;
goto Block_5;}

Block_4:
var_4_0 = getWorkerIdProxy(var_4_106);
var_4_1 = static_cast<uint8_t*>(getNLJSliceRefProxy(var_4_116,var_4_117));
var_4_3 = getNLJSliceStartProxy(var_4_1);
var_4_5 = getNLJSliceEndProxy(var_4_1);
// prepare block arguments
var_6_121 = var_4_105;
var_6_122 = var_4_106;
var_6_123 = var_4_107;
var_6_124 = var_4_108;
var_6_125 = var_4_109;
var_6_126 = var_4_110;
var_6_127 = var_4_111;
var_6_128 = var_4_112;
var_6_129 = var_4_113;
var_6_130 = var_4_114;
var_6_131 = var_4_115;
var_6_132 = var_4_1;
goto Block_6;

Block_6:
var_4_7 = getWorkerIdProxy(var_6_122);
var_4_8 = 0;
var_4_9 = static_cast<uint8_t*>(getNLJPagedVectorProxy(var_6_132,var_4_7,var_4_8));
var_4_10 = allocateEntryProxy(var_4_9);
var_4_11 = static_cast<uint8_t*>(entryDataProxy(var_4_9,var_4_10));
*reinterpret_cast<uint64_t*>(var_4_11) = var_6_131;
var_4_13 = 8;
var_4_14 = var_4_11+var_4_13;
*reinterpret_cast<uint64_t*>(var_4_14) = var_6_130;
var_4_17 = 8;
var_4_18 = var_4_14+var_4_17;
*reinterpret_cast<uint64_t*>(var_4_18) = var_6_129;
var_4_21 = 8;
var_4_22 = var_4_18+var_4_21;
deleteNonKeyedSlice(var_6_128);
var_4_25 = 1;
var_4_26 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_6_121,var_4_25));
var_4_27 = 3;
var_4_28 = 1;
checkWindowsTriggerProxy(var_4_26,var_6_121,var_6_122,var_6_123,var_6_124,var_6_125,var_6_126,var_6_127,var_4_27,var_4_28);
return;

Block_5:
// prepare block arguments
var_6_121 = var_5_101;
var_6_122 = var_5_102;
var_6_123 = var_5_103;
var_6_124 = var_5_104;
var_6_125 = var_5_105;
var_6_126 = var_5_106;
var_6_127 = var_5_107;
var_6_128 = var_5_108;
var_6_129 = var_5_109;
var_6_130 = var_5_110;
var_6_131 = var_5_111;
var_6_132 = var_5_112;
goto Block_6;

}

static inline auto terminate(uint8_t* var_0_113 ,uint8_t* var_0_114 ){
//variable declarations
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_0_0 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
uint64_t var_0_4;
uint64_t var_0_5;
bool var_0_6;
uint64_t var_0_7;
uint8_t* var_0_8;
uint64_t var_0_9;
uint64_t var_0_10;
//basic blocks
Block_0:
var_0_0 = NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>(0);
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 0;
var_0_5 = 0;
var_0_6 = 1;
var_0_7 = 1;
var_0_8 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_113,var_0_7));
var_0_9 = 3;
var_0_10 = 1;
triggerAllWindowsProxy(var_0_8,var_0_113,var_0_9,var_0_10);
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
	return getSharedOperatorHandler<1>(0);default:
	assert(false && "Bad OperatorHandler index");
	return nullptr;}}
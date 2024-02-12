#include <ProxyFunctions.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-label"
#pragma GCC diagnostic ignored "-Wtautological-compare"
static inline auto setup(uint8_t* var_0_110 ,uint8_t* var_0_111 ){
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
var_0_4 = 0;
var_0_5 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_110,var_0_4));
var_0_6 = 8;
var_0_7 = 8;
setupWindowHandlerKeyed(var_0_5,var_0_110,var_0_6,var_0_7);
return;

}

static inline auto execute(uint8_t* var_0_116 ,uint8_t* var_0_117 ,uint8_t* var_0_118 ){
//variable declarations
uint64_t var_0_0;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
uint64_t var_0_4;
uint64_t var_0_6;
uint64_t var_0_8;
uint64_t var_0_10;
uint8_t* var_0_11;
uint64_t var_0_12;
uint8_t* var_0_13;
uint64_t var_0_14;
uint64_t var_0_15;
uint8_t* var_0_16;
uint64_t var_0_17;
uint8_t* var_11_102;
uint8_t* var_11_103;
uint64_t var_11_104;
uint64_t var_11_105;
uint64_t var_11_106;
uint64_t var_11_107;
uint64_t var_11_108;
uint8_t* var_11_109;
uint8_t* var_11_110;
bool var_0_18;
uint8_t* var_1_120;
uint8_t* var_1_121;
uint64_t var_1_122;
uint64_t var_1_123;
uint64_t var_1_124;
uint64_t var_1_125;
uint8_t* var_1_126;
uint64_t var_1_127;
uint8_t* var_1_128;
uint64_t var_1_0;
uint64_t var_1_1;
uint8_t* var_1_2;
uint64_t var_1_3;
uint8_t* var_1_4;
uint64_t var_1_5;
uint64_t var_1_6;
uint8_t* var_1_7;
uint64_t var_1_8;
uint64_t var_1_9;
uint8_t* var_1_10;
uint64_t var_1_11;
uint64_t var_1_12;
uint8_t* var_1_13;
uint64_t var_1_14;
uint64_t var_1_15;
uint8_t* var_1_16;
double var_1_17;
bool var_1_19;
uint8_t* var_3_101;
uint8_t* var_3_102;
uint64_t var_3_103;
uint64_t var_3_104;
uint64_t var_3_105;
uint64_t var_3_106;
uint64_t var_3_107;
uint8_t* var_3_108;
uint64_t var_3_109;
uint8_t* var_3_110;
uint8_t* var_12_105;
uint8_t* var_12_106;
uint64_t var_12_107;
uint64_t var_12_108;
uint64_t var_12_109;
uint64_t var_12_110;
uint64_t var_12_111;
uint64_t var_12_112;
uint8_t* var_12_113;
uint64_t var_12_114;
uint8_t* var_12_115;
uint8_t* var_3_2;
uint64_t var_3_3;
uint64_t var_3_4;
uint8_t* var_3_6;
uint8_t* var_15_104;
uint8_t* var_15_105;
uint64_t var_15_106;
uint64_t var_15_107;
uint64_t var_15_108;
uint64_t var_15_109;
uint64_t var_15_110;
uint64_t var_15_111;
uint8_t* var_15_112;
uint64_t var_15_113;
uint8_t* var_15_114;
uint8_t* var_15_115;
uint8_t* var_15_116;
int32_t var_3_7;
bool var_3_8;
bool var_3_9;
uint8_t* var_5_109;
uint8_t* var_5_110;
uint64_t var_5_111;
uint64_t var_5_112;
uint64_t var_5_113;
uint64_t var_5_114;
uint64_t var_5_115;
uint64_t var_5_116;
uint8_t* var_5_117;
uint64_t var_5_118;
uint8_t* var_5_119;
uint8_t* var_5_120;
uint8_t* var_5_121;
bool var_5_0;
uint64_t var_5_1;
uint8_t* var_5_2;
uint64_t var_5_3;
bool var_5_4;
bool var_5_5;
uint64_t var_5_7;
uint8_t* var_5_8;
uint8_t* var_7_101;
uint8_t* var_7_102;
uint64_t var_7_103;
uint64_t var_7_104;
uint64_t var_7_105;
uint64_t var_7_106;
uint64_t var_7_107;
uint64_t var_7_108;
uint8_t* var_7_109;
uint64_t var_7_110;
uint8_t* var_7_111;
uint8_t* var_7_112;
uint8_t* var_7_113;
uint8_t* var_13_103;
uint8_t* var_13_104;
uint64_t var_13_105;
uint64_t var_13_106;
uint64_t var_13_107;
uint64_t var_13_108;
uint64_t var_13_109;
uint64_t var_13_110;
uint8_t* var_13_111;
uint64_t var_13_112;
uint8_t* var_13_113;
uint8_t* var_13_114;
uint8_t* var_13_115;
int32_t var_7_0;
bool var_7_1;
uint8_t* var_9_113;
uint8_t* var_9_114;
uint64_t var_9_115;
uint64_t var_9_116;
uint64_t var_9_117;
uint64_t var_9_118;
uint64_t var_9_119;
uint64_t var_9_120;
uint8_t* var_9_121;
uint64_t var_9_122;
uint8_t* var_9_123;
uint8_t* var_9_124;
uint8_t* var_9_0;
uint64_t var_9_1;
uint8_t* var_9_2;
uint64_t var_9_4;
uint8_t* var_9_5;
uint64_t var_9_8;
uint8_t* var_9_9;
uint64_t var_9_10;
uint64_t var_9_12;
uint8_t* var_9_13;
uint8_t* var_14_111;
uint8_t* var_14_112;
uint64_t var_14_113;
uint64_t var_14_114;
uint64_t var_14_115;
uint64_t var_14_116;
uint64_t var_14_117;
uint8_t* var_14_118;
uint8_t* var_14_119;
uint8_t* var_14_120;
uint64_t var_9_15;
uint8_t* var_9_16;
uint64_t var_9_17;
uint64_t var_9_18;
uint64_t var_9_19;
uint64_t var_9_21;
uint8_t* var_9_22;
uint64_t var_9_24;
uint64_t var_9_25;
uint8_t* var_10_101;
uint8_t* var_10_102;
uint64_t var_10_103;
uint64_t var_10_104;
uint64_t var_10_105;
uint64_t var_10_106;
uint64_t var_10_107;
uint8_t* var_10_108;
uint8_t* var_10_109;
uint8_t* var_10_110;
uint8_t* var_8_104;
uint8_t* var_8_105;
uint64_t var_8_106;
uint64_t var_8_107;
uint64_t var_8_108;
uint64_t var_8_109;
uint64_t var_8_110;
uint64_t var_8_111;
uint8_t* var_8_112;
uint64_t var_8_113;
uint8_t* var_8_114;
uint8_t* var_8_115;
uint8_t* var_8_116;
uint64_t var_8_0;
uint8_t* var_8_1;
uint8_t* var_8_2;
uint8_t* var_6_101;
uint8_t* var_6_102;
uint64_t var_6_103;
uint64_t var_6_104;
uint64_t var_6_105;
uint64_t var_6_106;
uint64_t var_6_107;
uint64_t var_6_108;
uint8_t* var_6_109;
uint64_t var_6_110;
uint8_t* var_6_111;
uint8_t* var_6_112;
uint8_t* var_6_113;
uint8_t* var_4_101;
uint8_t* var_4_102;
uint64_t var_4_103;
uint64_t var_4_104;
uint64_t var_4_105;
uint64_t var_4_106;
uint64_t var_4_107;
uint64_t var_4_108;
uint8_t* var_4_109;
uint64_t var_4_110;
uint8_t* var_4_111;
uint8_t* var_2_104;
uint8_t* var_2_105;
uint64_t var_2_106;
uint64_t var_2_107;
uint64_t var_2_108;
uint64_t var_2_1;
uint8_t* var_2_2;
//basic blocks
Block_0:
var_0_0 = 0;
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = NES__Runtime__TupleBuffer__Watermark(var_0_118);
var_0_6 = NES__Runtime__TupleBuffer__getOriginId(var_0_118);
var_0_8 = NES__Runtime__TupleBuffer__getSequenceNumber(var_0_118);
var_0_10 = 0;
var_0_11 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_116,var_0_10));
var_0_12 = getWorkerIdProxy(var_0_117);
var_0_13 = static_cast<uint8_t*>(getKeyedSliceStoreProxy(var_0_11,var_0_12));
var_0_14 = 0;
var_0_15 = NES__Runtime__TupleBuffer__getNumberOfTuples(var_0_118);
var_0_16 = static_cast<uint8_t*>(NES__Runtime__TupleBuffer__getBuffer(var_0_118));
var_0_17 = 0;
// prepare block arguments
var_11_102 = var_0_117;
var_11_103 = var_0_116;
var_11_104 = var_0_6;
var_11_105 = var_0_8;
var_11_106 = var_0_14;
var_11_107 = var_0_17;
var_11_108 = var_0_15;
var_11_109 = var_0_13;
var_11_110 = var_0_16;
goto Block_11;

Block_11:
var_0_18 = var_11_107 < var_11_108;
if (var_0_18){
// prepare block arguments
var_1_120 = var_11_102;
var_1_121 = var_11_103;
var_1_122 = var_11_104;
var_1_123 = var_11_105;
var_1_124 = var_11_108;
var_1_125 = var_11_107;
var_1_126 = var_11_109;
var_1_127 = var_11_106;
var_1_128 = var_11_110;
goto Block_1;
}else{
// prepare block arguments
var_2_104 = var_11_102;
var_2_105 = var_11_103;
var_2_106 = var_11_104;
var_2_107 = var_11_105;
var_2_108 = var_11_106;
goto Block_2;}

Block_1:
var_1_0 = 40;
var_1_1 = var_1_0*var_1_125;
var_1_2 = var_1_128+var_1_1;
var_1_3 = 0;
var_1_4 = var_1_2+var_1_3;
var_1_5 = *reinterpret_cast<uint64_t*>(var_1_4);
var_1_6 = 8;
var_1_7 = var_1_2+var_1_6;
var_1_8 = *reinterpret_cast<uint64_t*>(var_1_7);
var_1_9 = 16;
var_1_10 = var_1_2+var_1_9;
var_1_11 = *reinterpret_cast<uint64_t*>(var_1_10);
var_1_12 = 24;
var_1_13 = var_1_2+var_1_12;
var_1_14 = *reinterpret_cast<uint64_t*>(var_1_13);
var_1_15 = 32;
var_1_16 = var_1_2+var_1_15;
var_1_17 = *reinterpret_cast<double*>(var_1_16);
var_1_19 = var_1_14 > var_1_127;
if (var_1_19){
// prepare block arguments
var_3_101 = var_1_120;
var_3_102 = var_1_121;
var_3_103 = var_1_122;
var_3_104 = var_1_123;
var_3_105 = var_1_124;
var_3_106 = var_1_125;
var_3_107 = var_1_8;
var_3_108 = var_1_126;
var_3_109 = var_1_14;
var_3_110 = var_1_128;
goto Block_3;
}else{
// prepare block arguments
var_4_101 = var_1_120;
var_4_102 = var_1_121;
var_4_103 = var_1_122;
var_4_104 = var_1_123;
var_4_105 = var_1_127;
var_4_106 = var_1_124;
var_4_107 = var_1_125;
var_4_108 = var_1_8;
var_4_109 = var_1_126;
var_4_110 = var_1_14;
var_4_111 = var_1_128;
goto Block_4;}

Block_3:
// prepare block arguments
var_12_105 = var_3_101;
var_12_106 = var_3_102;
var_12_107 = var_3_103;
var_12_108 = var_3_104;
var_12_109 = var_3_109;
var_12_110 = var_3_105;
var_12_111 = var_3_106;
var_12_112 = var_3_107;
var_12_113 = var_3_108;
var_12_114 = var_3_109;
var_12_115 = var_3_110;
goto Block_12;

Block_12:
var_3_2 = static_cast<uint8_t*>(findKeyedSliceStateByTsProxy(var_12_113,var_12_114));
var_3_3 = 902850234;
var_3_4 = hashValueUI64(var_3_3,var_12_112);
var_3_6 = static_cast<uint8_t*>(findChainProxy(var_3_2,var_3_4));
// prepare block arguments
var_15_104 = var_12_105;
var_15_105 = var_12_106;
var_15_106 = var_12_107;
var_15_107 = var_12_108;
var_15_108 = var_12_109;
var_15_109 = var_12_110;
var_15_110 = var_12_111;
var_15_111 = var_12_112;
var_15_112 = var_3_2;
var_15_113 = var_3_4;
var_15_114 = var_3_6;
var_15_115 = var_12_113;
var_15_116 = var_12_115;
goto Block_15;

Block_15:
var_3_7 = 0;
var_3_8 = var_15_114 == nullptr;
var_3_9= !var_3_8;
if (var_3_9){
// prepare block arguments
var_5_109 = var_15_104;
var_5_110 = var_15_105;
var_5_111 = var_15_106;
var_5_112 = var_15_107;
var_5_113 = var_15_108;
var_5_114 = var_15_109;
var_5_115 = var_15_110;
var_5_116 = var_15_111;
var_5_117 = var_15_112;
var_5_118 = var_15_113;
var_5_119 = var_15_114;
var_5_120 = var_15_115;
var_5_121 = var_15_116;
goto Block_5;
}else{
// prepare block arguments
var_6_101 = var_15_104;
var_6_102 = var_15_105;
var_6_103 = var_15_106;
var_6_104 = var_15_107;
var_6_105 = var_15_108;
var_6_106 = var_15_109;
var_6_107 = var_15_110;
var_6_108 = var_15_111;
var_6_109 = var_15_112;
var_6_110 = var_15_113;
var_6_111 = var_15_114;
var_6_112 = var_15_115;
var_6_113 = var_15_116;
goto Block_6;}

Block_5:
var_5_0 = 1;
var_5_1 = 16;
var_5_2 = var_5_119+var_5_1;
var_5_3 = *reinterpret_cast<uint64_t*>(var_5_2);
var_5_4 = var_5_116 == var_5_3;
var_5_5 = var_5_0&&var_5_4;
var_5_7 = 8;
var_5_8 = var_5_2+var_5_7;
if (var_5_5){
// prepare block arguments
var_7_101 = var_5_109;
var_7_102 = var_5_110;
var_7_103 = var_5_111;
var_7_104 = var_5_112;
var_7_105 = var_5_113;
var_7_106 = var_5_114;
var_7_107 = var_5_115;
var_7_108 = var_5_116;
var_7_109 = var_5_117;
var_7_110 = var_5_118;
var_7_111 = var_5_119;
var_7_112 = var_5_120;
var_7_113 = var_5_121;
goto Block_7;
}else{
// prepare block arguments
var_8_104 = var_5_109;
var_8_105 = var_5_110;
var_8_106 = var_5_111;
var_8_107 = var_5_112;
var_8_108 = var_5_113;
var_8_109 = var_5_114;
var_8_110 = var_5_115;
var_8_111 = var_5_116;
var_8_112 = var_5_117;
var_8_113 = var_5_118;
var_8_114 = var_5_120;
var_8_115 = var_5_121;
var_8_116 = var_5_119;
goto Block_8;}

Block_7:
// prepare block arguments
var_13_103 = var_7_101;
var_13_104 = var_7_102;
var_13_105 = var_7_103;
var_13_106 = var_7_104;
var_13_107 = var_7_105;
var_13_108 = var_7_106;
var_13_109 = var_7_107;
var_13_110 = var_7_108;
var_13_111 = var_7_109;
var_13_112 = var_7_110;
var_13_113 = var_7_111;
var_13_114 = var_7_112;
var_13_115 = var_7_113;
goto Block_13;

Block_13:
var_7_0 = 0;
var_7_1 = var_13_113 == nullptr;
if (var_7_1){
// prepare block arguments
var_9_113 = var_13_103;
var_9_114 = var_13_104;
var_9_115 = var_13_105;
var_9_116 = var_13_106;
var_9_117 = var_13_107;
var_9_118 = var_13_108;
var_9_119 = var_13_109;
var_9_120 = var_13_110;
var_9_121 = var_13_111;
var_9_122 = var_13_112;
var_9_123 = var_13_114;
var_9_124 = var_13_115;
goto Block_9;
}else{
// prepare block arguments
var_10_101 = var_13_103;
var_10_102 = var_13_104;
var_10_103 = var_13_105;
var_10_104 = var_13_106;
var_10_105 = var_13_107;
var_10_106 = var_13_108;
var_10_107 = var_13_109;
var_10_108 = var_13_113;
var_10_109 = var_13_114;
var_10_110 = var_13_115;
goto Block_10;}

Block_9:
var_9_0 = static_cast<uint8_t*>(insertProxy(var_9_121,var_9_122));
var_9_1 = 16;
var_9_2 = var_9_0+var_9_1;
*reinterpret_cast<uint64_t*>(var_9_2) = var_9_120;
var_9_4 = 8;
var_9_5 = var_9_2+var_9_4;
var_9_8 = 24;
var_9_9 = var_9_0+var_9_8;
var_9_10 = 0;
*reinterpret_cast<uint64_t*>(var_9_9) = var_9_10;
var_9_12 = 8;
var_9_13 = var_9_9+var_9_12;
// prepare block arguments
var_14_111 = var_9_113;
var_14_112 = var_9_114;
var_14_113 = var_9_115;
var_14_114 = var_9_116;
var_14_115 = var_9_117;
var_14_116 = var_9_118;
var_14_117 = var_9_119;
var_14_118 = var_9_0;
var_14_119 = var_9_123;
var_14_120 = var_9_124;
goto Block_14;

Block_14:
var_9_15 = 24;
var_9_16 = var_14_118+var_9_15;
var_9_17 = *reinterpret_cast<uint64_t*>(var_9_16);
var_9_18 = 1;
var_9_19 = var_9_17+var_9_18;
*reinterpret_cast<uint64_t*>(var_9_16) = var_9_19;
var_9_21 = 8;
var_9_22 = var_9_16+var_9_21;
var_9_24 = 1;
var_9_25 = var_14_117+var_9_24;
// prepare block arguments
var_11_102 = var_14_111;
var_11_103 = var_14_112;
var_11_104 = var_14_113;
var_11_105 = var_14_114;
var_11_106 = var_14_115;
var_11_107 = var_9_25;
var_11_108 = var_14_116;
var_11_109 = var_14_119;
var_11_110 = var_14_120;
goto Block_11;

Block_10:
// prepare block arguments
var_14_111 = var_10_101;
var_14_112 = var_10_102;
var_14_113 = var_10_103;
var_14_114 = var_10_104;
var_14_115 = var_10_105;
var_14_116 = var_10_106;
var_14_117 = var_10_107;
var_14_118 = var_10_108;
var_14_119 = var_10_109;
var_14_120 = var_10_110;
goto Block_14;

Block_8:
var_8_0 = 0;
var_8_1 = var_8_116+var_8_0;
var_8_2 = *reinterpret_cast<uint8_t**>(var_8_1);
// prepare block arguments
var_15_104 = var_8_104;
var_15_105 = var_8_105;
var_15_106 = var_8_106;
var_15_107 = var_8_107;
var_15_108 = var_8_108;
var_15_109 = var_8_109;
var_15_110 = var_8_110;
var_15_111 = var_8_111;
var_15_112 = var_8_112;
var_15_113 = var_8_113;
var_15_114 = var_8_2;
var_15_115 = var_8_114;
var_15_116 = var_8_115;
goto Block_15;

Block_6:
// prepare block arguments
var_13_103 = var_6_101;
var_13_104 = var_6_102;
var_13_105 = var_6_103;
var_13_106 = var_6_104;
var_13_107 = var_6_105;
var_13_108 = var_6_106;
var_13_109 = var_6_107;
var_13_110 = var_6_108;
var_13_111 = var_6_109;
var_13_112 = var_6_110;
var_13_113 = var_6_111;
var_13_114 = var_6_112;
var_13_115 = var_6_113;
goto Block_13;

Block_4:
// prepare block arguments
var_12_105 = var_4_101;
var_12_106 = var_4_102;
var_12_107 = var_4_103;
var_12_108 = var_4_104;
var_12_109 = var_4_105;
var_12_110 = var_4_106;
var_12_111 = var_4_107;
var_12_112 = var_4_108;
var_12_113 = var_4_109;
var_12_114 = var_4_110;
var_12_115 = var_4_111;
goto Block_12;

Block_2:
var_2_1 = 0;
var_2_2 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_2_105,var_2_1));
triggerKeyedThreadLocalWindow(var_2_2,var_2_104,var_2_105,var_2_106,var_2_107,var_2_108);
return;

}

static inline auto terminate(uint8_t* var_0_105 ,uint8_t* var_0_106 ){
//variable declarations
uint64_t var_0_0;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
//basic blocks
Block_0:
var_0_0 = 0;
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
return;

}

#pragma GCC diagnostic pop
template<>
void NES::Unikernel::Stage<5>::terminate(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext) {
           ::terminate(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));
}
template<>
void NES::Unikernel::Stage<5>::setup(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext) {
           ::setup(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));
}
template<>
void NES::Unikernel::Stage<5>::execute(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext, NES::Runtime::TupleBuffer& buffer) {
           ::execute(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext), reinterpret_cast<uint8_t*>(&buffer));
}
template<>
NES::Runtime::Execution::OperatorHandler* NES::Unikernel::Stage<5>::getOperatorHandler(int index) {
static NES::Runtime::Execution::Operators::KeyedSlicePreAggregationHandler handler0(uint64_t(10000), uint64_t(2000), std::vector<uint64_t>{uint64_t(4)});
switch(index) {
case 0:
	return &handler0;
default:
	assert(false && "Bad OperatorHandler index");
	return nullptr;}}
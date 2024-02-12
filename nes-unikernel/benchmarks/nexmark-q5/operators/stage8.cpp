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
uint64_t var_0_9;
uint8_t* var_0_10;
uint64_t var_0_11;
uint64_t var_0_12;
uint64_t var_0_13;
uint8_t* var_0_16;
uint64_t var_0_17;
uint64_t var_0_19;
uint8_t* var_0_20;
//basic blocks
Block_0:
var_0_0 = 0;
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 0;
var_0_5 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_121,var_0_4));
var_0_6 = 8;
var_0_7 = 8;
setupKeyedSliceMergingHandler(var_0_5,var_0_121,var_0_6,var_0_7);
var_0_9 = 1;
var_0_10 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_121,var_0_9));
var_0_11 = 0;
var_0_12 = 8;
var_0_13 = var_0_11+var_0_12;
setupWindowHandlerNonKeyed(var_0_10,var_0_121,var_0_13);
var_0_16 = static_cast<uint8_t*>(getDefaultState(var_0_10));
var_0_17 = 0;
*reinterpret_cast<uint64_t*>(var_0_16) = var_0_17;
var_0_19 = 8;
var_0_20 = var_0_16+var_0_19;
return;

}

static inline auto execute(uint8_t* var_0_126 ,uint8_t* var_0_127 ,uint8_t* var_0_128 ){
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
uint64_t var_0_9;
uint8_t* var_0_10;
uint8_t* var_0_11;
uint64_t var_0_12;
uint8_t* var_0_13;
uint64_t var_0_14;
uint64_t var_0_15;
uint8_t* var_0_16;
uint64_t var_0_17;
uint64_t var_0_18;
uint8_t* var_0_19;
uint64_t var_0_20;
uint8_t* var_0_21;
uint8_t* var_0_22;
uint64_t var_0_23;
uint64_t var_0_24;
uint8_t* var_5_102;
uint8_t* var_5_103;
uint64_t var_5_104;
uint8_t* var_5_105;
uint64_t var_5_106;
uint64_t var_5_107;
uint8_t* var_5_108;
uint64_t var_5_109;
uint64_t var_5_110;
uint8_t* var_5_111;
uint8_t* var_5_112;
uint64_t var_5_113;
bool var_0_25;
uint8_t* var_1_116;
uint8_t* var_1_117;
uint64_t var_1_118;
uint8_t* var_1_119;
uint64_t var_1_120;
uint64_t var_1_121;
uint8_t* var_1_122;
uint64_t var_1_123;
uint64_t var_1_124;
uint8_t* var_1_125;
uint8_t* var_1_126;
uint64_t var_1_127;
uint8_t* var_1_0;
uint64_t var_1_1;
uint8_t* var_1_2;
uint64_t var_1_3;
uint64_t var_1_4;
uint64_t var_1_5;
uint64_t var_1_6;
uint8_t* var_1_7;
uint64_t var_1_8;
uint64_t var_1_9;
uint8_t* var_1_10;
uint64_t var_1_11;
uint64_t var_1_12;
uint64_t var_1_13;
uint64_t var_1_14;
uint8_t* var_16_102;
uint8_t* var_16_103;
uint64_t var_16_104;
uint8_t* var_16_105;
uint64_t var_16_106;
uint64_t var_16_107;
uint8_t* var_16_108;
uint64_t var_16_109;
uint64_t var_16_110;
uint64_t var_16_111;
uint64_t var_16_112;
uint8_t* var_16_113;
uint64_t var_16_114;
uint64_t var_16_115;
uint64_t var_16_116;
uint8_t* var_16_117;
uint8_t* var_16_118;
uint8_t* var_16_119;
uint64_t var_16_120;
bool var_1_15;
uint8_t* var_3_103;
uint8_t* var_3_104;
uint64_t var_3_105;
uint8_t* var_3_106;
uint64_t var_3_107;
uint64_t var_3_108;
uint8_t* var_3_109;
uint64_t var_3_110;
uint64_t var_3_111;
uint8_t* var_3_112;
uint8_t* var_3_113;
uint64_t var_3_114;
uint64_t var_3_0;
uint64_t var_3_1;
uint8_t* var_4_108;
uint8_t* var_4_109;
uint64_t var_4_110;
uint8_t* var_4_111;
uint64_t var_4_112;
uint64_t var_4_113;
uint8_t* var_4_114;
uint64_t var_4_115;
uint64_t var_4_116;
uint64_t var_4_117;
uint64_t var_4_118;
uint8_t* var_4_119;
uint64_t var_4_120;
uint64_t var_4_121;
uint64_t var_4_122;
uint8_t* var_4_123;
uint8_t* var_4_124;
uint8_t* var_4_125;
uint64_t var_4_126;
uint64_t var_4_0;
uint64_t var_4_1;
uint8_t* var_4_2;
uint64_t var_4_3;
uint8_t* var_4_4;
uint64_t var_4_5;
uint8_t* var_4_6;
uint8_t* var_25_104;
uint8_t* var_25_105;
uint64_t var_25_106;
uint8_t* var_25_107;
uint64_t var_25_108;
uint64_t var_25_109;
uint8_t* var_25_110;
uint64_t var_25_111;
uint64_t var_25_112;
uint64_t var_25_113;
uint64_t var_25_114;
uint8_t* var_25_115;
uint64_t var_25_116;
uint64_t var_25_117;
uint64_t var_25_118;
uint8_t* var_25_119;
uint8_t* var_25_120;
uint8_t* var_25_121;
uint8_t* var_25_122;
uint8_t* var_25_123;
uint64_t var_25_124;
int32_t var_4_7;
bool var_4_8;
bool var_4_9;
uint8_t* var_8_107;
uint8_t* var_8_108;
uint64_t var_8_109;
uint8_t* var_8_110;
uint64_t var_8_111;
uint64_t var_8_112;
uint8_t* var_8_113;
uint64_t var_8_114;
uint64_t var_8_115;
uint64_t var_8_116;
uint64_t var_8_117;
uint8_t* var_8_118;
uint64_t var_8_119;
uint64_t var_8_120;
uint64_t var_8_121;
uint8_t* var_8_122;
uint8_t* var_8_123;
uint8_t* var_8_124;
uint8_t* var_8_125;
uint8_t* var_8_126;
uint64_t var_8_127;
uint64_t var_8_0;
uint8_t* var_8_1;
uint64_t var_8_2;
uint8_t* var_8_3;
uint64_t var_8_4;
bool var_8_5;
uint8_t* var_10_115;
uint8_t* var_10_116;
uint64_t var_10_117;
uint8_t* var_10_118;
uint64_t var_10_119;
uint64_t var_10_120;
uint8_t* var_10_121;
uint64_t var_10_122;
uint64_t var_10_123;
uint64_t var_10_124;
uint64_t var_10_125;
uint8_t* var_10_126;
uint64_t var_10_127;
uint64_t var_10_128;
uint64_t var_10_129;
uint8_t* var_10_130;
uint8_t* var_10_131;
uint8_t* var_10_132;
uint8_t* var_10_133;
uint8_t* var_10_134;
uint64_t var_10_135;
uint64_t var_10_0;
uint8_t* var_10_1;
uint64_t var_10_2;
uint8_t* var_10_3;
uint64_t var_10_4;
uint8_t* var_10_5;
uint64_t var_10_6;
uint64_t var_10_7;
uint64_t var_10_8;
uint64_t var_10_10;
uint8_t* var_10_11;
uint64_t var_10_13;
uint8_t* var_10_14;
uint8_t* var_22_103;
uint8_t* var_22_104;
uint64_t var_22_105;
uint8_t* var_22_106;
uint64_t var_22_107;
uint64_t var_22_108;
uint8_t* var_22_109;
uint64_t var_22_110;
uint64_t var_22_111;
uint64_t var_22_112;
uint64_t var_22_113;
uint8_t* var_22_114;
uint64_t var_22_115;
uint64_t var_22_116;
uint64_t var_22_117;
uint8_t* var_22_118;
uint8_t* var_22_119;
uint8_t* var_22_120;
uint8_t* var_22_121;
uint8_t* var_22_122;
uint64_t var_22_123;
int32_t var_10_16;
bool var_10_17;
uint8_t* var_12_111;
uint8_t* var_12_112;
uint64_t var_12_113;
uint8_t* var_12_114;
uint64_t var_12_115;
uint64_t var_12_116;
uint8_t* var_12_117;
uint64_t var_12_118;
uint64_t var_12_119;
uint64_t var_12_120;
uint64_t var_12_121;
uint8_t* var_12_122;
uint64_t var_12_123;
uint64_t var_12_124;
uint64_t var_12_125;
uint8_t* var_12_126;
uint8_t* var_12_127;
uint8_t* var_12_128;
uint8_t* var_12_129;
uint64_t var_12_130;
uint64_t var_12_0;
uint8_t* var_12_1;
uint64_t var_12_2;
uint8_t* var_12_3;
uint64_t var_12_4;
uint8_t* var_12_5;
uint64_t var_12_6;
uint8_t* var_12_7;
uint64_t var_12_8;
uint8_t* var_12_9;
uint8_t* var_23_104;
uint8_t* var_23_105;
uint64_t var_23_106;
uint8_t* var_23_107;
uint64_t var_23_108;
uint64_t var_23_109;
uint8_t* var_23_110;
uint64_t var_23_111;
uint64_t var_23_112;
uint64_t var_23_113;
uint64_t var_23_114;
uint8_t* var_23_115;
uint64_t var_23_116;
uint64_t var_23_117;
uint64_t var_23_118;
uint8_t* var_23_119;
uint8_t* var_23_120;
uint8_t* var_23_121;
uint64_t var_23_122;
uint64_t var_12_10;
uint64_t var_12_11;
bool var_12_13;
uint8_t* var_14_105;
uint8_t* var_14_106;
uint64_t var_14_107;
uint8_t* var_14_108;
uint64_t var_14_109;
uint64_t var_14_110;
uint8_t* var_14_111;
uint64_t var_14_112;
uint64_t var_14_113;
uint64_t var_14_114;
uint64_t var_14_115;
uint8_t* var_14_116;
uint64_t var_14_117;
uint64_t var_14_118;
uint8_t* var_14_119;
uint8_t* var_14_120;
uint64_t var_14_121;
uint64_t var_14_0;
uint64_t var_14_2;
uint64_t var_14_3;
uint8_t* var_14_5;
uint8_t* var_24_103;
uint8_t* var_24_104;
uint64_t var_24_105;
uint8_t* var_24_106;
uint64_t var_24_107;
uint64_t var_24_108;
uint8_t* var_24_109;
uint64_t var_24_110;
uint64_t var_24_111;
uint64_t var_24_112;
uint64_t var_24_113;
uint8_t* var_24_114;
uint64_t var_24_115;
uint64_t var_24_116;
uint64_t var_24_117;
uint8_t* var_24_118;
uint8_t* var_24_119;
uint8_t* var_24_120;
uint64_t var_24_121;
uint64_t var_14_7;
uint64_t var_14_8;
uint8_t* var_15_101;
uint8_t* var_15_102;
uint64_t var_15_103;
uint8_t* var_15_104;
uint64_t var_15_105;
uint64_t var_15_106;
uint8_t* var_15_107;
uint64_t var_15_108;
uint64_t var_15_109;
uint64_t var_15_110;
uint64_t var_15_111;
uint8_t* var_15_112;
uint64_t var_15_113;
uint64_t var_15_114;
uint64_t var_15_115;
uint8_t* var_15_116;
uint8_t* var_15_117;
uint8_t* var_15_118;
uint64_t var_15_119;
uint8_t* var_13_101;
uint8_t* var_13_102;
uint64_t var_13_103;
uint8_t* var_13_104;
uint64_t var_13_105;
uint64_t var_13_106;
uint8_t* var_13_107;
uint64_t var_13_108;
uint64_t var_13_109;
uint64_t var_13_110;
uint64_t var_13_111;
uint8_t* var_13_112;
uint64_t var_13_113;
uint64_t var_13_114;
uint64_t var_13_115;
uint8_t* var_13_116;
uint8_t* var_13_117;
uint8_t* var_13_118;
uint64_t var_13_119;
uint8_t* var_11_104;
uint8_t* var_11_105;
uint64_t var_11_106;
uint8_t* var_11_107;
uint64_t var_11_108;
uint64_t var_11_109;
uint8_t* var_11_110;
uint64_t var_11_111;
uint64_t var_11_112;
uint64_t var_11_113;
uint64_t var_11_114;
uint8_t* var_11_115;
uint64_t var_11_116;
uint64_t var_11_117;
uint64_t var_11_118;
uint8_t* var_11_119;
uint8_t* var_11_120;
uint8_t* var_11_121;
uint8_t* var_11_122;
uint8_t* var_11_123;
uint64_t var_11_124;
uint64_t var_11_0;
uint8_t* var_11_1;
uint8_t* var_11_2;
uint8_t* var_9_101;
uint8_t* var_9_102;
uint64_t var_9_103;
uint8_t* var_9_104;
uint64_t var_9_105;
uint64_t var_9_106;
uint8_t* var_9_107;
uint64_t var_9_108;
uint64_t var_9_109;
uint64_t var_9_110;
uint64_t var_9_111;
uint8_t* var_9_112;
uint64_t var_9_113;
uint64_t var_9_114;
uint64_t var_9_115;
uint8_t* var_9_116;
uint8_t* var_9_117;
uint8_t* var_9_118;
uint8_t* var_9_119;
uint8_t* var_9_120;
uint64_t var_9_121;
uint8_t* var_2_118;
uint8_t* var_2_119;
uint64_t var_2_120;
uint8_t* var_2_121;
uint64_t var_2_122;
uint64_t var_2_123;
uint8_t* var_2_124;
uint8_t* var_2_125;
uint64_t var_2_126;
uint64_t var_2_2;
uint8_t* var_2_5;
uint64_t var_2_6;
uint8_t* var_2_7;
uint64_t var_2_8;
uint64_t var_2_9;
uint64_t var_2_10;
uint64_t var_2_11;
uint8_t* var_2_12;
uint64_t var_2_13;
uint64_t var_2_14;
uint8_t* var_2_15;
uint64_t var_2_16;
uint64_t var_2_17;
uint64_t var_2_18;
uint64_t var_2_19;
uint8_t* var_21_102;
uint8_t* var_21_103;
uint64_t var_21_104;
uint64_t var_21_105;
uint64_t var_21_106;
uint8_t* var_21_107;
uint64_t var_21_108;
uint64_t var_21_109;
uint8_t* var_21_110;
uint64_t var_21_111;
uint64_t var_21_112;
uint64_t var_21_113;
uint8_t* var_21_114;
uint64_t var_21_115;
uint8_t* var_21_116;
bool var_2_20;
uint8_t* var_6_106;
uint8_t* var_6_107;
uint64_t var_6_108;
uint64_t var_6_109;
uint64_t var_6_110;
uint8_t* var_6_111;
uint64_t var_6_2;
uint8_t* var_6_3;
uint64_t var_6_4;
uint8_t* var_7_115;
uint8_t* var_7_116;
uint64_t var_7_117;
uint64_t var_7_118;
uint8_t* var_7_119;
uint64_t var_7_120;
uint64_t var_7_121;
uint8_t* var_7_122;
uint64_t var_7_123;
uint64_t var_7_124;
uint64_t var_7_125;
uint8_t* var_7_126;
uint64_t var_7_127;
uint64_t var_7_128;
uint8_t* var_7_129;
uint64_t var_7_0;
uint64_t var_7_1;
uint8_t* var_7_2;
uint64_t var_7_3;
uint8_t* var_7_4;
uint64_t var_7_5;
uint64_t var_7_6;
uint8_t* var_7_7;
uint64_t var_7_9;
uint8_t* var_7_10;
uint64_t var_7_11;
uint64_t var_7_12;
uint8_t* var_7_13;
bool var_7_16;
uint8_t* var_17_101;
uint8_t* var_17_102;
uint64_t var_17_103;
uint64_t var_17_104;
uint8_t* var_17_105;
uint64_t var_17_106;
uint64_t var_17_107;
uint8_t* var_17_108;
uint64_t var_17_109;
uint64_t var_17_110;
uint64_t var_17_111;
uint64_t var_17_112;
uint8_t* var_17_113;
uint64_t var_17_114;
uint8_t* var_17_115;
uint8_t* var_26_110;
uint8_t* var_26_111;
uint64_t var_26_112;
uint64_t var_26_113;
uint64_t var_26_114;
uint8_t* var_26_115;
uint64_t var_26_116;
uint64_t var_26_117;
uint8_t* var_26_118;
uint64_t var_26_119;
uint64_t var_26_120;
uint64_t var_26_121;
uint64_t var_26_122;
uint8_t* var_26_123;
uint64_t var_26_124;
uint8_t* var_26_125;
uint8_t* var_17_2;
uint64_t var_17_3;
uint8_t* var_17_4;
uint64_t var_17_5;
uint64_t var_17_6;
uint64_t var_17_8;
uint64_t var_17_9;
bool var_17_11;
uint8_t* var_19_105;
uint8_t* var_19_106;
uint64_t var_19_107;
uint64_t var_19_108;
uint64_t var_19_109;
uint8_t* var_19_110;
uint64_t var_19_111;
uint64_t var_19_112;
uint8_t* var_19_113;
uint64_t var_19_114;
uint64_t var_19_115;
uint8_t* var_19_116;
uint64_t var_19_117;
uint64_t var_19_0;
uint64_t var_19_2;
uint64_t var_19_3;
uint8_t* var_19_5;
uint8_t* var_27_103;
uint8_t* var_27_104;
uint64_t var_27_105;
uint64_t var_27_106;
uint64_t var_27_107;
uint8_t* var_27_108;
uint64_t var_27_109;
uint64_t var_27_110;
uint8_t* var_27_111;
uint64_t var_27_112;
uint64_t var_27_113;
uint64_t var_27_114;
uint8_t* var_27_115;
uint64_t var_27_116;
uint8_t* var_27_117;
uint64_t var_19_7;
uint64_t var_19_8;
uint8_t* var_20_101;
uint8_t* var_20_102;
uint64_t var_20_103;
uint64_t var_20_104;
uint64_t var_20_105;
uint8_t* var_20_106;
uint64_t var_20_107;
uint64_t var_20_108;
uint8_t* var_20_109;
uint64_t var_20_110;
uint64_t var_20_111;
uint64_t var_20_112;
uint8_t* var_20_113;
uint64_t var_20_114;
uint8_t* var_20_115;
uint8_t* var_18_101;
uint8_t* var_18_102;
uint64_t var_18_103;
uint64_t var_18_104;
uint64_t var_18_105;
uint8_t* var_18_106;
uint64_t var_18_107;
uint64_t var_18_108;
uint8_t* var_18_109;
uint64_t var_18_110;
uint64_t var_18_111;
uint64_t var_18_112;
uint64_t var_18_113;
uint8_t* var_18_114;
uint64_t var_18_115;
uint8_t* var_18_116;
//basic blocks
Block_0:
var_0_0 = 0;
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 1;
var_0_5 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_126,var_0_4));
var_0_6 = getWorkerIdProxy(var_0_127);
var_0_7 = static_cast<uint8_t*>(getSliceStoreProxy(var_0_5,var_0_6));
var_0_8 = 0;
var_0_9 = 0;
var_0_10 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_126,var_0_9));
var_0_11 = static_cast<uint8_t*>(NES__Runtime__TupleBuffer__getBuffer(var_0_128));
var_0_12 = 8;
var_0_13 = var_0_11+var_0_12;
var_0_14 = *reinterpret_cast<uint64_t*>(var_0_13);
var_0_15 = 16;
var_0_16 = var_0_11+var_0_15;
var_0_17 = *reinterpret_cast<uint64_t*>(var_0_16);
var_0_18 = 0;
var_0_19 = var_0_11+var_0_18;
var_0_20 = *reinterpret_cast<uint64_t*>(var_0_19);
var_0_21 = static_cast<uint8_t*>(createKeyedState(var_0_10,var_0_11));
var_0_22 = static_cast<uint8_t*>(getKeyedSliceState(var_0_21));
var_0_23 = getKeyedNumberOfSlicesFromTask(var_0_11);
var_0_24 = 0;
// prepare block arguments
var_5_102 = var_0_127;
var_5_103 = var_0_126;
var_5_104 = var_0_8;
var_5_105 = var_0_21;
var_5_106 = var_0_20;
var_5_107 = var_0_17;
var_5_108 = var_0_11;
var_5_109 = var_0_24;
var_5_110 = var_0_23;
var_5_111 = var_0_22;
var_5_112 = var_0_7;
var_5_113 = var_0_14;
goto Block_5;

Block_5:
var_0_25 = var_5_109 < var_5_110;
if (var_0_25){
// prepare block arguments
var_1_116 = var_5_102;
var_1_117 = var_5_103;
var_1_118 = var_5_104;
var_1_119 = var_5_105;
var_1_120 = var_5_106;
var_1_121 = var_5_107;
var_1_122 = var_5_108;
var_1_123 = var_5_110;
var_1_124 = var_5_109;
var_1_125 = var_5_111;
var_1_126 = var_5_112;
var_1_127 = var_5_113;
goto Block_1;
}else{
// prepare block arguments
var_2_118 = var_5_102;
var_2_119 = var_5_103;
var_2_120 = var_5_104;
var_2_121 = var_5_105;
var_2_122 = var_5_106;
var_2_123 = var_5_107;
var_2_124 = var_5_108;
var_2_125 = var_5_112;
var_2_126 = var_5_113;
goto Block_2;}

Block_1:
var_1_0 = static_cast<uint8_t*>(getKeyedSliceStateFromTask(var_1_122,var_1_124));
var_1_1 = 48;
var_1_2 = var_1_0+var_1_1;
var_1_3 = *reinterpret_cast<uint64_t*>(var_1_2);
var_1_4 = 0;
var_1_5 = 0;
var_1_6 = 0;
var_1_7 = static_cast<uint8_t*>(getPageProxy(var_1_0,var_1_6));
var_1_8 = 0;
var_1_9 = 72;
var_1_10 = var_1_0+var_1_9;
var_1_11 = *reinterpret_cast<uint64_t*>(var_1_10);
var_1_12 = 0;
var_1_13 = 0;
var_1_14 = 0;
// prepare block arguments
var_16_102 = var_1_116;
var_16_103 = var_1_117;
var_16_104 = var_1_118;
var_16_105 = var_1_119;
var_16_106 = var_1_120;
var_16_107 = var_1_121;
var_16_108 = var_1_122;
var_16_109 = var_1_123;
var_16_110 = var_1_124;
var_16_111 = var_1_4;
var_16_112 = var_1_11;
var_16_113 = var_1_0;
var_16_114 = var_1_8;
var_16_115 = var_1_3;
var_16_116 = var_1_5;
var_16_117 = var_1_125;
var_16_118 = var_1_7;
var_16_119 = var_1_126;
var_16_120 = var_1_127;
goto Block_16;

Block_16:
var_1_15 = var_16_111 == var_16_112;
if (var_1_15){
// prepare block arguments
var_3_103 = var_16_102;
var_3_104 = var_16_103;
var_3_105 = var_16_104;
var_3_106 = var_16_105;
var_3_107 = var_16_106;
var_3_108 = var_16_107;
var_3_109 = var_16_108;
var_3_110 = var_16_109;
var_3_111 = var_16_110;
var_3_112 = var_16_117;
var_3_113 = var_16_119;
var_3_114 = var_16_120;
goto Block_3;
}else{
// prepare block arguments
var_4_108 = var_16_102;
var_4_109 = var_16_103;
var_4_110 = var_16_104;
var_4_111 = var_16_105;
var_4_112 = var_16_106;
var_4_113 = var_16_107;
var_4_114 = var_16_108;
var_4_115 = var_16_109;
var_4_116 = var_16_110;
var_4_117 = var_16_112;
var_4_118 = var_16_111;
var_4_119 = var_16_113;
var_4_120 = var_16_114;
var_4_121 = var_16_115;
var_4_122 = var_16_116;
var_4_123 = var_16_117;
var_4_124 = var_16_118;
var_4_125 = var_16_119;
var_4_126 = var_16_120;
goto Block_4;}

Block_3:
var_3_0 = 1;
var_3_1 = var_3_111+var_3_0;
// prepare block arguments
var_5_102 = var_3_103;
var_5_103 = var_3_104;
var_5_104 = var_3_105;
var_5_105 = var_3_106;
var_5_106 = var_3_107;
var_5_107 = var_3_108;
var_5_108 = var_3_109;
var_5_109 = var_3_1;
var_5_110 = var_3_110;
var_5_111 = var_3_112;
var_5_112 = var_3_113;
var_5_113 = var_3_114;
goto Block_5;

Block_4:
var_4_0 = 32;
var_4_1 = var_4_0*var_4_122;
var_4_2 = var_4_124+var_4_1;
var_4_3 = 8;
var_4_4 = var_4_2+var_4_3;
var_4_5 = *reinterpret_cast<uint64_t*>(var_4_4);
var_4_6 = static_cast<uint8_t*>(findChainProxy(var_4_123,var_4_5));
// prepare block arguments
var_25_104 = var_4_108;
var_25_105 = var_4_109;
var_25_106 = var_4_110;
var_25_107 = var_4_111;
var_25_108 = var_4_112;
var_25_109 = var_4_113;
var_25_110 = var_4_114;
var_25_111 = var_4_115;
var_25_112 = var_4_116;
var_25_113 = var_4_117;
var_25_114 = var_4_118;
var_25_115 = var_4_119;
var_25_116 = var_4_120;
var_25_117 = var_4_121;
var_25_118 = var_4_122;
var_25_119 = var_4_2;
var_25_120 = var_4_123;
var_25_121 = var_4_6;
var_25_122 = var_4_124;
var_25_123 = var_4_125;
var_25_124 = var_4_126;
goto Block_25;

Block_25:
var_4_7 = 0;
var_4_8 = var_25_121 == nullptr;
var_4_9= !var_4_8;
if (var_4_9){
// prepare block arguments
var_8_107 = var_25_104;
var_8_108 = var_25_105;
var_8_109 = var_25_106;
var_8_110 = var_25_107;
var_8_111 = var_25_108;
var_8_112 = var_25_109;
var_8_113 = var_25_110;
var_8_114 = var_25_111;
var_8_115 = var_25_112;
var_8_116 = var_25_113;
var_8_117 = var_25_114;
var_8_118 = var_25_115;
var_8_119 = var_25_116;
var_8_120 = var_25_117;
var_8_121 = var_25_118;
var_8_122 = var_25_119;
var_8_123 = var_25_120;
var_8_124 = var_25_121;
var_8_125 = var_25_122;
var_8_126 = var_25_123;
var_8_127 = var_25_124;
goto Block_8;
}else{
// prepare block arguments
var_9_101 = var_25_104;
var_9_102 = var_25_105;
var_9_103 = var_25_106;
var_9_104 = var_25_107;
var_9_105 = var_25_108;
var_9_106 = var_25_109;
var_9_107 = var_25_110;
var_9_108 = var_25_111;
var_9_109 = var_25_112;
var_9_110 = var_25_113;
var_9_111 = var_25_114;
var_9_112 = var_25_115;
var_9_113 = var_25_116;
var_9_114 = var_25_117;
var_9_115 = var_25_118;
var_9_116 = var_25_119;
var_9_117 = var_25_120;
var_9_118 = var_25_121;
var_9_119 = var_25_122;
var_9_120 = var_25_123;
var_9_121 = var_25_124;
goto Block_9;}

Block_8:
var_8_0 = 16;
var_8_1 = var_8_124+var_8_0;
var_8_2 = 16;
var_8_3 = var_8_122+var_8_2;
var_8_4 = 8;
var_8_5 = memeq(var_8_1,var_8_3,var_8_4);
if (var_8_5){
// prepare block arguments
var_10_115 = var_8_107;
var_10_116 = var_8_108;
var_10_117 = var_8_109;
var_10_118 = var_8_110;
var_10_119 = var_8_111;
var_10_120 = var_8_112;
var_10_121 = var_8_113;
var_10_122 = var_8_114;
var_10_123 = var_8_115;
var_10_124 = var_8_116;
var_10_125 = var_8_117;
var_10_126 = var_8_118;
var_10_127 = var_8_119;
var_10_128 = var_8_120;
var_10_129 = var_8_121;
var_10_130 = var_8_122;
var_10_131 = var_8_123;
var_10_132 = var_8_124;
var_10_133 = var_8_125;
var_10_134 = var_8_126;
var_10_135 = var_8_127;
goto Block_10;
}else{
// prepare block arguments
var_11_104 = var_8_107;
var_11_105 = var_8_108;
var_11_106 = var_8_109;
var_11_107 = var_8_110;
var_11_108 = var_8_111;
var_11_109 = var_8_112;
var_11_110 = var_8_113;
var_11_111 = var_8_114;
var_11_112 = var_8_115;
var_11_113 = var_8_116;
var_11_114 = var_8_117;
var_11_115 = var_8_118;
var_11_116 = var_8_119;
var_11_117 = var_8_120;
var_11_118 = var_8_121;
var_11_119 = var_8_122;
var_11_120 = var_8_123;
var_11_121 = var_8_124;
var_11_122 = var_8_125;
var_11_123 = var_8_126;
var_11_124 = var_8_127;
goto Block_11;}

Block_10:
var_10_0 = 16;
var_10_1 = var_10_130+var_10_0;
var_10_2 = 24;
var_10_3 = var_10_130+var_10_2;
var_10_4 = 24;
var_10_5 = var_10_132+var_10_4;
var_10_6 = *reinterpret_cast<uint64_t*>(var_10_5);
var_10_7 = *reinterpret_cast<uint64_t*>(var_10_3);
var_10_8 = var_10_6+var_10_7;
*reinterpret_cast<uint64_t*>(var_10_5) = var_10_8;
var_10_10 = 8;
var_10_11 = var_10_3+var_10_10;
var_10_13 = 8;
var_10_14 = var_10_5+var_10_13;
// prepare block arguments
var_22_103 = var_10_115;
var_22_104 = var_10_116;
var_22_105 = var_10_117;
var_22_106 = var_10_118;
var_22_107 = var_10_119;
var_22_108 = var_10_120;
var_22_109 = var_10_121;
var_22_110 = var_10_122;
var_22_111 = var_10_123;
var_22_112 = var_10_124;
var_22_113 = var_10_125;
var_22_114 = var_10_126;
var_22_115 = var_10_127;
var_22_116 = var_10_128;
var_22_117 = var_10_129;
var_22_118 = var_10_130;
var_22_119 = var_10_131;
var_22_120 = var_10_132;
var_22_121 = var_10_133;
var_22_122 = var_10_134;
var_22_123 = var_10_135;
goto Block_22;

Block_22:
var_10_16 = 0;
var_10_17 = var_22_120 == nullptr;
if (var_10_17){
// prepare block arguments
var_12_111 = var_22_103;
var_12_112 = var_22_104;
var_12_113 = var_22_105;
var_12_114 = var_22_106;
var_12_115 = var_22_107;
var_12_116 = var_22_108;
var_12_117 = var_22_109;
var_12_118 = var_22_110;
var_12_119 = var_22_111;
var_12_120 = var_22_112;
var_12_121 = var_22_113;
var_12_122 = var_22_114;
var_12_123 = var_22_115;
var_12_124 = var_22_116;
var_12_125 = var_22_117;
var_12_126 = var_22_118;
var_12_127 = var_22_119;
var_12_128 = var_22_121;
var_12_129 = var_22_122;
var_12_130 = var_22_123;
goto Block_12;
}else{
// prepare block arguments
var_13_101 = var_22_103;
var_13_102 = var_22_104;
var_13_103 = var_22_105;
var_13_104 = var_22_106;
var_13_105 = var_22_107;
var_13_106 = var_22_108;
var_13_107 = var_22_109;
var_13_108 = var_22_110;
var_13_109 = var_22_111;
var_13_110 = var_22_112;
var_13_111 = var_22_113;
var_13_112 = var_22_114;
var_13_113 = var_22_115;
var_13_114 = var_22_116;
var_13_115 = var_22_117;
var_13_116 = var_22_119;
var_13_117 = var_22_121;
var_13_118 = var_22_122;
var_13_119 = var_22_123;
goto Block_13;}

Block_12:
var_12_0 = 8;
var_12_1 = var_12_126+var_12_0;
var_12_2 = *reinterpret_cast<uint64_t*>(var_12_1);
var_12_3 = static_cast<uint8_t*>(insertProxy(var_12_127,var_12_2));
var_12_4 = 16;
var_12_5 = var_12_3+var_12_4;
var_12_6 = 16;
var_12_7 = var_12_126+var_12_6;
var_12_8 = 16;
var_12_9 = static_cast<uint8_t*>(memcpy(var_12_5,var_12_7,var_12_8));
// prepare block arguments
var_23_104 = var_12_111;
var_23_105 = var_12_112;
var_23_106 = var_12_113;
var_23_107 = var_12_114;
var_23_108 = var_12_115;
var_23_109 = var_12_116;
var_23_110 = var_12_117;
var_23_111 = var_12_118;
var_23_112 = var_12_119;
var_23_113 = var_12_120;
var_23_114 = var_12_121;
var_23_115 = var_12_122;
var_23_116 = var_12_123;
var_23_117 = var_12_124;
var_23_118 = var_12_125;
var_23_119 = var_12_127;
var_23_120 = var_12_128;
var_23_121 = var_12_129;
var_23_122 = var_12_130;
goto Block_23;

Block_23:
var_12_10 = 1;
var_12_11 = var_23_118+var_12_10;
var_12_13 = var_23_117 == var_12_11;
if (var_12_13){
// prepare block arguments
var_14_105 = var_23_104;
var_14_106 = var_23_105;
var_14_107 = var_23_106;
var_14_108 = var_23_107;
var_14_109 = var_23_108;
var_14_110 = var_23_109;
var_14_111 = var_23_110;
var_14_112 = var_23_111;
var_14_113 = var_23_112;
var_14_114 = var_23_113;
var_14_115 = var_23_114;
var_14_116 = var_23_115;
var_14_117 = var_23_116;
var_14_118 = var_23_117;
var_14_119 = var_23_119;
var_14_120 = var_23_121;
var_14_121 = var_23_122;
goto Block_14;
}else{
// prepare block arguments
var_15_101 = var_23_104;
var_15_102 = var_23_105;
var_15_103 = var_23_106;
var_15_104 = var_23_107;
var_15_105 = var_23_108;
var_15_106 = var_23_109;
var_15_107 = var_23_110;
var_15_108 = var_23_111;
var_15_109 = var_23_112;
var_15_110 = var_23_113;
var_15_111 = var_23_114;
var_15_112 = var_23_115;
var_15_113 = var_23_116;
var_15_114 = var_23_117;
var_15_115 = var_12_11;
var_15_116 = var_23_119;
var_15_117 = var_23_120;
var_15_118 = var_23_121;
var_15_119 = var_23_122;
goto Block_15;}

Block_14:
var_14_0 = 0;
var_14_2 = 1;
var_14_3 = var_14_117+var_14_2;
var_14_5 = static_cast<uint8_t*>(getPageProxy(var_14_116,var_14_3));
// prepare block arguments
var_24_103 = var_14_105;
var_24_104 = var_14_106;
var_24_105 = var_14_107;
var_24_106 = var_14_108;
var_24_107 = var_14_109;
var_24_108 = var_14_110;
var_24_109 = var_14_111;
var_24_110 = var_14_112;
var_24_111 = var_14_113;
var_24_112 = var_14_114;
var_24_113 = var_14_115;
var_24_114 = var_14_116;
var_24_115 = var_14_3;
var_24_116 = var_14_118;
var_24_117 = var_14_0;
var_24_118 = var_14_119;
var_24_119 = var_14_5;
var_24_120 = var_14_120;
var_24_121 = var_14_121;
goto Block_24;

Block_24:
var_14_7 = 1;
var_14_8 = var_24_113+var_14_7;
// prepare block arguments
var_16_102 = var_24_103;
var_16_103 = var_24_104;
var_16_104 = var_24_105;
var_16_105 = var_24_106;
var_16_106 = var_24_107;
var_16_107 = var_24_108;
var_16_108 = var_24_109;
var_16_109 = var_24_110;
var_16_110 = var_24_111;
var_16_111 = var_14_8;
var_16_112 = var_24_112;
var_16_113 = var_24_114;
var_16_114 = var_24_115;
var_16_115 = var_24_116;
var_16_116 = var_24_117;
var_16_117 = var_24_118;
var_16_118 = var_24_119;
var_16_119 = var_24_120;
var_16_120 = var_24_121;
goto Block_16;

Block_15:
// prepare block arguments
var_24_103 = var_15_101;
var_24_104 = var_15_102;
var_24_105 = var_15_103;
var_24_106 = var_15_104;
var_24_107 = var_15_105;
var_24_108 = var_15_106;
var_24_109 = var_15_107;
var_24_110 = var_15_108;
var_24_111 = var_15_109;
var_24_112 = var_15_110;
var_24_113 = var_15_111;
var_24_114 = var_15_112;
var_24_115 = var_15_113;
var_24_116 = var_15_114;
var_24_117 = var_15_115;
var_24_118 = var_15_116;
var_24_119 = var_15_117;
var_24_120 = var_15_118;
var_24_121 = var_15_119;
goto Block_24;

Block_13:
// prepare block arguments
var_23_104 = var_13_101;
var_23_105 = var_13_102;
var_23_106 = var_13_103;
var_23_107 = var_13_104;
var_23_108 = var_13_105;
var_23_109 = var_13_106;
var_23_110 = var_13_107;
var_23_111 = var_13_108;
var_23_112 = var_13_109;
var_23_113 = var_13_110;
var_23_114 = var_13_111;
var_23_115 = var_13_112;
var_23_116 = var_13_113;
var_23_117 = var_13_114;
var_23_118 = var_13_115;
var_23_119 = var_13_116;
var_23_120 = var_13_117;
var_23_121 = var_13_118;
var_23_122 = var_13_119;
goto Block_23;

Block_11:
var_11_0 = 0;
var_11_1 = var_11_121+var_11_0;
var_11_2 = *reinterpret_cast<uint8_t**>(var_11_1);
// prepare block arguments
var_25_104 = var_11_104;
var_25_105 = var_11_105;
var_25_106 = var_11_106;
var_25_107 = var_11_107;
var_25_108 = var_11_108;
var_25_109 = var_11_109;
var_25_110 = var_11_110;
var_25_111 = var_11_111;
var_25_112 = var_11_112;
var_25_113 = var_11_113;
var_25_114 = var_11_114;
var_25_115 = var_11_115;
var_25_116 = var_11_116;
var_25_117 = var_11_117;
var_25_118 = var_11_118;
var_25_119 = var_11_119;
var_25_120 = var_11_120;
var_25_121 = var_11_2;
var_25_122 = var_11_122;
var_25_123 = var_11_123;
var_25_124 = var_11_124;
goto Block_25;

Block_9:
// prepare block arguments
var_22_103 = var_9_101;
var_22_104 = var_9_102;
var_22_105 = var_9_103;
var_22_106 = var_9_104;
var_22_107 = var_9_105;
var_22_108 = var_9_106;
var_22_109 = var_9_107;
var_22_110 = var_9_108;
var_22_111 = var_9_109;
var_22_112 = var_9_110;
var_22_113 = var_9_111;
var_22_114 = var_9_112;
var_22_115 = var_9_113;
var_22_116 = var_9_114;
var_22_117 = var_9_115;
var_22_118 = var_9_116;
var_22_119 = var_9_117;
var_22_120 = var_9_118;
var_22_121 = var_9_119;
var_22_122 = var_9_120;
var_22_123 = var_9_121;
goto Block_22;

Block_2:
freeKeyedSliceMergeTask(var_2_124);
var_2_2 = 5;
var_2_5 = static_cast<uint8_t*>(getKeyedSliceState(var_2_121));
var_2_6 = 48;
var_2_7 = var_2_5+var_2_6;
var_2_8 = *reinterpret_cast<uint64_t*>(var_2_7);
var_2_9 = 0;
var_2_10 = 0;
var_2_11 = 0;
var_2_12 = static_cast<uint8_t*>(getPageProxy(var_2_5,var_2_11));
var_2_13 = 0;
var_2_14 = 72;
var_2_15 = var_2_5+var_2_14;
var_2_16 = *reinterpret_cast<uint64_t*>(var_2_15);
var_2_17 = 0;
var_2_18 = 0;
var_2_19 = 0;
// prepare block arguments
var_21_102 = var_2_118;
var_21_103 = var_2_119;
var_21_104 = var_2_2;
var_21_105 = var_2_122;
var_21_106 = var_2_120;
var_21_107 = var_2_121;
var_21_108 = var_2_9;
var_21_109 = var_2_16;
var_21_110 = var_2_5;
var_21_111 = var_2_13;
var_21_112 = var_2_8;
var_21_113 = var_2_10;
var_21_114 = var_2_125;
var_21_115 = var_2_126;
var_21_116 = var_2_12;
goto Block_21;

Block_21:
var_2_20 = var_21_108 == var_21_109;
if (var_2_20){
// prepare block arguments
var_6_106 = var_21_102;
var_6_107 = var_21_103;
var_6_108 = var_21_104;
var_6_109 = var_21_105;
var_6_110 = var_21_106;
var_6_111 = var_21_107;
goto Block_6;
}else{
// prepare block arguments
var_7_115 = var_21_102;
var_7_116 = var_21_103;
var_7_117 = var_21_104;
var_7_118 = var_21_105;
var_7_119 = var_21_107;
var_7_120 = var_21_109;
var_7_121 = var_21_108;
var_7_122 = var_21_110;
var_7_123 = var_21_111;
var_7_124 = var_21_112;
var_7_125 = var_21_113;
var_7_126 = var_21_114;
var_7_127 = var_21_115;
var_7_128 = var_21_106;
var_7_129 = var_21_116;
goto Block_7;}

Block_6:
deleteSliceKeyed(var_6_111);
var_6_2 = 1;
var_6_3 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_6_107,var_6_2));
var_6_4 = getWorkerIdProxy(var_6_106);
triggerThreadLocalStateProxy(var_6_3,var_6_106,var_6_107,var_6_4,var_6_108,var_6_109,var_6_110);
return;

Block_7:
var_7_0 = 32;
var_7_1 = var_7_0*var_7_125;
var_7_2 = var_7_129+var_7_1;
var_7_3 = 16;
var_7_4 = var_7_2+var_7_3;
var_7_5 = *reinterpret_cast<uint64_t*>(var_7_4);
var_7_6 = 8;
var_7_7 = var_7_4+var_7_6;
var_7_9 = 24;
var_7_10 = var_7_2+var_7_9;
var_7_11 = *reinterpret_cast<uint64_t*>(var_7_10);
var_7_12 = 8;
var_7_13 = var_7_10+var_7_12;
var_7_16 = var_7_127 > var_7_128;
if (var_7_16){
// prepare block arguments
var_17_101 = var_7_115;
var_17_102 = var_7_116;
var_17_103 = var_7_117;
var_17_104 = var_7_118;
var_17_105 = var_7_119;
var_17_106 = var_7_120;
var_17_107 = var_7_121;
var_17_108 = var_7_122;
var_17_109 = var_7_123;
var_17_110 = var_7_124;
var_17_111 = var_7_125;
var_17_112 = var_7_11;
var_17_113 = var_7_126;
var_17_114 = var_7_127;
var_17_115 = var_7_129;
goto Block_17;
}else{
// prepare block arguments
var_18_101 = var_7_115;
var_18_102 = var_7_116;
var_18_103 = var_7_117;
var_18_104 = var_7_118;
var_18_105 = var_7_128;
var_18_106 = var_7_119;
var_18_107 = var_7_120;
var_18_108 = var_7_121;
var_18_109 = var_7_122;
var_18_110 = var_7_123;
var_18_111 = var_7_124;
var_18_112 = var_7_125;
var_18_113 = var_7_11;
var_18_114 = var_7_126;
var_18_115 = var_7_127;
var_18_116 = var_7_129;
goto Block_18;}

Block_17:
// prepare block arguments
var_26_110 = var_17_101;
var_26_111 = var_17_102;
var_26_112 = var_17_103;
var_26_113 = var_17_104;
var_26_114 = var_17_114;
var_26_115 = var_17_105;
var_26_116 = var_17_106;
var_26_117 = var_17_107;
var_26_118 = var_17_108;
var_26_119 = var_17_109;
var_26_120 = var_17_110;
var_26_121 = var_17_111;
var_26_122 = var_17_112;
var_26_123 = var_17_113;
var_26_124 = var_17_114;
var_26_125 = var_17_115;
goto Block_26;

Block_26:
var_17_2 = static_cast<uint8_t*>(findSliceStateByTsProxy(var_26_123,var_26_124));
var_17_3 = 0;
var_17_4 = var_17_2+var_17_3;
var_17_5 = *reinterpret_cast<uint64_t*>(var_17_4);
var_17_6 = max(var_26_122,var_17_5);
*reinterpret_cast<uint64_t*>(var_17_4) = var_17_6;
var_17_8 = 1;
var_17_9 = var_26_121+var_17_8;
var_17_11 = var_26_120 == var_17_9;
if (var_17_11){
// prepare block arguments
var_19_105 = var_26_110;
var_19_106 = var_26_111;
var_19_107 = var_26_112;
var_19_108 = var_26_113;
var_19_109 = var_26_114;
var_19_110 = var_26_115;
var_19_111 = var_26_116;
var_19_112 = var_26_117;
var_19_113 = var_26_118;
var_19_114 = var_26_119;
var_19_115 = var_26_120;
var_19_116 = var_26_123;
var_19_117 = var_26_124;
goto Block_19;
}else{
// prepare block arguments
var_20_101 = var_26_110;
var_20_102 = var_26_111;
var_20_103 = var_26_112;
var_20_104 = var_26_113;
var_20_105 = var_26_114;
var_20_106 = var_26_115;
var_20_107 = var_26_116;
var_20_108 = var_26_117;
var_20_109 = var_26_118;
var_20_110 = var_26_119;
var_20_111 = var_26_120;
var_20_112 = var_17_9;
var_20_113 = var_26_123;
var_20_114 = var_26_124;
var_20_115 = var_26_125;
goto Block_20;}

Block_19:
var_19_0 = 0;
var_19_2 = 1;
var_19_3 = var_19_114+var_19_2;
var_19_5 = static_cast<uint8_t*>(getPageProxy(var_19_113,var_19_3));
// prepare block arguments
var_27_103 = var_19_105;
var_27_104 = var_19_106;
var_27_105 = var_19_107;
var_27_106 = var_19_108;
var_27_107 = var_19_109;
var_27_108 = var_19_110;
var_27_109 = var_19_111;
var_27_110 = var_19_112;
var_27_111 = var_19_113;
var_27_112 = var_19_3;
var_27_113 = var_19_115;
var_27_114 = var_19_0;
var_27_115 = var_19_116;
var_27_116 = var_19_117;
var_27_117 = var_19_5;
goto Block_27;

Block_27:
var_19_7 = 1;
var_19_8 = var_27_110+var_19_7;
// prepare block arguments
var_21_102 = var_27_103;
var_21_103 = var_27_104;
var_21_104 = var_27_105;
var_21_105 = var_27_106;
var_21_106 = var_27_107;
var_21_107 = var_27_108;
var_21_108 = var_19_8;
var_21_109 = var_27_109;
var_21_110 = var_27_111;
var_21_111 = var_27_112;
var_21_112 = var_27_113;
var_21_113 = var_27_114;
var_21_114 = var_27_115;
var_21_115 = var_27_116;
var_21_116 = var_27_117;
goto Block_21;

Block_20:
// prepare block arguments
var_27_103 = var_20_101;
var_27_104 = var_20_102;
var_27_105 = var_20_103;
var_27_106 = var_20_104;
var_27_107 = var_20_105;
var_27_108 = var_20_106;
var_27_109 = var_20_107;
var_27_110 = var_20_108;
var_27_111 = var_20_109;
var_27_112 = var_20_110;
var_27_113 = var_20_111;
var_27_114 = var_20_112;
var_27_115 = var_20_113;
var_27_116 = var_20_114;
var_27_117 = var_20_115;
goto Block_27;

Block_18:
// prepare block arguments
var_26_110 = var_18_101;
var_26_111 = var_18_102;
var_26_112 = var_18_103;
var_26_113 = var_18_104;
var_26_114 = var_18_105;
var_26_115 = var_18_106;
var_26_116 = var_18_107;
var_26_117 = var_18_108;
var_26_118 = var_18_109;
var_26_119 = var_18_110;
var_26_120 = var_18_111;
var_26_121 = var_18_112;
var_26_122 = var_18_113;
var_26_123 = var_18_114;
var_26_124 = var_18_115;
var_26_125 = var_18_116;
goto Block_26;

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
void NES::Unikernel::Stage<8>::terminate(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext) {
           ::terminate(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));
}
template<>
void NES::Unikernel::Stage<8>::setup(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext) {
           ::setup(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));
}
template<>
void NES::Unikernel::Stage<8>::execute(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext, NES::Runtime::TupleBuffer& buffer) {
           ::execute(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext), reinterpret_cast<uint8_t*>(&buffer));
}
template<>
NES::Runtime::Execution::OperatorHandler* NES::Unikernel::Stage<8>::getOperatorHandler(int index) {
static NES::Runtime::Execution::Operators::KeyedSliceMergingHandler handler0;
static NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler handler1(uint64_t(10000), uint64_t(10000), std::vector<uint64_t>{uint64_t(5)});
switch(index) {
case 0:
	return &handler0;
case 1:
	return &handler1;
default:
	assert(false && "Bad OperatorHandler index");
	return nullptr;}}
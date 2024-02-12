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
setupKeyedSliceMergingHandler(var_0_5,var_0_110,var_0_6,var_0_7);
return;

}

static inline auto execute(uint8_t* var_0_121 ,uint8_t* var_0_122 ,uint8_t* var_0_123 ){
//variable declarations
uint64_t var_0_0;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
uint64_t var_0_4;
uint8_t* var_0_5;
uint8_t* var_0_6;
uint64_t var_0_7;
uint8_t* var_0_8;
uint64_t var_0_9;
uint64_t var_0_10;
uint8_t* var_0_11;
uint64_t var_0_12;
uint64_t var_0_13;
uint8_t* var_0_14;
uint64_t var_0_15;
uint8_t* var_0_16;
uint8_t* var_0_17;
uint64_t var_0_18;
uint64_t var_0_19;
uint8_t* var_5_102;
uint8_t* var_5_103;
uint64_t var_5_104;
uint64_t var_5_105;
uint8_t* var_5_106;
uint8_t* var_5_107;
uint64_t var_5_108;
uint64_t var_5_109;
uint8_t* var_5_110;
bool var_0_20;
uint8_t* var_1_116;
uint8_t* var_1_117;
uint64_t var_1_118;
uint64_t var_1_119;
uint8_t* var_1_120;
uint8_t* var_1_121;
uint64_t var_1_122;
uint64_t var_1_123;
uint8_t* var_1_124;
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
uint8_t* var_14_102;
uint8_t* var_14_103;
uint64_t var_14_104;
uint64_t var_14_105;
uint8_t* var_14_106;
uint8_t* var_14_107;
uint64_t var_14_108;
uint64_t var_14_109;
uint64_t var_14_110;
uint64_t var_14_111;
uint8_t* var_14_112;
uint64_t var_14_113;
uint64_t var_14_114;
uint64_t var_14_115;
uint8_t* var_14_116;
uint8_t* var_14_117;
bool var_1_15;
uint8_t* var_3_103;
uint8_t* var_3_104;
uint64_t var_3_105;
uint64_t var_3_106;
uint8_t* var_3_107;
uint8_t* var_3_108;
uint64_t var_3_109;
uint64_t var_3_110;
uint8_t* var_3_111;
uint64_t var_3_0;
uint64_t var_3_1;
uint8_t* var_4_108;
uint8_t* var_4_109;
uint64_t var_4_110;
uint64_t var_4_111;
uint8_t* var_4_112;
uint8_t* var_4_113;
uint64_t var_4_114;
uint64_t var_4_115;
uint64_t var_4_116;
uint64_t var_4_117;
uint8_t* var_4_118;
uint64_t var_4_119;
uint64_t var_4_120;
uint64_t var_4_121;
uint8_t* var_4_122;
uint8_t* var_4_123;
uint64_t var_4_0;
uint64_t var_4_1;
uint8_t* var_4_2;
uint64_t var_4_3;
uint8_t* var_4_4;
uint64_t var_4_5;
uint8_t* var_4_6;
uint8_t* var_18_104;
uint8_t* var_18_105;
uint64_t var_18_106;
uint64_t var_18_107;
uint8_t* var_18_108;
uint8_t* var_18_109;
uint64_t var_18_110;
uint64_t var_18_111;
uint64_t var_18_112;
uint64_t var_18_113;
uint8_t* var_18_114;
uint64_t var_18_115;
uint64_t var_18_116;
uint64_t var_18_117;
uint8_t* var_18_118;
uint8_t* var_18_119;
uint8_t* var_18_120;
uint8_t* var_18_121;
int32_t var_4_7;
bool var_4_8;
bool var_4_9;
uint8_t* var_6_107;
uint8_t* var_6_108;
uint64_t var_6_109;
uint64_t var_6_110;
uint8_t* var_6_111;
uint8_t* var_6_112;
uint64_t var_6_113;
uint64_t var_6_114;
uint64_t var_6_115;
uint64_t var_6_116;
uint8_t* var_6_117;
uint64_t var_6_118;
uint64_t var_6_119;
uint64_t var_6_120;
uint8_t* var_6_121;
uint8_t* var_6_122;
uint8_t* var_6_123;
uint8_t* var_6_124;
uint64_t var_6_0;
uint8_t* var_6_1;
uint64_t var_6_2;
uint8_t* var_6_3;
uint64_t var_6_4;
bool var_6_5;
uint8_t* var_8_115;
uint8_t* var_8_116;
uint64_t var_8_117;
uint64_t var_8_118;
uint8_t* var_8_119;
uint8_t* var_8_120;
uint64_t var_8_121;
uint64_t var_8_122;
uint64_t var_8_123;
uint64_t var_8_124;
uint8_t* var_8_125;
uint64_t var_8_126;
uint64_t var_8_127;
uint64_t var_8_128;
uint8_t* var_8_129;
uint8_t* var_8_130;
uint8_t* var_8_131;
uint8_t* var_8_132;
uint64_t var_8_0;
uint8_t* var_8_1;
uint64_t var_8_2;
uint8_t* var_8_3;
uint64_t var_8_4;
uint8_t* var_8_5;
uint64_t var_8_6;
uint64_t var_8_7;
uint64_t var_8_8;
uint64_t var_8_10;
uint8_t* var_8_11;
uint64_t var_8_13;
uint8_t* var_8_14;
uint8_t* var_15_103;
uint8_t* var_15_104;
uint64_t var_15_105;
uint64_t var_15_106;
uint8_t* var_15_107;
uint8_t* var_15_108;
uint64_t var_15_109;
uint64_t var_15_110;
uint64_t var_15_111;
uint64_t var_15_112;
uint8_t* var_15_113;
uint64_t var_15_114;
uint64_t var_15_115;
uint64_t var_15_116;
uint8_t* var_15_117;
uint8_t* var_15_118;
uint8_t* var_15_119;
uint8_t* var_15_120;
int32_t var_8_16;
bool var_8_17;
uint8_t* var_10_111;
uint8_t* var_10_112;
uint64_t var_10_113;
uint64_t var_10_114;
uint8_t* var_10_115;
uint8_t* var_10_116;
uint64_t var_10_117;
uint64_t var_10_118;
uint64_t var_10_119;
uint64_t var_10_120;
uint8_t* var_10_121;
uint64_t var_10_122;
uint64_t var_10_123;
uint64_t var_10_124;
uint8_t* var_10_125;
uint8_t* var_10_126;
uint8_t* var_10_127;
uint64_t var_10_0;
uint8_t* var_10_1;
uint64_t var_10_2;
uint8_t* var_10_3;
uint64_t var_10_4;
uint8_t* var_10_5;
uint64_t var_10_6;
uint8_t* var_10_7;
uint64_t var_10_8;
uint8_t* var_10_9;
uint8_t* var_16_104;
uint8_t* var_16_105;
uint64_t var_16_106;
uint64_t var_16_107;
uint8_t* var_16_108;
uint8_t* var_16_109;
uint64_t var_16_110;
uint64_t var_16_111;
uint64_t var_16_112;
uint64_t var_16_113;
uint8_t* var_16_114;
uint64_t var_16_115;
uint64_t var_16_116;
uint64_t var_16_117;
uint8_t* var_16_118;
uint8_t* var_16_119;
uint64_t var_10_10;
uint64_t var_10_11;
bool var_10_13;
uint8_t* var_12_105;
uint8_t* var_12_106;
uint64_t var_12_107;
uint64_t var_12_108;
uint8_t* var_12_109;
uint8_t* var_12_110;
uint64_t var_12_111;
uint64_t var_12_112;
uint64_t var_12_113;
uint64_t var_12_114;
uint8_t* var_12_115;
uint64_t var_12_116;
uint64_t var_12_117;
uint8_t* var_12_118;
uint64_t var_12_0;
uint64_t var_12_2;
uint64_t var_12_3;
uint8_t* var_12_5;
uint8_t* var_17_103;
uint8_t* var_17_104;
uint64_t var_17_105;
uint64_t var_17_106;
uint8_t* var_17_107;
uint8_t* var_17_108;
uint64_t var_17_109;
uint64_t var_17_110;
uint64_t var_17_111;
uint64_t var_17_112;
uint8_t* var_17_113;
uint64_t var_17_114;
uint64_t var_17_115;
uint64_t var_17_116;
uint8_t* var_17_117;
uint8_t* var_17_118;
uint64_t var_12_7;
uint64_t var_12_8;
uint8_t* var_13_101;
uint8_t* var_13_102;
uint64_t var_13_103;
uint64_t var_13_104;
uint8_t* var_13_105;
uint8_t* var_13_106;
uint64_t var_13_107;
uint64_t var_13_108;
uint64_t var_13_109;
uint64_t var_13_110;
uint8_t* var_13_111;
uint64_t var_13_112;
uint64_t var_13_113;
uint64_t var_13_114;
uint8_t* var_13_115;
uint8_t* var_13_116;
uint8_t* var_11_101;
uint8_t* var_11_102;
uint64_t var_11_103;
uint64_t var_11_104;
uint8_t* var_11_105;
uint8_t* var_11_106;
uint64_t var_11_107;
uint64_t var_11_108;
uint64_t var_11_109;
uint64_t var_11_110;
uint8_t* var_11_111;
uint64_t var_11_112;
uint64_t var_11_113;
uint64_t var_11_114;
uint8_t* var_11_115;
uint8_t* var_11_116;
uint8_t* var_9_104;
uint8_t* var_9_105;
uint64_t var_9_106;
uint64_t var_9_107;
uint8_t* var_9_108;
uint8_t* var_9_109;
uint64_t var_9_110;
uint64_t var_9_111;
uint64_t var_9_112;
uint64_t var_9_113;
uint8_t* var_9_114;
uint64_t var_9_115;
uint64_t var_9_116;
uint64_t var_9_117;
uint8_t* var_9_118;
uint8_t* var_9_119;
uint8_t* var_9_120;
uint8_t* var_9_121;
uint64_t var_9_0;
uint8_t* var_9_1;
uint8_t* var_9_2;
uint8_t* var_7_101;
uint8_t* var_7_102;
uint64_t var_7_103;
uint64_t var_7_104;
uint8_t* var_7_105;
uint8_t* var_7_106;
uint64_t var_7_107;
uint64_t var_7_108;
uint64_t var_7_109;
uint64_t var_7_110;
uint8_t* var_7_111;
uint64_t var_7_112;
uint64_t var_7_113;
uint64_t var_7_114;
uint8_t* var_7_115;
uint8_t* var_7_116;
uint8_t* var_7_117;
uint8_t* var_7_118;
uint8_t* var_2_106;
uint8_t* var_2_107;
uint64_t var_2_108;
uint64_t var_2_109;
uint8_t* var_2_110;
uint8_t* var_2_111;
uint64_t var_2_1;
uint8_t* var_2_2;
//basic blocks
Block_0:
var_0_0 = 0;
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 0;
var_0_5 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_121,var_0_4));
var_0_6 = static_cast<uint8_t*>(NES__Runtime__TupleBuffer__getBuffer(var_0_123));
var_0_7 = 8;
var_0_8 = var_0_6+var_0_7;
var_0_9 = *reinterpret_cast<uint64_t*>(var_0_8);
var_0_10 = 16;
var_0_11 = var_0_6+var_0_10;
var_0_12 = *reinterpret_cast<uint64_t*>(var_0_11);
var_0_13 = 0;
var_0_14 = var_0_6+var_0_13;
var_0_15 = *reinterpret_cast<uint64_t*>(var_0_14);
var_0_16 = static_cast<uint8_t*>(createKeyedState(var_0_5,var_0_6));
var_0_17 = static_cast<uint8_t*>(getKeyedSliceState(var_0_16));
var_0_18 = getKeyedNumberOfSlicesFromTask(var_0_6);
var_0_19 = 0;
// prepare block arguments
var_5_102 = var_0_122;
var_5_103 = var_0_121;
var_5_104 = var_0_15;
var_5_105 = var_0_12;
var_5_106 = var_0_16;
var_5_107 = var_0_6;
var_5_108 = var_0_19;
var_5_109 = var_0_18;
var_5_110 = var_0_17;
goto Block_5;

Block_5:
var_0_20 = var_5_108 < var_5_109;
if (var_0_20){
// prepare block arguments
var_1_116 = var_5_102;
var_1_117 = var_5_103;
var_1_118 = var_5_104;
var_1_119 = var_5_105;
var_1_120 = var_5_106;
var_1_121 = var_5_107;
var_1_122 = var_5_109;
var_1_123 = var_5_108;
var_1_124 = var_5_110;
goto Block_1;
}else{
// prepare block arguments
var_2_106 = var_5_102;
var_2_107 = var_5_103;
var_2_108 = var_5_104;
var_2_109 = var_5_105;
var_2_110 = var_5_106;
var_2_111 = var_5_107;
goto Block_2;}

Block_1:
var_1_0 = static_cast<uint8_t*>(getKeyedSliceStateFromTask(var_1_121,var_1_123));
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
var_14_102 = var_1_116;
var_14_103 = var_1_117;
var_14_104 = var_1_118;
var_14_105 = var_1_119;
var_14_106 = var_1_120;
var_14_107 = var_1_121;
var_14_108 = var_1_122;
var_14_109 = var_1_123;
var_14_110 = var_1_4;
var_14_111 = var_1_11;
var_14_112 = var_1_0;
var_14_113 = var_1_8;
var_14_114 = var_1_3;
var_14_115 = var_1_5;
var_14_116 = var_1_124;
var_14_117 = var_1_7;
goto Block_14;

Block_14:
var_1_15 = var_14_110 == var_14_111;
if (var_1_15){
// prepare block arguments
var_3_103 = var_14_102;
var_3_104 = var_14_103;
var_3_105 = var_14_104;
var_3_106 = var_14_105;
var_3_107 = var_14_106;
var_3_108 = var_14_107;
var_3_109 = var_14_108;
var_3_110 = var_14_109;
var_3_111 = var_14_116;
goto Block_3;
}else{
// prepare block arguments
var_4_108 = var_14_102;
var_4_109 = var_14_103;
var_4_110 = var_14_104;
var_4_111 = var_14_105;
var_4_112 = var_14_106;
var_4_113 = var_14_107;
var_4_114 = var_14_108;
var_4_115 = var_14_109;
var_4_116 = var_14_111;
var_4_117 = var_14_110;
var_4_118 = var_14_112;
var_4_119 = var_14_113;
var_4_120 = var_14_114;
var_4_121 = var_14_115;
var_4_122 = var_14_116;
var_4_123 = var_14_117;
goto Block_4;}

Block_3:
var_3_0 = 1;
var_3_1 = var_3_110+var_3_0;
// prepare block arguments
var_5_102 = var_3_103;
var_5_103 = var_3_104;
var_5_104 = var_3_105;
var_5_105 = var_3_106;
var_5_106 = var_3_107;
var_5_107 = var_3_108;
var_5_108 = var_3_1;
var_5_109 = var_3_109;
var_5_110 = var_3_111;
goto Block_5;

Block_4:
var_4_0 = 32;
var_4_1 = var_4_0*var_4_121;
var_4_2 = var_4_123+var_4_1;
var_4_3 = 8;
var_4_4 = var_4_2+var_4_3;
var_4_5 = *reinterpret_cast<uint64_t*>(var_4_4);
var_4_6 = static_cast<uint8_t*>(findChainProxy(var_4_122,var_4_5));
// prepare block arguments
var_18_104 = var_4_108;
var_18_105 = var_4_109;
var_18_106 = var_4_110;
var_18_107 = var_4_111;
var_18_108 = var_4_112;
var_18_109 = var_4_113;
var_18_110 = var_4_114;
var_18_111 = var_4_115;
var_18_112 = var_4_116;
var_18_113 = var_4_117;
var_18_114 = var_4_118;
var_18_115 = var_4_119;
var_18_116 = var_4_120;
var_18_117 = var_4_121;
var_18_118 = var_4_2;
var_18_119 = var_4_122;
var_18_120 = var_4_6;
var_18_121 = var_4_123;
goto Block_18;

Block_18:
var_4_7 = 0;
var_4_8 = var_18_120 == nullptr;
var_4_9= !var_4_8;
if (var_4_9){
// prepare block arguments
var_6_107 = var_18_104;
var_6_108 = var_18_105;
var_6_109 = var_18_106;
var_6_110 = var_18_107;
var_6_111 = var_18_108;
var_6_112 = var_18_109;
var_6_113 = var_18_110;
var_6_114 = var_18_111;
var_6_115 = var_18_112;
var_6_116 = var_18_113;
var_6_117 = var_18_114;
var_6_118 = var_18_115;
var_6_119 = var_18_116;
var_6_120 = var_18_117;
var_6_121 = var_18_118;
var_6_122 = var_18_119;
var_6_123 = var_18_120;
var_6_124 = var_18_121;
goto Block_6;
}else{
// prepare block arguments
var_7_101 = var_18_104;
var_7_102 = var_18_105;
var_7_103 = var_18_106;
var_7_104 = var_18_107;
var_7_105 = var_18_108;
var_7_106 = var_18_109;
var_7_107 = var_18_110;
var_7_108 = var_18_111;
var_7_109 = var_18_112;
var_7_110 = var_18_113;
var_7_111 = var_18_114;
var_7_112 = var_18_115;
var_7_113 = var_18_116;
var_7_114 = var_18_117;
var_7_115 = var_18_118;
var_7_116 = var_18_119;
var_7_117 = var_18_120;
var_7_118 = var_18_121;
goto Block_7;}

Block_6:
var_6_0 = 16;
var_6_1 = var_6_123+var_6_0;
var_6_2 = 16;
var_6_3 = var_6_121+var_6_2;
var_6_4 = 8;
var_6_5 = memeq(var_6_1,var_6_3,var_6_4);
if (var_6_5){
// prepare block arguments
var_8_115 = var_6_107;
var_8_116 = var_6_108;
var_8_117 = var_6_109;
var_8_118 = var_6_110;
var_8_119 = var_6_111;
var_8_120 = var_6_112;
var_8_121 = var_6_113;
var_8_122 = var_6_114;
var_8_123 = var_6_115;
var_8_124 = var_6_116;
var_8_125 = var_6_117;
var_8_126 = var_6_118;
var_8_127 = var_6_119;
var_8_128 = var_6_120;
var_8_129 = var_6_121;
var_8_130 = var_6_122;
var_8_131 = var_6_123;
var_8_132 = var_6_124;
goto Block_8;
}else{
// prepare block arguments
var_9_104 = var_6_107;
var_9_105 = var_6_108;
var_9_106 = var_6_109;
var_9_107 = var_6_110;
var_9_108 = var_6_111;
var_9_109 = var_6_112;
var_9_110 = var_6_113;
var_9_111 = var_6_114;
var_9_112 = var_6_115;
var_9_113 = var_6_116;
var_9_114 = var_6_117;
var_9_115 = var_6_118;
var_9_116 = var_6_119;
var_9_117 = var_6_120;
var_9_118 = var_6_121;
var_9_119 = var_6_122;
var_9_120 = var_6_123;
var_9_121 = var_6_124;
goto Block_9;}

Block_8:
var_8_0 = 16;
var_8_1 = var_8_129+var_8_0;
var_8_2 = 24;
var_8_3 = var_8_129+var_8_2;
var_8_4 = 24;
var_8_5 = var_8_131+var_8_4;
var_8_6 = *reinterpret_cast<uint64_t*>(var_8_5);
var_8_7 = *reinterpret_cast<uint64_t*>(var_8_3);
var_8_8 = var_8_6+var_8_7;
*reinterpret_cast<uint64_t*>(var_8_5) = var_8_8;
var_8_10 = 8;
var_8_11 = var_8_3+var_8_10;
var_8_13 = 8;
var_8_14 = var_8_5+var_8_13;
// prepare block arguments
var_15_103 = var_8_115;
var_15_104 = var_8_116;
var_15_105 = var_8_117;
var_15_106 = var_8_118;
var_15_107 = var_8_119;
var_15_108 = var_8_120;
var_15_109 = var_8_121;
var_15_110 = var_8_122;
var_15_111 = var_8_123;
var_15_112 = var_8_124;
var_15_113 = var_8_125;
var_15_114 = var_8_126;
var_15_115 = var_8_127;
var_15_116 = var_8_128;
var_15_117 = var_8_129;
var_15_118 = var_8_130;
var_15_119 = var_8_131;
var_15_120 = var_8_132;
goto Block_15;

Block_15:
var_8_16 = 0;
var_8_17 = var_15_119 == nullptr;
if (var_8_17){
// prepare block arguments
var_10_111 = var_15_103;
var_10_112 = var_15_104;
var_10_113 = var_15_105;
var_10_114 = var_15_106;
var_10_115 = var_15_107;
var_10_116 = var_15_108;
var_10_117 = var_15_109;
var_10_118 = var_15_110;
var_10_119 = var_15_111;
var_10_120 = var_15_112;
var_10_121 = var_15_113;
var_10_122 = var_15_114;
var_10_123 = var_15_115;
var_10_124 = var_15_116;
var_10_125 = var_15_117;
var_10_126 = var_15_118;
var_10_127 = var_15_120;
goto Block_10;
}else{
// prepare block arguments
var_11_101 = var_15_103;
var_11_102 = var_15_104;
var_11_103 = var_15_105;
var_11_104 = var_15_106;
var_11_105 = var_15_107;
var_11_106 = var_15_108;
var_11_107 = var_15_109;
var_11_108 = var_15_110;
var_11_109 = var_15_111;
var_11_110 = var_15_112;
var_11_111 = var_15_113;
var_11_112 = var_15_114;
var_11_113 = var_15_115;
var_11_114 = var_15_116;
var_11_115 = var_15_118;
var_11_116 = var_15_120;
goto Block_11;}

Block_10:
var_10_0 = 8;
var_10_1 = var_10_125+var_10_0;
var_10_2 = *reinterpret_cast<uint64_t*>(var_10_1);
var_10_3 = static_cast<uint8_t*>(insertProxy(var_10_126,var_10_2));
var_10_4 = 16;
var_10_5 = var_10_3+var_10_4;
var_10_6 = 16;
var_10_7 = var_10_125+var_10_6;
var_10_8 = 16;
var_10_9 = static_cast<uint8_t*>(memcpy(var_10_5,var_10_7,var_10_8));
// prepare block arguments
var_16_104 = var_10_111;
var_16_105 = var_10_112;
var_16_106 = var_10_113;
var_16_107 = var_10_114;
var_16_108 = var_10_115;
var_16_109 = var_10_116;
var_16_110 = var_10_117;
var_16_111 = var_10_118;
var_16_112 = var_10_119;
var_16_113 = var_10_120;
var_16_114 = var_10_121;
var_16_115 = var_10_122;
var_16_116 = var_10_123;
var_16_117 = var_10_124;
var_16_118 = var_10_126;
var_16_119 = var_10_127;
goto Block_16;

Block_16:
var_10_10 = 1;
var_10_11 = var_16_117+var_10_10;
var_10_13 = var_16_116 == var_10_11;
if (var_10_13){
// prepare block arguments
var_12_105 = var_16_104;
var_12_106 = var_16_105;
var_12_107 = var_16_106;
var_12_108 = var_16_107;
var_12_109 = var_16_108;
var_12_110 = var_16_109;
var_12_111 = var_16_110;
var_12_112 = var_16_111;
var_12_113 = var_16_112;
var_12_114 = var_16_113;
var_12_115 = var_16_114;
var_12_116 = var_16_115;
var_12_117 = var_16_116;
var_12_118 = var_16_118;
goto Block_12;
}else{
// prepare block arguments
var_13_101 = var_16_104;
var_13_102 = var_16_105;
var_13_103 = var_16_106;
var_13_104 = var_16_107;
var_13_105 = var_16_108;
var_13_106 = var_16_109;
var_13_107 = var_16_110;
var_13_108 = var_16_111;
var_13_109 = var_16_112;
var_13_110 = var_16_113;
var_13_111 = var_16_114;
var_13_112 = var_16_115;
var_13_113 = var_16_116;
var_13_114 = var_10_11;
var_13_115 = var_16_118;
var_13_116 = var_16_119;
goto Block_13;}

Block_12:
var_12_0 = 0;
var_12_2 = 1;
var_12_3 = var_12_116+var_12_2;
var_12_5 = static_cast<uint8_t*>(getPageProxy(var_12_115,var_12_3));
// prepare block arguments
var_17_103 = var_12_105;
var_17_104 = var_12_106;
var_17_105 = var_12_107;
var_17_106 = var_12_108;
var_17_107 = var_12_109;
var_17_108 = var_12_110;
var_17_109 = var_12_111;
var_17_110 = var_12_112;
var_17_111 = var_12_113;
var_17_112 = var_12_114;
var_17_113 = var_12_115;
var_17_114 = var_12_3;
var_17_115 = var_12_117;
var_17_116 = var_12_0;
var_17_117 = var_12_118;
var_17_118 = var_12_5;
goto Block_17;

Block_17:
var_12_7 = 1;
var_12_8 = var_17_112+var_12_7;
// prepare block arguments
var_14_102 = var_17_103;
var_14_103 = var_17_104;
var_14_104 = var_17_105;
var_14_105 = var_17_106;
var_14_106 = var_17_107;
var_14_107 = var_17_108;
var_14_108 = var_17_109;
var_14_109 = var_17_110;
var_14_110 = var_12_8;
var_14_111 = var_17_111;
var_14_112 = var_17_113;
var_14_113 = var_17_114;
var_14_114 = var_17_115;
var_14_115 = var_17_116;
var_14_116 = var_17_117;
var_14_117 = var_17_118;
goto Block_14;

Block_13:
// prepare block arguments
var_17_103 = var_13_101;
var_17_104 = var_13_102;
var_17_105 = var_13_103;
var_17_106 = var_13_104;
var_17_107 = var_13_105;
var_17_108 = var_13_106;
var_17_109 = var_13_107;
var_17_110 = var_13_108;
var_17_111 = var_13_109;
var_17_112 = var_13_110;
var_17_113 = var_13_111;
var_17_114 = var_13_112;
var_17_115 = var_13_113;
var_17_116 = var_13_114;
var_17_117 = var_13_115;
var_17_118 = var_13_116;
goto Block_17;

Block_11:
// prepare block arguments
var_16_104 = var_11_101;
var_16_105 = var_11_102;
var_16_106 = var_11_103;
var_16_107 = var_11_104;
var_16_108 = var_11_105;
var_16_109 = var_11_106;
var_16_110 = var_11_107;
var_16_111 = var_11_108;
var_16_112 = var_11_109;
var_16_113 = var_11_110;
var_16_114 = var_11_111;
var_16_115 = var_11_112;
var_16_116 = var_11_113;
var_16_117 = var_11_114;
var_16_118 = var_11_115;
var_16_119 = var_11_116;
goto Block_16;

Block_9:
var_9_0 = 0;
var_9_1 = var_9_120+var_9_0;
var_9_2 = *reinterpret_cast<uint8_t**>(var_9_1);
// prepare block arguments
var_18_104 = var_9_104;
var_18_105 = var_9_105;
var_18_106 = var_9_106;
var_18_107 = var_9_107;
var_18_108 = var_9_108;
var_18_109 = var_9_109;
var_18_110 = var_9_110;
var_18_111 = var_9_111;
var_18_112 = var_9_112;
var_18_113 = var_9_113;
var_18_114 = var_9_114;
var_18_115 = var_9_115;
var_18_116 = var_9_116;
var_18_117 = var_9_117;
var_18_118 = var_9_118;
var_18_119 = var_9_119;
var_18_120 = var_9_2;
var_18_121 = var_9_121;
goto Block_18;

Block_7:
// prepare block arguments
var_15_103 = var_7_101;
var_15_104 = var_7_102;
var_15_105 = var_7_103;
var_15_106 = var_7_104;
var_15_107 = var_7_105;
var_15_108 = var_7_106;
var_15_109 = var_7_107;
var_15_110 = var_7_108;
var_15_111 = var_7_109;
var_15_112 = var_7_110;
var_15_113 = var_7_111;
var_15_114 = var_7_112;
var_15_115 = var_7_113;
var_15_116 = var_7_114;
var_15_117 = var_7_115;
var_15_118 = var_7_116;
var_15_119 = var_7_117;
var_15_120 = var_7_118;
goto Block_15;

Block_2:
freeKeyedSliceMergeTask(var_2_111);
var_2_1 = 1;
var_2_2 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_2_107,var_2_1));
appendToGlobalSliceStoreKeyed(var_2_2,var_2_110);
triggerSlidingWindowsKeyed(var_2_2,var_2_106,var_2_107,var_2_108,var_2_109);
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
void NES::Unikernel::Stage<9>::terminate(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext) {
           ::terminate(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));
}
template<>
void NES::Unikernel::Stage<9>::setup(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext) {
           ::setup(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));
}
template<>
void NES::Unikernel::Stage<9>::execute(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext, NES::Runtime::TupleBuffer& buffer) {
           ::execute(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext), reinterpret_cast<uint8_t*>(&buffer));
}
template<>
NES::Runtime::Execution::OperatorHandler* NES::Unikernel::Stage<9>::getOperatorHandler(int index) {
static NES::Runtime::Execution::Operators::KeyedSliceMergingHandler handler0;
static NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<NES::Runtime::Execution::Operators::KeyedSlice> handler1(uint64_t(10000), uint64_t(2000));
switch(index) {
case 0:
	return &handler0;
case 1:
	return &handler1;
default:
	assert(false && "Bad OperatorHandler index");
	return nullptr;}}
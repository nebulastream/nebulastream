#include <ProxyFunctions.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-label"
#pragma GCC diagnostic ignored "-Wtautological-compare"
static inline auto setup(uint8_t* var_0_108 ,uint8_t* var_0_109 ){
//variable declarations
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_0_0 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
uint64_t var_0_4;
uint64_t var_0_5;
bool var_0_6;
//basic blocks
Block_0:
var_0_0 = NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>(0);
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 0;
var_0_5 = 0;
var_0_6 = 1;
return;

}

static inline auto execute(uint8_t* var_0_137 ,uint8_t* var_0_138 ,uint8_t* var_0_139 ){
//variable declarations
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_0_0 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_0_1;
uint64_t var_0_2;
uint64_t var_0_3;
uint64_t var_0_4;
uint64_t var_0_5;
bool var_0_6;
uint64_t var_0_7;
uint64_t var_0_9;
uint64_t var_0_11;
bool var_0_13;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_0_15 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_0_17;
uint64_t var_0_19;
uint64_t var_0_21;
uint8_t* var_0_23;
uint64_t var_0_24;
uint8_t* var_0_25;
uint64_t var_0_26;
uint8_t* var_0_27;
uint8_t* var_0_28;
uint64_t var_0_29;
uint64_t var_0_30;
uint64_t var_0_31;
uint64_t var_0_32;
uint64_t var_0_33;
uint64_t var_0_34;
NES::NESStrongType<unsigned long, NES::WorkerThreadId_, 0, 1> var_0_35 = NES::INVALID<NES::NESStrongType<unsigned long, NES::WorkerThreadId_, 0, 1>>;
uint8_t* var_0_36;
uint8_t* var_0_37;
uint64_t var_0_38;
uint8_t* var_0_39;
uint64_t var_0_40;
uint8_t* var_0_41;
uint8_t* var_0_42;
uint8_t* var_0_43;
uint8_t* var_14_102;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_14_103 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_14_104;
uint8_t* var_14_105;
uint8_t* var_14_106;
uint64_t var_14_107;
uint64_t var_14_108;
uint64_t var_14_109;
uint64_t var_14_110;
uint64_t var_14_111;
bool var_14_112;
uint8_t* var_14_113;
uint8_t* var_14_114;
uint8_t* var_14_115;
uint8_t* var_14_116;
uint8_t* var_14_117;
bool var_0_44;
uint8_t* var_1_117;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_1_118 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_1_119;
uint8_t* var_1_120;
uint8_t* var_1_121;
uint64_t var_1_122;
uint64_t var_1_123;
uint64_t var_1_124;
uint64_t var_1_125;
uint64_t var_1_126;
bool var_1_127;
uint8_t* var_1_128;
uint8_t* var_1_129;
uint8_t* var_1_130;
uint8_t* var_1_131;
uint8_t* var_1_132;
uint64_t var_1_0;
uint8_t* var_1_1;
uint64_t var_1_2;
uint64_t var_1_3;
uint8_t* var_1_4;
uint64_t var_1_6;
uint64_t var_1_7;
uint8_t* var_1_8;
uint64_t var_1_10;
uint64_t var_1_11;
uint8_t* var_1_12;
uint64_t var_1_14;
uint64_t var_1_15;
uint8_t* var_1_16;
uint8_t* var_1_18;
uint8_t* var_1_19;
uint8_t* var_13_102;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_13_103 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_13_104;
uint8_t* var_13_105;
uint8_t* var_13_106;
uint64_t var_13_107;
uint64_t var_13_108;
uint64_t var_13_109;
uint64_t var_13_110;
uint64_t var_13_111;
bool var_13_112;
uint8_t* var_13_113;
uint8_t* var_13_114;
uint8_t* var_13_115;
uint8_t* var_13_116;
uint8_t* var_13_117;
uint8_t* var_13_118;
uint64_t var_13_119;
uint64_t var_13_120;
uint64_t var_13_121;
uint8_t* var_13_122;
bool var_1_20;
uint8_t* var_3_113;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_3_114 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_3_115;
uint8_t* var_3_116;
uint64_t var_3_117;
uint64_t var_3_118;
uint64_t var_3_119;
uint64_t var_3_120;
bool var_3_121;
uint8_t* var_3_122;
uint8_t* var_3_123;
uint8_t* var_3_124;
uint8_t* var_3_125;
uint8_t* var_3_126;
uint8_t* var_3_127;
uint64_t var_3_128;
uint64_t var_3_129;
uint8_t* var_3_130;
uint64_t var_3_131;
uint64_t var_3_132;
uint8_t* var_3_133;
uint64_t var_3_0;
uint8_t* var_3_1;
uint64_t var_3_2;
uint64_t var_3_3;
uint8_t* var_3_4;
uint64_t var_3_6;
uint64_t var_3_7;
uint8_t* var_3_8;
uint64_t var_3_10;
uint64_t var_3_11;
uint8_t* var_3_12;
bool var_3_14;
uint8_t* var_5_104;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_5_105 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_5_106;
uint8_t* var_5_107;
uint64_t var_5_108;
uint64_t var_5_109;
uint64_t var_5_110;
uint64_t var_5_111;
bool var_5_112;
uint8_t* var_5_113;
uint8_t* var_5_114;
uint8_t* var_5_115;
uint8_t* var_5_116;
uint8_t* var_5_117;
uint8_t* var_5_118;
uint64_t var_5_119;
uint64_t var_5_120;
uint8_t* var_5_121;
uint64_t var_5_122;
uint64_t var_5_123;
uint64_t var_5_124;
uint64_t var_5_125;
uint8_t* var_5_126;
bool var_5_2;
bool var_5_3;
bool var_5_4;
uint8_t* var_7_105;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_7_106 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_7_107;
uint8_t* var_7_108;
uint64_t var_7_109;
uint64_t var_7_110;
uint64_t var_7_111;
uint64_t var_7_112;
bool var_7_113;
uint8_t* var_7_114;
uint8_t* var_7_115;
uint8_t* var_7_116;
uint8_t* var_7_117;
uint8_t* var_7_118;
uint8_t* var_7_119;
uint64_t var_7_120;
uint64_t var_7_121;
uint8_t* var_7_122;
uint64_t var_7_123;
uint64_t var_7_124;
uint8_t* var_7_125;
uint64_t var_7_0;
bool var_7_1;
bool var_7_2;
bool var_7_3;
uint8_t* var_9_114;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_9_115 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_9_116;
uint8_t* var_9_117;
uint64_t var_9_118;
uint64_t var_9_119;
uint64_t var_9_120;
uint64_t var_9_121;
bool var_9_122;
uint8_t* var_9_123;
uint8_t* var_9_124;
uint8_t* var_9_125;
uint8_t* var_9_126;
uint8_t* var_9_127;
uint8_t* var_9_128;
uint64_t var_9_129;
uint64_t var_9_130;
uint8_t* var_9_131;
uint64_t var_9_132;
uint64_t var_9_133;
bool var_9_0;
uint64_t var_9_6;
bool var_9_11;
bool var_9_12;
uint8_t* var_11_102;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_11_103 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_11_104;
uint8_t* var_11_105;
uint64_t var_11_106;
uint64_t var_11_107;
uint64_t var_11_108;
uint64_t var_11_109;
bool var_11_110;
uint8_t* var_11_111;
uint8_t* var_11_112;
uint8_t* var_11_113;
uint8_t* var_11_114;
uint8_t* var_11_115;
uint8_t* var_11_116;
uint64_t var_11_117;
uint64_t var_11_118;
uint64_t var_11_119;
uint8_t* var_20_104;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_20_105 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_20_106;
uint8_t* var_20_107;
uint64_t var_20_108;
uint64_t var_20_109;
uint64_t var_20_110;
uint64_t var_20_111;
bool var_20_112;
uint8_t* var_20_113;
uint8_t* var_20_114;
uint8_t* var_20_115;
uint8_t* var_20_116;
uint8_t* var_20_117;
uint8_t* var_20_118;
uint64_t var_20_119;
uint64_t var_20_120;
uint64_t var_20_121;
uint8_t* var_11_1;
uint8_t* var_11_3;
uint64_t var_11_5;
uint8_t* var_19_112;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_19_113 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_19_114;
uint8_t* var_19_115;
uint8_t* var_19_116;
uint64_t var_19_117;
uint64_t var_19_118;
uint64_t var_19_119;
uint64_t var_19_120;
bool var_19_121;
uint8_t* var_19_122;
uint8_t* var_19_123;
uint8_t* var_19_124;
uint8_t* var_19_125;
uint8_t* var_19_126;
uint8_t* var_19_127;
uint64_t var_19_128;
uint64_t var_19_129;
uint64_t var_19_130;
uint8_t* var_19_131;
uint64_t var_19_132;
uint64_t var_11_7;
uint64_t var_11_8;
uint8_t* var_11_9;
uint64_t var_11_10;
uint8_t* var_11_11;
uint64_t var_11_13;
uint8_t* var_11_14;
uint64_t var_11_16;
uint64_t var_11_17;
uint8_t* var_17_101;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_17_102 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_17_103;
uint8_t* var_17_104;
uint8_t* var_17_105;
uint64_t var_17_106;
uint64_t var_17_107;
uint64_t var_17_108;
uint64_t var_17_109;
uint64_t var_17_110;
bool var_17_111;
uint8_t* var_17_112;
uint8_t* var_17_113;
uint8_t* var_17_114;
uint8_t* var_17_115;
uint8_t* var_17_116;
uint8_t* var_17_117;
uint64_t var_17_118;
uint64_t var_17_119;
uint64_t var_17_120;
uint8_t* var_17_121;
uint8_t* var_18_102;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_18_103 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_18_104;
uint8_t* var_18_105;
uint8_t* var_18_106;
uint64_t var_18_107;
uint64_t var_18_108;
uint64_t var_18_109;
uint64_t var_18_110;
uint64_t var_18_111;
bool var_18_112;
uint8_t* var_18_113;
uint8_t* var_18_114;
uint8_t* var_18_115;
uint8_t* var_18_116;
uint8_t* var_18_117;
uint8_t* var_18_118;
uint64_t var_18_119;
uint64_t var_18_120;
uint64_t var_18_121;
uint8_t* var_18_122;
uint8_t* var_12_101;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_12_102 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_12_103;
uint8_t* var_12_104;
uint64_t var_12_105;
uint64_t var_12_106;
uint64_t var_12_107;
uint64_t var_12_108;
bool var_12_109;
uint8_t* var_12_110;
uint8_t* var_12_111;
uint8_t* var_12_112;
uint8_t* var_12_113;
uint8_t* var_12_114;
uint8_t* var_12_115;
uint64_t var_12_116;
uint64_t var_12_117;
uint64_t var_12_118;
uint8_t* var_10_101;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_10_102 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_10_103;
uint8_t* var_10_104;
uint8_t* var_10_105;
uint64_t var_10_106;
uint64_t var_10_107;
uint64_t var_10_108;
uint64_t var_10_109;
bool var_10_110;
uint8_t* var_10_111;
uint8_t* var_10_112;
uint8_t* var_10_113;
uint8_t* var_10_114;
uint8_t* var_10_115;
uint8_t* var_10_116;
uint64_t var_10_117;
uint64_t var_10_118;
uint64_t var_10_119;
uint8_t* var_10_120;
uint64_t var_10_121;
uint8_t* var_8_101;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_8_102 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_8_103;
uint8_t* var_8_104;
uint8_t* var_8_105;
uint64_t var_8_106;
uint64_t var_8_107;
uint64_t var_8_108;
uint64_t var_8_109;
uint64_t var_8_110;
bool var_8_111;
uint8_t* var_8_112;
uint8_t* var_8_113;
uint8_t* var_8_114;
uint8_t* var_8_115;
uint8_t* var_8_116;
uint8_t* var_8_117;
uint64_t var_8_118;
uint64_t var_8_119;
uint64_t var_8_120;
uint8_t* var_8_121;
uint8_t* var_6_101;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_6_102 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_6_103;
uint8_t* var_6_104;
uint8_t* var_6_105;
uint64_t var_6_106;
uint64_t var_6_107;
uint64_t var_6_108;
uint64_t var_6_109;
uint64_t var_6_110;
bool var_6_111;
uint8_t* var_6_112;
uint8_t* var_6_113;
uint8_t* var_6_114;
uint8_t* var_6_115;
uint8_t* var_6_116;
uint8_t* var_6_117;
uint64_t var_6_118;
uint64_t var_6_119;
uint64_t var_6_120;
uint8_t* var_6_121;
uint8_t* var_4_104;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_4_105 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_4_106;
uint8_t* var_4_107;
uint8_t* var_4_108;
uint64_t var_4_109;
uint64_t var_4_110;
uint64_t var_4_111;
uint64_t var_4_112;
uint64_t var_4_113;
bool var_4_114;
uint8_t* var_4_115;
uint8_t* var_4_116;
uint8_t* var_4_117;
uint8_t* var_4_118;
uint8_t* var_4_119;
uint8_t* var_4_120;
uint8_t* var_4_121;
uint8_t* var_2_121;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_2_122 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_2_123;
uint8_t* var_2_124;
uint8_t* var_2_125;
uint64_t var_2_126;
uint64_t var_2_127;
uint64_t var_2_128;
uint64_t var_2_129;
uint64_t var_2_130;
bool var_2_131;
uint8_t* var_2_132;
uint8_t* var_2_133;
uint64_t var_2_2;
uint8_t* var_2_3;
uint64_t var_2_4;
uint64_t var_2_5;
bool var_2_7;
uint64_t var_2_13;
bool var_2_18;
bool var_2_19;
uint8_t* var_15_102;
NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1> var_15_103 = NES::INVALID<NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>>;
uint64_t var_15_104;
//basic blocks
Block_0:
var_0_0 = NES::NESStrongType<unsigned long, NES::OriginId_, 0, 1>(0);
var_0_1 = 0;
var_0_2 = 0;
var_0_3 = 0;
var_0_4 = 0;
var_0_5 = 0;
var_0_6 = 1;
var_0_7 = NES__Runtime__TupleBuffer__Watermark(var_0_139);
var_0_9 = NES__Runtime__TupleBuffer__getSequenceNumber(var_0_139);
var_0_11 = NES__Runtime__TupleBuffer__getChunkNumber(var_0_139);
var_0_13 = NES__Runtime__TupleBuffer__isLastChunk(var_0_139);
var_0_15 = NES__Runtime__TupleBuffer__getOriginId(var_0_139);
var_0_17 = 0;
var_0_19 = 0;
var_0_21 = 0;
var_0_23 = static_cast<uint8_t*>(allocateBufferProxy(var_0_138));
var_0_24 = 0;
var_0_25 = static_cast<uint8_t*>(NES__Runtime__TupleBuffer__getBuffer(var_0_23));
var_0_26 = 0;
var_0_27 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_137,var_0_26));
var_0_28 = static_cast<uint8_t*>(NES__Runtime__TupleBuffer__getBuffer(var_0_139));
var_0_29 = 1;
var_0_30 = getSliceIdNLJProxy(var_0_28,var_0_29);
var_0_31 = 0;
var_0_32 = getSliceIdNLJProxy(var_0_28,var_0_31);
var_0_33 = getNLJWindowStartProxy(var_0_28);
var_0_34 = getNLJWindowEndProxy(var_0_28);
var_0_35 = NES::NESStrongType<unsigned long, NES::WorkerThreadId_, 0, 1>(0);
var_0_36 = static_cast<uint8_t*>(getNLJSliceRefFromIdProxy(var_0_27,var_0_30));
var_0_37 = static_cast<uint8_t*>(getNLJSliceRefFromIdProxy(var_0_27,var_0_32));
var_0_38 = 1;
var_0_39 = static_cast<uint8_t*>(getNLJPagedVectorProxy(var_0_36,var_0_35,var_0_38));
var_0_40 = 0;
var_0_41 = static_cast<uint8_t*>(getNLJPagedVectorProxy(var_0_37,var_0_35,var_0_40));
var_0_42 = static_cast<uint8_t*>(pagedSizeVectorBeginProxy(var_0_39));
var_0_43 = static_cast<uint8_t*>(pagedSizeVectorEndProxy(var_0_39));
// prepare block arguments
var_14_102 = var_0_137;
var_14_103 = var_0_15;
var_14_104 = var_0_9;
var_14_105 = var_0_138;
var_14_106 = var_0_23;
var_14_107 = var_0_3;
var_14_108 = var_0_21;
var_14_109 = var_0_7;
var_14_110 = var_0_24;
var_14_111 = var_0_11;
var_14_112 = var_0_13;
var_14_113 = var_0_42;
var_14_114 = var_0_43;
var_14_115 = var_0_41;
var_14_116 = var_0_39;
var_14_117 = var_0_25;
goto Block_14;

Block_14:
var_0_44 = pagedVectorIteratorNotEqualsProxy(var_14_113,var_14_114);
if (var_0_44){
// prepare block arguments
var_1_117 = var_14_102;
var_1_118 = var_14_103;
var_1_119 = var_14_104;
var_1_120 = var_14_105;
var_1_121 = var_14_106;
var_1_122 = var_14_107;
var_1_123 = var_14_108;
var_1_124 = var_14_109;
var_1_125 = var_14_110;
var_1_126 = var_14_111;
var_1_127 = var_14_112;
var_1_128 = var_14_113;
var_1_129 = var_14_114;
var_1_130 = var_14_115;
var_1_131 = var_14_116;
var_1_132 = var_14_117;
goto Block_1;
}else{
// prepare block arguments
var_2_121 = var_14_102;
var_2_122 = var_14_103;
var_2_123 = var_14_104;
var_2_124 = var_14_105;
var_2_125 = var_14_106;
var_2_126 = var_14_107;
var_2_127 = var_14_108;
var_2_128 = var_14_109;
var_2_129 = var_14_110;
var_2_130 = var_14_111;
var_2_131 = var_14_112;
var_2_132 = var_14_113;
var_2_133 = var_14_114;
goto Block_2;}

Block_1:
var_1_0 = pagedVectorIteratorLoadProxy(var_1_128);
var_1_1 = static_cast<uint8_t*>(entryDataProxy(var_1_131,var_1_0));
var_1_2 = *reinterpret_cast<uint64_t*>(var_1_1);
var_1_3 = 8;
var_1_4 = var_1_1+var_1_3;
var_1_6 = *reinterpret_cast<uint64_t*>(var_1_4);
var_1_7 = 8;
var_1_8 = var_1_4+var_1_7;
var_1_10 = *reinterpret_cast<uint64_t*>(var_1_8);
var_1_11 = 8;
var_1_12 = var_1_8+var_1_11;
var_1_14 = *reinterpret_cast<uint64_t*>(var_1_12);
var_1_15 = 8;
var_1_16 = var_1_12+var_1_15;
var_1_18 = static_cast<uint8_t*>(pagedSizeVectorBeginProxy(var_1_130));
var_1_19 = static_cast<uint8_t*>(pagedSizeVectorEndProxy(var_1_130));
// prepare block arguments
var_13_102 = var_1_117;
var_13_103 = var_1_118;
var_13_104 = var_1_119;
var_13_105 = var_1_120;
var_13_106 = var_1_121;
var_13_107 = var_1_122;
var_13_108 = var_1_123;
var_13_109 = var_1_124;
var_13_110 = var_1_125;
var_13_111 = var_1_126;
var_13_112 = var_1_127;
var_13_113 = var_1_128;
var_13_114 = var_1_129;
var_13_115 = var_1_18;
var_13_116 = var_1_19;
var_13_117 = var_1_130;
var_13_118 = var_1_131;
var_13_119 = var_1_14;
var_13_120 = var_1_10;
var_13_121 = var_1_2;
var_13_122 = var_1_132;
goto Block_13;

Block_13:
var_1_20 = pagedVectorIteratorNotEqualsProxy(var_13_115,var_13_116);
if (var_1_20){
// prepare block arguments
var_3_113 = var_13_102;
var_3_114 = var_13_103;
var_3_115 = var_13_104;
var_3_116 = var_13_105;
var_3_117 = var_13_107;
var_3_118 = var_13_108;
var_3_119 = var_13_109;
var_3_120 = var_13_111;
var_3_121 = var_13_112;
var_3_122 = var_13_113;
var_3_123 = var_13_114;
var_3_124 = var_13_115;
var_3_125 = var_13_116;
var_3_126 = var_13_117;
var_3_127 = var_13_118;
var_3_128 = var_13_119;
var_3_129 = var_13_120;
var_3_130 = var_13_106;
var_3_131 = var_13_110;
var_3_132 = var_13_121;
var_3_133 = var_13_122;
goto Block_3;
}else{
// prepare block arguments
var_4_104 = var_13_102;
var_4_105 = var_13_103;
var_4_106 = var_13_104;
var_4_107 = var_13_105;
var_4_108 = var_13_106;
var_4_109 = var_13_107;
var_4_110 = var_13_108;
var_4_111 = var_13_109;
var_4_112 = var_13_110;
var_4_113 = var_13_111;
var_4_114 = var_13_112;
var_4_115 = var_13_113;
var_4_116 = var_13_114;
var_4_117 = var_13_115;
var_4_118 = var_13_116;
var_4_119 = var_13_117;
var_4_120 = var_13_118;
var_4_121 = var_13_122;
goto Block_4;}

Block_3:
var_3_0 = pagedVectorIteratorLoadProxy(var_3_124);
var_3_1 = static_cast<uint8_t*>(entryDataProxy(var_3_126,var_3_0));
var_3_2 = *reinterpret_cast<uint64_t*>(var_3_1);
var_3_3 = 8;
var_3_4 = var_3_1+var_3_3;
var_3_6 = *reinterpret_cast<uint64_t*>(var_3_4);
var_3_7 = 8;
var_3_8 = var_3_4+var_3_7;
var_3_10 = *reinterpret_cast<uint64_t*>(var_3_8);
var_3_11 = 8;
var_3_12 = var_3_8+var_3_11;
var_3_14 = var_3_132 == var_3_2;
if (var_3_14){
// prepare block arguments
var_5_104 = var_3_113;
var_5_105 = var_3_114;
var_5_106 = var_3_115;
var_5_107 = var_3_116;
var_5_108 = var_3_117;
var_5_109 = var_3_118;
var_5_110 = var_3_119;
var_5_111 = var_3_120;
var_5_112 = var_3_121;
var_5_113 = var_3_122;
var_5_114 = var_3_123;
var_5_115 = var_3_124;
var_5_116 = var_3_125;
var_5_117 = var_3_126;
var_5_118 = var_3_127;
var_5_119 = var_3_128;
var_5_120 = var_3_129;
var_5_121 = var_3_130;
var_5_122 = var_3_131;
var_5_123 = var_3_10;
var_5_124 = var_3_6;
var_5_125 = var_3_2;
var_5_126 = var_3_133;
goto Block_5;
}else{
// prepare block arguments
var_6_101 = var_3_113;
var_6_102 = var_3_114;
var_6_103 = var_3_115;
var_6_104 = var_3_116;
var_6_105 = var_3_130;
var_6_106 = var_3_117;
var_6_107 = var_3_118;
var_6_108 = var_3_119;
var_6_109 = var_3_131;
var_6_110 = var_3_120;
var_6_111 = var_3_121;
var_6_112 = var_3_122;
var_6_113 = var_3_123;
var_6_114 = var_3_124;
var_6_115 = var_3_125;
var_6_116 = var_3_126;
var_6_117 = var_3_127;
var_6_118 = var_3_128;
var_6_119 = var_3_129;
var_6_120 = var_3_132;
var_6_121 = var_3_133;
goto Block_6;}

Block_5:
var_5_2 = var_5_119 > var_5_123;
var_5_3 = var_5_119 == var_5_123;
var_5_4 = var_5_2||var_5_3;
if (var_5_4){
// prepare block arguments
var_7_105 = var_5_104;
var_7_106 = var_5_105;
var_7_107 = var_5_106;
var_7_108 = var_5_107;
var_7_109 = var_5_108;
var_7_110 = var_5_109;
var_7_111 = var_5_110;
var_7_112 = var_5_111;
var_7_113 = var_5_112;
var_7_114 = var_5_113;
var_7_115 = var_5_114;
var_7_116 = var_5_115;
var_7_117 = var_5_116;
var_7_118 = var_5_117;
var_7_119 = var_5_118;
var_7_120 = var_5_119;
var_7_121 = var_5_120;
var_7_122 = var_5_121;
var_7_123 = var_5_122;
var_7_124 = var_5_125;
var_7_125 = var_5_126;
goto Block_7;
}else{
// prepare block arguments
var_8_101 = var_5_104;
var_8_102 = var_5_105;
var_8_103 = var_5_106;
var_8_104 = var_5_107;
var_8_105 = var_5_121;
var_8_106 = var_5_108;
var_8_107 = var_5_109;
var_8_108 = var_5_110;
var_8_109 = var_5_122;
var_8_110 = var_5_111;
var_8_111 = var_5_112;
var_8_112 = var_5_113;
var_8_113 = var_5_114;
var_8_114 = var_5_115;
var_8_115 = var_5_116;
var_8_116 = var_5_117;
var_8_117 = var_5_118;
var_8_118 = var_5_119;
var_8_119 = var_5_120;
var_8_120 = var_5_125;
var_8_121 = var_5_126;
goto Block_8;}

Block_7:
var_7_0 = 512;
var_7_1 = var_7_123 > var_7_0;
var_7_2 = var_7_123 == var_7_0;
var_7_3 = var_7_1||var_7_2;
if (var_7_3){
// prepare block arguments
var_9_114 = var_7_105;
var_9_115 = var_7_106;
var_9_116 = var_7_107;
var_9_117 = var_7_108;
var_9_118 = var_7_109;
var_9_119 = var_7_110;
var_9_120 = var_7_111;
var_9_121 = var_7_112;
var_9_122 = var_7_113;
var_9_123 = var_7_114;
var_9_124 = var_7_115;
var_9_125 = var_7_116;
var_9_126 = var_7_117;
var_9_127 = var_7_118;
var_9_128 = var_7_119;
var_9_129 = var_7_120;
var_9_130 = var_7_121;
var_9_131 = var_7_122;
var_9_132 = var_7_123;
var_9_133 = var_7_124;
goto Block_9;
}else{
// prepare block arguments
var_10_101 = var_7_105;
var_10_102 = var_7_106;
var_10_103 = var_7_107;
var_10_104 = var_7_108;
var_10_105 = var_7_122;
var_10_106 = var_7_109;
var_10_107 = var_7_110;
var_10_108 = var_7_111;
var_10_109 = var_7_112;
var_10_110 = var_7_113;
var_10_111 = var_7_114;
var_10_112 = var_7_115;
var_10_113 = var_7_116;
var_10_114 = var_7_117;
var_10_115 = var_7_118;
var_10_116 = var_7_119;
var_10_117 = var_7_123;
var_10_118 = var_7_120;
var_10_119 = var_7_121;
var_10_120 = var_7_125;
var_10_121 = var_7_124;
goto Block_10;}

Block_9:
var_9_0 = 0;
NES__Runtime__TupleBuffer__setNumberOfTuples(var_9_131,var_9_132);
NES__Runtime__TupleBuffer__setWatermark(var_9_131,var_9_120);
NES__Runtime__TupleBuffer__setOriginId(var_9_131,var_9_115);
NES__Runtime__TupleBuffer__setStatisticId(var_9_131,var_9_119);
NES__Runtime__TupleBuffer__setSequenceNr(var_9_131,var_9_116);
var_9_6 = getNextChunkNumberProxy(var_9_114,var_9_115,var_9_116);
NES__Runtime__TupleBuffer__setChunkNumber(var_9_131,var_9_6);
NES__Runtime__TupleBuffer__setLastChunk(var_9_131,var_9_0);
NES__Runtime__TupleBuffer__setCreationTimestampInMS(var_9_131,var_9_118);
emitBufferProxy(var_9_117,var_9_114,var_9_131);
var_9_11 = 1;
var_9_12 = var_9_0 == var_9_11;
if (var_9_12){
// prepare block arguments
var_11_102 = var_9_114;
var_11_103 = var_9_115;
var_11_104 = var_9_116;
var_11_105 = var_9_117;
var_11_106 = var_9_118;
var_11_107 = var_9_119;
var_11_108 = var_9_120;
var_11_109 = var_9_121;
var_11_110 = var_9_122;
var_11_111 = var_9_123;
var_11_112 = var_9_124;
var_11_113 = var_9_125;
var_11_114 = var_9_126;
var_11_115 = var_9_127;
var_11_116 = var_9_128;
var_11_117 = var_9_129;
var_11_118 = var_9_130;
var_11_119 = var_9_133;
goto Block_11;
}else{
// prepare block arguments
var_12_101 = var_9_114;
var_12_102 = var_9_115;
var_12_103 = var_9_116;
var_12_104 = var_9_117;
var_12_105 = var_9_118;
var_12_106 = var_9_119;
var_12_107 = var_9_120;
var_12_108 = var_9_121;
var_12_109 = var_9_122;
var_12_110 = var_9_123;
var_12_111 = var_9_124;
var_12_112 = var_9_125;
var_12_113 = var_9_126;
var_12_114 = var_9_127;
var_12_115 = var_9_128;
var_12_116 = var_9_129;
var_12_117 = var_9_130;
var_12_118 = var_9_133;
goto Block_12;}

Block_11:
removeSequenceStateProxy(var_11_102,var_11_103,var_11_104);
// prepare block arguments
var_20_104 = var_11_102;
var_20_105 = var_11_103;
var_20_106 = var_11_104;
var_20_107 = var_11_105;
var_20_108 = var_11_106;
var_20_109 = var_11_107;
var_20_110 = var_11_108;
var_20_111 = var_11_109;
var_20_112 = var_11_110;
var_20_113 = var_11_111;
var_20_114 = var_11_112;
var_20_115 = var_11_113;
var_20_116 = var_11_114;
var_20_117 = var_11_115;
var_20_118 = var_11_116;
var_20_119 = var_11_117;
var_20_120 = var_11_118;
var_20_121 = var_11_119;
goto Block_20;

Block_20:
var_11_1 = static_cast<uint8_t*>(allocateBufferProxy(var_20_107));
var_11_3 = static_cast<uint8_t*>(NES__Runtime__TupleBuffer__getBuffer(var_11_1));
var_11_5 = 0;
// prepare block arguments
var_19_112 = var_20_104;
var_19_113 = var_20_105;
var_19_114 = var_20_106;
var_19_115 = var_20_107;
var_19_116 = var_11_1;
var_19_117 = var_20_108;
var_19_118 = var_20_109;
var_19_119 = var_20_110;
var_19_120 = var_20_111;
var_19_121 = var_20_112;
var_19_122 = var_20_113;
var_19_123 = var_20_114;
var_19_124 = var_20_115;
var_19_125 = var_20_116;
var_19_126 = var_20_117;
var_19_127 = var_20_118;
var_19_128 = var_11_5;
var_19_129 = var_20_119;
var_19_130 = var_20_120;
var_19_131 = var_11_3;
var_19_132 = var_20_121;
goto Block_19;

Block_19:
var_11_7 = 16;
var_11_8 = var_11_7*var_19_128;
var_11_9 = var_19_131+var_11_8;
var_11_10 = 0;
var_11_11 = var_11_9+var_11_10;
*reinterpret_cast<uint64_t*>(var_11_11) = var_19_130;
var_11_13 = 8;
var_11_14 = var_11_9+var_11_13;
*reinterpret_cast<uint64_t*>(var_11_14) = var_19_129;
var_11_16 = 1;
var_11_17 = var_19_128+var_11_16;
// prepare block arguments
var_17_101 = var_19_112;
var_17_102 = var_19_113;
var_17_103 = var_19_114;
var_17_104 = var_19_115;
var_17_105 = var_19_116;
var_17_106 = var_19_117;
var_17_107 = var_19_118;
var_17_108 = var_19_119;
var_17_109 = var_11_17;
var_17_110 = var_19_120;
var_17_111 = var_19_121;
var_17_112 = var_19_122;
var_17_113 = var_19_123;
var_17_114 = var_19_124;
var_17_115 = var_19_125;
var_17_116 = var_19_126;
var_17_117 = var_19_127;
var_17_118 = var_19_129;
var_17_119 = var_19_130;
var_17_120 = var_19_132;
var_17_121 = var_19_131;
goto Block_17;

Block_17:
// prepare block arguments
var_18_102 = var_17_101;
var_18_103 = var_17_102;
var_18_104 = var_17_103;
var_18_105 = var_17_104;
var_18_106 = var_17_105;
var_18_107 = var_17_106;
var_18_108 = var_17_107;
var_18_109 = var_17_108;
var_18_110 = var_17_109;
var_18_111 = var_17_110;
var_18_112 = var_17_111;
var_18_113 = var_17_112;
var_18_114 = var_17_113;
var_18_115 = var_17_114;
var_18_116 = var_17_115;
var_18_117 = var_17_116;
var_18_118 = var_17_117;
var_18_119 = var_17_118;
var_18_120 = var_17_119;
var_18_121 = var_17_120;
var_18_122 = var_17_121;
goto Block_18;

Block_18:
pagedVectorIteratorIncProxy(var_18_115);
// prepare block arguments
var_13_102 = var_18_102;
var_13_103 = var_18_103;
var_13_104 = var_18_104;
var_13_105 = var_18_105;
var_13_106 = var_18_106;
var_13_107 = var_18_107;
var_13_108 = var_18_108;
var_13_109 = var_18_109;
var_13_110 = var_18_110;
var_13_111 = var_18_111;
var_13_112 = var_18_112;
var_13_113 = var_18_113;
var_13_114 = var_18_114;
var_13_115 = var_18_115;
var_13_116 = var_18_116;
var_13_117 = var_18_117;
var_13_118 = var_18_118;
var_13_119 = var_18_119;
var_13_120 = var_18_120;
var_13_121 = var_18_121;
var_13_122 = var_18_122;
goto Block_13;

Block_12:
// prepare block arguments
var_20_104 = var_12_101;
var_20_105 = var_12_102;
var_20_106 = var_12_103;
var_20_107 = var_12_104;
var_20_108 = var_12_105;
var_20_109 = var_12_106;
var_20_110 = var_12_107;
var_20_111 = var_12_108;
var_20_112 = var_12_109;
var_20_113 = var_12_110;
var_20_114 = var_12_111;
var_20_115 = var_12_112;
var_20_116 = var_12_113;
var_20_117 = var_12_114;
var_20_118 = var_12_115;
var_20_119 = var_12_116;
var_20_120 = var_12_117;
var_20_121 = var_12_118;
goto Block_20;

Block_10:
// prepare block arguments
var_19_112 = var_10_101;
var_19_113 = var_10_102;
var_19_114 = var_10_103;
var_19_115 = var_10_104;
var_19_116 = var_10_105;
var_19_117 = var_10_106;
var_19_118 = var_10_107;
var_19_119 = var_10_108;
var_19_120 = var_10_109;
var_19_121 = var_10_110;
var_19_122 = var_10_111;
var_19_123 = var_10_112;
var_19_124 = var_10_113;
var_19_125 = var_10_114;
var_19_126 = var_10_115;
var_19_127 = var_10_116;
var_19_128 = var_10_117;
var_19_129 = var_10_118;
var_19_130 = var_10_119;
var_19_131 = var_10_120;
var_19_132 = var_10_121;
goto Block_19;

Block_8:
// prepare block arguments
var_18_102 = var_8_101;
var_18_103 = var_8_102;
var_18_104 = var_8_103;
var_18_105 = var_8_104;
var_18_106 = var_8_105;
var_18_107 = var_8_106;
var_18_108 = var_8_107;
var_18_109 = var_8_108;
var_18_110 = var_8_109;
var_18_111 = var_8_110;
var_18_112 = var_8_111;
var_18_113 = var_8_112;
var_18_114 = var_8_113;
var_18_115 = var_8_114;
var_18_116 = var_8_115;
var_18_117 = var_8_116;
var_18_118 = var_8_117;
var_18_119 = var_8_118;
var_18_120 = var_8_119;
var_18_121 = var_8_120;
var_18_122 = var_8_121;
goto Block_18;

Block_6:
// prepare block arguments
var_17_101 = var_6_101;
var_17_102 = var_6_102;
var_17_103 = var_6_103;
var_17_104 = var_6_104;
var_17_105 = var_6_105;
var_17_106 = var_6_106;
var_17_107 = var_6_107;
var_17_108 = var_6_108;
var_17_109 = var_6_109;
var_17_110 = var_6_110;
var_17_111 = var_6_111;
var_17_112 = var_6_112;
var_17_113 = var_6_113;
var_17_114 = var_6_114;
var_17_115 = var_6_115;
var_17_116 = var_6_116;
var_17_117 = var_6_117;
var_17_118 = var_6_118;
var_17_119 = var_6_119;
var_17_120 = var_6_120;
var_17_121 = var_6_121;
goto Block_17;

Block_4:
releasePagedVectorIteratorProxy(var_4_118);
releasePagedVectorIteratorProxy(var_4_117);
pagedVectorIteratorIncProxy(var_4_115);
// prepare block arguments
var_14_102 = var_4_104;
var_14_103 = var_4_105;
var_14_104 = var_4_106;
var_14_105 = var_4_107;
var_14_106 = var_4_108;
var_14_107 = var_4_109;
var_14_108 = var_4_110;
var_14_109 = var_4_111;
var_14_110 = var_4_112;
var_14_111 = var_4_113;
var_14_112 = var_4_114;
var_14_113 = var_4_115;
var_14_114 = var_4_116;
var_14_115 = var_4_119;
var_14_116 = var_4_120;
var_14_117 = var_4_121;
goto Block_14;

Block_2:
releasePagedVectorIteratorProxy(var_2_133);
releasePagedVectorIteratorProxy(var_2_132);
var_2_2 = 0;
var_2_3 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_2_121,var_2_2));
var_2_4 = 3;
var_2_5 = 1;
deleteAllSlicesProxy(var_2_3,var_2_128,var_2_123,var_2_130,var_2_131,var_2_122,var_2_4,var_2_5);
var_2_7 = isLastChunkProxy(var_2_121,var_2_122,var_2_123,var_2_130,var_2_131);
NES__Runtime__TupleBuffer__setNumberOfTuples(var_2_125,var_2_129);
NES__Runtime__TupleBuffer__setWatermark(var_2_125,var_2_128);
NES__Runtime__TupleBuffer__setOriginId(var_2_125,var_2_122);
NES__Runtime__TupleBuffer__setStatisticId(var_2_125,var_2_127);
NES__Runtime__TupleBuffer__setSequenceNr(var_2_125,var_2_123);
var_2_13 = getNextChunkNumberProxy(var_2_121,var_2_122,var_2_123);
NES__Runtime__TupleBuffer__setChunkNumber(var_2_125,var_2_13);
NES__Runtime__TupleBuffer__setLastChunk(var_2_125,var_2_7);
NES__Runtime__TupleBuffer__setCreationTimestampInMS(var_2_125,var_2_126);
emitBufferProxy(var_2_124,var_2_121,var_2_125);
var_2_18 = 1;
var_2_19 = var_2_7 == var_2_18;
if (var_2_19){
// prepare block arguments
var_15_102 = var_2_121;
var_15_103 = var_2_122;
var_15_104 = var_2_123;
goto Block_15;
}else{
// prepare block arguments
goto Block_16;}

Block_15:
removeSequenceStateProxy(var_15_102,var_15_103,var_15_104);
// prepare block arguments
goto Block_21;

Block_21:
return;

Block_16:
// prepare block arguments
goto Block_21;

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
var_0_7 = 0;
var_0_8 = static_cast<uint8_t*>(getGlobalOperatorHandlerProxy(var_0_113,var_0_7));
var_0_9 = 3;
var_0_10 = 1;
deleteAllWindowsProxy(var_0_8,var_0_113,var_0_9,var_0_10);
return;

}

#pragma GCC diagnostic pop
template<>
void NES::Unikernel::Stage<2>::terminate(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext) {
           ::terminate(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));
}
template<>
void NES::Unikernel::Stage<2>::setup(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext) {
           ::setup(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext));
}
template<>
void NES::Unikernel::Stage<2>::execute(NES::Unikernel::UnikernelPipelineExecutionContext & context,
NES::Runtime::WorkerContext * workerContext, NES::Runtime::TupleBuffer& buffer) {
           ::execute(reinterpret_cast<uint8_t*>(&context), reinterpret_cast<uint8_t*>(workerContext), reinterpret_cast<uint8_t*>(&buffer));
}
template<size_t SubQueryPlanId>
extern NES::Runtime::Execution::OperatorHandler* getSharedOperatorHandler(int index);
template<>
NES::Runtime::Execution::OperatorHandler* NES::Unikernel::Stage<2>::getOperatorHandler(int index) {
switch(index) {
case 0:
	return getSharedOperatorHandler<1>(0);default:
	assert(false && "Bad OperatorHandler index");
	return nullptr;}}
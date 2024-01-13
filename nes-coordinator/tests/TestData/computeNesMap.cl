__kernel void map(__global uchar *value, __global uchar *output, __private ulong numberOfTuples)
{
  ulong ul_0, ul_1, ul_14, ul_8; 
  float f_24, f_22, f_23, f_28, f_26, f_27, f_16, f_17, f_15, f_20, f_21, f_18, f_19; 
  int i_2, i_3, i_4, i_5, i_10, i_11, i_29; 
  long l_6, l_7, l_12, l_13; 
  float4 v4f_9; 
  float3 v3f_25; 

  // BLOCK 0
  ul_0  =  (ulong) value;
  ul_1  =  (ulong) output;
  i_2  =  get_global_size(0);
  i_3  =  get_global_id(0);
  // BLOCK 1 MERGES [0 2 ]
  i_4  =  i_3;
  for(;i_4 < numberOfTuples;)
  {
    // BLOCK 2
    i_5  =  i_4 << 2;
    l_6  =  (long) i_5;
    l_7  =  l_6 << 2;
    ul_8  =  ul_0 + l_7;
    v4f_9  =  vload4(0, (__global float *) ul_8);
    i_10  =  i_4 << 1;
    i_11  =  i_10 + i_4;
    l_12  =  (long) i_11;
    l_13  =  l_12 << 2;
    ul_14  =  ul_1 + l_13;
    f_15  =  v4f_9.s3 / 3.6F;
    f_16  =  pow(f_15, 2.0F);
    f_17  =  radians(v4f_9.s1);
    f_18  =  native_cos(f_17);
    f_19  =  native_sin(f_17);
    f_20  =  f_18 / f_19;
    f_21  =  fabs(f_20);
    f_22  =  f_16 * f_21;
    f_23  =  f_22 / 9.81F;
    f_24  =  fabs(v4f_9.s1);
    f_26  =  f_23;
    f_27  =  f_24;
    f_28  =  v4f_9.s3;
    v3f_25  =  (float3)(f_26, f_27, f_28);
    vstore3(v3f_25, 0, (__global float *) ul_14);
    i_29  =  i_2 + i_4;
    i_4  =  i_29;
  }  // B2
  // BLOCK 3
  return;
}  //  kernel
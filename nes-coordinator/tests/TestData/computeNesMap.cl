#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable
__kernel void computeNesMap(__global uchar *value, __global uchar *output, __private ulong numberOfTuples)
{
  float3 v3f_28;
  ulong ul_8, ul_14, ul_0, ul_1;
  long l_12, l_13, l_6, l_7;
  float f_29, f_15, f_31, f_30, f_25, f_27, f_26, f_21, f_22;
  float4 v4f_9;
  int i_4, i_5, i_10, i_11, i_2, i_3, i_32;
  double d_24, d_23, d_20, d_19, d_16, d_18, d_17;

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
    f_15  =  radians(v4f_9.s1);
    d_16  =  (double) f_15;
    d_17  =  native_cos(d_16);
    d_18  =  native_sin(d_16);
    d_19  =  d_17 / d_18;
    d_20  =  fabs(d_19);
    f_21  =  v4f_9.s3 / 3.6F;
    f_22  =  pow(f_21, 2.0F);
    d_23  =  (double) f_22;
    d_24  =  d_20 * d_23;
    f_25  =  (float) d_24;
    f_26  =  f_25 / 9.81F;
    f_27  =  fabs(v4f_9.s1);
    f_29  =  f_26;
    f_30  =  f_27;
    f_31  =  v4f_9.s3;
    v3f_28  =  (float3)(f_29, f_30, f_31);
    vstore3(v3f_28, 0, (__global float *) ul_14);
    i_32  =  i_2 + i_4;
    i_4  =  i_32;
  }  // B2
  // BLOCK 3
  return;
}  //  kernel


#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable
__kernel void computeNesMap(__global uchar *inputTuples, __global uchar *resultTuples, __private ulong numberOfTuples)
{
    int2 v2i_12;
    int4 v4i_22;
    ulong ul_0, ul_1, ul_2, ul_6, ul_7, ul_14, ul_15, ul_16, ul_11;
    int i_25, i_24, i_23, i_28, i_27, i_26, i_21, i_20, i_4, i_3, i_2, i_16, i_8;
    long l_0, l_1, l_17, l_18, l_9, l_10;

    // BLOCK 0
    ul_0  =  (ulong) inputTuples;
    ul_1  =  (ulong) resultTuples;
    i_2  =  (ulong) numberOfTuples;
    i_3  =  get_global_id(0);
    // BLOCK 1 MERGES [0 2 ]
    i_4  =  i_3;
    for(;i_4 < i_2;)
    {
        // BLOCK 2
        ul_2 = (__global ulong *) inputTuples;

        i_8  =  i_4 << 2;
        l_9  =  (long) i_8;
        l_10  =  l_9 << 1;
        ul_6  =  ul_2 + l_10;
        v2i_12 = vload2(0, (__global int *) (ul_6));

        i_20  =  v2i_12.s0 * 2;
        i_21  =  v2i_12.s0 + 2;
        i_23  =  v2i_12.s0;
        i_24  =  v2i_12.s1;
        i_25  =  i_20;
        i_26  =  i_21;
        v4i_22  =  (int4)(i_23, i_24, i_25, i_26);
        ul_14  =  *((__global ulong *) ul_1);
        ul_15  =  ul_1 + ul_14;
        i_16  =  i_4 << 2;
        l_17  =  (long) i_16;
        l_18  =  l_17 << 2;
        ul_16  =  ul_15 + l_18;
        vstore4(v4i_22, 0, (__global int *) ul_16);
        i_27  =  get_global_size(0);
        i_28  =  i_27 + i_4;
        i_4  =  i_28;
    }  // B2
    // BLOCK 3
    return;
}  //  kernel
module {
  func.func @main_graph(%arg0: !torch.vtensor<[?,4],f32>) -> !torch.vtensor<[?,3],f32> attributes {torch.onnx_meta.ir_version = 8 : si64, torch.onnx_meta.opset_version = 17 : si64, torch.onnx_meta.producer_name = "pytorch", torch.onnx_meta.producer_version = "2.6.0"} {
    %0 = torch.operator "onnx.Constant"() {torch.onnx.value = dense_resource<_fc1.weight> : tensor<8x4xf32>} : () -> !torch.vtensor<[8,4],f32> 
    %1 = torch.operator "onnx.Constant"() {torch.onnx.value = dense_resource<_fc1.bias> : tensor<8xf32>} : () -> !torch.vtensor<[8],f32> 
    %2 = torch.operator "onnx.Constant"() {torch.onnx.value = dense_resource<_fc2.weight> : tensor<16x8xf32>} : () -> !torch.vtensor<[16,8],f32> 
    %3 = torch.operator "onnx.Constant"() {torch.onnx.value = dense_resource<_fc2.bias> : tensor<16xf32>} : () -> !torch.vtensor<[16],f32> 
    %4 = torch.operator "onnx.Constant"() {torch.onnx.value = dense_resource<_fc3.weight> : tensor<3x16xf32>} : () -> !torch.vtensor<[3,16],f32> 
    %5 = torch.operator "onnx.Constant"() {torch.onnx.value = dense_resource<_fc3.bias> : tensor<3xf32>} : () -> !torch.vtensor<[3],f32> 
    %none = torch.constant.none
    %6 = torch.operator "onnx.Gemm"(%arg0, %0, %1) {torch.onnx.alpha = 1.000000e+00 : f32, torch.onnx.beta = 1.000000e+00 : f32, torch.onnx.transB = 1 : si64} : (!torch.vtensor<[?,4],f32>, !torch.vtensor<[8,4],f32>, !torch.vtensor<[8],f32>) -> !torch.vtensor<[?,8],f32> 
    %7 = torch.operator "onnx.Gemm"(%6, %2, %3) {torch.onnx.alpha = 1.000000e+00 : f32, torch.onnx.beta = 1.000000e+00 : f32, torch.onnx.transB = 1 : si64} : (!torch.vtensor<[?,8],f32>, !torch.vtensor<[16,8],f32>, !torch.vtensor<[16],f32>) -> !torch.vtensor<[?,16],f32> 
    %8 = torch.operator "onnx.Relu"(%7) : (!torch.vtensor<[?,16],f32>) -> !torch.vtensor<[?,16],f32> 
    %9 = torch.operator "onnx.Gemm"(%8, %4, %5) {torch.onnx.alpha = 1.000000e+00 : f32, torch.onnx.beta = 1.000000e+00 : f32, torch.onnx.transB = 1 : si64} : (!torch.vtensor<[?,16],f32>, !torch.vtensor<[3,16],f32>, !torch.vtensor<[3],f32>) -> !torch.vtensor<[?,3],f32> 
    %10 = torch.operator "onnx.Softmax"(%9) {torch.onnx.axis = 1 : si64} : (!torch.vtensor<[?,3],f32>) -> !torch.vtensor<[?,3],f32> 
    return %10 : !torch.vtensor<[?,3],f32>
  }
}

{-#
  dialect_resources: {
    builtin: {
      _fc1.weight: "0x08000000A828C73E5193A53E21FCC73B8A5A1B3F0F8AA8BD6ABC493E850ABEBE456BFA3D3BADD13E42B108BF804E273F16CFA83E6C34AC3E7CDA093EE21A073EEC9442BEC660D73EDB0D6E3E1DDBD3BEA4918CBD513D80BED78A40BE33148CBDD5DCF83E392BF4BE9A3983BE678864BEA53AC0BEFF406ABB64A827BFE684293F12CB4ABE",
      _fc1.bias: "0x080000007522A13E209C3C3E45CE92BE171AA63EBBE44B3EB4359F3E969640BC5C9E92BE",
      _fc2.weight: "0x080000002D6BE33DD9198BBC3D6AA43D9C9A9A3E0C15833E1AEE10BE4B0B5C3E722494BD1DDB373E16AA5CBE4C31B3BEA1E00BBE91D88ABE8D88943E958ED03D19F6153E6533133EBB1D53BE13A9EF3EE4E04DBEEFD1E4BDB16E67BEA0CC8E3CBB97CA3D6716C73D1B1380BDC870903EC1F861BE2D4F63BEB88A4CBE0E83A83EDD84DA3D9C2FAE3E166495BE7B8CB3BE7B9F8DBE1A8A73BEE4A3123E26A3013ED369963E49F73ABE58CE76BEFD16403E705612BEDABA5B3EF2D2ABBD4D1A4F3E6FA58CBEEAC3AABDEA10963E9C15D1BD6B5668BCEAF3CD3E4A5CD93D49CF90BE9AD4D0BE5131C63E3BA3F53BAC2E76BEEA2997BE88C328BD19BBA9B9420D88BDD50BF7BE2CE0073C4A4D77BE829399BE755C47BE296E9EBE188666BED3F2B43E94C2883D57B7243E5CDE18BE2DEECFBE3F2BBEBDB61E653EE47BC9BE9A8308BEB2F5D9BE274D7FBC5489F1BD15C81EBDD91083BEE43185BE9AE441BEC089D5BD09EE84BE2765913EEA257EBE7639F33E7AEEF23D3F99EABE25549B3E538722BE5DF27B3D414296BE59F6B3BE8E2ACF3D962B9EBD35F30C3E538E94BEDF64863EBFE084BE613269BDFA4E703EFE48D93CAF8B8E3EF1BEF83EE5ACAEBE0A94173EBD6FB2BEDADE60BDF52F7EBE43B09C3E6B148CBE588F333E40C50C3EB01F063E03DE56BE400B9D3E7E27D4BD8E99603ED4429BBED25407BC6F9C723B4D9762BE82BF003F",
      _fc2.bias: "0x0800000001FEA93DB54E48BEEF225CBE296FE5BCFC254A3EB806B9BDC46565BD94BBC6BC283C87BEC22A6F3EC887833EE7CCDABEDF2AA83DDE7B913E1D504A3D49CB5CBE",
      _fc3.weight: "0x080000004741183E0888063E066CF0BDD85F5DBD089487BDD018DB3C85F4373E84A5F43D10A3233E8D2ACC3E885C53BC838D58BE80146F3BAE2BB63D5FE8563D8BE7C0BDB72A57BE84F2BCBD1B28723E8404B13DA0B94BBC08581ABEE0DF89BE1950CABE8CE4A6BDE08DF23C7109173E2E1EE73D284A62BE40D50BBE11945BBD4A527A3EBDA10A3CD09784BD12A4933EE087ACBD8CB225BEE4D93A3EE2E5BBBE4BF080BE7EAB7EBE55A91BBD66DF353ED970653E908032BE548BBABE00A046BEFD7CA33E",
      _fc3.bias: "0x08000000CCEA403E9F19693E5C123D3D"
    }
  }
#-}


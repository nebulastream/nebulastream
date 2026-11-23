# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["sim"], ["parda"], ["rt-pcr"], ["confirmado laboratorial"], ["internado em uti"], ["nao informado"], ["teste rapido - antigeno"], ["feminino"]]
# real-udf: origin=github-spark id=pamepi_strings_to_numbers requires= wrapped=1 ported=0
# Wrapper: the UDF returns an int code or the input text and is registered with udf() (StringType);
# Wrapper: case_entry returns str() of the result.
# Source: https://github.com/PAMepi/PAMepi_scripts_datalake/blob/404673c8c0e7b45f0f5ddcba963310f3a15979db/Brazilian%20COVID-19%20data%20streaming/Data%20curation/myfunctions.py#L44-L107
# Repository: PAMepi/PAMepi_scripts_datalake @ commit 404673c8c0e7b45f0f5ddcba963310f3a15979db
# License: MIT
# Copied verbatim from Brazilian COVID-19 data streaming/Data curation/myfunctions.py (L44-L107); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf()

def strings_sg_para_numeros(col):
    try:
        return {
            # profissionalSaude / profissionalSeguranca
            'sim': 1,
            'nao': 2,

            # racaCor
            'branca': 1,
            'preta': 2,
            'amarela': 3,
            'parda': 4,
            'indigena': 5,

            # resultadoTeste
            'positivo': 1,
            'negativo': 2,
            'nao detectavel': 3,
            'inconclusivo ou indeterminado': 9,
            'inconclusivo': 9,
            'indeterminado': 9,

            # sexo
            'masculino': 1,
            'feminino': 2,

            # tipoTeste
            'rt-pcr': 1,
            'imunoensaio por eletroquimioluminescencia eclia': 2,
            'imunoensaio por eletroquimioluminescencia - eclia igG': 2,
            'quimioluminescencia - clia': 2,
            'teste rapido - anticorpo': 3,
            'teste rapido - antigeno': 4,
            'enzimaimunoensaio-elisa igm': 5,

            # classificacaoFinal
            'confirmado': 1,
            'confirmado clinico-epidemiologico': 2,
            'confirmado por criterio clinico': 3,
            'confirmado laboratorial': 4,
            'confirmacao laboratorial': 4,
            'confirmado clinico-imagem': 5,
            'sindrome gripal nao especificada': 6,
            'descartado': 7,

            # estadoTeste
            'solicitado': 1,
            'coletado': 2,
            'concluido': 3,
            'exame nao solicitado': 4,

            # evolucaoCaso
            'obito': 1,
            'cura': 2,
            'internado': 3,
            'internado em uti': 4,
            'cancelado': 5,
            'em tratamento domiciliar': 6,

            'ignorado': 9,
        }[col]
    except KeyError:
        return col


def case_entry(col):
    return str(strings_sg_para_numeros(col))

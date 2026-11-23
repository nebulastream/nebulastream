# codon-fuzz: entry=gen_secao_cnae args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["01"], ["47"], ["62"], ["99"], ["35"], ["34"], ["86"], ["0"]]
# real-udf: origin=github-spark id=tlhop_gen_secao_cnae requires= wrapped=0 ported=0
# Source: https://github.com/lucasmsp/tlhop-library/blob/c2943f775bbc9bc99705d5db2495dea3f6528fe3/tlhop/library.py#L821-L846
# Repository: lucasmsp/tlhop-library @ commit c2943f775bbc9bc99705d5db2495dea3f6528fe3
# License: Apache-2.0
# Copied verbatim from tlhop/library.py (L821-L846); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf

def gen_secao_cnae(x):
    if x:
        x = int(x)
        if (x <= 3): return	"A - AGRICULTURA, PECUÁRIA, PRODUÇÃO FLORESTAL, PESCA E AQÜICULTURA"
        elif ( 5 <= x <= 9): return 	"B - INDÚSTRIAS EXTRATIVAS"
        elif ( 10 <= x <= 33): return	"C - INDÚSTRIAS DE TRANSFORMAÇÃO"
        elif ( 35 <= x <= 35): return	"D - ELETRICIDADE E GÁS"
        elif ( 36 <= x <= 39): return	"E - ÁGUA, ESGOTO, ATIVIDADES DE GESTÃO DE RESÍDUOS E DESCONTAMINAÇÃO"
        elif ( 41 <= x <= 43): return	"F - CONSTRUÇÃO"
        elif ( 45 <= x <= 47): return	"G - COMÉRCIO; REPARAÇÃO DE VEÍCULOS AUTOMOTORES E MOTOCICLETAS"
        elif ( 49 <= x <= 53): return	"H - TRANSPORTE, ARMAZENAGEM E CORREIO"
        elif ( 55 <= x <= 56): return	"I - ALOJAMENTO E ALIMENTAÇÃO"
        elif ( 58 <= x <= 63): return	"J - INFORMAÇÃO E COMUNICAÇÃO"
        elif ( 64 <= x <= 66): return	"K - ATIVIDADES FINANCEIRAS, DE SEGUROS E SERVIÇOS RELACIONADOS"
        elif ( 68 <= x <= 68): return	"L - ATIVIDADES IMOBILIÁRIAS"
        elif ( 69 <= x <= 75): return	"M - ATIVIDADES PROFISSIONAIS, CIENTÍFICAS E TÉCNICAS"
        elif ( 77 <= x <= 82): return	"N - ATIVIDADES ADMINISTRATIVAS E SERVIÇOS COMPLEMENTARES"
        elif ( 84 <= x <= 84): return	"O - ADMINISTRAÇÃO PÚBLICA, DEFESA E SEGURIDADE SOCIAL"
        elif ( 85 <= x <= 85): return	"P - EDUCAÇÃO"
        elif ( 86 <= x <= 88): return	"Q - SAÚDE HUMANA E SERVIÇOS SOCIAIS"
        elif ( 90 <= x <= 93): return	"R - ARTES, CULTURA, ESPORTE E RECREAÇÃO"
        elif ( 94 <= x <= 96): return	"S - OUTRAS ATIVIDADES DE SERVIÇOS"
        elif ( 97 <= x <= 97): return	"T - SERVIÇOS DOMÉSTICOS"
        elif ( 99 <= x <= 99): return	"U - ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUIÇÕES EXTRATERRITORIAIS"
    return "DESCONHECIDA"

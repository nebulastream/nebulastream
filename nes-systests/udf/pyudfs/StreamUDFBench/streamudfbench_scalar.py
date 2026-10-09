"""StreamUDFBench scalar UDFs (NebulaStream Python UDFs).

Ported from UDFBench engines/duckdb/udfs/scalar: same logic, `self` removed. One module-level function per UDF,
registered via CREATE FUNCTION ... ENTRYPOINT 'StreamUDFBench.streamudfbench_scalar.<name>'. NebulaStream passes VARSIZED values as bytes,
so text arguments are decoded with _str() first; NULL arguments never reach these functions (strict calls).
"""
import heapq
import json
import math
import random
import re
from collections import Counter


def _str(x):
    return x.decode('utf-8') if isinstance(x, (bytes, bytearray)) else x


# Type checks on parsed json values. Codon decides isinstance(x, T) and type(x) at compile time, and its json module
# returns a single json.Value type, so these ask the value itself when it can tell (hasattr is also decided at compile
# time) and fall back to isinstance otherwise. Under CPython and PyPy they are plain isinstance checks.
# The unadapted versions of jaccard and jsonparse live in streamudfbench_original_typechecks.py.

def _is_list(x):
    if hasattr(x, "is_list"):
        return x.is_list()
    return isinstance(x, list)


def _is_dict(x):
    if hasattr(x, "is_dict"):
        return x.is_dict()
    return isinstance(x, dict)


def _hashable(x):
    """Python lists are unhashable, so they become tuples; a Codon json.Value list is hashable as is."""
    if hasattr(x, "is_list"):
        return x
    return tuple(x) if isinstance(x, list) else x

# U1. Add_noise : adds gaussian noise to a value and returns a float 
def addnoise(val:int)->float:

    def add_noise(val, mean=0, std_dev=2):
        if val:       
            noise = random.gauss(mean, std_dev)
            result = float(val) + noise
            return result
        else:
            return None
    try: 
        return add_noise(val)
    except:
        return None

# U2.	Clean: Performs a simple data cleaning task on the string tokens of a json list

def clean(val: str)->str:
    val = _str(val)
    def removeshortwords(name):
        return " ".join([word for word in name.split(' ') if len(word) > 2])

    def sortname(name):
        return " ".join(sorted(name.split(' ')))

    def cleanpy(val):
        name_list = json.loads(val)
        name_list = [name.lower() for name in name_list]
        name_list = [removeshortwords(name) for name in name_list]
        name_list = [sortname(name) for name in name_list]
        return json.dumps(sorted(name_list))
    
    if val:
        try:
            return cleanpy(val)
        except:
            return "[]"
    else:
        return None

# U3.	Cleandate: Reads a date and converts it to a common format if it is not, handles also dirty dates

def cleandate(pubdate: str)->str:
    pubdate = _str(pubdate)
    if pubdate:
        try:
            if "-" in pubdate:
                splitnum = pubdate.count('-')
                pubdate_split = pubdate.split("-")
                if splitnum ==1:
                    return pubdate_split[0] + "/" + pubdate_split[1] + "/" + "01"
                elif splitnum ==2:
                    return pubdate_split[0] + "/" + pubdate_split[1] + "/" + pubdate_split[2]
                else:
                    return None
            elif "/" in pubdate:
                splitnum = pubdate.count('/')
                pubdate_split = pubdate.split("/")
                if splitnum ==1:
                    return pubdate_split[0] + "/" + pubdate_split[1] + "/" + "01"
                elif splitnum ==2:
                    return pubdate_split[0] + "-" + pubdate_split[1] + "-" + pubdate_split[2]
                else:
                    return None
            else:
                return None
        except:
            return None
    else:
        return None

# U4.	Converttoeuro: : Converts currency to euro, returns a float

def converttoeuro(x:float,y:str)->float:
    y = _str(y)

    euro_equals ={
    'EUR':1.00,
    '':1.00,
    'NOK':11.59,
    'AUD':1.63,
    'CAD':1.44,
    '$':1.09,
    'USD':1.09,
    'GBP':0.85,
    'CHF':0.98,
    'ZAR':20.41,
    'SGD':1.47,
    'INR':89.61,

    }
    if x is not None and y is not None:
        try:
            return float(x)/euro_equals[y]
        except:
            return 0.0
    else:
        return None

# U5.	Extractclass: extracts class from string with format funder::class::projectid 
def extractclass(project:str)->str:
    project = _str(project)
    if project:
        try:
            return project.split("::")[1]
        except:
            return None
    else:
        return None

# U6.	Extractcode: Processes a structured string containing the funder’s id, the funding class and the project id, and extracts the project id

def extractcode(project: str)->str:
    project = _str(project)
    if project:
        try:
            return project.split("::")[2]
        except:
            return None
    else:
        return None

# U7.	Extractday: Reads a date (as a string) and extracts an integer with the day 

def extractday(arg: str) -> int:
    arg = _str(arg)
    if arg:
        try:
            return int(arg[arg.rfind('-')+1:])
        except:
            return -1
    else:
        return None
        

# U8.	Extractfunder: extracts funder from string with format funder::class::projectid
def extractfunder(project:str)->str:
    project = _str(project)
    if project:
        try:
            if '::' in project:
                return project.split("::")[0]
            else:
                return None
        except:
            return None
    else:
        return None

def extractid(project:str)->str:
    project = _str(project)
    if project:
        try:
            return project.split("::")[2]
        except:
            return None
    else:
        return None

# U10. Extractmonth: Reads a date (as a string) and extracts an integer with the month

def extractmonth(arg: str) -> int:
    arg = _str(arg)
    if arg:
        try:
            return int(arg[arg.find('-')+1:arg.rfind('-')])
        except:
            return -1
    else:
        return None

# U11.	Extractprojectid: Processes a text snippet and extracts a 6 digit project identifier 

def extractprojectid(input: str)->str:
    input = _str(input)
    if input:
        try:
            return re.findall(r"(?<!\d)[0-9]{6}(?!\d)",input)[0]
        except: return ''
    else:
        return None

# U12.  Extractyear : Reads a date (as a string) and extracts an integer with the year

def extractyear(arg: str) -> int:
    arg = _str(arg)
    if arg:
        try:
            return int(arg[:arg.find('-')])
        except:
            return -1
    else:
        return None

# U14.	Frequentterms: Returns a space separated text containing the most N% frequent tokens 

def frequentterms(input:str,N:int)->str:
    input = _str(input)
    if input:
        try:
            words_list = input.split()
            word_counts = Counter(word.lower() for word in words_list)
            frequent_terms = heapq.nlargest(N, word_counts, key=word_counts.get)
            return ' '.join([word for word in frequent_terms])
        except:
            return ''
    else:
        return None

# U15.	Jaccard: Processes two json lists with tokens and calculated the jaccard distance

def jaccard(arg1:str,arg2:str)->float:
    arg1 = _str(arg1)
    arg2 = _str(arg2)

    if arg1 is not None and arg2 is not None:
        try:
            r=json.loads(arg1)
            s=json.loads(arg2)
            rset=set([_hashable(x) for x in r])
            sset=set([_hashable(x) for x in s])
            return float(len( rset & sset ))/(len( rset | sset ))
        except:
            return None
    else:
        return None

# U16.	Jpack: Converts a string to a json list with tokens

def jpack(input:str)->str:
    input = _str(input)
    if input:
        try:
            string_split = input.split()
            return json.dumps([word for word in string_split])
        except:
            return ''
    else:
        return None

# U17.	Jsoncount: Returns the length of a json list
def jsoncount(jval: str) -> int:
    jval = _str(jval)
    try:
        if jval[0]=='[':
            tot_json = json.loads(jval)
            return int(len(tot_json))
        else:
            return 1
    except:
        return None

# U18.	Jsonparse: Parses a json dict per time and returns a string with the value

# No return annotation: Codon enforces '-> str', so 'return None' would raise there instead of yielding NULL; without
# it Codon infers Optional[str]. CPython ignores annotations either way.
def jsonparse(json_content: str,key1: str):
    json_content = _str(json_content)
    key1 = _str(key1)

    try:
        data = json.loads(json_content)
        if _is_list(data):
            for item in data:
                value = item.get(key1)
                return None if value is None else str(value)
        elif _is_dict(data):
            value = data.get(key1)
            return None if value is None else str(value)
        else:
            return None
    except:
        return None

# U19.	Jsort: processes a json list and returns a sorted json list 
def jsort(jval:str)->str:
    jval = _str(jval)
    try:
        return json.dumps(sorted(json.loads(jval)))
    except:
        return "[]"

# U20.	Jsortvalues: processes a json list where each value contains more than one tokens, sorts the tokens in each value 

def jsortvalues(jval:str)->str:
    jval = _str(jval)
    def sortname(name):
        return " ".join(sorted(name.split(' ')))
    try:
        return json.dumps([sortname(name) for name in json.loads(jval)])
    except:
        return "[]"

# U21.	Keywords: Removes any punctuation from text and returns the keywords in one string 

def keywords(input:str)->str:
    input = _str(input)

    text_tokens = re.compile(r'([\d.]+\b|\w+)', re.UNICODE)

    if input:
        try:
            res=text_tokens.findall(input)

            return ' '.join((x for x in res if x != '.' ))
        except:
            return ''
    else:
        return None

# U22.	Log_10: Calculates and returns the logarithm

def log_10(x:float)->float:
    try:
        return math.log10(x)
    except:
        return 0.0

# U23. Lowerize: Converts to lower case the input text 
def lowerize(val: str)->str:
    val = _str(val)
    if val:
        try:
            return val.lower()
        except:
            return ''
    else:
        return None

# U24.	Removeshortterms:  processes a json list where each value contains more than one tokens and removes tokens with length less than 3 chars 

def removeshortterms(jval:str)->str:
    jval = _str(jval)
    def removeshortwords(name):
        return " ".join([word for word in name.split(' ') if len(word) > 2])
    try:
        return json.dumps([removeshortwords(name) for name in json.loads(jval)])
    except:
        return "[]"

# U13.	Filterstopwords: It processes an input text and returns it after removing the stopwords, using a list of stopwords

_stopwords=set([r".", r"_", r"stessi", r"można", r"einseitiger", r"wären", r"fÛr", r"olette", r"würde", r"andre", r"για", r"ebben", r"negl", r"steigen", r"einseitigen",
r"dere", r"niissä", r"ergänzen", r"otro", r"noista", r"dazu", r"περί", r"tobie", r"erst", r"mußt", r"szinte", r"otra", r"drauf", r"gänzlich", r"aus",
r"under", r"aux", r"gesehen", r"shouldn't", r"dela", r"neun", r"dele", r"berichteten", r"neue", r"voie", r"ayantes", r"letztes", r"vase", r"staremo",
r"algunos", r"letzten", r"tandis", r"tinham", r"senin", r"olisimme", r"we'll", r"ÔÏÇÏ", r"mog±", r"haben", r"zde", r"geworden", r"zda", r"upp",
r"teníais", r"fuera", r"esos", r"zuletzt", r"abbiate", r"zonder", r"hors", r"fece", r"angefangen", r"varit", r"ezt", r"verrate", r"n",
r"ellas", r"alt", r"avrebbero", r"nel", r"nem", r"ÍÏÖÎÏ", r"tipo", r"nei", r"emilyen", r"ned", r"klaren", r"while", r"ebbe", r"tipy", r"kun",
r"estuvo", r"nez", r"sobre", r"farebbero", r"estuve", r"ekkor", r"fall", r"neu", r"nes", r"dahi", r"med", r"meg", r"powinni", r"powinno", r"daha",
r"derjenige", r"wezen", r"men", r"flere", r"here", r"meu", r"met", r"nicht", r"avere", r"mes", r"mer", r"hers", r"ungefähr", r"amolyan", r"schätzt",
r"sidan", r"même", r"gli", r"auf", r"ÓÅÊÞÁÓ", r"anstatt", r"näiksi", r"aki", r"i'd", r"ueber", r"där", r"i'm", r"aurai", r"kýrk", r"müssten", r"esteve",
r"arról", r"desselben", r"także", r"magst", r"कोई", r"outro", r"जो", r"bra", r"sarai", r"tels", r"estive", r"would", r"heille", r"einiger",
r"einiges", r"fussions", r"loro", r"seraient", r"vermögen", r"cali", r"niiden", r"einigem", r"ellos", r"उसके", r"sembra", r"właśnie", r"által", r"willst",
r"Á", r"sabeu", r"musi", r"wenig", r"saber", r"sabes", r"किया", r"tendrías", r"herein", r"wohingegen", r"facemmo", r"nigdy", r"ÔÏÖÅ", r"ια", r"sabem",
r"saben", r"quale", r"me", r"×ÅÄØ", r"olet", r"solament", r"ma", r"जा", r"weiteres", r"weiterer", r"mi", r"maiorias", r"ने", r"mu", r"ÍÏÊ", r"weiterem",
r"era", r"ÂÙÌÁ", r"weiteren", r"olen", r"której", r"जब", r"ero", r"my", r"avaient", r"wieviel", r"czy", r"étiez", r"jest", r"tuvieses", r"czemu",
r"consigue", r"who's", r"gegen", r"wann", r"tuoksi", r"tuvierais", r"auraient", r"geben", r"ambos", r"wirklich", r"want", r"ना", r"muesste", r"hundert",
r"újra", r"buna", r"anderes", r"hoe", r"end", r"ene", r"weiterhin", r"czyli", r"przy", r"woraufhin", r"off", r"enn", r"algmas", r"startete", r"how",
r"ens", r"bunu", r"hos", r"wenige", r"stille", r"anderen", r"doesn't", r"jsme", r"mÅ", r"facesti", r"vocês", r"ÈÏÒÏÛÏ", r"sanki", r"lag", r"hubierais",
r"mest", r"वर्ग", r"kimś", r"Ö", r"lat", r"sería", r"lav", r"mogą", r"las", r"senkte", r"gdyż", r"je¶li", r"mí", r"estem", r"cinque", r"hasta",
r"getragen", r"letztlich", r"reagiere", r"mý", r"sont", r"stettero", r"vart", r"बहुत", r"josta", r"vars", r"aient", r"over", r"zei", r"teniu", r"peut",
r"hubiese", r"nostra", r"vara", r"nostre", r"ezzel", r"nostri", r"वे", r"avevo", r"vite", r"nostro", r"úgy", r"begge", r"gente", r"bisher", r"bundan",
r"übel", r"hon", r"vilken", r"rá", r"valor", r"teilen", r"minden", r"fin", r"dorthin", r"fim", r"then", r"them", r"sinulta", r"keissä", r"deinem",
r"deinen", r"plötzlich", r"deines", r"deiner", r"bana", r"they", r"mucho", r"pelas", r"wenn", r"mindestens", r"ausser", r"nawet", r"hubieran", r"worden",
r"dinge", r"l", r"estaréis", r"até", r"Õ", r"ÌÕÞÛÅ", r"each", r"ενός", r"ihn", r"akkor", r"verriet", r"kterou", r"diye", r"darüberhinaus", r"mają",
r"बाद", r"estivermos", r"nokon", r"navn", r"autre", r"mikor", r"zahlreich", r"deira", r"findest", r"nötigenfalls", r"aurais", r"aurait", r"deverá",
r"behielt", r"begann", r"hätte", r"sang", r"god", r"diesem", r"diesen", r"slutt", r"re", r"dieser", r"dieses", r"suyas", r"werde", r"avoir",
r"geteilt", r"frei", r"nirgends", r"verdadero", r"icke", r"verdadera", r"consegueixo", r"ÚÁ", r"więc", r"doppelt", r"elle", r"brauchen", r"weniger",
r"inom", r"hennar", r"ÓËÁÚÁÔØ", r"einführte", r"legen", r"intentan", r"bár", r"çünkü", r"versorgten", r"birkaç", r"estas", r"dahin", r"ist", r"att",
r"eramos", r"bräuchte", r"solch", r"gehabt", r"podemos", r"staranno", r"intentas", r"intentar", r"einer", r"eines", r"serei", r"avrebbe", r"näitä",
r"minä", r"esté", r"ÂÅÚ", r"näissä", r"tuon", r"einen", r"für", r"hogyan", r"ÔÏÇÄÁ", r"unmöglicher", r"drunter", r"peggio", r"τον", r"är",
r"perquè", r"ingen", r"tot", r"estuvieseis", r"brachten", r"quinto", r"toi", r"ton", r"solamente", r"tom", r"tuossa", r"doppio", r"ganze",
r"beitrugen", r"seré", r"llarg", r"beides", r"poderá", r"estavam", r"nahm", r"beiden", r"του", r"ależ", r"verte", r"både", r"vielen", r"sille",
r"teiltä", r"dürfen", r"minua", r"houve", r"verraten", r"kto", r"hemos", r"ancora", r"sta", r"vieles", r"vieler", r"sekä", r"sposób", r"yirmi",
r"este", r"esta", r"jederlei", r"nehmen", r"durchaus", r"esto", r"mine", r"vagyok", r"korso", r"jeres", r"már", r"further", r"eröffnetes", r"vorüber",
r"häneltä", r"habrás", r"mint", r"künftig", r"niçin", r"birþey", r"fosti", r"degl", r"foste", r"alors", r"altri", r"deres", r"derer", r"ÌÉ", r"deren",
r"derem", r"feci", r"tym", r"eût", r"oltre", r"inzwischen", r"dafür", r"tylko", r"ohne", r"erano", r"meidät", r"doit", r"ÂÙÌ", r"état", r"meidän",
r"dois", r"ihres", r"sarò", r"nämlich", r"γι", r"fusse", r"avranno", r"ahhoz", r"hanno", r"seks", r"cała", r"m", r"dog", r"noihin", r"cały", r"mithin",
r"dos", r"dov", r"yani", r"singen", r"tuolta", r"tatsächlich", r"hur", r"muß", r"estais", r"sage", r"liest", r"ebbi", r"én", r"nélkül", r"sagt",
r"somt", r"och", r"wirst", r"egyetlen", r"dessen", r"henne", r"höchstens", r"danach", r"sarebbero", r"sto", r"किसी", r"daher", r"estadas", r"sien",
r"sehen", r"irá", r"avrete", r"nebenan", r"do", r"meget", r"gdzieś", r"above", r"klein", r"di", r"de", r"stor", r"da", r"geen", r"vsak", r"du",
r"einmal", r"rispetto", r"ikkje", r"furent", r"außerhalb", r"stiamo", r"anfangen", r"των", r"nämä", r"rund", r"nichts", r"tenías", r"estados", r"iets",
r"gjÛre", r"starei", r"blev", r"iż", r"dort", r"ÚÄÅÓØ", r"skulle", r"tenham", r"nur", r"inny", r"nun", r"gefällt", r"inne", r"faig",
r"machst", r"inna", r"schreiben", r"iemand", r"amikor", r"blei", r"inni", r"eres", r"staremmo", r"houvéssemos", r"weiter", r"bekannt", r"wogegen",
r"bien", r"estiveram", r"ÔÅÐÅÒØ", r"erhalten", r"आप", r"eren", r"którego", r"erem", r"habrían", r"we", r"powinna", r"welche", r"legyen", r"diese",
r"wo", r"soyons", r"were", r"anche", r"sagten", r"legten", r"hätt", r"stiegen", r"niż", r"möglicherweise", r"ezen", r"direkter", r"ÖÉÚÎØ", r"drei",
r"ile", r"questa", r"clanku", r"mancherorts", r"ËÁË", r"estuviera", r"clanky", r"coz", r"questi", r"against", r"genug", r"una", r"kommer", r"und",
r"ils", r"då", r"coi", r"vorher", r"com", r"col", r"con", r"habían", r"foram", r"vaig", r"tovább", r"elf", r"jakaś", r"durante", r"niye",
r"befragen", r"ni¿", r"vais", r"ÜÔÉ", r"ele", r"habías", r"podria", r"fra", r"aren't", r"हैं", r"tendrás", r"tuolla", r"been", r"estaban", r"estabas",
r"tinc", r"toen", r"tenemos", r"joiden", r"jolta", r"amelyekben", r"jossa", r"nagyon", r"tendré", r"i¿", r"बनी", r"pueden", r"zich", r"मे", r"olisit",
r"hvilken", r"stavano", r"suas", r"mivel", r"könnt", r"letztendlich", r"avessimo", r"keistä", r"aqueles", r"auras", r"þey", r"ait", r"avrei", r"ayants",
r"podriais", r"egyik", r"dello", r"miltä", r"serai", r"ÎÉÂÕÄØ", r"vagy", r"aie", r"delle", r"della", r"faremmo", r"unos", r"pokud", r"unwichtig",
r"indietro", r"tässä", r"is", r"ir", r"it", r"desto", r"estos", r"ik", r"im", r"il", r"io", r"in", r"dedans", r"if", r"quand", r"meitä", r"manchem",
r"manchen", r"manches", r"mancher", r"quant", r"nokor", r"tätä", r"makt", r"än", r"angesetze", r"elli", r"keressünk", r"manche", r"gekonnt", r"nim",
r"nic", r"coś", r"jetzt", r"nie", r"angesetzt", r"elõtt", r"sollt", r"hayan", r"kim", r"ells", r"kvarhelst", r"ÍÎÏÇÏ", r"depois", r"he'll", r"hacer",
r"haces", r"ihrem", r"singt", r"ihren", r"tienes", r"algun", r"ÔÅÂÑ", r"gekommen", r"suyo", r"drin", r"enhver", r"thi", r"twój", r"folgendes", r"the",
r"ihrer", r"hacen", r"yours", r"jeszcze", r"andere", r"she'll", r"denen", r"anderm", r"andern", r"करने", r"mientras", r"था", r"fueseis", r"anderr",
r"anders", r"veja", r"eus", r"farei", r"hai", r"antes", r"ausdrückt", r"ham", r"han", r"kell", r"vollständig", r"hab", r"ela", r"had", r"estuvieron",
r"conseguimos", r"hay", r"niemals", r"ÜÔÏÇÏ", r"els", r"har", r"has", r"hat", r"zou", r"opp", r"trägt", r"उनकी", r"unter", r"gewollt", r"dessa",
r"zurück", r"unten", r"ένα", r"mille", r"étantes", r"között", r"stavate", r"również", r"gemocht", r"थे", r"mykje", r"थी", r"näiden",
r"की", r"वाले", r"करना", r"ËÁÖÅÔÓÑ", r"szemben", r"ole", r"kunne", r"oli", r"bort", r"teria", r"meer", r"facendo", r"habéis", r"tendríamos",
r"miêdzy", r"seja", r"vårt", r"ici", r"ich", r"consiguen", r"for", r"sota", r"bizi", r"ÂÕÄÅÔ", r"valaki", r"może", r"reagieren", r"muito", r"olisin",
r"uma", r"seríamos", r"tuviese", r"consigues", r"müßt", r"ÜÔÏÊ", r"foi", r"elõ", r"annak", r"unse", r"who", r"इसी", r"encima", r"tutto", r"arbeiten",
r"machte", r"conhecido", r"eivät", r"azután", r"tentaram", r"tomto", r"ÎÅÇÏ", r"houvera", r"innych", r"detta", r"tenidas", r"τις", r"regelmäßig",
r"zur", r"darunter", r"morgige", r"theirs", r"ÐÅÒÅÄ", r"o", r"serás", r"serán", r"algo", r"kellett", r"ÂÙÌÉ", r"niin", r"zum", r"soc", r"aber",
r"sog", r"fuisteis", r"drüber", r"som", r"nÅ", r"erre", r"son", r"üç", r"sou", r"podle", r"stavamo", r"soy", r"varte", r"jene", r"dürfte",
r"novo", r"hallo", r"giu", r"fûmes", r"तो", r"avait", r"habríais", r"nove", r"keille", r"uw", r"avais", r"forsÛke", r"újabb", r"minusta", r"tilbake",
r"éssent", r"gib", r"novy", r"lage", r"jeśli", r"wat", r"was", r"altijd", r"podrias", r"Å", r"einigen", r"uten", r"some", r"fora", r"avions",
r"très", r"leider", r"felé", r"dieselbe", r"alguno", r"när", r"podrian", r"trug", r"verdi", r"hjå", r"woher", r"tengan", r"estiverem", r"nerde", r"×ÓÀ",
r"until", r"gemacht", r"sondern", r"तक", r"azt", r"tengas", r"tenía", r"zeitweise", r"facevano", r"niinä", r"ça", r"zudem", r"cokolwiek", r"ÍÅÖÄÕ",
r"gängig", r"molt", r"efter", r"abbiano", r"habíamos", r"door", r"bedarf", r"ni", r"allzu", r"równie¿", r"no", r"wollten", r"na", r"tivemos", r"fusses",
r"when", r"ne", r"einstmals", r"til", r"ny", r"wolltet", r"zelf", r"tid", r"zuviel", r"hát", r"nu", r"denn", r"×ÁÍ", r"tuyas", r"ÖÅ", r"kva",
r"besteht", r"dies", r"lenni", r"żadnych", r"havemos", r"kvi", r"let's", r"vår", r"×ÁÓ", r"tendremos", r"quindi", r"apontar", r"joita", r"więcej",
r"wolltest", r"bazý", r"którzy", r"gängiges", r"ondan", r"inkje", r"sujet", r"rief", r"ÂÏÌÅÅ", r"sicher", r"oss", r"weshalb", r"muss", r"aurions",
r"sieht", r"selbst", r"sejam", r"¿e", r"primer", r"lungo", r"macht", r"by³o", r"ver", r"ÞÔÏ", r"habidas", r"tust", r"seither", r"þuna", r"quienes",
r"deswegen", r"by³a", r"mache", r"quasi", r"by³y", r"þunu", r"व", r"vem", r"från", r"fuiste", r"befragten", r"ÔÏÔ", r"éramos", r"hattet", r"than",
r"ihnen", r"houveria", r"anfing", r"hatten", r"koennen", r"li", r"befragter", r"trabajais", r"belki", r"din", r"estéis", r"selber", r"did", r"die",
r"warst", r"dig", r"ÍÅÎÑ", r"meille", r"mehrere", r"somos", r"avremo", r"altý", r"suoi", r"prvni", r"dit", r"sogar", r"dir", r"jedenfalls",
r"ville", r"serons", r"hvem", r"cosa", r"ciertas", r"इसे", r"einführen", r"estuvieran", r"anerkannter", r"hver", r"są", r"versorgen", r"दिया", r"trop",
r"facessi", r"ÐÒÉ", r"estou", r"ausserdem", r"szerint", r"sagte", r"joilla", r"joille", r"dem", r"schätzte", r"sareste", r"jährige", r"pouze", r"saresti",
r"serait", r"ÂÙÌÏ", r"nimmt", r"infolge", r"hadn't", r"negli", r"semmi", r"É", r"उस", r"jednak", r"fuer", r"tuviéramos", r"koennt", r"niistä", r"akik",
r"magát", r"legalább", r"molts", r"meillä", r"molto", r"molti", r"bom", r"bon", r"wszystkich", r"deg", r"molta", r"keineswegs", r"keresztül", r"amely",
r"bevor", r"nejsou", r"hade", r"hänellä", r"prawie", r"ÅÍÕ", r"till", r"że", r"lesen", r"estuvieras", r"åt", r"melyek", r"pièce", r"sådana", r"tages",
r"ÎÁÄ", r"ÓÏ×ÓÅÍ", r"pred", r"s±", r"pero", r"abban", r"daneben", r"ये", r"sofern", r"konnte", r"ayante", r"ÐÏÄ", r"steige", r"pres", r"vous",
r"eussent", r"auront", r"aurons", r"ook", r"unsere", r"siate", r"एस", r"unserm", r"sÅ", r"ambdós", r"você", r"abgerufen", r"gestern", r"seremos",
r"só", r"muitos", r"sous", r"dykk", r"clanek", r"schließlich", r"एक", r"olimme", r"så", r"ihre", r"fossem", r"ÞÅÇÏ", r"tengamos", r"noch", r"सभी",
r"nommés", r"hayamos", r"ÍÏÑ", r"onlarýn", r"bastante", r"parce", r"tuviesen", r"होने", r"aquel", r"nyt", r"però", r"korleis", r"machten", r"पे",
r"allerdings", r"anderer", r"mas", r"habremos", r"erhält", r"पर", r"anerkannte", r"trilyon", r"tuyos", r"niemand", r"aos", r"bislang", r"mag",
r"lidt", r"lhes", r"dove", r"mal", r"man", r"sofort", r"häneen", r"poikki", r"deve", r"geb", r"devo", r"avant", r"su", r"saranno", r"το", r"si",
r"so", r"vaya", r"mange", r"sa", r"bardziej", r"se", r"τα", r"contro", r"täksi", r"sejamos", r"eravate", r"×Ï", r"hende", r"immer", r"facesse",
r"około", r"obgleich", r"stia", r"×Ù", r"þeyden", r"आदि", r"ved", r"nimm", r"mais", r"he's", r"habréis", r"meiste", r"estarías", r"अपना", r"gebracht",
r"facciate", r"estuviese", r"estarían", r"dieselben", r"einerseits", r"jiz", r"into", r"nirgendwo", r"twoje", r"non", r"twoja", r"noi", r"beim", r"nog",
r"aquilo", r"keitä", r"अपनी", r"halb", r"अपने", r"hiesige", r"not", r"qu", r"ktoś", r"trabajamos", r"nor", r"nos", r"ÓÅÂÅ", r"bearbeitete", r"avons",
r"olit", r"ketä", r"इसमें", r"desde", r"ÓÅÂÑ", r"anderem", r"sabeis", r"kann", r"el", r"en", r"versorgt", r"reeds", r"ej", r"ennek", r"siihen", r"eg",
r"ÓÅÇÏÄÎÑ", r"ÉÌÉ", r"había", r"versorge", r"houverão", r"et", r"wegen", r"es", r"er", r"außer", r"fôramos", r"stiano", r"tuvieseis", r"meistä",
r"ponad", r"ÅÊ", r"hajam", r"poco", r"fazer", r"bli", r"valeur", r"obwohl", r"ble", r"voltunk", r"außen", r"denselben", r"geteilte", r"tienen", r"punkt",
r"tuviera", r"auch", r"às", r"terão", r"czasem", r"klares", r"vier", r"mimo", r"ora", r"starebbe", r"takze", r"terá", r"eûmes", r"×ÄÒÕÇ",
r"horas", r"siebte", r"wichtig", r"zal", r"tenir", r"noget", r"एवं", r"ona", r"teríamos", r"yourself", r"belül", r"sizi", r"ÇÄÅ", r"völlig",
r"behalten", r"ons", r"tenim", r"ένας", r"nogen", r"ont", r"onu", r"jenem", r"droite", r"jenen", r"anerkannt", r"ÞÅÒÅÚ", r"están", r"ikke", r"estás",
r"setzen", r"nutzen", r"skal", r"ähnlich", r"jenes", r"jener", r"vilka", r"altro", r"más", r"sodaß", r"vort", r"that", r"fordert", r"hun", r"altre",
r"laut", r"padding", r"qual", r"quan", r"bajo", r"kvifor", r"jährig", r"नहीं", r"podeu", r"here's", r"abbiamo", r"konkrete", r"έναν", r"vostre",
r"birkez", r"reagiert", r"pegar", r"över", r"houverá", r"sechs", r"koennten", r"suyos", r"gdzie", r"avevano", r"geht", r"bestehen", r"stessero",
r"and", r"alles", r"aller", r"pro", r"dass", r"pri", r"ani", r"tendrían", r"sull", r"azok", r"ÐÏÔÏÍ", r"hubiéramos", r"ans", r"allen", r"allem",
r"sap", r"miksi", r"their", r"dorther", r"étions", r"bruke", r"facessimo", r"azon", r"nebo", r"zog", r"taka", r"take", r"deux", r"taki",
r"benim", r"sect", r"meglio", r"soweit", r"têm", r"dla", r"falls", r"våra", r"mü", r"dykkar", r"ahogy", r"joksi", r"einseitige", r"medan", r"otuz",
r"alguna", r"derjenigen", r"saremo", r"heistä", r"será", r"alguns", r"solche", r"lenne", r"corrente", r"seria", r"estejam", r"eneste", r"lassen",
r"teilte", r"tínhamos", r"veröffentlicher", r"tém", r"elas", r"veröffentlichen", r"estoy", r"qualquer", r"bereits", r"takie", r"hace", r"überdies",
r"sobie", r"weitere", r"diz", r"setzt", r"siltä", r"bringen", r"daar", r"serais", r"dich", r"éppen", r"jakkolwiek", r"तरह", r"only", r"ε", r"olisi",
r"tuviste", r"hão", r"aufhören", r"info", r"toch", r"gemäss", r"ehhez", r"ÐÏÞÔÉ", r"nell", r"wiêc", r"jó", r"bylo", r"keneltä", r"cannot", r"fragte",
r"dasselbe", r"já", r"ergänze", r"primo", r"sollten", r"abans", r"för", r"leur", r"byli", r"amíg", r"wiewohl", r"through", r"where", r"onlar",
r"nouveaux", r"fueran", r"seas", r"volna", r"up", r"freies", r"freier", r"jos", r"ausgenommen", r"sean", r"estamos", r"vermag", r"hvorfor", r"estad",
r"einst", r"haja", r"καθ", r"και", r"estan", r"estat", r"chaque", r"estar", r"svym", r"utolsó", r"between", r"दो", r"tiverem", r"podczas", r"ÉÚ",
r"ju", r"tuolle", r"tut", r"mellan", r"tästä", r"tego", r"ÉÈ", r"jo", r"ÉÍ", r"ji", r"como", r"fez", r"tuve", r"je", r"κατ", r"come", r"ja",
r"koska", r"ciertos", r"eröffnen", r"darin", r"entweder", r"avuti", r"tämä", r"allo", r"eröffnet", r"tous", r"mand", r"aura", r"schnell", r"quien",
r"avuto", r"gjorde", r"somit", r"mann", r"comprido", r"vuestro", r"tun", r"damals", r"été", r"hva", r"devrait", r"tuo", r"por", r"haceis", r"teillä",
r"tua", r"ante", r"pod", r"seulement", r"siete", r"veröffentlicht", r"oder", r"tue", r"ιαν", r"aufgrund", r"kdyz", r"hepsi", r"viele",
r"aquellos", r"tenían", r"noen", r"zpravy", r"minuun", r"ceux", r"hoch", r"ÏÄÉÎ", r"those", r"houverei", r"myself", r"κατά", r"eit", r"einzig",
r"minulle", r"minulla", r"ergänzte", r"εις", r"vaikka", r"ein", r"ÄÏ", r"estávamos", r"eran", r"eram", r"joka", r"soit", r"legte",
r"eras", r"dört", r"geehrt", r"eri", r"miért", r"par", r"pas", r"yo", r"επι", r"einseitig", r"same", r"hvilke", r"jól", r"otto",
r"möglich", r"tout", r"pak", r"otte", r"todos", r"några", r"teraz", r"eitt", r"zwanzig", r"persone", r"sotto", r"kenenä", r"algunes", r"stareste",
r"entonces", r"tenha", r"defa", r"naszego", r"hebben", r"tenho", r"perché", r"estivessem", r"berichtet", r"noko", r"tivessem", r"veröffentlichten",
r"ausdrückte", r"veröffentlichtes", r"machen", r"can't", r"noka", r"então", r"wessen", r"siitä", r"promesso", r"berichten", r"konkreter", r"konkretes",
r"deze", r"senken", r"×", r"nachdem", r"being", r"gdy", r"keillä", r"bietet", r"verdad", r"át", r"tuviésemos", r"þundan", r"konkreten", r"quella",
r"quelle", r"tim", r"we've", r"quelli", r"quello", r"jona", r"vergangene", r"konnten", r"abgerufenes", r"hacemos", r"którym", r"onder", r"avevamo",
r"ami", r"ditt", r"ama", r"jako", r"amb", r"neuen", r"usamos", r"mot", r"neuer", r"neues", r"moi", r"mon", r"einbaün", r"tanto", r"sådan",
r"वह", r"serez", r"darf", r"już", r"mod", r"aurez", r"acute", r"eussiez", r"साथ", r"ahol", r"soient", r"vergangenes", r"t", r"où", r"stava",
r"ËÁËÁÑ", r"intet", r"avrai", r"debaixo", r"são", r"τη", r"aufzusuchen", r"nå", r"bloß", r"sollen", r"össze", r"houveríamos", r"eddig", r"geehrte",
r"naar", r"blive", r"avevi", r"sommes", r"στις", r"aż", r"solltest", r"besser", r"meinen", r"meinem", r"meiner", r"meines", r"tänä", r"igitt",
r"heihin", r"klare", r"kenen", r"sarebbe", r"továbbá", r"starai", r"seine", r"gestrige", r"fuésemos", r"secondo", r"serão", r"kenet", r"habe",
r"siehe", r"on", r"om", r"här", r"og", r"of", r"od", r"ob", r"erneut", r"neden", r"it's", r"acz", r"ou", r"ÏÐÑÔØ", r"os", r"or", r"op",
r"seriez", r"keinä", r"trabajan", r"irgendeine", r"tallä", r"esses", r"estés", r"απ", r"még", r"näihin", r"tuvisteis", r"darfst",
r"trabajas", r"trabajar", r"het", r"unas", r"welches", r"welcher", r"hep", r"how's", r"her", r"there", r"gesagt", r"tehát", r"los", r"több",
r"ehe", r"heb", r"euer", r"hem", r"hen", r"welchem", r"hei", r"σε", r"welchen", r"tendríais", r"vÅr", r"sabemos", r"eröffne", r"bunda",
r"jednakże", r"gibi", r"geehrter", r"gibt", r"mich", r"último", r"with", r"gängigen", r"vere", r"they're", r"vuestras", r"wasn't", r"õ",
r"agora", r"saját", r"sera", r"voient", r"we'd", r"sve", r"ad", r"af", r"ai", r"hänessä", r"aj", r"am", r"al", r"ao", r"an", r"intenta",
r"ÔÏ", r"tengo", r"saremmo", r"pourquoi", r"uit", r"av", r"az", r"tenga", r"δια", r"intento", r"ÄÒÕÇÏÊ", r"again", r"estão", r"wurde", r"pfui",
r"missä", r"pedig", r"bearbeiteten", r"gleichwohl", r"jsem", r"jses", r"personnes", r"starò", r"eigentlich", r"woraus", r"avessero", r"यदि",
r"στους", r"fazia", r"hubiste", r"etwas", r"möglichen", r"wohlweislich", r"important", r"sullo", r"gängiger", r"sulla", r"nutzung", r"genommen",
r"ιας", r"sulle", r"tenida", r"ella", r"tendréis", r"derzeit", r"unsen", r"mellett", r"unsem", r"ÍÙ", r"oraz", r"eusse", r"sokat", r"u",
r"unser", r"unses", r"mías", r"all", r"ali", r"z.B.", r"ampleamos", r"ale", r"noilta", r"pta", r"andererseits", r"ÎÁËÏÎÅÃ", r"näiltä",
r"siinä", r"noissa", r"ÎÏ", r"fortsetzt", r"als", r"bearbeiten", r"ÎÉ", r"müsste", r"êtes", r"ty", r"ÎÁ", r"tu", r"gegeben", r"ÎÅ", r"to",
r"niiltä", r"lavoro", r"derselbe", r"maioria", r"ti", r"umso", r"kvar", r"te", r"dlatego", r"ÒÁÚ×Å", r"ta", r"faz", r"estando", r"über",
r"estaba", r"very", r"fas", r"wohin", r"sono", r"verdadeiro", r"fan", r"hinein", r"te¿", r"való", r"żadna", r"tuvimos", r"strana", r"yetmiþ",
r"żadne", r"tät", r"sant", r"könnten", r"anar", r"sans", r"sollte", r"sinulla", r"hendes", r"joista", r"sinulle", r"teneis", r"hattest",
r"habré", r"largo", r"étés", r"ecco", r"habrá", r"i'll", r"twoim", r"tej", r"tem", r"ten", r"immerhin", r"facevi", r"faites", r"kunnen",
r"unmöglichen", r"tes", r"teu", r"what", r"sekiz", r"tú", r"hänestä", r"sua", r"mindig", r"suo", r"sul", r"egyes", r"sui", r"letze",
r"sus", r"sur", r"deles", r"aby", r"közül", r"jede", r"vamos", r"stavi", r"neni", r"ÂÕÄÔÏ", r"aquells", r"habiendo", r"einigermaßen", r"seiten",
r"quieto", r"kimi", r"ggf", r"niiksi", r"hast", r"meisten", r"guten", r"hadde", r"kime", r"egész", r"à", r"avessi", r"stavo", r"meist", r"avesse",
r"when's", r"unbedingt", r"ÉÎÏÇÄÁ", r"durch", r"senden", r"seréis", r"hänelle", r"zawsze", r"joihin", r"vid", r"forderte", r"vil", r"την",
r"habíais", r"करते", r"otros", r"hogy", r"nokre", r"fueras", r"hvordan", r"minussa", r"tutti", r"etc", r"wiele", r"jejich", r"trabaja", r"houver",
r"folgende", r"seras", r"darum", r"रखें", r"wielu", r"trabajo", r"more", r"mellom", r"þeyi", r"milyon", r"noina", r"noille", r"hubieras", r"obok",
r"kenestä", r"schlechter", r"potser", r"få", r"ÄÁ", r"według", r"bizden", r"unmögliche", r"noilla", r"tähän", r"करता", r"teistä", r"muze",
r"nerede", r"stiate", r"der", r"des", r"det", r"minut", r"ÜÔÏÔ", r"της", r"dei", r"minhas", r"fomos", r"del", r"fÅ", r"den", r"tuas", r"bunun",
r"teille", r"beitragen", r"wieder", r"sois", r"erhielt", r"iniciar", r"lichten", r"ÎÁÓ", r"seríais", r"też", r"zumal", r"sense", r"mehr", r"उनका",
r"gratulierte", r"avemmo", r"sitt", r"folgender", r"étant", r"können", r"gewissermaßen", r"voltak", r"before", r"vorbei", r"nagyobb", r"jährigen",
r"twym", r"másik", r"fu", r"gängige", r"yedi", r"ÅÅ", r"notre", r"vosaltres", r"fa", r"tuona", r"numa", r"não", r"fi", r"versorgtes", r"teile",
r"ËÁËÏÊ", r"a", r"egy", r"estuviéramos", r"kein", r"उनके", r"abgerufene", r"proto", r"unterhalb", r"ÄÌÑ", r"dentro", r"या", r"milyar", r"ise",
r"यह", r"verrieten", r"ferner", r"mezi", r"लिये", r"that's", r"don't", r"sämtliche", r"consigueix", r"något", r"zugleich", r"hayáis", r"pana",
r"quelles", r"bloss", r"està", r"pani", r"its", r"utána", r"está", r"allg", r"alle", r"alla", r"sopra", r"joissa", r"sinusta", r"teidän",
r"hubiésemos", r"voran", r"allt", r"teidät", r"nimmer", r"hvor", r"nossos", r"ÍÏÖÅÔ", r"haven't", r"musste", r"müssen", r"ya", r"consigo",
r"mitkä", r"ces", r"you've", r"ją", r"tragen", r"dnes", r"abgerufener", r"tausend", r"mussten", r"tuoi", r"solange", r"næsten", r"ilyenkor",
r"tuvieran", r"fünf", r"würden", r"tra", r"denne", r"hvis", r"denna", r"inclòs", r"vannak", r"isn't", r"nein", r"staresti", r"tuosta",
r"stemmo", r"hubieseis", r"nossa", r"möglicher", r"teníamos", r"ugyanis", r"sagtest", r"todo", r"nosso", r"somente", r"einem", r"houvemos",
r"serions", r"nuovo", r"tenido", r"nuovi", r"ÎÕ", r"buono", r"ÔÁÍ", r"jolle", r"seáis", r"they'll", r"jolla", r"estaremos", r"lehet", r"mukaan",
r"dehors", r"gefiel", r"fecero", r"puc", r"verdade", r"hiszen", r"jakiż", r"asi", r"ÎÅÔ", r"enquanto", r"bekannter", r"blieb", r"jähriges",
r"estada", r"kon", r"dabei", r"voor", r"हुए", r"nosotros", r"kimden", r"हुई", r"w", r"estejamos", r"tivesse", r"हुआ", r"jakiś", r"mindenki",
r"hubiera", r"dina", r"onlari", r"nach", r"étants", r"blitt", r"tuvieras", r"tedy", r"cada", r"donc", r"dins", r"étante", r"jer", r"tato", r"kom",
r"seksen", r"teissä", r"nome", r"esas", r"avec", r"przez", r"avez", r"contra", r"przed", r"ÔÏÌØËÏ", r"jeg", r"jed", r"jej", r"jen",
r"dlaczego", r"wenigstens", r"míos", r"para", r"jego", r"teitä", r"part", r"darüber", r"tive", r"schon", r"tak¿e", r"ÐÏÓÌÅ", r"musst", r"aan",
r"dans", r"इस", r"dann", r"sillä", r"dank", r"seamos", r"teve", r"maga", r"birþeyi", r"überall", r"teriam", r"heiltä", r"vielmals", r"schreibens",
r"ebenfalls", r"aussi", r"tiver", r"svych", r"benne", r"oda", r"elõször", r"pomimo", r"eingesetzt", r"için", r"anerkanntes", r"sånn", r"adesso",
r"invece", r"sizden", r"euch", r"faccia", r"später", r"lik", r"also", r"zwölf", r"näinä", r"estuviesen", r"quattro", r"nuestras", r"selv", r"habrán",
r"jakie", r"such", r"tentar", r"ÅÓÌÉ", r"dokuz", r"any", r"tuvo", r"kan", r"kam", r"essa", r"mely", r"esse", r"starteten", r"tivera",
r"bzw", r"ke", r"sehr", r"minha", r"meine", r"ki", r"varför", r"hubieses", r"swoje", r"jeżeli", r"ËÔÏ", r"ansetzen", r"stessimo", r"wieso", r"aztán",
r"fosse", r"haar", r"sobą", r"unseres", r"unserer", r"tener", r"új", r"tinha", r"fino", r"font", r"unseren", r"fôssemos", r"में",
r"quem", r"quel", r"mina", r"INSERmi", r"irgend", r"από", r"στην", r"fareste", r"valami", r"hatte", r"svymi", r"porque", r"minun", r"steste",
r"गया", r"algún", r"esteja", r"×ÏÔ", r"vorgestern", r"timto", r"fûtes", r"étée", r"they'd", r"mío", r"bedurfte", r"stando", r"during", r"hij",
r"podeis", r"him", r"hin", r"direkt", r"ÏÎ", r"fussent", r"ÏÂ", r"vilket", r"schätzten", r"etter", r"vissza", r"ÏÔ", r"koennte", r"seu",
r"सकता", r"ses", r"ser", r"होती", r"forderten", r"fuesen", r"होते", r"seg", r"fueses", r"vorne", r"bare", r"are", r"sea", r"sen", r"llavors",
r"tohle", r"sei", r"einführten", r"fand", r"varje", r"ktery", r"vont", r"irgendwer", r"ktera", r"consecutivo", r"estará", r"consecutivi", r"kteri",
r"dein", r"deim", r"solo", r"soll", r"dalla", r"jeden", r"jedem", r"dalle", r"ebenso", r"ncht", r"solc", r"ÔÒÉ", r"ison", r"estivemos", r"trage",
r"altmýþ", r"hiç", r"jedes", r"jeder", r"iki", r"ligado", r"sols", r"both", r"fortsetzen", r"avesti", r"wouldn't", r"etliche", r"yüz",
r"angesetzter", r"számára", r"होता", r"ÞÕÔØ", r"सकते", r"dadurch", r"aveste", r"cierto", r"samma", r"nter", r"sonstwo", r"était", r"samme",
r"olla", r"pessoas", r"इसका", r"zpet", r"cierta", r"ÞÅÍ", r"avrò", r"estivesse", r"auriez", r"zusammen", r"dizer", r"moins", r"arbeid", r"fummo",
r"estuvisteis", r"न", r"tälle", r"weder", r"due", r"allmählich", r"whom", r"Ë", r"forrige", r"rett", r"txt", r"ollut", r"dus", r"po", r"lett",
r"ÍÎÅ", r"fire", r"what's", r"stattdessen", r"amelyeket", r"gar", r"johon", r"estuvierais", r"heute", r"gab", r"सबसे", r"fut", r"अभी", r"ez",
r"×ÓÅ", r"irgendwo", r"sizin", r"fossimo", r"इसकी", r"fue", r"इसके", r"möchten", r"mögliche", r"fui", r"alatt", r"ketkä", r"vom", r"von",
r"étaient", r"voi", r"hänet", r"itself", r"vor", r"pÅ", r"zieht", r"nós", r"triplo", r"voy", r"beiderlei", r"steht", r"pela", r"acaba",
r"direita", r"fueron", r"på", r"estes", r"atrás", r"utan", r"esteu", r"które", r"ktere", r"keiden", r"warum", r"która", r"tellement", r"zwar",
r"geweest", r"eues", r"eure", r"joilta", r"neuem", r"keine", r"cikkeket", r"który", r"elsõ", r"obschon", r"entre", r"hinterher", r"noiksi",
r"tengáis", r"hago", r"szét", r"estuvieses", r"eles", r"senkten", r"õk", r"y", r"ËÏÇÄÁ", r"kívül", r"konkret", r"incluso", r"eigenen",
r"rechts", r"tych", r"fÛrst", r"kenelle", r"stesso", r"mie", r"freie", r"nuestra", r"mo¿na", r"vielleicht", r"amp", r"eigenes", r"mitä",
r"ÓËÁÚÁÌ", r"nuestro", r"bliver", r"finden", r"mintha", r"vagyis", r"gueno", r"kven", r"danke", r"arriba", r"tus", r"findet", r"acerca",
r"keihin", r"durften", r"cui", r"bin", r"emme", r"wil", r"bij", r"lehetett", r"womit", r"folgenden", r"aviez", r"bé", r"Ï", r"habida",
r"hayas", r"biz", r"żeby", r"jste", r"bir", r"bis", r"habido", r"weiß", r"facciamo", r"d", r"erste", r"houveram", r"jota", r"valamint",
r"een", r"sokkal", r"trabalho", r"sehrwohl", r"mycket", r"folglich", r"wir", r"mögen", r"sue", r"estaríais", r"kenellä", r"hinten", r"bowiem",
r"hinter", r"aquell", r"allgemein", r"bastant", r"ilyen", r"aquele", r"nÅr", r"ourselves", r"aquela", r"beträchtlich", r"minulta", r"terei",
r"néhány", r"peu", r"depuis", r"per", r"poza", r"estivera", r"pelo", r"bêdzie", r"inicio", r"czasami", r"desligado", r"proc", r"ÔÁË", r"be",
r"nello", r"acht", r"nella", r"siendo", r"bo", r"nelle", r"avresti", r"bu", r"mutta", r"weil", r"bezüglich", r"nosotras", r"by", r"υπό",
r"être", r"juste", r"wenngleich", r"bist", r"darauf", r"promeiro", r"daraus", r"ide", r"benden", r"yli", r"inte", r"allora", r"ÐÏÔÏÍÕ",
r"teniendo", r"steigt", r"będą", r"diesseits", r"gewesen", r"kanssa", r"vaan", r"because", r"fordern", r"empleais", r"przecież", r"neki",
r"poden", r"podem", r"degli", r"fossero", r"keneksi", r"ezek", r"avremmo", r"fast", r"häntä", r"suis", r"längstens", r"liegt", r"we're",
r"aufgehört", r"besonders", r"fortsetzten", r"atras", r"poder", r"ca.", r"somme", r"himself", r"estabais", r"derart", r"collapsed",
r"napiste", r"voltam", r"comprare", r"ÞÔÏÂÙ", r"habidos", r"były", r"sowohl", r"å", r"możliwe", r"uz", r"nasýl", r"ut", r"schwierig",
r"bizim", r"hasn't", r"cikk", r"leicht", r"um", r"majd", r"un", r"द्वारा", r"było", r"doc", r"była", r"ud", r"z", r"nogle", r"befragte",
r"titel", r"कुल", r"tiveram", r"seront", r"ÅÇÏ", r"teilten", r"as", r"कुछ", r"mochte", r"nad", r"fossi", r"elles", r"eller", r"demnach",
r"beide", r"wollte", r"nam", r"nas", r"nich", r"hubiesen", r"ellen", r"hanem", r"toho", r"siden", r"temos", r"lediglich", r"eurer", r"eures",
r"olleet", r"consigueixen", r"dall", r"wohl", r"aveva", r"dalt", r"derselben", r"jakichś", r"egyéb", r"euren", r"consigueixes", r"eurem",
r"habrías", r"langsam", r"lang", r"glücklicherweise", r"agl", r"e", r"vice", r"tämän", r"muchos", r"gehen", r"she'd", r"teto", r"she's",
r"tendría", r"nasi", r"having", r"once", r"benutzt", r"noiden", r"oberhalb", r"dannen", r"eûtes", r"ganz", r"sitta", r"essas", r"farò",
r"heutige", r"będzie", r"setzte", r"essai", r"ge", r"unes", r"go", r"επ", r"dagegen", r"katrilyon", r"zogen", r"seni", r"ÜÔÏÍ", r"keneen",
r"lille", r"könnte", r"entsprechend", r"sarei", r"sooft", r"nuestros", r"sieben", r"tyto", r"talán", r"übrigens", r"és", r"wszystkim",
r"tivermos", r"tuto", r"wszystkie", r"maar", r"eso", r"naszych", r"mustn't", r"yourselves", r"sinä", r"möchtest", r"εξ", r"vos", r"εκ",
r"també", r"onlardan", r"tre", r"they've", r"ÐÏ", r"roku", r"általában", r"bleiben", r"niitä", r"ÔÁËÏÊ", r"él", r"starebbero",
r"nuo", r"zo", r"you'd", r"fus", r"gratulieren", r"ze", r"vele", r"direkte", r"za", r"ÓËÁÚÁÌÁ", r"gmbh", r"στη", r"στα", r"någon",
r"zu", r"teljes", r"στο", r"übermorgen", r"folk", r"zapewne", r"sinussa", r"õket", r"eurent", r"unserem", r"deras", r"unmöglich", r"biri",
r"gÅ", r"einige", r"indem", r"wachen", r"tivéramos", r"tened", r"ÎÉÞÅÇÏ", r"cela", r"έσα", r"saps", r"sí", r"lei", r"się", r"quels",
r"mikä", r"kannst", r"étais", r"les", r"tendrá", r"cuando", r"mnie", r"gedurft", r"sind", r"sine", r"bedürfen", r"sina", r"honom",
r"avreste", r"egyre", r"olyan", r"dell", r"tegen", r"gleichzeitig", r"ersten", r"erster", r"nær", r"रहे", r"niets", r"avendo", r"ovat",
r"mentre", r"wie", r"रहा", r"hennes", r"amelynek", r"wij", r"että", r"volt", r"veröffentlichte", r"últim", r"zijn", r"mens", r"meihin",
r"viszont", r"toteż", r"dreißig", r"from", r"che", r"usa", r"uso", r"chi", r"fel", r"fem", r"gern", r"usw", r"few", r"feu", r"fer",
r"kuin", r"kunde", r"fuese", r"mindent", r"siê", r"themselves", r"zij", r"faresti", r"fai", r"fará", r"farà", r"wurden", r"didn't",
r"estarás", r"azért", r"Ñ", r"unterbrach", r"ette", r"jeste", r"olivat", r"startet", r"vuestra", r"farebbe", r"sembrava", r"joiksi",
r"this", r"siksi", r"nekem", r"pour", r"ÇÏ×ÏÒÉÌ", r"jakoś", r"dritte", r"sabe", r"jeho", r"votre", r"faceste", r"noita", r"links",
r"heidät", r"caminho", r"slik", r"tüm", r"beni", r"heidän", r"his", r"zwischen", r"tal", r"tam", r"seriam", r"gefälligst", r"tak",
r"keinerlei", r"usais", r"sobald", r"sit", r"siz", r"syv", r"mein", r"tai", r"gratuliert", r"sia", r"diejenige", r"sig", r"fuéramos",
r"sie", r"cual", r"delas", r"itse", r"sin", r"einig", r"fare", r"hubisteis", r"ËÏÎÅÞÎÏ", r"klar", r"facevate", r"estivéssemos",
r"amelyet", r"ausdrücken", r"frau", r"stesti", r"hän", r"×ÐÒÏÞÅÍ", r"estivéramos", r"hätten", r"tenéis", r"eher", r"ÔÅÍ", r"isso",
r"ÔÙ", r"worin", r"कहा", r"waere", r"primero", r"insofern", r"subito", r"olisitte", r"sido", r"eröffnete", r"azonban", r"comme", r"own",
r"won't", r"ÓÏ", r"da?", r"míg", r"znowu", r"volte", r"le", r"la", r"eue", r"lo", r"byt", r"stanno", r"før", r"demselben", r"aucuns",
r"byl", r"eux", r"eut", r"dess", r"facevo", r"lecz", r"morgen", r"erhielten", r"waren", r"tatsächlichen", r"dal", r"wäre", r"dan", r"danken",
r"dai", r"devem", r"dat", r"doch", r"wolle", r"das", r"qué", r"quê", r"natomiast", r"stette", r"ÂÏÌØÛÅ", r"komme", r"stetti", r"irgendwen",
r"eben", r"tras", r"kommt", r"modo", r"solches", r"riktig", r"bearbeite", r"houveriam", r"ÎÅÊ", r"habría", r"doing", r"mijn", r"ÎÅÅ",
r"tente", r"fortsetzte", r"wszyscy", r"our", r"beginnen", r"tento", r"solchen", r"solchem", r"out", r"böden", r"tuya", r"kiedy", r"là",
r"début", r"tohoto", r"sugl", r"werdet", r"tuyo", r"ÐÒÏ", r"your", r"aquelles", r"mate", r"minkä", r"omdat", r"pan", r"करें", r"bekennen",
r"fuerais", r"faceva", r"heillä", r"eravamo", r"meiltä", r"versorgte", r"näille", r"tempo", r"vergangen", r"formos", r"qua", r"ill.", r"τους",
r"næste", r"que", r"kommen", r"kaum", r"qui", r"fuimos", r"hänen", r"ÂÙ", r"milyen", r"tatsächliches", r"tatsächlicher", r"heraus", r"eens",
r"ono", r"innen", r"deshalb", r"daß", r"fait", r"nachhinein", r"tomuto", r"ihr", r"furono", r"sjøl", r"और", r"powinien",
r"suya", r"damit", r"vergangener", r"illetve", r"ihm", r"był", r"you're", r"ÏÎÉ", r"haya", r"ÚÁÞÅÍ", r"ÏÎÁ", r"estemos", r"wodurch",
r"starete", r"mo¿e", r"which", r"abbia", r"millä", r"aufhörte", r"plupart", r"haver", r"blivit", r"fût", r"bude", r"ÞÅÌÏ×ÅË", r"quando", r"ebbero",
r"jenseits", r"too", r"być", r"brachte", r"algunas", r"herself", r"sicherlich", r"sinuun", r"bei", r"ben", r"weren't", r"bem", r"quanti", r"sok",
r"bez", r"num", r"których", r"azzal", r"encore", r"seinem", r"seinen", r"elég", r"ÎÉËÏÇÄÁ", r"ÔÕÔ", r"heitä", r"eusses", r"blir", r"have",
r"manchmal", r"båe", r"sem", r"seiner", r"seines", r"ista", r"itt", r"mij", r"iste", r"mio", r"min", r"mia", r"ÂÙÔØ", r"fussiez",
r"während", r"mig", r"isto", r"veel", r"को", r"übrig", r"beinahe", r"के", r"mir", r"mit", r"intentais", r"कि", r"का", r"empleo", r"tältä",
r"dette", r"कर", r"senke", r"where's", r"jemand", r"nous", r"sedan", r"sist", r"heissä", r"why", r"senkt", r"ingi", r"veya",
r"cima", r"været", r"nutzt", r"muessen", r"you'll", r"beþ", r"कई", r"ÎÅÌØÚÑ", r"tuotä", r"így", r"gdziekolwiek", r"जैसे", r"muy", r"serías",
r"nagy", r"ismét", r"niet", r"zuerst", r"×ÓÅÈ", r"ÅÓÔØ", r"soyez", r"sådant", r"dunklen", r"moet", r"facevamo", r"should", r"nereye", r"w³a¶nie",
r"there's", r"jak", r"forem", r"noin", r"sonst", r"intentamos", r"ayez", r"begonnen", r"podia", r"joina", r"sinun", r"deine", r"sinua",
r"avete", r"ËÕÄÁ", r"autor", r"wordt", r"Ó", r"tilstand", r"etwa", r"nacher", r"sinut", r"aquí", r"viel", r"keiner", r"keines", r"ganzem",
r"estaré", r"ganzen", r"seitdem", r"στον", r"gbr", r"she", r"ganzes", r"ganzer", r"keinem", r"keinen", r"muj", r"×ÓÅÇÏ", r"währenddessen",
r"aquelas", r"stets", r"gefallen", r"seite", r"vært", r"direkten", r"podriamos", r"soprattutto", r"neben", r"unnötig", r"være", r"schreibe",
r"ert", r"ponieważ", r"miei", r"wird", r"statt", r"seht", r"ÓÁÍ", r"setzten", r"hiermit", r"facciano", r"berichtete", r"below", r"tene",
r"tuvieron", r"lub", r"sehe", r"byla", r"nossas", r"stato", r"lui", r"ús", r"stati", r"wollen", r"houveremos", r"olisivat", r"moim",
r"kei", r"faremo", r"tema", r"trabalhar", r"einfach", r"dallo", r"tat", r"tentei", r"ende", r"kez", r"avuta", r"avute", r"ets", r"aqui",
r"ett", r"soeben", r"è", r"से", r"conseguim", r"conseguir", r"también", r"doksan", r"houvessem", r"möchte", r"s", r"suchen",
r"ci", r"co", r"author", r"sangen", r"bsp.", r"ca", r"sitä", r"étées", r"ce", r"trzeba", r"cz", r"ÄÁÖÅ", r"ab", r"heeft", r"muesst",
r"ÜÔÕ", r"havde", r"durfte", r"niihin", r"estaría", r"aczkolwiek", r"tiene", r"he'd", r"själv", r"geehrten", r"zehn", r"jsou", r"sara",
r"farai", r"keiksi", r"faccio", r"estén", r"vors", r"tenidos", r"estava", r"igen", r"natürlich", r"houverem", r"allerlei", r"estáis",
r"alsbald", r"queste", r"aquellas", r"c", r"meus", r"siamo", r"sollst", r"uns", r"derartig", r"ours", r"jobban", r"könn", r"ott",
r"estuvimos", r"facessero", r"indessen", r"hubo", r"tivéssemos", r"é", r"umas", r"hube", r"will", r"estiver", r"keiltä", r"au",
r"vilkas", r"leer", r"ill", r"at", r"kuka", r"va", r"questo", r"ve", r"letztens", r"vi", r"ÈÏÔØ", r"längst", r"ziehen", r"nincs",
r"vu", r"nützt", r"angesetzten", r"mesmo", r"sowie", r"þunda", r"parte", r"δι", r"sarà", r"ÞÔÏÂ", r"ju¿", r"vore", r"noe", r"ergänzten",
r"fleste", r"nächste", r"nada", r"une", r"tiempo", r"näillä", r"bald", r"näistä", r"tuohon", r"essendo", r"pelos", r"estado",
r"starà", r"sugli", r"soviel", r"i", r"serían", r"ÕÖ", r"avevate", r"these", r"estuviésemos", r"oft", r"mÅte", r"senza", r"terzo",
r"uno", r"bekannte", r"onde", r"emplean", r"lesz", r"ÒÁÚ", r"empleas", r"emplear", r"avrà", r"nosaltres", r"meissä", r"jeji", r"कहते",
r"weg", r"przede", r"çok", r"estaríamos", r"vÖre", r"getan", r"wen", r"wem", r"wel", r"gleich", r"wer", r"habríamos", r"werd", r"protoze",
r"vÖrt", r"houvermos", r"gute", r"ought", r"sich", r"mis", r"amit", r"haut", r"लिए", r"þu", r"jonka", r"został", r"ayant", r"niillä",
r"þeyler", r"you", r"houvesse", r"estarán", r"budes", r"kilka", r"hubimos", r"like", r"×ÓÅÇÄÁ", r"dennoch", r"spielen", r"cikkek",
r"olitte", r"mía", r"vermutlich", r"sagen", r"jedoch", r"teremos", r"unterbrechen", r"hossen", r"budem", r"why's", r"kdo", r"irgendwie",
r"mihin", r"schätzen", r"otras", r"kde", r"vosotros", r"ktokolwiek", r"lagen", r"jotka", r"shan't", r"csak", r"est", r"zwei", r"dagl",
r"fanno", r"kleinen", r"doen", r"stieg", r"kleines", r"kleiner", r"bardzo", r"does", r"esa", r"między", r"olin", r"sette", r"estic",
r"erais", r"teihin", r"puedo", r"usan", r"hajamos", r"ÅÝÅ", r"mitt", r"fordi", r"puede", r"agli", r"ayons", r"oben", r"hoss", r"usas",
r"usar", r"niille", r"after", r"vosotras", r"about", r"também", r"anden", r"estábamos", r"daran", r"lhe", r"ander", r"hier", r"andet",
r"irgendwas", r"em", r"wszystko", r"eins", r"ÎÁÄÏ", r"solcher", r"mert", r"ön", r"teus", r"più", r"donde", r"eine", r"ÔÏÍ", r"ei",
r"mere", r"wollt", r"jine", r"है", r"हो", r"vai", r"van", r"vam", r"ही", r"vad", r"quante", r"eussions", r"quanta", r"ed", r"var", r"vas",
r"quanto", r"når", r"hvornår", r"Ó×ÏÀ", r"faranno", r"persze", r"i've", r"haette", r"andernfalls", r"but", r"kenessä", r"ho", r"farete", r"ha",
r"ultimo", r"geblieben", r"he", r"ezért", r"davor", r"néha", r"seit", r"dagli", r"houvéramos", r"znów", r"j", r"ÕÖÅ", r"zufolge", r"wär", r"olemme",
r"seid", r"sein", r"davon", r"hvad", r"braucht", r"stesse", r"povo", r"vuestros", r"amelyek", r"innerhalb", r"ins", r"vostri", r"inn", r"überallhin",
r"jag", r"vostro", r"ind", r"vostra", r"dalsi", r"mye", r"piu", r"aies", r"arra", r"tendrán", r"deires", r"siano", r"other", r"seus", r"disse",
r"prave", r"sarete", r"tenhamos", r"også", r"után", r"fois", r"hubieron", r"quarto", r"ese", r"müßte", r"há", r"stai", r"reagierte", r"ÎÉÈ",
r"trotzdem", r"ogsÅ", r"ÎÉÍ", r"pode", r"außerdem", r"estuviste", r"Ä×Á", r"allein", r"mistä"])
 

def filterstopwords(words:str)->str:
    words = _str(words)
    if words:
        try:
            return ' '.join([k for k in words.split(' ') if k!='' and k[0].lower()+k[1:] not in _stopwords])
        except:
            return ''
    else:
        return None

# U25.	Stem: Stems the input text using Porter2 stemming algorithm.
# Constants of stem's porter2, at module level because porter2's nested helpers call each other and Codon rejects a
# nested function calling a sibling that captures a local of the enclosing function. The original, nested version lives
# in streamudfbench_original_stem.py.
r_exp = re.compile(r"[^aeiouy]*[aeiouy]+[^aeiouy](\w*)")
ewss_exp1 = re.compile(r"^[aeiouy][^aeiouy]$")
ewss_exp2 = re.compile(r".*[^aeiouy][aeiouy][^aeiouywxY]$")
ccy_exp = re.compile(r"([aeiouy])y")
s1a_exp = re.compile(r"[aeiouy].")
s1b_exp = re.compile(r"[aeiouy]")

doubles = ('bb', 'dd', 'ff', 'gg', 'mm', 'nn', 'pp', 'rr', 'tt')

s1b_suffixes = ('ed', 'edly', 'ing', 'ingly')

# Typed, because Codon types a bare [] as a list of None.
_no_prev: list[str] = []
s2_triples = (('ization', 'ize', _no_prev),
            ('ational', 'ate', _no_prev),
            ('fulness', 'ful', _no_prev),
            ('ousness', 'ous', _no_prev),
            ('iveness', 'ive', _no_prev),
            ('tional', 'tion', _no_prev),
            ('biliti', 'ble', _no_prev),
            ('lessli', 'less', _no_prev),
            ('entli', 'ent', _no_prev),
            ('ation', 'ate', _no_prev),
            ('alism', 'al', _no_prev),
            ('aliti', 'al', _no_prev),
            ('ousli', 'ous', _no_prev),
            ('iviti', 'ive', _no_prev),
            ('fulli', 'ful', _no_prev),
            ('enci', 'ence', _no_prev),
            ('anci', 'ance', _no_prev),
            ('abli', 'able', _no_prev),
            ('izer', 'ize', _no_prev),
            ('ator', 'ate', _no_prev),
            ('alli', 'al', _no_prev),
            ('bli', 'ble', _no_prev),
            ('ogi', 'og', ['l']),
            ('li', '', ['c', 'd', 'e', 'g', 'h', 'k', 'm', 'n', 'r', 't']))

s3_triples = (('ational', 'ate', False),
            ('tional', 'tion', False),
            ('alize', 'al', False),
            ('icate', 'ic', False),
            ('iciti', 'ic', False),
            ('ative', '', True),
            ('ical', 'ic', False),
            ('ness', '', False),
            ('ful', '', False))

s4_delete_list = ('al', 'ance', 'ence', 'er', 'ic', 'able', 'ible', 'ant', 'ement',
                'ment', 'ent', 'ism', 'ate', 'iti', 'ous', 'ive', 'ize')


def stem(input:str)->str:
    input = _str(input)
    
    def porter2(word):
        # Copyright (c) 2008 Michael Dirolf (mike at dirolf dot com)
    
        # Permission is hereby granted, free of charge, to any person
        # obtaining a copy of this software and associated documentation
        # files (the "Software"), to deal in the Software without
        # restriction, including without limitation the rights to use,
        # copy, modify, merge, publish, distribute, sublicense, and/or sell
        # copies of the Software, and to permit persons to whom the
        # Software is furnished to do so, subject to the following
        # conditions:
        
        # The above copyright notice and this permission notice shall be
        # included in all copies or substantial portions of the Software.
        
        # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
        # EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
        # OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
        # NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
        # HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
        # WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
        # FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
        # OTHER DEALINGS IN THE SOFTWARE.
        
        """An implementation of the Porter2 stemming algorithm.
        See http://snowball.tartarus.org/algorithms/english/stemmer.html

        Adapted from pyporter2 by Michael Dirolf.

        This algorithm is more correct but (at least in this implementation)
        several times slower than the original porter algorithm as implemented
        in whoosh.lang.porter.
        """

        

        def get_r1(word):
            # exceptional forms
            if word.startswith('gener') or word.startswith('arsen'):
                return 5
            if word.startswith('commun'):
                return 6
        
            # normal form
            match = r_exp.match(word)
            if match:
                return match.start(1)
            return len(word)
        
        def get_r2(word):
            match = r_exp.match(word, get_r1(word))
            if match:
                return match.start(1)
            return len(word)
        
        def ends_with_short_syllable(word):
            if len(word) == 2:
                if ewss_exp1.match(word):
                    return True
            if ewss_exp2.match(word):
                return True
            return False
        
        def is_short_word(word):
            if ends_with_short_syllable(word):
                if get_r1(word) == len(word):
                    return True
            return False
        
        def remove_initial_apostrophe(word):
            if word.startswith("'"):
                return word[1:]
            return word
        
        def capitalize_consonant_ys(word):
            if word.startswith('y'):
                word = 'Y' + word[1:]
            return ccy_exp.sub(r'\g<1>Y', word)
        
        def step_0(word):
            if word.endswith("'s'"):
                return word[:-3]
            if word.endswith("'s"):
                return word[:-2]
            if word.endswith("'"):
                return word[:-1]
            return word
        
        def step_1a(word):
            if word.endswith('sses'):
                return word[:-4] + 'ss'
            if word.endswith('ied') or word.endswith('ies'):
                if len(word) > 4:
                    return word[:-3] + 'i'
                else:
                    return word[:-3] + 'ie'
            if word.endswith('us') or word.endswith('ss'):
                return word
            if word.endswith('s'):
                preceding = word[:-1]
                if s1a_exp.search(preceding):
                    return preceding
                return word
            return word

        def ends_with_double(word):
            for double in doubles:
                if word.endswith(double):
                    return True
            return False
        def step_1b_helper(word):
            if word.endswith('at') or word.endswith('bl') or word.endswith('iz'):
                return word + 'e'
            if ends_with_double(word):
                return word[:-1]
            if is_short_word(word):
                return word + 'e'
            return word

        def step_1b(word, r1):
            if word.endswith('eedly'):
                if len(word) - 5 >= r1:
                    return word[:-3]
                return word
            if word.endswith('eed'):
                if len(word) - 3 >= r1:
                    return word[:-1]
                return word
        
            for suffix in s1b_suffixes:
                if word.endswith(suffix):
                    preceding = word[:-len(suffix)]
                    if s1b_exp.search(preceding):
                        return step_1b_helper(preceding)
                    return word
        
            return word
        
        def step_1c(word):
            if word.endswith('y') or word.endswith('Y'):
                if word[-2] not in 'aeiouy':
                    if len(word) > 2:
                        return word[:-1] + 'i'
            return word

        def step_2_helper(word, r1, end, repl, prev):
                if word.endswith(end):
                    if len(word) - len(end) >= r1:
                        if prev == []:
                            return word[:-len(end)] + repl
                        for p in prev:
                            if word[:-len(end)].endswith(p):
                                return word[:-len(end)] + repl
                    return word
                return ''  # Codon: None would conflict with the str returned above; callers only test truthiness.

        def step_2(word, r1):
            for trip in s2_triples:
                attempt = step_2_helper(word, r1, trip[0], trip[1], trip[2])
                if attempt:
                    return attempt
            return word

        def step_3_helper(word, r1, r2, end, repl, r2_necessary):
            if word.endswith(end):
                if len(word) - len(end) >= r1:
                    if not r2_necessary:
                        return word[:-len(end)] + repl
                    else:
                        if len(word) - len(end) >= r2:
                            return word[:-len(end)] + repl
                return word
            return ''  # Codon: None would conflict with the str returned above; callers only test truthiness.
        def step_3(word, r1, r2):
            for trip in s3_triples:
                attempt = step_3_helper(word, r1, r2, trip[0], trip[1], trip[2])
                if attempt:
                    return attempt
            return word


        def step_4(word, r2):
            for end in s4_delete_list:
                if word.endswith(end):
                    if len(word) - len(end) >= r2:
                        return word[:-len(end)]
                    return word
        
            if word.endswith('sion') or word.endswith('tion'):
                if len(word) - 3 >= r2:
                    return word[:-3]
        
            return word
        
        def step_5(word, r1, r2):
            if word.endswith('l'):
                if len(word) - 1 >= r2 and word[-2] == 'l':
                    return word[:-1]
                return word
        
            if word.endswith('e'):
                if len(word) - 1 >= r2:
                    return word[:-1]
                if len(word) - 1 >= r1 and not ends_with_short_syllable(word[:-1]):
                    return word[:-1]
        
            return word
        
        def normalize_ys(word):
            return word.replace('Y', 'y')
        
        exceptional_forms = {'skis': 'ski',
                            'skies': 'sky',
                            'dying': 'die',
                            'lying': 'lie',
                            'tying': 'tie',
                            'idly': 'idl',
                            'gently': 'gentl',
                            'ugly': 'ugli',
                            'early': 'earli',
                            'only': 'onli',
                            'singly': 'singl',
                            'sky': 'sky',
                            'news': 'news',
                            'howe': 'howe',
                            'atlas': 'atlas',
                            'cosmos': 'cosmos',
                            'bias': 'bias',
                            'andes': 'andes'}
        
        exceptional_early_exit_post_1a = frozenset(['inning', 'outing', 'canning', 'herring',
                                                    'earring', 'proceed', 'exceed', 'succeed'])
        

        def stem(word):
            if len(word) <= 2:
                return word
            word = remove_initial_apostrophe(word)

            # handle some exceptional forms
            if word in exceptional_forms:
                return exceptional_forms[word]

            word = capitalize_consonant_ys(word)
            r1 = get_r1(word)
            r2 = get_r2(word)
            word = step_0(word)
            word = step_1a(word)

            # handle some more exceptional forms
            if word in exceptional_early_exit_post_1a:
                return word

            word = step_1b(word, r1)
            word = step_1c(word)
            word = step_2(word, r1)
            word = step_3(word, r1, r2)
            word = step_4(word, r2)
            word = step_5(word, r1, r2)
            word = normalize_ys(word)

            return word
        return stem(word)

    if input:
        try:
            list_string = input.split()

            return ' '.join([porter2(x) for x in list_string])
        except:
            return ''
    else:
        return None

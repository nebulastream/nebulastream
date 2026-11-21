# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""UDFBench filterstopwords for LPython. Changes: LPython types, no `self`, no try/except, the list comprehension and
str.join (neither implemented) as a loop, `k[0].lower()` (a call on a subscript is not implemented) through a variable,
None is "NULL". The stopwords are those of the original, in the same order. They are one string that the module splits
into the set when it starts: set() of a list is not implemented, and `lpython --fast` needs time that grows about with the
square of the elements of a set literal (30 s for 400 elements, the 3881 stopwords would need about 45 minutes). The set is
a dict with the stopwords as keys: in LPython 0.22.0, `key in s` for a set[str] crashes (SIGSEGV) for some keys that are
not in the set, for example "x" in {".", "the", "and", "a"}."""
from lpython import ccallable, i32

_words: str = (
    ". _ stessi można einseitiger wären fÛr olette würde andre για ebben negl steigen einseitigen dere niissä "
    "ergänzen otro noista dazu περί tobie erst mußt szinte otra drauf gänzlich aus under aux gesehen shouldn't "
    "dela neun dele berichteten neue voie ayantes letztes vase staremo algunos letzten tandis tinham senin "
    "olisimme we'll ÔÏÇÏ mog± haben zde geworden zda upp teníais fuera esos zuletzt abbiate zonder hors fece "
    "angefangen varit ezt verrate n ellas alt avrebbero nel nem ÍÏÖÎÏ tipo nei emilyen ned klaren while ebbe tipy "
    "kun estuvo nez sobre farebbero estuve ekkor fall neu nes dahi med meg powinni powinno daha derjenige wezen "
    "men flere here meu met nicht avere mes mer hers ungefähr amolyan schätzt sidan même gli auf ÓÅÊÞÁÓ anstatt "
    "näiksi aki i'd ueber där i'm aurai kýrk müssten esteve arról desselben także magst कोई outro जो bra sarai "
    "tels estive would heille einiger einiges fussions loro seraient vermögen cali niiden einigem ellos उसके "
    "sembra właśnie által willst Á sabeu musi wenig saber sabes किया tendrías herein wohingegen facemmo nigdy ÔÏÖÅ "
    "ια sabem saben quale me ×ÅÄØ olet solament ma जा weiteres weiterer mi maiorias ने mu ÍÏÊ weiterem era ÂÙÌÁ "
    "weiteren olen której जब ero my avaient wieviel czy étiez jest tuvieses czemu consigue who's gegen wann tuoksi "
    "tuvierais auraient geben ambos wirklich want ना muesste hundert újra buna anderes hoe end ene weiterhin czyli "
    "przy woraufhin off enn algmas startete how ens bunu hos wenige stille anderen doesn't jsme mÅ facesti vocês "
    "ÈÏÒÏÛÏ sanki lag hubierais mest वर्ग kimś Ö lat sería lav mogą las senkte gdyż je¶li mí estem cinque hasta "
    "getragen letztlich reagiere mý sont stettero vart बहुत josta vars aient over zei teniu peut hubiese nostra "
    "vara nostre ezzel nostri वे avevo vite nostro úgy begge gente bisher bundan übel hon vilken rá valor teilen "
    "minden fin dorthin fim then them sinulta keissä deinem deinen plötzlich deines deiner bana they mucho pelas "
    "wenn mindestens ausser nawet hubieran worden dinge l estaréis até Õ ÌÕÞÛÅ each ενός ihn akkor verriet kterou "
    "diye darüberhinaus mają बाद estivermos nokon navn autre mikor zahlreich deira findest nötigenfalls aurais "
    "aurait deverá behielt begann hätte sang god diesem diesen slutt re dieser dieses suyas werde avoir geteilt "
    "frei nirgends verdadero icke verdadera consegueixo ÚÁ więc doppelt elle brauchen weniger inom hennar ÓËÁÚÁÔØ "
    "einführte legen intentan bár çünkü versorgten birkaç estas dahin ist att eramos bräuchte solch gehabt podemos "
    "staranno intentas intentar einer eines serei avrebbe näitä minä esté ÂÅÚ näissä tuon einen für hogyan ÔÏÇÄÁ "
    "unmöglicher drunter peggio τον är perquè ingen tot estuvieseis brachten quinto toi ton solamente tom tuossa "
    "doppio ganze beitrugen seré llarg beides poderá estavam nahm beiden του ależ verte både vielen sille teiltä "
    "dürfen minua houve verraten kto hemos ancora sta vieles vieler sekä sposób yirmi este esta jederlei nehmen "
    "durchaus esto mine vagyok korso jeres már further eröffnetes vorüber häneltä habrás mint künftig niçin birþey "
    "fosti degl foste alors altri deres derer ÌÉ deren derem feci tym eût oltre inzwischen dafür tylko ohne erano "
    "meidät doit ÂÙÌ état meidän dois ihres sarò nämlich γι fusse avranno ahhoz hanno seks cała m dog noihin cały "
    "mithin dos dov yani singen tuolta tatsächlich hur muß estais sage liest ebbi én nélkül sagt somt och wirst "
    "egyetlen dessen henne höchstens danach sarebbero sto किसी daher estadas sien sehen irá avrete nebenan do "
    "meget gdzieś above klein di de stor da geen vsak du einmal rispetto ikkje furent außerhalb stiamo anfangen "
    "των nämä rund nichts tenías estados iets gjÛre starei blev iż dort ÚÄÅÓØ skulle tenham nur inny nun gefällt "
    "inne faig machst inna schreiben iemand amikor blei inni eres staremmo houvéssemos weiter bekannt wogegen bien "
    "estiveram ÔÅÐÅÒØ erhalten आप eren którego erem habrían we powinna welche legyen diese wo soyons were anche "
    "sagten legten hätt stiegen niż möglicherweise ezen direkter ÖÉÚÎØ drei ile questa clanku mancherorts ËÁË "
    "estuviera clanky coz questi against genug una kommer und ils då coi vorher com col con habían foram vaig "
    "tovább elf jakaś durante niye befragen ni¿ vais ÜÔÉ ele habías podria fra aren't हैं tendrás tuolla been "
    "estaban estabas tinc toen tenemos joiden jolta amelyekben jossa nagyon tendré i¿ बनी pueden zich मे olisit "
    "hvilken stavano suas mivel könnt letztendlich avessimo keistä aqueles auras þey ait avrei ayants podriais "
    "egyik dello miltä serai ÎÉÂÕÄØ vagy aie delle della faremmo unos pokud unwichtig indietro tässä is ir it "
    "desto estos ik im il io in dedans if quand meitä manchem manchen manches mancher quant nokor tätä makt än "
    "angesetze elli keressünk manche gekonnt nim nic coś jetzt nie angesetzt elõtt sollt hayan kim ells kvarhelst "
    "ÍÎÏÇÏ depois he'll hacer haces ihrem singt ihren tienes algun ÔÅÂÑ gekommen suyo drin enhver thi twój "
    "folgendes the ihrer hacen yours jeszcze andere she'll denen anderm andern करने mientras था fueseis anderr "
    "anders veja eus farei hai antes ausdrückt ham han kell vollständig hab ela had estuvieron conseguimos hay "
    "niemals ÜÔÏÇÏ els har has hat zou opp trägt उनकी unter gewollt dessa zurück unten ένα mille étantes között "
    "stavate również gemocht थे mykje थी näiden की वाले करना ËÁÖÅÔÓÑ szemben ole kunne oli bort teria meer facendo "
    "habéis tendríamos miêdzy seja vårt ici ich consiguen for sota bizi ÂÕÄÅÔ valaki może reagieren muito olisin "
    "uma seríamos tuviese consigues müßt ÜÔÏÊ foi elõ annak unse who इसी encima tutto arbeiten machte conhecido "
    "eivät azután tentaram tomto ÎÅÇÏ houvera innych detta tenidas τις regelmäßig zur darunter morgige theirs "
    "ÐÅÒÅÄ o serás serán algo kellett ÂÙÌÉ niin zum soc aber sog fuisteis drüber som nÅ erre son üç sou podle "
    "stavamo soy varte jene dürfte novo hallo giu fûmes तो avait habríais nove keille uw avais forsÛke újabb "
    "minusta tilbake éssent gib novy lage jeśli wat was altijd podrias Å einigen uten some fora avions très leider "
    "felé dieselbe alguno när podrian trug verdi hjå woher tengan estiverem nerde ×ÓÀ until gemacht sondern तक azt "
    "tengas tenía zeitweise facevano niinä ça zudem cokolwiek ÍÅÖÄÕ gängig molt efter abbiano habíamos door bedarf "
    "ni allzu równie¿ no wollten na tivemos fusses when ne einstmals til ny wolltet zelf tid zuviel hát nu denn "
    "×ÁÍ tuyas ÖÅ kva besteht dies lenni żadnych havemos kvi let's vår ×ÁÓ tendremos quindi apontar joita więcej "
    "wolltest bazý którzy gängiges ondan inkje sujet rief ÂÏÌÅÅ sicher oss weshalb muss aurions sieht selbst sejam "
    "¿e primer lungo macht by³o ver ÞÔÏ habidas tust seither þuna quienes deswegen by³a mache quasi by³y þunu व "
    "vem från fuiste befragten ÔÏÔ éramos hattet than ihnen houveria anfing hatten koennen li befragter trabajais "
    "belki din estéis selber did die warst dig ÍÅÎÑ meille mehrere somos avremo altý suoi prvni dit sogar dir "
    "jedenfalls ville serons hvem cosa ciertas इसे einführen estuvieran anerkannter hver są versorgen दिया trop "
    "facessi ÐÒÉ estou ausserdem szerint sagte joilla joille dem schätzte sareste jährige pouze saresti serait "
    "ÂÙÌÏ nimmt infolge hadn't negli semmi É उस jednak fuer tuviéramos koennt niistä akik magát legalább molts "
    "meillä molto molti bom bon wszystkich deg molta keineswegs keresztül amely bevor nejsou hade hänellä prawie "
    "ÅÍÕ till że lesen estuvieras åt melyek pièce sådana tages ÎÁÄ ÓÏ×ÓÅÍ pred s± pero abban daneben ये sofern "
    "konnte ayante ÐÏÄ steige pres vous eussent auront aurons ook unsere siate एस unserm sÅ ambdós você abgerufen "
    "gestern seremos só muitos sous dykk clanek schließlich एक olimme så ihre fossem ÞÅÇÏ tengamos noch सभी nommés "
    "hayamos ÍÏÑ onlarýn bastante parce tuviesen होने aquel nyt però korleis machten पे allerdings anderer mas "
    "habremos erhält पर anerkannte trilyon tuyos niemand aos bislang mag lidt lhes dove mal man sofort häneen "
    "poikki deve geb devo avant su saranno το si so vaya mange sa bardziej se τα contro täksi sejamos eravate ×Ï "
    "hende immer facesse około obgleich stia ×Ù þeyden आदि ved nimm mais he's habréis meiste estarías अपना "
    "gebracht facciate estuviese estarían dieselben einerseits jiz into nirgendwo twoje non twoja noi beim nog "
    "aquilo keitä अपनी halb अपने hiesige not qu ktoś trabajamos nor nos ÓÅÂÅ bearbeitete avons olit ketä इसमें "
    "desde ÓÅÂÑ anderem sabeis kann el en versorgt reeds ej ennek siihen eg ÓÅÇÏÄÎÑ ÉÌÉ había versorge houverão et "
    "wegen es er außer fôramos stiano tuvieseis meistä ponad ÅÊ hajam poco fazer bli valeur obwohl ble voltunk "
    "außen denselben geteilte tienen punkt tuviera auch às terão czasem klares vier mimo ora starebbe takze terá "
    "eûmes ×ÄÒÕÇ horas siebte wichtig zal tenir noget एवं ona teríamos yourself belül sizi ÇÄÅ völlig behalten ons "
    "tenim ένας nogen ont onu jenem droite jenen anerkannt ÞÅÒÅÚ están ikke estás setzen nutzen skal ähnlich jenes "
    "jener vilka altro más sodaß vort that fordert hun altre laut padding qual quan bajo kvifor jährig नहीं podeu "
    "here's abbiamo konkrete έναν vostre birkez reagiert pegar över houverá sechs koennten suyos gdzie avevano "
    "geht bestehen stessero and alles aller pro dass pri ani tendrían sull azok ÐÏÔÏÍ hubiéramos ans allen allem "
    "sap miksi their dorther étions bruke facessimo azon nebo zog taka take deux taki benim sect meglio soweit têm "
    "dla falls våra mü dykkar ahogy joksi einseitige medan otuz alguna derjenigen saremo heistä será alguns solche "
    "lenne corrente seria estejam eneste lassen teilte tínhamos veröffentlicher tém elas veröffentlichen estoy "
    "qualquer bereits takie hace überdies sobie weitere diz setzt siltä bringen daar serais dich éppen jakkolwiek "
    "तरह only ε olisi tuviste hão aufhören info toch gemäss ehhez ÐÏÞÔÉ nell wiêc jó bylo keneltä cannot fragte "
    "dasselbe já ergänze primo sollten abans för leur byli amíg wiewohl through where onlar nouveaux fueran seas "
    "volna up freies freier jos ausgenommen sean estamos vermag hvorfor estad einst haja καθ και estan estat "
    "chaque estar svym utolsó between दो tiverem podczas ÉÚ ju tuolle tut mellan tästä tego ÉÈ jo ÉÍ ji como fez "
    "tuve je κατ come ja koska ciertos eröffnen darin entweder avuti tämä allo eröffnet tous mand aura schnell "
    "quien avuto gjorde somit mann comprido vuestro tun damals été hva devrait tuo por haceis teillä tua ante pod "
    "seulement siete veröffentlicht oder tue ιαν aufgrund kdyz hepsi viele aquellos tenían noen zpravy minuun ceux "
    "hoch ÏÄÉÎ those houverei myself κατά eit einzig minulle minulla ergänzte εις vaikka ein ÄÏ estávamos eran "
    "eram joka soit legte eras dört geehrt eri miért par pas yo επι einseitig same hvilke jól otto möglich tout "
    "pak otte todos några teraz eitt zwanzig persone sotto kenenä algunes stareste entonces tenha defa naszego "
    "hebben tenho perché estivessem berichtet noko tivessem veröffentlichten ausdrückte veröffentlichtes machen "
    "can't noka então wessen siitä promesso berichten konkreter konkretes deze senken × nachdem being gdy keillä "
    "bietet verdad át tuviésemos þundan konkreten quella quelle tim we've quelli quello jona vergangene konnten "
    "abgerufenes hacemos którym onder avevamo ami ditt ama jako amb neuen usamos mot neuer neues moi mon einbaün "
    "tanto sådan वह serez darf już mod aurez acute eussiez साथ ahol soient vergangenes t où stava ËÁËÁÑ intet "
    "avrai debaixo são τη aufzusuchen nå bloß sollen össze houveríamos eddig geehrte naar blive avevi sommes στις "
    "aż solltest besser meinen meinem meiner meines tänä igitt heihin klare kenen sarebbe továbbá starai seine "
    "gestrige fuésemos secondo serão kenet habe siehe on om här og of od ob erneut neden it's acz ou ÏÐÑÔØ os or "
    "op seriez keinä trabajan irgendeine tallä esses estés απ még näihin tuvisteis darfst trabajas trabajar het "
    "unas welches welcher hep how's her there gesagt tehát los több ehe heb euer hem hen welchem hei σε welchen "
    "tendríais vÅr sabemos eröffne bunda jednakże gibi geehrter gibt mich último with gängigen vere they're "
    "vuestras wasn't õ agora saját sera voient we'd sve ad af ai hänessä aj am al ao an intenta ÔÏ tengo saremmo "
    "pourquoi uit av az tenga δια intento ÄÒÕÇÏÊ again estão wurde pfui missä pedig bearbeiteten gleichwohl jsem "
    "jses personnes starò eigentlich woraus avessero यदि στους fazia hubiste etwas möglichen wohlweislich "
    "important sullo gängiger sulla nutzung genommen ιας sulle tenida ella tendréis derzeit unsen mellett unsem ÍÙ "
    "oraz eusse sokat u unser unses mías all ali z.B. ampleamos ale noilta pta andererseits ÎÁËÏÎÅÃ näiltä siinä "
    "noissa ÎÏ fortsetzt als bearbeiten ÎÉ müsste êtes ty ÎÁ tu gegeben ÎÅ to niiltä lavoro derselbe maioria ti "
    "umso kvar te dlatego ÒÁÚ×Å ta faz estando über estaba very fas wohin sono verdadeiro fan hinein te¿ való "
    "żadna tuvimos strana yetmiþ żadne tät sant könnten anar sans sollte sinulla hendes joista sinulle teneis "
    "hattest habré largo étés ecco habrá i'll twoim tej tem ten immerhin facevi faites kunnen unmöglichen tes teu "
    "what sekiz tú hänestä sua mindig suo sul egyes sui letze sus sur deles aby közül jede vamos stavi neni ÂÕÄÔÏ "
    "aquells habiendo einigermaßen seiten quieto kimi ggf niiksi hast meisten guten hadde kime egész à avessi "
    "stavo meist avesse when's unbedingt ÉÎÏÇÄÁ durch senden seréis hänelle zawsze joihin vid forderte vil την "
    "habíais करते otros hogy nokre fueras hvordan minussa tutti etc wiele jejich trabaja houver folgende seras "
    "darum रखें wielu trabajo more mellom þeyi milyon noina noille hubieras obok kenestä schlechter potser få ÄÁ "
    "według bizden unmögliche noilla tähän करता teistä muze nerede stiate der des det minut ÜÔÏÔ της dei minhas "
    "fomos del fÅ den tuas bunun teille beitragen wieder sois erhielt iniciar lichten ÎÁÓ seríais też zumal sense "
    "mehr उनका gratulierte avemmo sitt folgender étant können gewissermaßen voltak before vorbei nagyobb jährigen "
    "twym másik fu gängige yedi ÅÅ notre vosaltres fa tuona numa não fi versorgtes teile ËÁËÏÊ a egy estuviéramos "
    "kein उनके abgerufene proto unterhalb ÄÌÑ dentro या milyar ise यह verrieten ferner mezi लिये that's don't "
    "sämtliche consigueix något zugleich hayáis pana quelles bloss està pani its utána está allg alle alla sopra "
    "joissa sinusta teidän hubiésemos voran allt teidät nimmer hvor nossos ÍÏÖÅÔ haven't musste müssen ya consigo "
    "mitkä ces you've ją tragen dnes abgerufener tausend mussten tuoi solange næsten ilyenkor tuvieran fünf würden "
    "tra denne hvis denna inclòs vannak isn't nein staresti tuosta stemmo hubieseis nossa möglicher teníamos "
    "ugyanis sagtest todo nosso somente einem houvemos serions nuovo tenido nuovi ÎÕ buono ÔÁÍ jolle seáis they'll "
    "jolla estaremos lehet mukaan dehors gefiel fecero puc verdade hiszen jakiż asi ÎÅÔ enquanto bekannter blieb "
    "jähriges estada kon dabei voor हुए nosotros kimden हुई w estejamos tivesse हुआ jakiś mindenki hubiera dina "
    "onlari nach étants blitt tuvieras tedy cada donc dins étante jer tato kom seksen teissä nome esas avec przez "
    "avez contra przed ÔÏÌØËÏ jeg jed jej jen dlaczego wenigstens míos para jego teitä part darüber tive schon "
    "tak¿e ÐÏÓÌÅ musst aan dans इस dann sillä dank seamos teve maga birþeyi überall teriam heiltä vielmals "
    "schreibens ebenfalls aussi tiver svych benne oda elõször pomimo eingesetzt için anerkanntes sånn adesso "
    "invece sizden euch faccia später lik also zwölf näinä estuviesen quattro nuestras selv habrán jakie such "
    "tentar ÅÓÌÉ dokuz any tuvo kan kam essa mely esse starteten tivera bzw ke sehr minha meine ki varför hubieses "
    "swoje jeżeli ËÔÏ ansetzen stessimo wieso aztán fosse haar sobą unseres unserer tener új tinha fino font "
    "unseren fôssemos में quem quel mina INSERmi irgend από στην fareste valami hatte svymi porque minun steste "
    "गया algún esteja ×ÏÔ vorgestern timto fûtes étée they'd mío bedurfte stando during hij podeis him hin direkt "
    "ÏÎ fussent ÏÂ vilket schätzten etter vissza ÏÔ koennte seu सकता ses ser होती forderten fuesen होते seg fueses "
    "vorne bare are sea sen llavors tohle sei einführten fand varje ktery vont irgendwer ktera consecutivo estará "
    "consecutivi kteri dein deim solo soll dalla jeden jedem dalle ebenso ncht solc ÔÒÉ ison estivemos trage "
    "altmýþ hiç jedes jeder iki ligado sols both fortsetzen avesti wouldn't etliche yüz angesetzter számára होता "
    "ÞÕÔØ सकते dadurch aveste cierto samma nter sonstwo était samme olla pessoas इसका zpet cierta ÞÅÍ avrò "
    "estivesse auriez zusammen dizer moins arbeid fummo estuvisteis न tälle weder due allmählich whom Ë forrige "
    "rett txt ollut dus po lett ÍÎÅ fire what's stattdessen amelyeket gar johon estuvierais heute gab सबसे fut अभी "
    "ez ×ÓÅ irgendwo sizin fossimo इसकी fue इसके möchten mögliche fui alatt ketkä vom von étaient voi hänet itself "
    "vor pÅ zieht nós triplo voy beiderlei steht pela acaba direita fueron på estes atrás utan esteu które ktere "
    "keiden warum która tellement zwar geweest eues eure joilta neuem keine cikkeket który elsõ obschon entre "
    "hinterher noiksi tengáis hago szét estuvieses eles senkten õk y ËÏÇÄÁ kívül konkret incluso eigenen rechts "
    "tych fÛrst kenelle stesso mie freie nuestra mo¿na vielleicht amp eigenes mitä ÓËÁÚÁÌ nuestro bliver finden "
    "mintha vagyis gueno kven danke arriba tus findet acerca keihin durften cui bin emme wil bij lehetett womit "
    "folgenden aviez bé Ï habida hayas biz żeby jste bir bis habido weiß facciamo d erste houveram jota valamint "
    "een sokkal trabalho sehrwohl mycket folglich wir mögen sue estaríais kenellä hinten bowiem hinter aquell "
    "allgemein bastant ilyen aquele nÅr ourselves aquela beträchtlich minulta terei néhány peu depuis per poza "
    "estivera pelo bêdzie inicio czasami desligado proc ÔÁË be nello acht nella siendo bo nelle avresti bu mutta "
    "weil bezüglich nosotras by υπό être juste wenngleich bist darauf promeiro daraus ide benden yli inte allora "
    "ÐÏÔÏÍÕ teniendo steigt będą diesseits gewesen kanssa vaan because fordern empleais przecież neki poden podem "
    "degli fossero keneksi ezek avremmo fast häntä suis längstens liegt we're aufgehört besonders fortsetzten "
    "atras poder ca. somme himself estabais derart collapsed napiste voltam comprare ÞÔÏÂÙ habidos były sowohl å "
    "możliwe uz nasýl ut schwierig bizim hasn't cikk leicht um majd un द्वारा było doc była ud z nogle befragte "
    "titel कुल tiveram seront ÅÇÏ teilten as कुछ mochte nad fossi elles eller demnach beide wollte nam nas nich "
    "hubiesen ellen hanem toho siden temos lediglich eurer eures olleet consigueixen dall wohl aveva dalt "
    "derselben jakichś egyéb euren consigueixes eurem habrías langsam lang glücklicherweise agl e vice tämän "
    "muchos gehen she'd teto she's tendría nasi having once benutzt noiden oberhalb dannen eûtes ganz sitta essas "
    "farò heutige będzie setzte essai ge unes go επ dagegen katrilyon zogen seni ÜÔÏÍ keneen lille könnte "
    "entsprechend sarei sooft nuestros sieben tyto talán übrigens és wszystkim tivermos tuto wszystkie maar eso "
    "naszych mustn't yourselves sinä möchtest εξ vos εκ també onlardan tre they've ÐÏ roku általában bleiben niitä "
    "ÔÁËÏÊ él starebbero nuo zo you'd fus gratulieren ze vele direkte za ÓËÁÚÁÌÁ gmbh στη στα någon zu teljes στο "
    "übermorgen folk zapewne sinussa õket eurent unserem deras unmöglich biri gÅ einige indem wachen tivéramos "
    "tened ÎÉÞÅÇÏ cela έσα saps sí lei się quels mikä kannst étais les tendrá cuando mnie gedurft sind sine "
    "bedürfen sina honom avreste egyre olyan dell tegen gleichzeitig ersten erster nær रहे niets avendo ovat "
    "mentre wie रहा hennes amelynek wij että volt veröffentlichte últim zijn mens meihin viszont toteż dreißig "
    "from che usa uso chi fel fem gern usw few feu fer kuin kunde fuese mindent siê themselves zij faresti fai "
    "fará farà wurden didn't estarás azért Ñ unterbrach ette jeste olivat startet vuestra farebbe sembrava joiksi "
    "this siksi nekem pour ÇÏ×ÏÒÉÌ jakoś dritte sabe jeho votre faceste noita links heidät caminho slik tüm beni "
    "heidän his zwischen tal tam seriam gefälligst tak keinerlei usais sobald sit siz syv mein tai gratuliert sia "
    "diejenige sig fuéramos sie cual delas itse sin einig fare hubisteis ËÏÎÅÞÎÏ klar facevate estivéssemos "
    "amelyet ausdrücken frau stesti hän ×ÐÒÏÞÅÍ estivéramos hätten tenéis eher ÔÅÍ isso ÔÙ worin कहा waere primero "
    "insofern subito olisitte sido eröffnete azonban comme own won't ÓÏ da? míg znowu volte le la eue lo byt "
    "stanno før demselben aucuns byl eux eut dess facevo lecz morgen erhielten waren tatsächlichen dal wäre dan "
    "danken dai devem dat doch wolle das qué quê natomiast stette ÂÏÌØÛÅ komme stetti irgendwen eben tras kommt "
    "modo solches riktig bearbeite houveriam ÎÅÊ habría doing mijn ÎÅÅ tente fortsetzte wszyscy our beginnen tento "
    "solchen solchem out böden tuya kiedy là début tohoto sugl werdet tuyo ÐÒÏ your aquelles mate minkä omdat pan "
    "करें bekennen fuerais faceva heillä eravamo meiltä versorgte näille tempo vergangen formos qua ill. τους "
    "næste que kommen kaum qui fuimos hänen ÂÙ milyen tatsächliches tatsächlicher heraus eens ono innen deshalb "
    "daß fait nachhinein tomuto ihr furono sjøl और powinien suya damit vergangener illetve ihm był you're ÏÎÉ haya "
    "ÚÁÞÅÍ ÏÎÁ estemos wodurch starete mo¿e which abbia millä aufhörte plupart haver blivit fût bude ÞÅÌÏ×ÅË "
    "quando ebbero jenseits too być brachte algunas herself sicherlich sinuun bei ben weren't bem quanti sok bez "
    "num których azzal encore seinem seinen elég ÎÉËÏÇÄÁ ÔÕÔ heitä eusses blir have manchmal båe sem seiner seines "
    "ista itt mij iste mio min mia ÂÙÔØ fussiez während mig isto veel को übrig beinahe के mir mit intentais कि का "
    "empleo tältä dette कर senke where's jemand nous sedan sist heissä why senkt ingi veya cima været nutzt "
    "muessen you'll beþ कई ÎÅÌØÚÑ tuotä így gdziekolwiek जैसे muy serías nagy ismét niet zuerst ×ÓÅÈ ÅÓÔØ soyez "
    "sådant dunklen moet facevamo should nereye w³a¶nie there's jak forem noin sonst intentamos ayez begonnen "
    "podia joina sinun deine sinua avete ËÕÄÁ autor wordt Ó tilstand etwa nacher sinut aquí viel keiner keines "
    "ganzem estaré ganzen seitdem στον gbr she ganzes ganzer keinem keinen muj ×ÓÅÇÏ währenddessen aquelas stets "
    "gefallen seite vært direkten podriamos soprattutto neben unnötig være schreibe ert ponieważ miei wird statt "
    "seht ÓÁÍ setzten hiermit facciano berichtete below tene tuvieron lub sehe byla nossas stato lui ús stati "
    "wollen houveremos olisivat moim kei faremo tema trabalhar einfach dallo tat tentei ende kez avuta avute ets "
    "aqui ett soeben è से conseguim conseguir también doksan houvessem möchte s suchen ci co author sangen bsp. ca "
    "sitä étées ce trzeba cz ÄÁÖÅ ab heeft muesst ÜÔÕ havde durfte niihin estaría aczkolwiek tiene he'd själv "
    "geehrten zehn jsou sara farai keiksi faccio estén vors tenidos estava igen natürlich houverem allerlei estáis "
    "alsbald queste aquellas c meus siamo sollst uns derartig ours jobban könn ott estuvimos facessero indessen "
    "hubo tivéssemos é umas hube will estiver keiltä au vilkas leer ill at kuka va questo ve letztens vi ÈÏÔØ "
    "längst ziehen nincs vu nützt angesetzten mesmo sowie þunda parte δι sarà ÞÔÏÂ ju¿ vore noe ergänzten fleste "
    "nächste nada une tiempo näillä bald näistä tuohon essendo pelos estado starà sugli soviel i serían ÕÖ avevate "
    "these estuviésemos oft mÅte senza terzo uno bekannte onde emplean lesz ÒÁÚ empleas emplear avrà nosaltres "
    "meissä jeji कहते weg przede çok estaríamos vÖre getan wen wem wel gleich wer habríamos werd protoze vÖrt "
    "houvermos gute ought sich mis amit haut लिए þu jonka został ayant niillä þeyler you houvesse estarán budes "
    "kilka hubimos like ×ÓÅÇÄÁ dennoch spielen cikkek olitte mía vermutlich sagen jedoch teremos unterbrechen "
    "hossen budem why's kdo irgendwie mihin schätzen otras kde vosotros ktokolwiek lagen jotka shan't csak est "
    "zwei dagl fanno kleinen doen stieg kleines kleiner bardzo does esa między olin sette estic erais teihin puedo "
    "usan hajamos ÅÝÅ mitt fordi puede agli ayons oben hoss usas usar niille after vosotras about também anden "
    "estábamos daran lhe ander hier andet irgendwas em wszystko eins ÎÁÄÏ solcher mert ön teus più donde eine ÔÏÍ "
    "ei mere wollt jine है हो vai van vam ही vad quante eussions quanta ed var vas quanto når hvornår Ó×ÏÀ faranno "
    "persze i've haette andernfalls but kenessä ho farete ha ultimo geblieben he ezért davor néha seit dagli "
    "houvéramos znów j ÕÖÅ zufolge wär olemme seid sein davon hvad braucht stesse povo vuestros amelyek innerhalb "
    "ins vostri inn überallhin jag vostro ind vostra dalsi mye piu aies arra tendrán deires siano other seus disse "
    "prave sarete tenhamos også után fois hubieron quarto ese müßte há stai reagierte ÎÉÈ trotzdem ogsÅ ÎÉÍ pode "
    "außerdem estuviste Ä×Á allein mistä "
)
_stopwords: dict[str, i32] = {".": 1}  # the first stopword, _fill adds the others


def _fill() -> None:
    parts: list[str] = _words.split(" ")
    i: i32
    for i in range(len(parts)):
        if parts[i] != "":
            _stopwords[parts[i]] = 1


_fill()


def filterstopwords(words: str) -> str:
    if words != "":
        parts: list[str] = words.split(" ")
        result: str = ""
        first: bool = True
        i: i32
        k: str
        head: str
        for i in range(len(parts)):
            k = parts[i]
            if k != "":
                head = k[0]
                if not ((head.lower() + k[1:]) in _stopwords):
                    if not first:
                        result += " "
                    result += k
                    first = False
        return result
    else:
        return "NULL"


@ccallable
def udf_entry(words: str) -> str:
    copy: str = words  # LPython miscompiles a @ccallable function that passes its str argument on
    return filterstopwords(copy)

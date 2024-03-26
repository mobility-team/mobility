# Présentation de Mobility
## Concepts de base
Mobility est un outil qui permet d'obtenir des échantillons de **déplacements** pour une population d'**usagers** d'un **territoire** d'étude. Ces déplacements peuvent être effectués pour différents **motifs**, avec différents **modes de transport**, dans le cadre de **voyages** ou d'une **mobilité quotidienne**.

Les données de base utilisées sont issues d'**enquêtes**, puis modifiées à l'aide de **modèles de mobilité** qui permettent d'estimer les probabilités motif - origine - destination - mode des déplacements en fonction des caractéristiques des territoires étudiés.

## Génération d'échantillons de déplacements pour un usager
### Caractérisation des usagers
Dans Mobility, l'usager est caractérisé par plusieurs informations, collectées dans le cadre des enquêtes déplacement nationales et locales :
- Catégorie socioprofessionnelle.
- Catégorie professionnelle du ménage.
- Catégorie d'unité urbaine de son lieu de résidence principale.
- Taille du ménage.
- Nombre de voitures du ménage.

Le croisement de ces informations donne des types d'usager, comme par exemple : "retraité vivant seul dans une ville centre, sans voiture", ou encore "enfant vivant dans un ménage de cadres, en banlieue, avec deux voitures".

### Méthode d'échantillonnage des déplacements
Pour chacun de ces types d'usagers, Mobility utilise les données détaillées des enquêtes nationales sur les déplacements des personnes (ENDT 2008 ou EMP 2018/2019) pour échantillonner un ensemble de déplacements pendant une ou plusieurs années.

La stratégie d'échantillonnage consiste à distinguer plusieurs types de déplacements personnels et professionnels : 
- Les déplacements effectués pour des voyages, pour aller à la destination et revenir au point d'origine.
- Les déplacements effectués au sein d'un voyage, une fois à destination.
- Les déplacements quotidiens, en distinguant mobilité de semaine et du week-end.

La méthode commence par fixer un nombre de voyages sur la période étudiée, selon la catégorie socioprofessionnelle de l'usager. On échantillonne ensuite ce nombre de voyages, ce qui permet de calculer un nombre de jours passés en voyage, et d'obtenir un premier ensemble de déplacements pour aller à la destination et revenir au point d'origine .

Au sein de chaque voyage, la méthode échantillonne ensuite chaque jour que dure le voyage des journées de déplacements, composée de chaînes de déplacements effectués pour différents motifs par l'usager (faire des courses, se rendre dans un lieu de loisir...). Si le voyage a un motif professionnel, on échantillonne uniquement des journées de semaine. Si le voyage a un motif personnel, on échantillonne uniquement des journées de week-end. On reflète ainsi les différences de motifs de déplacements au lieu de destination entre voyages professionnels et personnels, même si cela reste une approximation.

La méthode calcule ensuite un nombre de jours de semaine et de week-end hors voyages, pour lesquels l'usager peut être immobile ou mobile (au moins un déplacement dans la journée). Le nombre de jours d'immobilité dépend de la catégorie socio-professionnelle de l'usager. Pour les jours où l'usager est mobile, la méthode échantillonne enfin un ensemble de journées de déplacements de semaine et de week-end. 

On s'assure de la cohérence quotidienne des chaines de déplacement en faisant ce tirage au niveau de la journée, plutôt que directement au niveau du déplacement : le fait que l'usager part de chez lui le matin et revient chez lui le soir la plupart des jours est par exemple bien représenté.

### Résultats de l'échantillonnage
On obtient donc à ce stade un ensemble de déplacements sur une ou plusieurs années pour un type d'usager donné, avec les informations suivantes pour chaque déplacement :
- Motif du déplacement.
- Motif du déplacement précédent.
- Mode de transport.
- Distance parcourue.
- Nombre de co-passagers.

## Génération d'échantillons de déplacements pour un territoire
### Echantillonnage d'une population locale, puis des déplacements de cette population
Si l'on dispose d'un échantillon représentatif de la population d'un territoire, basé par exemple sur les données INSEE du recensement, on peut donc obtenir un échantillon représentatif de la mobilité quotidienne et longue distance de la population de ce territoire. Il est possible de faire ce calcul pour un diagnostic ou un travail prospectif, si l'on construit un échantillon représentatif de la population future, et qu'on fait l'hypothèse d'une stabilité dans le temps des comportements de mobilité par type d'usager.

Il est alors possible d'agréger les résultats pour calculer des distances parcourues par mode, par motif, par portée de déplacement, ou des émissions de gaz à effet de serre. Il est également possible d'étudier des scénarios en faisant des hypothèses a priori sur l'évolution des distances parcourues, des parts modales, du taux de covoiturage, ou encore de la progression du télétravail.

## Adaptation de l'échantillon de déplacements aux caractéristiques urbaines et de transport du territoire
### Limites de l'échantillonnage des données d'enquête
Les résultats obtenus sont incertains, avec une méthode qui s'appuie sur des enquêtes forcément incertaines et datées. L'échantillonnage des déplacements donne une vision statistiquement correcte pour une population mais individuellement très peu probable pour certains types de trajet : un actif voit par exemple sa distance domicile-travail et le mode de transport utilisé pour ce motif varier potentiellement tous les jours. 

Nous faisons cependant l'hypothèse que nous pouvons réduire cette incertitude, pour les déplacements quotidiens, en ajoutant une seconde étape au modèle, de territorialisation.

A ce stade, seule l'information sur la catégorie d'unité urbaine du lieu de résidence de l'usager introduit des différences en fonction des territoires étudiés. Deux retraités vivant seuls et sans voiture auront des comportements de mobilité différents s'ils habitent en ville ou dans une commune rurale. Par contre, ils auront les mêmes comportements s'ils habitent à Paris ou à Lyon (pas exactement, mais statistiquement, puisque l'on va échantillonner les mêmes sous ensembles des enquêtes déplacements).

### Méthode d'adaptation locale
Pour améliorer ce point, nous introduisons dans le modèle plusieurs informations spécifiques au territoire étudié :
- Equilibres entre offres et demandes de plusieurs "services", correspondant aux différents motifs de déplacements des usagers : actifs et emplois, clients et commerces, patients et offre de soin...
- Coût du transport entre zones du territoires, par mode de transport (les zones pouvant désigner des communes, des IRIS, ou n'importe quel type de zones segmentant le territoire en polygones contigus).

Ces données nous permettent de mettre en place un modèle de choix des destinations en fonction des origines et des modes de transport pour effectuer les trajets correspondants. Par exemple, sachant que j'habite dans le 7ème arrondissement de Lyon, quelle est la probabilité que j'occupe un emploi dans le 3ème arrondissement ?

### Application des modèles de mobilité
Plusieurs familles de modèles sont utilisables pour estimer ces probabilités, dont les modèles de radiation, que Mobility utilise à ce stade. Dans ce type de modèle, on fait l'hypothèse que les usagers classent les opportunités en fonction de leur coût d'accès (la distance dans le modèle originel). La probabilité de chaque destination dépend ensuite du nombre d'opportunités disponible dans la zone de départ, dans la zone de destination, et dans toutes les zones intermédiaires ayant un coût d'accès inférieur à celui de la destination.

Mobility implémente une version itérative de ce modèle de radiation, qui permet de refléter la concurrence d'accès au mêmes services par une population, et de respecter la répartition offre - demande spécifiée initialement.

Une fois la probabilité de choisir une destination selon une origine est établie, on établit la probabilité d'utiliser tel ou tel mode à l'aide d'un modèle à choix discret, prenant en compte les coûts de transport de chaque mode.

On peut alors préciser certains des déplacements des usagers, en fonction de leurs motifs :
- On fixe l'origine ou la destination des déplacements en lien avec le domicile à un point de la zone de résidence de l'usager (le centroide par exemple).
- On fixe l'origine ou la destination des déplacements en lien avec le travail à un point de la zone de lieu de travail de l'usager, ainsi que le mode de transport utilisé pour le trajet. Pour cela, on échantillonne une destination et un mode à partir des probabilités déterminées précédemment. On fait l'hypothèse que le lieu de travail et le mode de transport utilisés sont ne changent pas au sein de la période étudiée, pour un usagé donné.
- On tire des origines et des destinations possibles pour les autres motifs de déplacement, en fonction des probabilités et des origines destinations determinées précédemment. Par exemple, en partant du travail, que l'on sait désormais localiser, l'usager va faire ses courses à différents endroits, et potentiellement avec différents modes, en fonction des résultats du modèle de radiation. On ne fait cette fois pas l'hypothèse que les lieux d'achats, de loisirs ou de soins sont fixes pour un usager donné, et on tire un lieu et un mode par déplacement.

Une fois les origines - destinations - modes de certains déplacements ainsi modifiées, il faut alors remplacer les distances issus de l'enquête échantillonnée par les distances "réelles" parcourues entre les origines et destinations maintenant localisées. Il faut également remplacer le nombre de co-passagers en fonction des modes et des motifs des déplacements issus de l'enquête : un trajet domicile - travail en transports en commun a pu être remplacé par un trajet en voiture ou en vélo, qui n'a bien sûr pas le même nombre de co-passagers.

### Résultats de l'adaptation
Cette seconde étape de modélisation permet alors d'étudier directement l'effet de modifications du système de mobilité territorial :
- Demande de transport : évolution de la répartition de la population ou des services sur le territoire (emplois, commerces, lieux de loisir...), évolution de l'attractivité des services (salaires plus importants par exemple).
- Offre de transport : vitesses moyennes des véhicules, infrastructures, lignes de transports en commun. A ce stade, la congestion n'est pas prise en compte, il n'est donc pas possible d'étudier une augmentation des capacités des infrastructures ou de la fréquence des TC.

Toutes ces modifications vont en effet faire varier les données d'entrée des modèles de radiation, donc les probabilités motif - origine - destination - mode, et donc les distances parcourues ainsi que les modes de transport utilisés.

## Limites de Mobility
L'approche de Mobility a plusieurs limites :
- Utilisation de données d'enquêtes nationales datées.
- Pas de possibilité d'adapter le nombre et les motifs de déplacements au territoire, alors qu'il existe un lien entre accessibilité et fréquence des déplacements.
- Pas de prise en compte de l'effet rebond éventuel d'une diminution des temps de trajet, alors que le temps gagné a tendance à être réinvesti dans des déplacements plus longs ou plus fréquents.

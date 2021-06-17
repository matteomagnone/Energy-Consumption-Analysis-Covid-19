
rm(list = ls())
library(RODBC)
library(data.table)
library(lubridate)
library(cluster)
library(fpc)
library(data.table)

#Stringhe di connessione
SQLconn_dsn <- "2G_Analysis"
SQLconn_user <- "xxxxx"
SQLconn_pwd <- "xxxxx"

#Connessione al database
conn <-
  odbcConnect(dsn = SQLconn_dsn, 
              uid = SQLconn_user, 
              pwd = SQLconn_pwd)

#Lettura dei dati anagrafici dei punti di prelievo 2G
Anagrafica_2G <-
  sqlQuery(
    conn,
    paste0(
      "select cSpillingPointKey, cPod, cConsumptionClass
        from dbo.Anagrafica_2G"
    )
  )

#Inserimento dei dati anagrafici in una struttra data.table
Anagrafica_2G <- data.table(Anagrafica_2G)

#Conteggio dei punti di prelievo presenti in anagrafica
nSpk <- nrow(Anagrafica_2G)

#Creazione step per ciclo di salvataggio delle misure di consumo su file
step <- c(1, 
          which(1:nrow(Anagrafica_2G) %% 4000 == 0),
          nSpk)

yearStep <- c(1,'20190101','20200101')

for (n in 1:(length(yearStep) - 1))
{
  #Ciclo di salvataggio misure di consumo su file
  for (i in 1:(length(step) - 1))
  {
    
    startDate <- format(as.Date(yearStep[2], format = "%Y%m%d"), "%Y%m%d")
    endDate <- format(as.Date(yearStep[2], format = "%Y%m%d") %m+% years(1), "%Y%m%d") 
    
    #Estrazione dei valori di consumo
    Misure_2G <-
      sqlQuery(
        conn,
        paste0(
          "select spd.cPod, convert(varchar(19), dVal, 120) as dVal, eValue
            from dbo.Anagrafica_2G spd
            inner join dbo.SpillingPoints sp
              on sp.cPod = spd.cPod
            inner join dbo.EA ea
              on ea.cSpillingPointID = sp.cSpillingPointID
            where dVal >= ",startDate,"
            and dVal < ",endDate,"
            and cSpillingPointKey >= ",
          step[i],
          "and cSpillingPointKey < ",
          step[i + 1]
        )
      )
    
    #Inserimento dei dati di misura di consumo in una struttura data.table
    Misure_2G <- data.table(Misure_2G)
    
    #Eliminazione dei valori NA eventualmente presenti dall'input del database
    Misure_2G <- Misure_2G[!is.na(eValue)]
    
    #Conversione della data nel formato YYYY-mm-dd H24:MM:SS
    Misure_2G[, dVal := as.POSIXct(dVal,
                                   tz = "GMT", 
                                   format = "%Y-%m-%d %H:%M:%S")]
    
    #Merge dei dati anagrafica e di misura per punto di prelievo
    Misure_2G <- merge(Misure_2G, 
                       Anagrafica_2G, 
                       by = "cPod")
    
    #Calcolo della media di ogni punto di prelievo per tutto lo
    #storico disponibile
    Misure_2G[, 
              AVG := mean(eValue), 
              by = list(cPod, cConsumptionClass)]
    
    #Gestione outlier in base al consumo annuo e alla media
    Misure_2G[eValue > 1 &
                (eValue > 10 * cConsumptionClass / (365 * 24) |
                   eValue > AVG * 20),
              Out := 1]
    Misure_2G[eValue < 0.01 * AVG,
              Out := 1]
    
    #Sovrascrittura del data table con solo i dati non contrassegnati
    #come outlier
    Misure_2G <- Misure_2G[is.na(Out)]
    
    #Inserimento nuova colonna estraendo l'anno dalla data dei valori di misura
    Misure_2G[, 
              Year := format(dVal, format = "%Y")]
    
    #Inserimento nuova colonna estraendo il mese dalla data dei valori di misura
    Misure_2G[,
              Month := format(dVal, format = "%m")]
    
    #Inserimento nuova colonna estraendo il giorno dalla data dei valori
    #di misura
    Misure_2G[, 
              Day := format(dVal, format = "%d")]
    
    #Inserimento nuova colonna concatenando l'anno e il mese
    Misure_2G[, 
              ym := paste0(Year, Month)]
    
    #Inserimento nuova colonna con il conteggio dei numeri dei mesi dei valori
    #di misura per ogni punto di prelievo
    Misure_2G[,
              cntYM := length(unique(ym)),
              by = list(cPod)]
    
    #Viene contrassegnato ogni punto di prelievo con uno storico minore
    #di dodici mesi
    Misure_2G[cntYM < 12, 
              outHist := 1]
    
    #Sovrascrittura del data table con solo i punti di prelievo aventi
    #dodici mesi
    Misure_2G <- Misure_2G[is.na(outHist)]
    
    #Sovrascrittura del data table con solo le colonne interessate per
    #il calcolo dei profili
    Misure_2G <-
      Misure_2G[,
                .(cPod, cConsumptionClass, dVal, eValue, Year, Month, Day)]
    
    #Calcolo della cumulata giornaliera per ogni punto di prelievo
    PodProfileDaily <-
      Misure_2G[,
                list(eValue = sum(eValue)),
                by = list(cPod, Year, Month, Day)]
    
    #Pulizia del data table non più utilizzato
    rm(Misure_2G)
    gc()
    
    #Calcolo della mediana della cumulata giornaliera per ogni punto
    #di prelievo e mese
    PodProfileMonthly <-
      PodProfileDaily[,
                      median(eValue),
                      by = list(cPod, Year, Month)]
    
    #Nomenclatura della nuova colonna
    setnames(PodProfileMonthly,
             4,
             c("MonthlyMedian"))
    
    #Calcolo della cumulata della mediana mensile per ogni punto di prelievo
    #e anno
    PodProfileYearly <-
      PodProfileMonthly[,
                        sum(MonthlyMedian),
                        by = list(cPod, Year)]
    
    #Nomenclatura della nuova colonna
    setnames(PodProfileYearly,
             3,
             c("YearlyMedian"))
    
    #Merge dei del dato mensile e annuale per punto di prelievo e anno
    PodProfile <-
      merge(PodProfileMonthly,
            PodProfileYearly, by
            = c("cPod", "Year"))
    
    #Calcolo prodifilo per ogni punto di prelievo, mese e anno
    PodProfile <-
      PodProfile[, 
                 Profile := (MonthlyMedian / YearlyMedian), 
                 by = list(cPod, Year, Month)]
    
    #Eliminazione dei valori NA eventualmente presenti dopo il calcolo
    #del profilo
    PodProfile <- PodProfile[!is.na(Profile)]
    
    #Sovrascrittura del data frame con solo le colonne interessate per
    #la cluster analysis
    PodProfile <- PodProfile[,
                             .(cPod, Month, Profile)]
    
    #Estrazione anno di competenza
    yearCompetence <- format(as.Date(yearStep[n], format = "%Y%m%d"), format = "%Y")
    
    #Creazione nome del file dei profili
    fileName <- paste("C:/2G_Analysis/Profili_2G/Profili_2G_",yearCompetence, sep = "")
    
    fwrite(
      x = PodProfile,
      file = paste0(fileName, i , ".csv"),
      sep = ";",
      dec = ".",
      row.names = FALSE
    )
    
  }
  
}


#Pulizia ambiente per la cluster analysis
rm(list = ls())
library(cluster)
library(fpc)
library(NbClust)

#Recupero lista di file dei profili
csv.list <- list.files("C:/2G_Analysis/Profili_2G/")

#Lettura dei file
lst <-
  lapply(file.path("C:/2G_Analysis/Profili_2G/", csv.list),
         fread)
Profile_ClusterAnalysis <- rbindlist(lst)

#Controllo del totale dei punti di prelievo caricati
length(unique(Profile_ClusterAnalysis$cPod))

#Impostazione del valore di profilo come numerico
Profile_ClusterAnalysis[,
                        Profile := as.numeric(Profile)]

#Creazione Pivot con punto di prelievo, mese e valore del profilo
Profile_ClusterAnalysis_Pivot <-
  dcast(Profile_ClusterAnalysis,
        ("cPod ~ Month"),
        value.var = "Profile")

#Creazione variabile dei casi riferiti ai punti di prelievo
case_Pod <- unique(Profile_ClusterAnalysis$cPod)

#Creazione matrice di input alla cluster analysis
Profile_ClusterAnalysis_Matrix <-
  as.matrix(Profile_ClusterAnalysis_Pivot[, 
                                          2:ncol(Profile_ClusterAnalysis_Pivot)])

#Verifica cluster da creare tramite libreria NbClust
NbClust(Profile_ClusterAnalysis_Matrix, distance = "euclidean",
        min.nc = 3, max.nc = 9, 
        method = "kmeans")

#Cluster analysis con raggruppamento a 3 cluster
ClusterAnalysis_3 <-
  kmeans(
    Profile_ClusterAnalysis_Matrix,
    centers = 3,
    algorithm = "MacQueen",
    iter.max = 1000
  )

#Cluster analysis con raggruppamento a 5 cluster
ClusterAnalysis_5 <-
  kmeans(
    Profile_ClusterAnalysis_Matrix,
    centers = 5,
    algorithm = "MacQueen",
    iter.max = 1000
  )

#Cluster analysis con raggruppamento a 7 cluster
ClusterAnalysis_7 <-
  kmeans(
    Profile_ClusterAnalysis_Matrix,
    centers = 7,
    algorithm = "MacQueen",
    iter.max = 1000
  )

#Cluster analysis con raggruppamento a 9 cluster
ClusterAnalysis_9 <-
  kmeans(
    Profile_ClusterAnalysis_Matrix,
    centers = 9,
    algorithm = "MacQueen",
    iter.max = 1000
  )

#Verifica composizione dei cluster per tutte le tipologie di raggruppamenti
data.frame(cluster = 1:3,
           clusterSize = ClusterAnalysis_3$size)
data.frame(cluster = 1:5,
           clusterSize = ClusterAnalysis_5$size)
data.frame(cluster = 1:7,
           clusterSize = ClusterAnalysis_7$size)
data.frame(cluster = 1:9,
           clusterSize = ClusterAnalysis_9$size)

#Creazione data frame con punto di prelievo e cluster di appartenenza per
#tutte le tipologie di raggruppamenti
clusterPod_3 <-
  data.frame(cPod = case_Pod,
             ClusterAnalysis_3$cluster)
clusterPod_5 <-
  data.frame(cPod = case_Pod,
             ClusterAnalysis_5$cluster)
clusterPod_7 <-
  data.frame(cPod = case_Pod,
             ClusterAnalysis_7$cluster)
clusterPod_9 <-
  data.frame(cPod = case_Pod,
             ClusterAnalysis_9$cluster)

#Ricerca e salvataggio del minimo e massimo profilo per tutte le tipologie
#di raggruppamenti
minProfile_3 <- min(ClusterAnalysis_3$center)
maxProfile_3 <- max(ClusterAnalysis_3$center)

minProfile_5 <- min(ClusterAnalysis_5$center)
maxProfile_5 <- max(ClusterAnalysis_5$center)

minProfile_7 <- min(ClusterAnalysis_7$center)
maxProfile_7 <- max(ClusterAnalysis_7$center)

minProfile_9 <- min(ClusterAnalysis_9$center)
maxProfile_9 <- max(ClusterAnalysis_9$center)

#Grafici delle quattro differenti cluster analysis
par(mfrow = c(2, 2))
plot(
  ClusterAnalysis_3$center[1, ],
  type = "o",
  col = "black",
  ylim = c(minProfile_3, maxProfile_3),
  ylab = "Profilo",
  xlab = "Mese",
  main = "3 cluster"
)
lines(ClusterAnalysis_3$center[2, ], 
      type = "o",
      col = "green")
lines(ClusterAnalysis_3$center[3, ],
      type = "o",
      col = "red")

plot(
  ClusterAnalysis_5$center[1, ],
  type = "o",
  col = "blue",
  ylim = c(minProfile_5, maxProfile_5),
  ylab = "Profilo",
  xlab = "Mese",
  main = "5 cluster"
)
lines(ClusterAnalysis_5$center[2, ],
      type = "o",
      col = "green")
lines(ClusterAnalysis_5$center[3, ],
      type = "o",
      col = "red")
lines(ClusterAnalysis_5$center[4, ],
      type = "o",
      col = "grey")
lines(ClusterAnalysis_5$center[5, ],
      type = "o",
      col = "black")

plot(
  ClusterAnalysis_7$center[1, ],
  type = "o",
  col = "green",
  ylim = c(minProfile_7, maxProfile_7),
  ylab = "Profilo",
  xlab = "Mese",
  main = "7 cluster"
)
lines(ClusterAnalysis_7$center[2, ],
      type = "o",
      col = "blue")
lines(ClusterAnalysis_7$center[3, ],
      type = "o",
      col = "black")
lines(ClusterAnalysis_7$center[4, ],
      type = "o",
      col = "red")
lines(ClusterAnalysis_7$center[5, ],
      type = "o",
      col = "gray")
lines(ClusterAnalysis_7$center[6, ],
      type = "o",
      col = "violet")
lines(ClusterAnalysis_7$center[7, ],
      type = "o",
      col = "orange")

plot(
  ClusterAnalysis_9$center[1, ],
  type = "o",
  col = "yellow",
  ylim = c(minProfile_9, maxProfile_9),
  ylab = "Profilo",
  xlab = "Mese",
  main = "9 cluster"
)
lines(ClusterAnalysis_9$center[2, ],
      type = "o",
      col = "black")
lines(ClusterAnalysis_9$center[3, ],
      type = "o", 
      col = "grey")
lines(ClusterAnalysis_9$center[4, ],
      type = "o",
      col = "green")
lines(ClusterAnalysis_9$center[5, ],
      type = "o",
      col = "violet")
lines(ClusterAnalysis_9$center[6, ],
      type = "o",
      col = "brown")
lines(ClusterAnalysis_9$center[7, ],
      type = "o",
      col = "blue")
lines(ClusterAnalysis_9$center[8, ],
      type = "o",
      col = "red")
lines(ClusterAnalysis_9$center[9, ],
      type = "o",
      col = "orange")

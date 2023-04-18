# Load libraries
library(RSQLite)
library(XML)
library(DBI)
library(knitr)
library(sqldf)

# Create database and connect to database 
dbfile = "pubmed.db"
dbcon <- dbConnect(RSQLite::SQLite(), dbfile)

# Create Articles table
dbGetQuery(dbcon, 'DROP TABLE IF EXISTS Articles')
dbGetQuery(dbcon, 'CREATE TABLE Articles(
                                PMID INT PRIMARY KEY,
                                Joirnal_Id INTEGER,
                                Article_Title TEXT,
                                Volume INTEGER,
                                ISSUE INTEGER,
                                Publish_year INT,
                                Publish_month INT,
                                Publish_day INT
                                )'
           )

# Create Authors table
dbGetQuery(dbcon, 'DROP TABLE IF EXISTS Authors')
dbGetQuery(dbcon, 'CREATE TABLE Authors(
                                ID INT PRIMARY KEY,
                                AuthorName)'
            )

# Create Journals table 
dbGetQuery(dbcon, 'DROP TABLE IF EXISTS Journals')
dbGetQuery(dbcon, 'CREATE TABLE Journals(
                                Journal_Id INT PRIMARY KEY,
                                Title INT,
                                ISOAbbreviation TEXT)'
            )

# Create Article_Author table 
dbGetQuery(dbcon, 'DROP TABLE IF EXISTS Article_Author')
dbGetQuery(dbcon, 'CREATE TABLE Article_Author(
                                PMID INT ,
                                Author_Id INT,
                                PRIMARY KEY (PMID, Author_Id))'
            )

# Load the XML with the DTD into R using validation
xmlFile <- "pubmed22n0001-tf.xml"
dom1 <- xmlParse(xmlFile, validate=T)
xmlDOM <- xmlParse(xmlFile)
r <- xmlRoot(xmlDOM)
Publications <- xmlSize(r)

################################################################################
# Create Journal dataframe 
Journals.df <- data.frame (
  Title = character(),
  ISOAbbreviation = character(),
  stringsAsFactors = F)

# Insert records into journals dataframe 
PMID <- xpathSApply(xmlDOM,"//Article",xmlAttrs)
Title <- xpathSApply(xmlDOM,"//Title",xmlValue)
ISOAbbreviation <- xpathSApply(xmlDOM,"//ISOAbbreviation",xmlValue)
for (i in 1:length(PMID)) {
  article <- r[[i]]
  if (xmlName(article[[1]][[1]][[1]]) == "ISSN") {
    ISSN <- xmlValue(article[[1]][[1]][[1]])
  } else {
    ISSN <- NULL
  }
  if (Title[i] %in% Journals.df$Title & 
      ISOAbbreviation[i] %in% Journals.df$ISOAbbreviation){
    next
  } else {
    Journals.df[nrow(Journals.df) + 1,]$Title <- Title[i]
    Journals.df[nrow(Journals.df),]$ISOAbbreviation <- ISOAbbreviation[i]
    
  }
}
Journals.df$Journal_Id <- row.names(Journals.df)
Journals.df$Journal_Id <- as.numeric(Journals.df$Journal_Id)
Journals.df<- Journals.df[, c(3, 1, 2)]
Journals.df["Journal_Id"][is.na(Journals.df["Journal_Id"])] <-  1
################################################################################

# Create articles dataframe
Articles.df <- data.frame (
  Id = character(),
  Journal_Id = character(),
  Article_Title = character(),
  Volume = character(), 
  Issue = character(),
  Pub_date = character(), 
  Publish_year = character(), 
  Publish_month = character(), 
  Publish_day = character(),
  stringsAsFactors = F)

# Process volume and issue information for journals 
PMID <- xpathSApply(xmlDOM,"//Article",xmlAttrs)
Title <- xpathSApply(xmlDOM,"//Title",xmlValue)
ISOAbbreviation <- xpathSApply(xmlDOM,"//ISOAbbreviation",xmlValue)
for(i in 1:length(PMID)) {
  article <- r[[i]]
  id <- PMID[i]
  Title_I <- Title[i]
  ISOAbbreviation_I <- ISOAbbreviation[i]
  Journal <- Journals.df$Journal_Id[Journals.df$Title == toString(Title_I) & Journals.df$ISOAbbreviation == toString(ISOAbbreviation_I) ]
  if (xmlName(article[[1]][[1]][[1]]) == "ISSN") {
    if (xmlName(article[[1]][[1]][[2]][[1]]) == "Volume") {
      Volume <- xmlValue(article[[1]][[1]][[2]][[1]])
      if (xmlName(article[[1]][[1]][[2]][[2]]) == "Issue") {
        Issue <- xmlValue(article[[1]][[1]][[2]][[2]])
      } else {
        Issue <- NULL
      }
    } 
    else if (xmlName(article[[1]][[1]][[2]][[1]]) == "Issue") {
      Issue <- xmlValue(article[[1]][[1]][[2]][[1]])
      Volume <- NULL
    } else {
      Issue <- NULL
      Volume <- NULL
    }
  } else {
    if (xmlName(article[[1]][[1]][[1]][[1]]) == "Volume") {
      Volume <- xmlValue(article[[1]][[1]][[1]][[1]])
      if (xmlName(article[[1]][[1]][[1]][[2]]) == "Issue") {
        Issue <- xmlValue(article[[1]][[1]][[1]][[2]])
      } else {
        Issue <- NULL
      }
    }
    else if (xmlName(article[[1]][[1]][[1]][[1]]) == "Issue") {
      Issue <- xmlValue(article[[1]][[1]][[1]][[1]])
      Volume <- NULL
    } else{
      Issue <- NULL
      Volume <- NULL
    }
  }
  Articles.df[nrow(Articles.df) + 1,]$Id <- id
  Articles.df[nrow(Articles.df),]$Journal_Id <- Journal
  Articles.df[nrow(Articles.df) ,]$Volume<- Volume
  Articles.df[nrow(Articles.df) ,]$Issue<- Issue
}

# Create hashmap to process date field with month
Month_abb <- c("jan","feb","mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec") 
Month_num <- c("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")
names(Month_num) <- Month_abb

# In handling dates with season, seasons are mapped to month as follow:
# Spring:   Feb, Mar, Apr
# Summer:   May, Jun, Jul
# Fall:     Aug, Sep, Oct
# Winter:   Nov, Dec, Jan

# In handing dates with missing day of the month, first day of the month will be 
# default value for the day. 

# Process date field for articles 
PMID <- xpathSApply(xmlDOM,"//Article",xmlAttrs)
Pub_Date <- xpathSApply(xmlDOM,"//PubDate",xmlValue)
for (i in 1:length(PMID)) {
  id <- PMID[i]
  publish_date  <- str_replace_all(Pub_Date[i], fixed(" "), "")
  publish_date  <- tolower(publish_date)
  if (str_length(publish_date) == 4){
    year <- publish_date
    month <- "01"
    day <- "01"
  } 
  else if (str_length(publish_date) == 9){
    if (substr(publish_date, 5, 5) == "-") {
      year <- substr(publish_date,1, 4)
      month <- "01"
      day <- "01"
    } 
    else if (!is.numeric(substr(publish_date, 5, 5))){
      year <- substr(publish_date,1, 4)
      month <- Month_num[[substr(publish_date,5,  7)]]
      day <- substr(publish_date,8,  9)
    } else {
      year <- substr(publish_date,1, 4)
      month <- "toDebug"
      day <- "toDebug"
    }
  }
  
  else if (str_length(publish_date) == 12){
    year <- substr(publish_date,1, 4)
    month <- Month_num[[substr(publish_date,5,  7)]]
    day <- substr(publish_date,8,  9)
  } 
  
  else if (str_length(publish_date) == 15){
    year <- substr(publish_date,1, 4)
    if (substr(publish_date, 5, 5) == "-") {
      if (substr(publish_date,10, 15) == "winter") {
        month <- "11"
        day <- "01"
      } else if (substr(publish_date,10, 15) == "spring") {
        month <- "02"
        day <- "01"
      } else if (substr(publish_date,10, 15) == "summer") {
        month <- "05"
        day <- "01"
      } 
    } else {
      month <- Month_num[[substr(publish_date,5,  7)]]
      day <- substr(publish_date,8,  9)
    }
  }
  
  else if (str_length(publish_date) == 13){
    year <- substr(publish_date,1, 4)
    month <- "08"
    day <- "01"
  }
  
  else if (str_length(publish_date) == 6){
    year <- substr(publish_date,1, 4)
    month <- substr(publish_date,5, 6)
    day <- '01'
  } 
  
  else if (str_length(publish_date) == 8){
    year <- substr(publish_date,1, 4)
    if (substr(publish_date,5, 8) == "fall") {
      month <- "08"
      day <- "01"
    } else {
      month <- substr(publish_date,5, 6)
      day <- substr(publish_date, 7, 8)
  } 
  
  else if (str_length(publish_date) == 11) {
    year <- substr(publish_date,1, 4)
    month <- Month_num[[substr(publish_date,5,  7)]]
    day <- "01"
  } 
  
  else if (str_length(publish_date) == 7) {
    year <- substr(publish_date,1, 4)
    month <- Month_num[[substr(publish_date,5,  7)]]
    day <- "01"
  } 
  
  else if (str_length(publish_date) == 10){
    year <- substr(publish_date,1, 4)
    if (substr(publish_date,5, 10) == "summer"){
      month <- "05"
      day <- "01"
    } 
    else if (substr(publish_date,5, 10) == "winter"){
      month <- "11"
      day <- "01"
    } 
    else if (substr(publish_date,5, 10) == "spring"){
      month <- "02"
      day <- "01"
    }
    else if (!is.numeric(substr(publish_date, 10, 10))){
      month <- Month_num[[substr(publish_date,5, 7)]]
      day <- substr(publish_date, 8, 8)
    } else {
      month <- "toDebug"
      day <- "toDebug"
    }
  }
  
  else if (str_length(publish_date) == 8){
    year <- substr(publish_date,1, 4)
    if (substr(publish_date,5, 10) == "fall") {
      month <- "08"
      day <- "01"
    } else {
      month <- "toDebug"
      day <- "toDebug"
    }
  } else {
    year <- "toDebug"
    month <- "toDebug"
    day <- "toDebug"
  }
  
  Articles.df[i,]$Pub_date <- publish_date
  Articles.df[i,]$Publish_year <- year
  Articles.df[i,]$Publish_month <- month
  Articles.df[i,]$Publish_day <- day
}

# Use PMID to find rows that with date field need to be changed manually
# PMID (8769, 21940, 26215, 26216) need to be changed manually

# Change for PMID 8769
Articles.df$Publish_year[Articles.df$Id == '8769'] <- "1976"
Articles.df$Publish_month[Articles.df$Id == '8769'] <- "08"
Articles.df$Publish_day[Articles.df$Id == '8769'] <- "28"
# Change for PMID 22808
Articles.df$Publish_year[Articles.df$Id == '22808'] <- "1976"
Articles.df$Publish_month[Articles.df$Id == '22808'] <- "09"
Articles.df$Publish_day[Articles.df$Id == '22808'] <- "30"
# Change for PMID 26215
Articles.df$Publish_year[Articles.df$Id == '26215'] <- "1977"
Articles.df$Publish_month[Articles.df$Id == '26215'] <- "11"
Articles.df$Publish_day[Articles.df$Id == '26215'] <- "01"
# Change for PMID 22808
Articles.df$Publish_year[Articles.df$Id == '26216'] <- "1977"
Articles.df$Publish_month[Articles.df$Id == '26216'] <- "11"
Articles.df$Publish_day[Articles.df$Id == '26216'] <- "01"

# Insert article into Aarticle dataframe 
ArticleTitle <- xpathSApply(xmlDOM,"//ArticleTitle",xmlValue)
for (i in 1:length(PMID)) {
  Articles.df[i,]$Article_Title <- ArticleTitle[i]
}
# Remove Pub_date field 
Articles.df <- Articles.df[, -6]
################################################################################

# Create authors dataframe 
Authors.df <- data.frame (
  Author = character(),
  stringsAsFactors = F)

# Process and insert records into authors dataframe
PMID <- xpathSApply(xmlDOM,"//Article",xmlAttrs)
Authors <- xpathSApply(xmlDOM,"//Author",xmlValue)
for (i in 1:length(Authors)) {
  Authors.df[i,] <- Authors[i]
}

# Drop duplicate for authors dataframe 
Authors.df <- unique(Authors.df)

# Create an auto increment author id field for author table 
Authors.df$Author_Id <- row.names(Authors.df)
Authors.df<- Authors.df[, c(2, 1)]
################################################################################

# Create a table to store article and author
# Each row in this table corresponding to an article author pair
Article_Author.df <- data.frame (
  PMID = character(),
  Author = character(),
  stringsAsFactors = F)
PMID <- xpathSApply(xmlDOM,"//Article",xmlAttrs)
Journals <- xpathSApply(xmlDOM,"//ArticleTitle",xmlValue)
a <- 0
for (i in 1:length(PMID)) {
  article <- r[[i]]
  if (is.null(article[[1]][[3]])){
    Article_Author.df[nrow(Article_Author.df) + 1,]$PMID <- PMID[i]
    author <- NULL
    Article_Author.df[nrow(Article_Author.df),]$Author <- author
  } else {
    j = 1
    while (!is.null(article[[1]][[3]][[j]])){
      Article_Author.df[nrow(Article_Author.df) + 1,]$PMID <- PMID[i]
      author <- Authors.df$Author_Id[Authors.df$Author == xmlValue(article[[1]][[3]][[j]])]
      Article_Author.df[nrow(Article_Author.df),]$Author <- author
      j = j + 1
    }
  }
}

# Turn Publish_year, Publish_month and Publish_day into numberic field
Articles.df$Publish_year <- as.numeric(Articles.df$Publish_year)
Articles.df$Publish_month <- as.numeric(Articles.df$Publish_month)
Articles.df$Publish_day <- as.numeric(Articles.df$Publish_day)
################################################################################

# Write each dataframe into table
dbWriteTable(dbcon,"Articles",Articles.df,overwrite=T)
dbWriteTable(dbcon,"Authors",Authors.df,overwrite=T)
dbWriteTable(dbcon,"Journals",Journals.df,overwrite=T)
dbWriteTable(dbcon,"Article_Author",Article_Author.df,overwrite=T)
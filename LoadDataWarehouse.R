#Load packages 
require(RMySQL)
library(sqldf)
library(RSQLite)

###############################################################################
# Connect sqlite 
dbfile = "pubmed.db"
dbcon <- dbConnect(RSQLite::SQLite(), dbfile)

# Connect to mysql database 
mysqlconnection = dbConnect(RMySQL::MySQL(),
                            dbname='PracticumII',
                            host='localhost',
                            port=3306,
                            user='root',
                            password='rootroot')



# Create article dimension table 
create_article_table <- function(connection) {
  dbGetQuery(connection, 'DROP TABLE IF EXISTS Dim_Article')
  dbGetQuery(connection, 'CREATE TABLE Dim_Article(
                                journalId INTEGER PRIMARY KEY,
                                publishYear INTEGER, 
                                publishQuarter VARCHAR(50),
                                publishMonth INTEGER,
                                articleCount INTEGER
                                )'
              )
  
}
create_article_table(mysqlconnection)

# Create author dimension table 
create_author_year_table <- function(connection) {
  dbGetQuery(connection, 'DROP TABLE IF EXISTS Dim_Author_Year')
  dbGetQuery(connection, 'CREATE TABLE Dim_Author_Year(
                                journalId INTEGER PRIMARY KEY,
                                publishYear INTEGER, 
                                authorCountYear INTEGER
                                )'
  )
  
}
create_author_year_table(mysqlconnection)

# Create author dimension table 2
create_author_quarter_table <- function(connection) {
  dbGetQuery(connection, 'DROP TABLE IF EXISTS Dim_Author_Quarter')
  dbGetQuery(connection, 'CREATE TABLE Dim_Author_Quarter(
                                journalId INTEGER PRIMARY KEY,
                                publishYear INTEGER, 
                                publishQuarter VARCHAR(50),
                                authorCountQuarter INTEGER
                                )'
  )
  
}
create_author_quarter_table(mysqlconnection)

# Create author dimension table 3
create_author_month_table <- function(connection) {
  dbGetQuery(connection, 'DROP TABLE IF EXISTS Dim_Author_Month')
  dbGetQuery(connection, 'CREATE TABLE Dim_Author_Month(
                                journalId INTEGER PRIMARY KEY,
                                title VARCHAR(50),
                                publishYear INTEGER, 
                                publishQuarter VARCHAR(50),
                                publishMonth INTEGER,
                                authorCountMonth INTEGER
                                )'
  )
  
}
create_author_month_table(mysqlconnection)

# Create journal fact table 
create_journal_table <- function(connection) {
  dbGetQuery(connection, 'DROP TABLE IF EXISTS Fact_Journal')
  dbGetQuery(connection, 'CREATE TABLE Fact_Journal(
                                journalId INTEGER,
                                title VARCHAR(255),
                                publishYear INTEGER, 
                                publishQuarter VARCHAR(50),
                                publishMonth INTEGER,
                                articleCount INTEGER, 
                                authorCountYear INTEGER,
                                authorCountQuarter INTEGER,
                                authorCountMonth INTEGER
                                )'
  )
  
}
create_journal_table(mysqlconnection)

################################################################################

# Process data for Dim_Article
article_count_by_date <- function(connection){
  dbGetQuery(connection, 'SELECT Journal_Id, 
                                 Publish_year, 
                                 CASE 
                                   WHEN Publish_month IN ("1", "2", "3") THEN "Q1"
                                   WHEN Publish_month IN ("4", "5", "6") THEN "Q2"
                                   WHEN Publish_month IN ("7", "8", "9") THEN "Q3"
                                   WHEN Publish_month IN ("10", "11", "12") THEN "Q4"
                                 END AS Publish_quarter,
                                 Publish_month, 
                                 COUNT(Id) as Article_count
                           FROM Articles
                           GROUP BY 1, 2, 3')
}

Articles.df <- article_count_by_date(dbcon)

# Process data for Dim_Author_Year table 
author_count_by_year <- function(connection){
  dbGetQuery(connection, 'SELECT Journal_Id, 
                                 Publish_year, 
                                 COUNT(DISTINCT Article_Author.Author) as Author_count_year
                          FROM Articles
                          LEFT JOIN Article_Author_year
                          ON Articles.Id = Article_Author.PMID
                          GROUP BY 1, 2')
}
Authors_year.df <- author_count_by_year(dbcon)

# Process data for Dim_Author_Quarter table 
author_count_by_quarter <- function(connection){
  dbGetQuery(connection, 'SELECT Journal_Id, 
                                  Publish_year, 
                                  CASE 
                                    WHEN Publish_month IN ("1", "2", "3") THEN "Q1"
                                    WHEN Publish_month IN ("4", "5", "6") THEN "Q2"
                                    WHEN Publish_month IN ("7", "8", "9") THEN "Q3"
                                    WHEN Publish_month IN ("10", "11", "12") THEN "Q4"
                                  END AS Publish_quarter,
                                  COUNT(DISTINCT Article_Author.Author) as Author_count_quarter
                                  FROM Articles
                                  LEFT JOIN Article_Author
                                  ON Articles.Id = Article_Author.PMID
                                  GROUP BY 1, 2, 3')
}
Authors_quarter.df <- author_count_by_quarter(dbcon)

# Process data for Dim_Author_Month table 
author_count_by_month <- function(connection){
  dbGetQuery(connection, 'SELECT Articles.Journal_Id, 
                                 Title,
                                 Publish_year, 
                                 CASE 
                                    WHEN Publish_month IN ("1", "2", "3") THEN "Q1"
                                    WHEN Publish_month IN ("4", "5", "6") THEN "Q2"
                                    WHEN Publish_month IN ("7", "8", "9") THEN "Q3"
                                    WHEN Publish_month IN ("10", "11", "12") THEN "Q4"
                                  END AS Publish_quarter,
                                  Publish_month,
                                  COUNT(DISTINCT Article_Author.Author) as Author_count_month
                                  FROM Articles
                                  LEFT JOIN Article_Author
                                  ON Articles.Id = Article_Author.PMID
                                  LEFT JOIN Journals
                                  ON Articles.Journal_Id = Journals.Journal_Id 
                                  GROUP BY 1, 2, 3, 4')
}
Authors_month.df <- author_count_by_month(dbcon)

# Process data for Fact Journals table 
fact_journal <- function(connection){
  dbGetQuery(connection, 'SELECT a.Journal_Id, 
                                 a.Title,
                                 a.Publish_year, 
                                 a.Publish_quarter, 
                                 a.Publish_month, 
                                 Author_count_month,
                                 Author_count_quarter,
                                 Author_count_year,
                                 Article_count
                           FROM PracticumII.Dim_Author_Month as a
                           LEFT JOIN PracticumII.Dim_Author_Quarter as b
                           ON a.Journal_Id = b.journal_Id
                           AND a.Publish_year = b.Publish_year
                           AND a.Publish_quarter = b.Publish_quarter
                           LEFT JOIN PracticumII.Dim_Author_Year as c
                           ON a.Journal_Id = c.journal_Id
                           AND a.Publish_year = c.Publish_year
                           LEFT JOIN PracticumII.Dim_Article  as d
                           ON a.Journal_Id = d.journal_Id
                           AND a.Publish_year = d.Publish_year
                           AND a.Publish_quarter = d.Publish_quarter
                           AND a.Publish_month = d.Publish_month')
}
fact_journal.df <- fact_journal(mysqlconnection)

# Write dataframe to mysql Dim_Article table 
write_table <- function(connection, table, df){
  dbWriteTable(connection, table, df, overwrite = T)
}

write_table(mysqlconnection, "Dim_Article", Articles.df)
write_table(mysqlconnection, "Dim_Author_Year", Authors_year.df)
write_table(mysqlconnection, "Dim_Author_Quarter", Authors_quarter.df)
write_table(mysqlconnection, "Dim_Author_Month", Authors_month.df)
write_table(mysqlconnection, "Fact_Journal", fact_journal.df)
dbGetQuery(mysqlconnection, 'ALTER TABLE Dim_Article DROP COLUMN row_names')
dbGetQuery(mysqlconnection, 'ALTER TABLE Dim_Author_Year DROP COLUMN row_names')
dbGetQuery(mysqlconnection, 'ALTER TABLE Dim_Author_Quarter DROP COLUMN row_names')
dbGetQuery(mysqlconnection, 'ALTER TABLE Dim_Author_Month DROP COLUMN row_names')
dbGetQuery(mysqlconnection, 'ALTER TABLE Fact_Journal DROP COLUMN row_names')

#################### database connection ####################
# Load necessary packages
require('RPostgreSQL')
library(plyr)

# Load the PostgreSQL driver
drv <- dbDriver('PostgreSQL')

# Create a connection to AWS
con <- dbConnect(drv, dbname = 'postgres',
                 host = 'group2.czl0pji2xj8f.us-east-2.rds.amazonaws.com', port = 5432,
                 user = 'postgres', password = 'group200')

# Pass the SQL statements that create all tables
stmt <- "
CREATE TABLE country(
  CountryID int,
  CountryName varchar(100) NOT NULL,
  Continent varchar(100),
  Population int,
  WHO_Region varchar(100),
  PRIMARY KEY (CountryID)
);

CREATE TABLE nutritional_status (
  CountryID int NOT NULL,
  Obesity numeric(4,2),
  UndernourishedLevel varchar(10),
  PRIMARY KEY (CountryID),
  FOREIGN KEY (CountryID) REFERENCES country(CountryID)
);

CREATE TABLE fat (
  CountryID int NOT NULL,
  AlcoholicBeverages numeric(6,4),
  AnimalProducts numeric(6,4),
  AnimalFats numeric(6,4),
  AquaticProducts numeric(6,4),
  CerealsExcludingBeer numeric(6,4),
  Eggs numeric(6,4),
  FishSeafood numeric(6,4),
  FruitsExcludingWine numeric(6,4),
  Meat numeric(6,4),
  Miscellaneous numeric(6,4),
  MilkExcludingButter numeric(6,4),
  Offals numeric(6,4),
  OilCrops numeric(6,4),
  Pulses numeric(6,4),
  Spices numeric(6,4),
  StarchyRoots numeric(6,4),
  Stimulants numeric(6,4),
  SugarCrops numeric(6,4),
  SugarSweeteners numeric(6,4),
  TreeNuts numeric(6,4),
  VegetalProducts numeric(6,4),
  VegetableOils numeric(6,4),
  Vegetables numeric(6,4),
  PRIMARY KEY (CountryID),
  FOREIGN KEY (CountryID) REFERENCES country(CountryID)
);

CREATE TABLE food_kcal (
  CountryID int NOT NULL,
  AlcoholicBeverages numeric(6,4),
  AnimalProducts numeric(6,4),
  AnimalFats numeric(6,4),
  AquaticProducts numeric(6,4),
  CerealsExcludingBeer numeric(6,4),
  Eggs numeric(6,4),
  FishSeafood numeric(6,4),
  FruitsExcludingWine numeric(6,4),
  Meat numeric(6,4),
  Miscellaneous numeric(6,4),
  MilkExcludingButter numeric(6,4),
  Offals numeric(6,4),
  OilCrops numeric(6,4),
  Pulses numeric(6,4),
  Spices numeric(6,4),
  StarchyRoots numeric(6,4),
  Stimulants numeric(6,4),
  SugarCrops numeric(6,4),
  SugarSweeteners numeric(6,4),
  TreeNuts numeric(6,4),
  VegetalProducts numeric(6,4),
  VegetableOils numeric(6,4),
  Vegetables numeric(6,4),
  PRIMARY KEY (CountryID),
  FOREIGN KEY (CountryID) REFERENCES country(CountryID)
);

CREATE TABLE food_kg (
CountryID int NOT NULL,
  AlcoholicBeverages numeric(6,4),
  AnimalProducts numeric(6,4),
  AnimalFats numeric(6,4),
  AquaticProducts numeric(6,4),
  CerealsExcludingBeer numeric(6,4),
  Eggs numeric(6,4),
  FishSeafood numeric(6,4),
  FruitsExcludingWine numeric(6,4),
  Meat numeric(6,4),
  Miscellaneous numeric(6,4),
  MilkExcludingButter numeric(6,4),
  Offals numeric(6,4),
  OilCrops numeric(6,4),
  Pulses numeric(6,4),
  Spices numeric(6,4),
  StarchyRoots numeric(6,4),
  Stimulants numeric(6,4),
  SugarCrops numeric(6,4),
  SugarSweeteners numeric(6,4),
  TreeNuts numeric(6,4),
  VegetalProducts numeric(6,4),
  VegetableOils numeric(6,4),
  Vegetables numeric(6,4),
  PRIMARY KEY (CountryID),
  FOREIGN KEY (CountryID) REFERENCES country(CountryID)
);

CREATE TABLE protein (
  CountryID int NOT NULL,
  AlcoholicBeverages numeric(6,4),
  AnimalProducts numeric(6,4),
  AnimalFats numeric(6,4),
  AquaticProducts numeric(6,4),
  CerealsExcludingBeer numeric(6,4),
  Eggs numeric(6,4),
  FishSeafood numeric(6,4),
  FruitsExcludingWine numeric(6,4),
  Meat numeric(6,4),
  Miscellaneous numeric(6,4),
  MilkExcludingButter numeric(6,4),
  Offals numeric(6,4),
  OilCrops numeric(6,4),
  Pulses numeric(6,4),
  Spices numeric(6,4),
  StarchyRoots numeric(6,4),
  Stimulants numeric(6,4),
  SugarCrops numeric(6,4),
  SugarSweeteners numeric(6,4),
  TreeNuts numeric(6,4),
  VegetalProducts numeric(6,4),
  VegetableOils numeric(6,4),
  Vegetables numeric(6,4),
  PRIMARY KEY (CountryID),
  FOREIGN KEY (CountryID) REFERENCES country(CountryID)
);

CREATE TABLE covid_19_cases (
  CountryID int NOT NULL,
  ConfirmedRate numeric(10,8),
  DeathsRate numeric(10,8),
  RecoveredRate numeric(10,8),
  ActiveRate numeric(10,8),
  PRIMARY KEY (CountryID),
  FOREIGN KEY (CountryID) REFERENCES country(CountryID)
);

CREATE TABLE worldometer_data (
  CountryID int NOT NULL,
  TotalCases int,
  TotalDeaths int,
  TotalRecovered int,
  ActiveCases int,
  Serious_Critical int,
  TotCases int,
  Deaths int,
  TotalTests int,
  Tests int,
  PRIMARY KEY (CountryID),
  FOREIGN KEY (CountryID) REFERENCES country(CountryID)
);

CREATE TABLE happinessreport2020 (
  CountryID int NOT NULL,
  ladder_score numeric(6,4), 
  Upper_whisker numeric(6,4),
  Lower_whisker numeric(6,4), 
  Socialsupport numeric(6,4), 
  Healthy_life_expectancy numeric(6,4), 
  Freedom_choice numeric(6,4), 
  PRIMARY KEY (CountryID),
  FOREIGN KEY (CountryID) REFERENCES country(CountryID)
); 

"

# Execute the statement to create tables
dbGetQuery(con, stmt)

#################### process data ####################
# read dataset
df = read.csv('/Users/yanyiwen/Downloads/merged_dataset update.csv', na.strings=c(""," ","NA","NaN"))

# check duplicate & NAs
df=df[!duplicated(df$Country),]
df=df[!is.na(df$Country),]

#################### country table ####################
# add PK
df$CountryID <- 1:nrow(df)

# remane columns
df=rename(df, c("Country"="CountryName", "WHO.Region"="WHO_Region"))

# change data types
df$CountryName=as.character(df$CountryName)
df$Continent=as.character(df$Continent)
df$Population=as.integer(df$Population)
df$WHO_Region=as.character(df$WHO_Region)

df_country = df[c('CountryID', 'CountryName', 'Continent', 'Population', 'WHO_Region')]
names(df_country) = tolower(names(df_country))

dbWriteTable(con, name="country", value=df_country, row.names=FALSE, append=TRUE)

#################### nutritional_status table ####################
df_nutritional_status = df[c('CountryID', 'Obesity_fat', 'Undernourished_fat')]
df_nutritional_status = rename(df_nutritional_status, c("Obesity_fat"="Obesity", "Undernourished_fat"="Undernourishedlevel"))
names(df_nutritional_status) = tolower(names(df_nutritional_status))

dbWriteTable(con, name="nutritional_status", value=df_nutritional_status, row.names=FALSE, append=TRUE)

#################### fat table ####################
df_fat = df[c('CountryID', 
              "Alcoholic.Beverages_fat",
              "Animal.Products_fat",
              "Animal.fats_fat",
              "Aquatic.Products..Other_fat",
              "Cereals...Excluding.Beer_fat",
              "Eggs_fat",
              "Fish..Seafood_fat",
              "Fruits...Excluding.Wine_fat",
              "Meat_fat",
              "Miscellaneous_fat",
              "Milk...Excluding.Butter_fat",
              "Offals_fat",
              "Oilcrops_fat",
              "Pulses_fat",
              "Spices_fat",
              "Starchy.Roots_fat",
              "Stimulants_fat",
              "Sugar.Crops_fat",
              "Sugar...Sweeteners_fat",
              "Treenuts_fat",
              "Vegetal.Products_fat",
              "Vegetable.Oils_fat",
              "Vegetables_fat")]

df_fat = rename(df_fat, c("Alcoholic.Beverages_fat" = "AlcoholicBeverages",
                          "Animal.Products_fat" = "AnimalProducts",
                          "Animal.fats_fat" = "AnimalFats",
                          "Aquatic.Products..Other_fat" = "AquaticProducts",
                          "Cereals...Excluding.Beer_fat" = "CerealsExcludingBeer",
                          "Eggs_fat" = "Eggs",
                          "Fish..Seafood_fat" = "FishSeafood",
                          "Fruits...Excluding.Wine_fat" = "FruitsExcludingWine",
                          "Meat_fat" = "Meat",
                          "Miscellaneous_fat" = "Miscellaneous",
                          "Milk...Excluding.Butter_fat" = "MilkExcludingButter",
                          "Offals_fat" = "Offals",
                          "Oilcrops_fat" = "OilCrops",
                          "Pulses_fat" = "Pulses",
                          "Spices_fat" = "Spices",
                          "Starchy.Roots_fat" = "StarchyRoots",
                          "Stimulants_fat" = "Stimulants",
                          "Sugar.Crops_fat" = "SugarCrops",
                          "Sugar...Sweeteners_fat" = "SugarSweeteners",
                          "Treenuts_fat" = "TreeNuts",
                          "Vegetal.Products_fat" = "VegetalProducts",
                          "Vegetable.Oils_fat" = "VegetableOils",
                          "Vegetables_fat" = "Vegetables"))

names(df_fat) = tolower(names(df_fat))

dbWriteTable(con, name="fat", value=df_fat, row.names=FALSE, append=TRUE)

#################### food_kcal table ####################
df_food_kcal = df[c('CountryID', 
                    "Alcoholic.Beverages_kcal",
                    "Animal.Products_kcal",
                    "Animal.fats_kcal",
                    "Aquatic.Products..Other_kcal",
                    "Cereals...Excluding.Beer_kcal",
                    "Eggs_kcal",
                    "Fish..Seafood_kcal",
                    "Fruits...Excluding.Wine_kcal",
                    "Meat_kcal",
                    "Miscellaneous_kcal",
                    "Milk...Excluding.Butter_kcal",
                    "Offals_kcal",
                    "Oilcrops_kcal",
                    "Pulses_kcal",
                    "Spices_kcal",
                    "Starchy.Roots_kcal",
                    "Stimulants_kcal",
                    "Sugar.Crops_kcal",
                    "Sugar...Sweeteners_kcal",
                    "Treenuts_kcal",
                    "Vegetal.Products_kcal",
                    "Vegetable.Oils_kcal",
                    "Vegetables_kcal")]

df_food_kcal = rename(df_food_kcal, c("Alcoholic.Beverages_kcal" = "AlcoholicBeverages",
                                      "Animal.Products_kcal" = "AnimalProducts",
                                      "Animal.fats_kcal" = "AnimalFats",
                                      "Aquatic.Products..Other_kcal" = "AquaticProducts",
                                      "Cereals...Excluding.Beer_kcal" = "CerealsExcludingBeer",
                                      "Eggs_kcal" = "Eggs",
                                      "Fish..Seafood_kcal" = "FishSeafood",
                                      "Fruits...Excluding.Wine_kcal" = "FruitsExcludingWine",
                                      "Meat_kcal" = "Meat",
                                      "Miscellaneous_kcal" = "Miscellaneous",
                                      "Milk...Excluding.Butter_kcal" = "MilkExcludingButter",
                                      "Offals_kcal" = "Offals",
                                      "Oilcrops_kcal" = "OilCrops",
                                      "Pulses_kcal" = "Pulses",
                                      "Spices_kcal" = "Spices",
                                      "Starchy.Roots_kcal" = "StarchyRoots",
                                      "Stimulants_kcal" = "Stimulants",
                                      "Sugar.Crops_kcal" = "SugarCrops",
                                      "Sugar...Sweeteners_kcal" = "SugarSweeteners",
                                      "Treenuts_kcal" = "TreeNuts",
                                      "Vegetal.Products_kcal" = "VegetalProducts",
                                      "Vegetable.Oils_kcal" = "VegetableOils",
                                      "Vegetables_kcal" = "Vegetables"))

names(df_food_kcal) = tolower(names(df_food_kcal))

dbWriteTable(con, name="food_kcal", value=df_food_kcal, row.names=FALSE, append=TRUE)

#################### food_kg table ####################
df_food_kg = df[c('CountryID', 
                  "Alcoholic.Beverages_kg",
                  "Animal.Products_kg",
                  "Animal.fats_kg",
                  "Aquatic.Products..Other_kg",
                  "Cereals...Excluding.Beer_kg",
                  "Eggs_kg",
                  "Fish..Seafood_kg",
                  "Fruits...Excluding.Wine_kg",
                  "Meat_kg",
                  "Miscellaneous_kg",
                  "Milk...Excluding.Butter_kg",
                  "Offals_kg",
                  "Oilcrops_kg",
                  "Pulses_kg",
                  "Spices_kg",
                  "Starchy.Roots_kg",
                  "Stimulants_kg",
                  "Sugar.Crops_kg",
                  "Sugar...Sweeteners_kg",
                  "Treenuts_kg",
                  "Vegetal.Products_kg",
                  "Vegetable.Oils_kg",
                  "Vegetables_kg")]

df_food_kg = rename(df_food_kg, c("Alcoholic.Beverages_kg" = "AlcoholicBeverages",
                                  "Animal.Products_kg" = "AnimalProducts",
                                  "Animal.fats_kg" = "AnimalFats",
                                  "Aquatic.Products..Other_kg" = "AquaticProducts",
                                  "Cereals...Excluding.Beer_kg" = "CerealsExcludingBeer",
                                  "Eggs_kg" = "Eggs",
                                  "Fish..Seafood_kg" = "FishSeafood",
                                  "Fruits...Excluding.Wine_kg" = "FruitsExcludingWine",
                                  "Meat_kg" = "Meat",
                                  "Miscellaneous_kg" = "Miscellaneous",
                                  "Milk...Excluding.Butter_kg" = "MilkExcludingButter",
                                  "Offals_kg" = "Offals",
                                  "Oilcrops_kg" = "OilCrops",
                                  "Pulses_kg" = "Pulses",
                                  "Spices_kg" = "Spices",
                                  "Starchy.Roots_kg" = "StarchyRoots",
                                  "Stimulants_kg" = "Stimulants",
                                  "Sugar.Crops_kg" = "SugarCrops",
                                  "Sugar...Sweeteners_kg" = "SugarSweeteners",
                                  "Treenuts_kg" = "TreeNuts",
                                  "Vegetal.Products_kg" = "VegetalProducts",
                                  "Vegetable.Oils_kg" = "VegetableOils",
                                  "Vegetables_kg" = "Vegetables"))

names(df_food_kg) = tolower(names(df_food_kg))

dbWriteTable(con, name="food_kg", value=df_food_kg, row.names=FALSE, append=TRUE)

#################### protein table ####################
df_protein = df[c('CountryID', 
                  "Alcoholic.Beverages_protein",
                  "Animal.Products_protein",
                  "Animal.fats_protein",
                  "Aquatic.Products..Other_protein",
                  "Cereals...Excluding.Beer_protein",
                  "Eggs_protein",
                  "Fish..Seafood_protein",
                  "Fruits...Excluding.Wine_protein",
                  "Meat_protein",
                  "Miscellaneous_protein",
                  "Milk...Excluding.Butter_protein",
                  "Offals_protein",
                  "Oilcrops_protein",
                  "Pulses_protein",
                  "Spices_protein",
                  "Starchy.Roots_protein",
                  "Stimulants_protein",
                  "Sugar.Crops_protein",
                  "Sugar...Sweeteners_protein",
                  "Treenuts_protein",
                  "Vegetal.Products_protein",
                  "Vegetable.Oils_protein",
                  "Vegetables_protein")]

df_protein = rename(df_protein, c("Alcoholic.Beverages_protein" = "AlcoholicBeverages",
                                  "Animal.Products_protein" = "AnimalProducts",
                                  "Animal.fats_protein" = "AnimalFats",
                                  "Aquatic.Products..Other_protein" = "AquaticProducts",
                                  "Cereals...Excluding.Beer_protein" = "CerealsExcludingBeer",
                                  "Eggs_protein" = "Eggs",
                                  "Fish..Seafood_protein" = "FishSeafood",
                                  "Fruits...Excluding.Wine_protein" = "FruitsExcludingWine",
                                  "Meat_protein" = "Meat",
                                  "Miscellaneous_protein" = "Miscellaneous",
                                  "Milk...Excluding.Butter_protein" = "MilkExcludingButter",
                                  "Offals_protein" = "Offals",
                                  "Oilcrops_protein" = "OilCrops",
                                  "Pulses_protein" = "Pulses",
                                  "Spices_protein" = "Spices",
                                  "Starchy.Roots_protein" = "StarchyRoots",
                                  "Stimulants_protein" = "Stimulants",
                                  "Sugar.Crops_protein" = "SugarCrops",
                                  "Sugar...Sweeteners_protein" = "SugarSweeteners",
                                  "Treenuts_protein" = "TreeNuts",
                                  "Vegetal.Products_protein" = "VegetalProducts",
                                  "Vegetable.Oils_protein" = "VegetableOils",
                                  "Vegetables_protein" = "Vegetables"))

names(df_protein) = tolower(names(df_protein))

dbWriteTable(con, name="protein", value=df_protein, row.names=FALSE, append=TRUE)

#################### COVID_19_cases table ####################
df_COVID_19_cases = df[c('CountryID','Confirmed_fat', 'Deaths_fat','Recovered_fat','Active_fat')]
df_COVID_19_cases = rename(df_COVID_19_cases, 
                           c('Confirmed_fat'='ConfirmedRate', 'Deaths_fat'='DeathsRate','Recovered_fat'='RecoveredRate',
                             'Active_fat'='ActiveRate'))

names(df_COVID_19_cases) = tolower(names(df_COVID_19_cases))

dbWriteTable(con, name="covid_19_cases", value=df_COVID_19_cases, row.names=FALSE, append=TRUE)

#################### worldometer_data table ####################
df$Deaths.1M.pop=as.integer(df$Deaths.1M.pop)

df_worldometer = df[c('CountryID','TotalCases','TotalDeaths','TotalRecovered','ActiveCases',
                      'Serious.Critical','Tot.Cases.1M.pop','Deaths.1M.pop','TotalTests','Tests.1M.pop')]

df_worldometer = rename(df_worldometer, 
                        c('Serious.Critical'='Serious_Critical','Tot.Cases.1M.pop'= 'TotCases',
                          'Deaths.1M.pop'= 'Deaths','Tests.1M.pop'='Tests'))

names(df_worldometer) = tolower(names(df_worldometer))

dbWriteTable(con, name="worldometer_data", value=df_worldometer, row.names=FALSE, append=TRUE)

#################### happinessreport2020 table ####################
df_happinessreport2020 = df[c('CountryID','Ladder.score', 'upperwhisker','lowerwhisker', 'Social.support',
                              'Healthy.life.expectancy', 'Freedom.to.make.life.choices')]

df_happinessreport2020 = rename(df_happinessreport2020, 
                                c('Ladder.score'='ladder_score', 'upperwhisker'='Upper_whisker','lowerwhisker'='Lower_whisker',
                                  'Social.support'='Socialsupport', 'Healthy.life.expectancy'='Healthy_life_expectancy',
                                  'Freedom.to.make.life.choices'='Freedom_choice'))

names(df_happinessreport2020) = tolower(names(df_happinessreport2020))

dbWriteTable(con, name="happinessreport2020", value=df_happinessreport2020, row.names=FALSE, append=TRUE)

#################### Close the connection ####################
dbDisconnect(con)
closeAllConnections()

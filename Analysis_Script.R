# Clearing the environment
rm(list = ls())

# Clearing the console
cat("\014")

jan1 = read.csv("/Users/aausuman/Downloads/Thesis Dataset/siri.20130101.csv", header = FALSE)
colnames(jan1) = c("Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", "BlockID", "VehicleID", "StopID", "AtStop")

jan2 = read.csv("/Users/aausuman/Downloads/Thesis Dataset/siri.20130102.csv", header = FALSE)
colnames(jan2) = c("Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", "BlockID", "VehicleID", "StopID", "AtStop")

jan3 = read.csv("/Users/aausuman/Downloads/Thesis Dataset/siri.20130103.csv", header = FALSE)
colnames(jan3) = c("Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", "BlockID", "VehicleID", "StopID", "AtStop")

levels(jan1$Operator)
nlevels(jan1$Operator)

levels(as.factor(jan1$LineID))
nlevels(as.factor(jan1$LineID))

levels(jan2$Operator)
nlevels(jan2$Operator)

levels(as.factor(jan2$LineID))
nlevels(as.factor(jan2$LineID))

levels(jan3$Operator)
nlevels(jan3$Operator)

levels(as.factor(jan3$LineID))
nlevels(as.factor(jan3$LineID))

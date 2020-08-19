# Analysis Script
data = read.csv("/Users/aausuman/Documents/Thesis/Dataset-Day1/siri.20130101.csv", header = FALSE)

colnames(data) = c("Timestamp", "LineID", "Direction", "Journey Pattern ID", "Time Frame", "Vehicle Journey ID", "Operator", "Congestion", "Lon", "Lat", "Delay", "Block ID", "Vehicle ID", "Stop ID", "At Stop")

attach(data)

nlevels(Operator)
nlevels(Direction)

data$Direction = as.factor(data$Direction)
levels(Direction)

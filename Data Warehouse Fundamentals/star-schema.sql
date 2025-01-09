-- DimDate Table
CREATE TABLE DimDate (
    dateid SERIAL PRIMARY KEY NOT NULL,
    date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    quartername VARCHAR(9) NOT NULL,
    month INT NOT NULL,
    monthname VARCHAR(9) NOT NULL,
    day INT NOT NULL,
    weekday INT NOT NULL,
    weekdayname VARCHAR(9) NOT NULL
);

-- DimStation Table
CREATE TABLE DimStation (
    stationid SERIAL PRIMARY KEY NOT NULL,
    city VARCHAR(255) NOT NULL
);

-- DimTruck Table
CREATE TABLE DimTruck (
    truckid SERIAL PRIMARY KEY NOT NULL,
    trucktype VARCHAR(255) NOT NULL
);

-- FactTrips Table
CREATE TABLE FactTrips (
    tripid SERIAL PRIMARY KEY NOT NULL,
    dateid INT NOT NULL,
    stationid INT NOT NULL,
    truckid INT NOT NULL,
    wastecollected DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (dateid) REFERENCES DimDate(dateid),
    FOREIGN KEY (stationid) REFERENCES DimStation(stationid),
    FOREIGN KEY (truckid) REFERENCES DimTruck(truckid)
);

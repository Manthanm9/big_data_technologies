
CREATE TABLE zipcodes(
RecordNumber int,
Country string,
City string,
Zipcode int)
PARTITIONED BY(state string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA INPATH '/data/zipcodes.csv' INTO TABLE zipcodes;

SHOW PARTITIONS zipcodes;

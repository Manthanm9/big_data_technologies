DEFINE max_by_group(X, group_key, max_field) RETURNS Y { 
A = GROUP $X by $group_key;
$Y = FOREACH A GENERATE group, MAX($X.$max_field);
};

records = LOAD 'temp_data.txt' using PigStorage(',') AS (year:chararray, temperature:int, quality:int);
filtered_records = FILTER records BY temperature != 9999 AND quality IN (0, 1, 4, 5, 9);
max_temp = max_by_group(filtered_records, year, temperature);
macro_max_by_group_A_0 = GROUP filtered_records by (year);
max_temp2 = FOREACH macro_max_by_group_A_0 GENERATE group, MAX(filtered_records.(temperature));
DUMP max_temp2

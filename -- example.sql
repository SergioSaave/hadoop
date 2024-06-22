-- example.pig
data = LOAD '/user/hadoop/input/whiskey_trace-1_1748052322638680856.branch_trace.426708.csv' USING PigStorage(',') AS (col1:chararray, col2:int);
-- Operaciones adicionales...
STORE data INTO '/user/hadoop/output/';


-- CREATE TABLE branch_analysis (
--     branch_addr STRING,
--     branch_type STRING,
--     taken INT,
--     target STRING
-- )
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY ','
-- STORED AS TEXTFILE;
CREATE TABLE charlie_analysis (
    branch_addr STRING,
    branch_type STRING,
    taken INT,
    target STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE delta_analysis (
    branch_addr STRING,
    branch_type STRING,
    taken INT,
    target STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE merced_analysis (
    branch_addr STRING,
    branch_type STRING,
    taken INT,
    target STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE whiskey_analysis (
    branch_addr STRING,
    branch_type STRING,
    taken INT,
    target STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
-- Cargar los datos en la tabla desde HDFS
LOAD DATA INPATH 'hdfs://localhost:9000/user/hadoop/input/charlie_trace-1_17571657100049929577.branch_trace.1006511.csv' INTO TABLE charlie_analysis;
LOAD DATA INPATH 'hdfs://localhost:9000/user/hadoop/input/delta_trace-1_10058381926338669845.branch_trace.507251.csv' INTO TABLE delta_analysis;
LOAD DATA INPATH 'hdfs://localhost:9000/user/hadoop/input/merced_trace-1_13378400607429273214.branch_trace.467769.csv' INTO TABLE merced_analysis;
LOAD DATA INPATH 'hdfs://localhost:9000/user/hadoop/input/whiskey_trace-1_1748052322638680856.branch_trace.426708.csv' INTO TABLE whiskey_analysis;


-- SACA LA PROPORCION POR NOMBRES
CREATE TABLE IF NOT EXISTS branch_proportion (
    branch_name STRING,
    proportion DOUBLE
);

INSERT INTO branch_proportion
SELECT
    'charlie' AS branch_name,
    AVG(taken) AS proportion
FROM
    charlie_analysis;


INSERT INTO branch_proportion
SELECT
    'delta' AS branch_name,
    AVG(taken) AS proportion
FROM
    delta_analysis;


INSERT INTO branch_proportion
SELECT
    'merced' AS branch_name,
    AVG(taken) AS proportion
FROM
    merced_analysis;


INSERT INTO branch_proportion
SELECT
    'whiskey' AS branch_name,
    AVG(taken) AS proportion
FROM
    whiskey_analysis;


-- SACA LA PROPORCION POR TIPO
CREATE TABLE IF NOT EXISTS branch_type_proportion (
    branch_type STRING,
    proportion_taken_1 DOUBLE
);


-- Calcular las proporciones para table1
CREATE TEMPORARY TABLE table1_avg AS
SELECT
    branch_type,
    AVG(taken) AS proportion_taken_1
FROM charlie_analysis
WHERE branch_type IN ("conditional_jump", "direct_call", "direct_jump", "indirect_call", "indirect_jump", "interrump", "return")
GROUP BY branch_type;

-- Calcular las proporciones para table2
CREATE TEMPORARY TABLE table2_avg AS
SELECT
    branch_type,
    AVG(taken) AS proportion_taken_1
FROM delta_analysis
WHERE branch_type IN ("conditional_jump", "direct_call", "direct_jump", "indirect_call", "indirect_jump", "interrump", "return")
GROUP BY branch_type;

-- Calcular las proporciones para table3
CREATE TEMPORARY TABLE table3_avg AS
SELECT
    branch_type,
    AVG(taken) AS proportion_taken_1
FROM merced_analysis
WHERE branch_type IN ("conditional_jump", "direct_call", "direct_jump", "indirect_call", "indirect_jump", "interrump", "return")
GROUP BY branch_type;

-- Calcular las proporciones para table4
CREATE TEMPORARY TABLE table4_avg AS
SELECT
    branch_type,
    AVG(taken) AS proportion_taken_1
FROM whiskey_analysis
WHERE branch_type IN ("conditional_jump", "direct_call", "direct_jump", "indirect_call", "indirect_jump", "interrump", "return")
GROUP BY branch_type;

-- Sumar las proporciones e insertar en la tabla final
INSERT INTO branch_type_proportion
SELECT
    branch_type,
    SUM(proportion_taken_1) AS proportion_taken_1
FROM (
    SELECT branch_type, proportion_taken_1 FROM table1_avg
    UNION ALL
    SELECT branch_type, proportion_taken_1 FROM table2_avg
    UNION ALL
    SELECT branch_type, proportion_taken_1 FROM table3_avg
    UNION ALL
    SELECT branch_type, proportion_taken_1 FROM table4_avg
) AS all_avgs
GROUP BY branch_type;


INSERT OVERWRITE DIRECTORY '/user/hadoop/branch_proportion_csv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM branch_proportion;

INSERT OVERWRITE DIRECTORY '/user/hadoop/branch_type_proportion_csv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM branch_type_proportion;


-- TOTAL DE ELEMENTOS EN EL DATASET
SELECT SUM(total_filas) AS total_elementos
FROM (
    SELECT COUNT(*) AS total_filas FROM charlie_analysis
    UNION ALL
    SELECT COUNT(*) AS total_filas FROM delta_analysis
    UNION ALL
    SELECT COUNT(*) AS total_filas FROM merced_analysis
    UNION ALL
    SELECT COUNT(*) AS total_filas FROM whiskey_analysis
) t;

-- Franco

INSERT INTO taken_branch_count
SELECT 
    branch_type,
    COUNT(CASE WHEN taken = 0 THEN 1 ELSE NULL END) AS taken_0,
    COUNT(CASE WHEN taken = 1 THEN 1 ELSE NULL END) AS taken_1,
    COUNT(taken) as total
FROM (
    SELECT branch_type, taken FROM charlie_analysis
    UNION ALL
    SELECT branch_type, taken FROM delta_analysis
    UNION ALL
    SELECT branch_type, taken FROM merced_analysis
    UNION ALL
    SELECT branch_type, taken FROM whiskey_analysis
) combined_branches
GROUP BY
    branch_type;

INSERT OVERWRITE DIRECTORY '/user/hadoop/taken_branch_count'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM taken_branch_count;

hdfs dfs -get /user/hadoop/taken_branch_count/000000_0 /tmp/taken_branch_count.csv

CREATE TABLE IF NOT EXISTS
        branch_frecuency(
            branch_type STRING,
            frecuency DOUBLE,
            frecuency_percent FLOAT
        );

SELECT 
    branch_type, 
    COUNT(*) AS frequency,
    COUNT(*) * 100.0 / total_count AS frequency_percent
FROM (
    SELECT branch_type FROM charlie_analysis
    UNION ALL
    SELECT branch_type FROM delta_analysis
    UNION ALL
    SELECT branch_type FROM merced_analysis
    UNION ALL
    SELECT branch_type FROM whiskey_analysis
) combined_branches
CROSS JOIN (
    SELECT COUNT(*) AS total_count FROM (
        SELECT branch_type FROM charlie_analysis
        UNION ALL
        SELECT branch_type FROM delta_analysis
        UNION ALL
        SELECT branch_type FROM merced_analysis
        UNION ALL
        SELECT branch_type FROM whiskey_analysis
    ) sub_query
) total_counts
GROUP BY 
    branch_type, total_count;



hdfs dfs -get /user/hadoop/branch_proportion_csv/ /tmp/branch_proportion.csv

INSERT INTO branch_frecuency
SELECT 
    branch_type, 
    COUNT(*) AS frequency
FROM 
    (
        SELECT branch_type FROM charlie_analysis
        UNION ALL
        SELECT branch_type FROM delta_analysis
        UNION ALL
        SELECT branch_type FROM merced_analysis
        UNION ALL
        SELECT branch_type FROM whiskey_analysis
    ) combined_branches
GROUP BY 
    branch_type;


INSERT OVERWRITE DIRECTORY '/user/hadoop/megacueri'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    'charlie_analysis' AS table_name,
    branch_type,
    COUNT(*) AS frequency
FROM 
    charlie_analysis
GROUP BY 
    branch_type

UNION ALL

SELECT 
    'delta_analysis' AS table_name,
    branch_type,
    COUNT(*) AS frequency
FROM 
    delta_analysis
GROUP BY 
    branch_type

UNION ALL

SELECT 
    'merced_analysis' AS table_name,
    branch_type,
    COUNT(*) AS frequency
FROM 
    merced_analysis
GROUP BY 
    branch_type

UNION ALL

SELECT 
    'whiskey_analysis' AS table_name,
    branch_type,
    COUNT(*) AS frequency
FROM 
    whiskey_analysis
GROUP BY 
    branch_type;

hdfs dfs -get /user/hadoop/megacueri/000000_0 /tmp/megacueri.csv

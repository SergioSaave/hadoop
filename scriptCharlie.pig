-- Cargar archivo CSV desde HDFS
data = LOAD 'hdfs://localhost:9000/user/hadoop/input/charlie_trace-1_17571657100049929577.branch_trace.1006511.csv' USING PigStorage(',') 
       AS (column1:chararray, column2:int, column3:float);

-- Realizar análisis (ejemplo: calcular la media de column3)
grouped_data = GROUP data ALL;
average = FOREACH grouped_data GENERATE AVG(data.column3);

-- Guardar los resultados en HDFS
STORE average INTO 'hdfs://localhost:9000/user/hadoop/outputCharlieAVG' USING PigStorage(',');
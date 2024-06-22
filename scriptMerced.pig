-- Cargar archivo CSV desde HDFS
data = LOAD 'hdfs://localhost:9000/user/hadoop/input/merced_trace-1_13378400607429273214.branch_trace.467769.csv' USING PigStorage(',') 
       AS (column1:chararray, column2:int, column3:float);

-- Realizar análisis (ejemplo: calcular la media de column3)
grouped_data = GROUP data ALL;
average = FOREACH grouped_data GENERATE AVG(data.column3);

-- Guardar los resultados en HDFS
STORE average INTO 'hdfs://localhost:9000/user/hadoop/outputMercedAVG' USING PigStorage(',');
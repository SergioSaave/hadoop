-- Cargar archivo CSV desde HDFS
data = LOAD 'hdfs://localhost:9000/user/hadoop/input/charlie_trace-1_17571657100049929577.branch_trace.1006511.csv' USING PigStorage(',')
       AS (branch_addr:chararray, branch_type:chararray, taken:int, target:chararray);

-- Calcular la cantidad de filas donde taken es 1 y donde es 0
grouped_data = GROUP data ALL;
counts = FOREACH grouped_data {
    taken_1 = FILTER data BY taken == 1;
    taken_0 = FILTER data BY taken == 0;
    GENERATE
        COUNT(taken_1) as count_taken_1,
        COUNT(taken_0) as count_taken_0,
        COUNT(data) as total_count;
}

-- Calcular la relación
relation_taken_1 = FOREACH counts GENERATE
    count_taken_1 as count_taken_1,
    count_taken_0 as count_taken_0,
    total_count as total_count,
    (double)count_taken_1 / (double)total_count as ratio_taken_1;

-- Guardar los resultados en HDFS
STORE relation_taken_1 INTO 'hdfs://localhost:9000/user/hadoop/outputCharlie' USING PigStorage(',');

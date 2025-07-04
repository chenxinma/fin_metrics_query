DROP TABLE IF EXISTS USES_DIMENSION;
DROP TABLE IF EXISTS FROM_TABLE;

DROP TABLE IF EXISTS Metric;
DROP TABLE IF EXISTS Dimension;
DROP TABLE IF EXISTS MetricDimension;
DROP TABLE IF EXISTS DataSource;

CREATE NODE TABLE Metric(id STRING, 
                        name STRING, 
                        catalog STRING,
                        alias STRING,
                        formula STRING, 
                        description STRING, 
                        dependent_metrics STRING[],
                        PRIMARY KEY (id));

CREATE NODE TABLE Dimension(id STRING,
                            name STRING, 
                            type STRING, 
                            hierarchy STRING[], 
                            with_table STRING, 
                            physical_fields MAP(STRING, STRING), 
                            join_condition STRING,
                            annotations STRING, 
                            required BOOL,
                            PRIMARY KEY (id));

CREATE NODE TABLE MetricDimension(id STRING,
                            name STRING,
                            PRIMARY KEY (id));

CREATE NODE TABLE DataSource(table_name STRING, columns STRING[], PRIMARY KEY (table_name));

CREATE REL TABLE USES_DIMENSION (from Metric to Dimension, from Metric to MetricDimension);
CREATE REL TABLE FROM_TABLE (from Metric to DataSource);

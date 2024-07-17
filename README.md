# Big Data project - An analisys between PostgreSQL and Cassandra

## Cassandra setup

```sh
# Download Cassandra using docker
sudo docker pull cassandra:latest

# Create network
sudo docker network create cassandra

# Start Cassandra
sudo docker run --rm -d --name cassandra --hostname cassandra --network cassandra cassandra
```

### Create test CQL file (data.cql)

```sql
-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS store WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create a table
CREATE TABLE IF NOT EXISTS store.shopping_cart (
userid text PRIMARY KEY,
item_count int,
last_update_timestamp timestamp
);

-- Insert some data
INSERT INTO store.shopping_cart
(userid, item_count, last_update_timestamp)
VALUES ('9876', 2, toTimeStamp(now()));
INSERT INTO store.shopping_cart
(userid, item_count, last_update_timestamp)
VALUES ('1234', 5, toTimeStamp(now()));
```

### Insertin data in Cassandra

```sh
# Insertin data in Cassandra from data.cql
sudo docker run --rm --network cassandra -v "$(pwd)/data.cql:/scripts/data.cql" -e CQLSH_HOST=cassandra -e CQLSH_PORT=9042 -e CQLVERSION=3.4.6 nuvo/docker-cqlsh

# Interactive CQLSH
sudo docker run --rm -it --network cassandra nuvo/docker-cqlsh cqlsh cassandra 9042 --cqlversion='3.4.6'
```

You're going to see something like this:

```sh
Connected to Test Cluster at cassandra:9042.
[cqlsh 5.0.1 | Cassandra 4.0.4 | CQL spec 3.4.5 | Native protocol v5]
Use HELP for help.
cqlsh>
```

Some more basic funcions:

```sql
-- Show databases
DESCRIBE keyspaces;

USE database_name;

SHOW tables;

SELECT * FROM store.shopping_cart;

INSERT INTO store.shopping_cart (userid, item_count) VALUES ('4567', 20);
```

Clean up

```sh
sudo docker kill cassandra
sudo docker network rm cassandra
```

## Python setup

```sh
# Download get-pip.py
curl -O https://bootstrap.pypa.io/get-pip.py

# Install pip for Python 3.10
python3.10 get-pip.py

# Verify pip installation
python3.10 -m pip --version

# Install required libraries
python3.10 -m pip install pandas sqlalchemy psycopg2-binary cassandra-driver

# Run the analysis
python3.10 bigdata_analysis.py
```

## Explicação do Script (bigdata_analysis.py)

### Conexão e Importação dos Dados:

O script conecta-se ao PostgreSQL e ao Cassandra.
Os dados do arquivo postings.csv são carregados e importados para ambas as bases de dados.

### Função de Medição de Tempo:

Uma função decoradora measure_time é usada para medir o tempo de execução das consultas.

### Consultas:

São definidas quatro consultas SQL/CQL para realizar:

-   Condicional simples (condicional_simples)
-   Busca de texto (busca_texto)
-   Agregações (máximo, mínimo, média) (max_min_avg)
-   Agrupamento e ordenação (agrupamento)

### Execução e Medição:

As consultas são executadas em ambas as bases de dados (PostgreSQL e Cassandra), e o tempo de execução é medido e armazenado.

### Exibição dos Resultados:

Os resultados e os tempos de execução das consultas são exibidos para comparação.

### Fechamento das Conexões:

As conexões com PostgreSQL e Cassandra são fechadas após a execução.

### Considerações Finais

Certifique-se de ajustar os detalhes da conexão (username, password, localhost, dbname) de acordo com sua configuração. O script oferece uma base sólida para realizar as análises e comparações de performance entre PostgreSQL e Cassandra usando o dataset de vagas de emprego do LinkedIn.

# Projeto Big Data: Comparação de Performance entre PostgreSQL e Cassandra NoSQL

## Objetivo:

Avaliar a performance de PostgreSQL e Cassandra NoSQL no armazenamento e manipulação de dados de vagas de emprego do LinkedIn, utilizando o dataset do Kaggle.

## Descrição do Estudo de Caso:

O estudo de caso envolve a análise de dados de vagas de emprego postadas no LinkedIn para comparar a eficiência de PostgreSQL (SGBD relacional) e Cassandra (NoSQL).

## Etapas do Projeto:

### Preparação do Ambiente:

-   Configure um ambiente de desenvolvimento com PostgreSQL e Cassandra.
-   Instale ferramentas necessárias, como drivers de conexão, e bibliotecas de manipulação de dados (ex: pandas para Python).
    Importação dos Dados:

### Baixe e explore o [dataset do Kaggle LinkedIn Job Postings](https://www.kaggle.com/datasets/arshkon/linkedin-job-postings).

É necessário baixar o arquivo `job_postings.csv` através do link acima. Este dataset contém os seguintes campos:

-   `job_id`: The job ID as defined by LinkedIn (https://www.linkedin.com/jobs/view/ job_id )
-   `company_id`: Identifier for the company associated with the job posting (maps to companies.csv)
-   `title`: Job title.
-   `description`: Job description.
-   `max_salary`: Maximum salary
-   `med_salary`: Median salary
-   `min_salary`: Minimum salary
-   `pay_period`: Pay period for salary (Hourly, Monthly, Yearly)
-   `formatted_work_type`: Type of work (Fulltime, Parttime, Contract)
-   `location`: Job location
-   `applies`: Number of applications that have been submitted
-   `original_listed_time`: Original time the job was listed
-   `remote_allowed`: Whether job permits remote work
-   `views`: Number of times the job posting has been viewed
-   `job_posting_url`: URL to the job posting on a platform
-   `application_url`: URL where applications can be submitted
-   `application_type`: Type of application process (offsite, complex/simple onsite)
-   `expiry`: Expiration date or time for the job listing
-   `closed_time`: Time to close job listing
-   `formatted_experience_level`: Job experience level (entry, associate, executive, etc)
-   `skills_desc`: Description detailing required skills for job
-   `listed_time`: Time when the job was listed
-   `posting_domain`: Domain of the website with application
-   `sponsored`: Whether the job listing is sponsored or promoted.
-   `work_type`: Type of work associated with the job
-   `currency`: Currency in which the salary is provided.
-   `compensation_type`: Type of compensation for the job.

Com isso, usaremos scripts para importar os dados para PostgreSQL e Cassandra e estruturar as tabelas em PostgreSQL e os keyspaces e tabelas em Cassandra conforme a natureza dos dados.

### Armazenamento de Dados:

-   Insira os dados no PostgreSQL e no Cassandra, garantindo que os dados sejam armazenados corretamente.
-   Avalie o tempo de inserção dos dados em ambos os sistemas.

### Consultas e Manipulação de Dados:

-   Desenvolva consultas SQL para PostgreSQL e CQL para Cassandra para manipular e extrair informações dos dados.
-   Realize operações de leitura, atualização e exclusão em ambos os sistemas.

### Comparação de Performance:

-   Meça a performance de operações de leitura, escrita, atualização e exclusão em ambos os sistemas.
    Avalie a escalabilidade e eficiência de recursos (CPU, memória, etc.).
-   Compare a facilidade de uso e flexibilidade de cada solução.

### Análise dos Resultados:

-   Documente os resultados das comparações de performance.
-   Utilize gráficos e tabelas para apresentar as diferenças de desempenho entre PostgreSQL e Cassandra.
-   Analise as vantagens e desvantagens de cada tecnologia no contexto dos dados de vagas de emprego.

### Relatório Final:

-   Compile todos os dados, análises e conclusões em um relatório final.
-   Apresente recomendações baseadas nos resultados obtidos.
-   Proponha possíveis melhorias ou ajustes para futuros estudos.

## Análises Específicas:

### Tempo Médio de Execuções:

-   Realize cada consulta três vezes e calcule o tempo médio de execução.
-   Compare os tempos médios entre PostgreSQL e Cassandra.

### Consultas Otimizadas:

-   Identifique consultas onde o NoSQL apresenta melhor desempenho e onde não performa tão bem.
-   Avalie a simplicidade e complexidade das consultas em ambos os sistemas.

### Consultas com Única Tabela:

-   Condicional Simples: Realize consultas com condições básicas.
-   Busca de Texto: Pesquise por termos específicos em campos textuais.
-   Agregações: Calcule valores máximos, mínimos e médias, com agrupamento e ordenação.

### Consultas com Duas Tabelas:

-   Cruzamento de Dados: Realize joins (mesmo que forçados) para normalizar dados.
-   Busca de Texto: Pesquise termos específicos cruzando dados de duas tabelas.
-   Agregações: Calcule valores máximos, mínimos e médias, com agrupamento e ordenação em duas tabelas.

### Análises Adicionais:

-   Consultas de Notas: Avalie consultas envolvendo valores máximos, mínimos e médias, com dados divididos em duas tabelas.
-   Buscas Textuais: Realize buscas por partes de textos, nomes e descrições.
-   Atualizações: Atualize dados via consultas ou diretamente nos arquivos/tabelas.

### Comparação de Desempenho:

-   Avalie a rapidez e simplicidade de execução.
-   Verifique se a modelagem dos dados foi adequada para cada sistema.
-   Determine qual opção apresentou melhor performance e facilidade de uso.

### Recomendações e Observações:

-   Com base nas análises, apresente recomendações sobre quando utilizar cada tecnologia.
-   Discuta possíveis aplicações práticas das descobertas.
-   Faça observações sobre a experiência de uso e os desafios enfrentados.

## Preparação do Ambiente:

### PostgreSQL setup

```sh
# Install PostgreSQL
sudo apt install postgresql postgresql-contrib

# Connect into PostgreSQL
psql -h localhost -p 5432 -U postgres

# Create database
CREATE DATABASE linkedin_job_postings;

# List databases
\l
```

### Cassandra setup

```sh
# Download Cassandra using docker
sudo docker pull cassandra:latest

# Create network
sudo docker network create cassandra

# Start Cassandra
sudo docker run --rm -d --name cassandra --hostname cassandra --network cassandra cassandra
```

#### Create test CQL file (data.cql)

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

#### Inserting data in Cassandra

```sh
# Inserting data in Cassandra from data.cql
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

### Python setup

```sh
# Download get-pip.py
curl -O https://bootstrap.pypa.io/get-pip.py

# Install pip for Python 3.10
python3.10 get-pip.py

# Verify pip installation
python3.10 -m pip --version

# Install required libraries
python3.10 -m pip install pandas sqlalchemy cassandra-driver

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

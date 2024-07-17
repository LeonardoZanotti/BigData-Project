import pandas as pd
import sqlalchemy
import psycopg2
from cassandra.cluster import Cluster
import time

# Configurações de conexão
postgres_conn_str = 'postgresql+psycopg2://postgres:postgres@localhost/bigdatabd3'
cassandra_cluster = Cluster(['127.0.0.1'])
keyspace = 'linkedin_job_postings'

# Função para medir o tempo de execução
def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        return result, end_time - start_time
    return wrapper

# Conexão com PostgreSQL
postgres_engine = sqlalchemy.create_engine(postgres_conn_str)
postgres_conn = postgres_engine.connect()

# Conexão com Cassandra
cassandra_session = cassandra_cluster.connect()

# Carregar dados do CSV
data = pd.read_csv('postings.csv')

# Importação para PostgreSQL
data.to_sql('postings', postgres_conn, if_exists='replace', index=False)

# Importação para Cassandra
cassandra_session.execute(f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
""")
cassandra_session.set_keyspace(keyspace)
cassandra_session.execute("""
CREATE TABLE IF NOT EXISTS postings (
    id UUID PRIMARY KEY,
    title TEXT,
    company TEXT,
    location TEXT,
    description TEXT,
    date_posted DATE
)
""")
for i, row in data.iterrows():
    cassandra_session.execute("""
    INSERT INTO postings (id, title, company, location, description, date_posted)
    VALUES (uuid(), %s, %s, %s, %s, %s)
    """, (row['title'], row['company'], row['location'], row['description'], row['date_posted']))

# Funções para realizar as consultas e medir o tempo
@measure_time
def postgres_query(query):
    return postgres_conn.execute(query).fetchall()

@measure_time
def cassandra_query(query):
    return cassandra_session.execute(query).all()

# Consultas
queries = {
    "condicional_simples": "SELECT * FROM postings WHERE location = 'San Francisco'",
    "busca_texto": "SELECT * FROM postings WHERE description LIKE '%data%'",
    "max_min_avg": "SELECT MAX(date_posted), MIN(date_posted), AVG(date_posted) FROM postings",
    "agrupamento": "SELECT company, COUNT(*) FROM postings GROUP BY company ORDER BY COUNT(*) DESC"
}

# Executar e medir o tempo das consultas no PostgreSQL
postgres_results = {}
for key, query in queries.items():it
    result, exec_time = postgres_query(query)
    postgres_results[key] = {"result": result, "time": exec_time}

# Executar e medir o tempo das consultas no Cassandra
cassandra_results = {}
for key, query in queries.items():
    result, exec_time = cassandra_query(query)
    cassandra_results[key] = {"result": result, "time": exec_time}

# Exibir os resultados
for key in queries.keys():
    print(f"Consulta: {key}")
    print(f"PostgreSQL: Tempo = {postgres_results[key]['time']:.4f}s")
    print(f"Cassandra: Tempo = {cassandra_results[key]['time']:.4f}s")
    print("\n")

# Fechar conexões
postgres_conn.close()
cassandra_cluster.shutdown()

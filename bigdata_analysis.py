import pandas as pd
import sqlalchemy
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time

# https://chatgpt.com/c/04611492-c55e-4a62-9c4b-dab35544b753
# https://www.overleaf.com/project/669834656ef23d0f88fe6c4b
# https://github.com/LeonardoZanotti/BigData-Project
# https://www.kaggle.com/datasets/arshkon/linkedin-job-postings
# https://www.dropbox.com/scl/fo/c3i0wbk636qltrkjqfhz7/AH-g4P2JjSSxbJnVqvWxa-g/ds340_aX2_ProjetoBigData.pdf?rlkey=tvjexe3zoryv98385ppy23fj0&e=2&dl=0

# Configurações de conexão
database = "linkedin_job_postings"
table = "job_postings"
psql_usr = "postgres"
psql_psw = "postgres"
psql_host = "localhost"

########################################################################################

# Função para medir o tempo de execução


def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        return result, end_time - start_time
    return wrapper


###################################### POSTGRESQL ##################################################
# Conexão com PostgreSQL
postgres_conn_str = "postgresql://{psql_usr}:{psql_psw}@{psql_host}/{database}"
postgresql_engine = sqlalchemy.create_engine(postgres_conn_str)
postgres_conn = postgresql_engine.connect()

# Ler o arquivo CSV
# Lendo apenas as primeiras 1000 linhas para testes
job_postings = pd.read_csv('datasets/postings.csv', nrows=100)

# Importar dados para PostgreSQL
job_postings.to_sql(table, postgresql_engine, if_exists='replace', index=False)

print("Dados importados para o PostgreSQL com sucesso.")

###################################### CASSANDRA ##################################################

# Configurações do Cassandra
keyspace = database
cassandra_cluster = Cluster(['127.0.0.1'])
cassandra_session = cassandra_cluster.connect()

# Criar keyspace e tabela
cassandra_session.execute("""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
""")

cassandra_session.set_keyspace(keyspace)

cassandra_session.execute("""
    CREATE TABLE IF NOT EXISTS {table} (
        job_id text PRIMARY KEY,
        company_id text,
        title text,
        description text,
        max_salary float,
        med_salary float,
        min_salary float,
        pay_period text,
        formatted_work_type text,
        location text,
        applies int,
        original_listed_time text,
        remote_allowed boolean,
        views int,
        job_posting_url text,
        application_url text,
        application_type text,
        expiry text,
        closed_time text,
        formatted_experience_level text,
        skills_desc text,
        listed_time text,
        posting_domain text,
        sponsored boolean,
        work_type text,
        currency text,
        compensation_type text
    )
""")

# Importar dados para Cassandra
for _, row in job_postings.iterrows():
    cassandra_session.execute("""
        INSERT INTO job_postings (job_id, company_id, title, description, max_salary, med_salary, min_salary,
                                  pay_period, formatted_work_type, location, applies, original_listed_time,
                                  remote_allowed, views, job_posting_url, application_url, application_type,
                                  expiry, closed_time, formatted_experience_level, skills_desc, listed_time,
                                  posting_domain, sponsored, work_type, currency, compensation_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))

print("Dados importados para o Cassandra com sucesso.")


###################################### FUNÇÕES ##################################################

# Funções para realizar as consultas e medir o tempo
@measure_time
def postgres_query(query):
    return postgres_conn.execute(query).fetchall()


@measure_time
def cassandra_query(query):
    return cassandra_session.execute(query).all()

###################################### QUERIES ##################################################


# PostgreSQL
queries = [
    {
        "description": "Salário máximo, mínimo e médio por tipo de trabalho",
        "sql": """
            SELECT formatted_work_type, MAX(max_salary) AS max_salary, MIN(min_salary) AS min_salary, AVG(med_salary) AS avg_salary
            FROM job_postings
            GROUP BY formatted_work_type
        """,
        "cql": """
            SELECT formatted_work_type, MAX(max_salary) AS max_salary, MIN(min_salary) AS min_salary, AVG(med_salary) AS avg_salary
            FROM job_postings
            GROUP BY formatted_work_type
        """
    },
    {
        "description": "Buscas por títulos contendo 'Engineer'",
        "sql": "SELECT * FROM job_postings WHERE title ILIKE '%Engineer%'",
        "cql": "SELECT * FROM job_postings WHERE title LIKE '%Engineer%' ALLOW FILTERING"
    },
    {
        "description": "Atualizar salário mínimo de 'Developer' para 4000",
        "sql": "UPDATE job_postings SET min_salary = 4000 WHERE title ILIKE '%Developer%'",
        "cql": "UPDATE job_postings SET min_salary = 4000 WHERE title LIKE '%Developer%'"
    }
]

###################################### EXECUÇÃO ##################################################

# Executar e medir o tempo das consultas no PostgreSQL e no Cassandra
results = []

for key, query in queries.items():
    psql_result, psql_exec_time = postgres_query(sqlalchemy.text(query["sql"]))
    cassandra_result, cassandra_exec_time = cassandra_query(
        SimpleStatement(query["cql"]))

    results.append({
        "description": query["description"],
        "psql_result": psql_result,
        "psql_time": psql_exec_time,
        "cassandra_result": cassandra_result,
        "cassandra_time": cassandra_exec_time
    })


# Exibir os resultados
for result in results:
    print(f"Consulta: {result['description']}")
    print(f"PostgreSQL: Tempo = {result['psql_time']:.4f}s")
    print(f"Cassandra: Tempo = {result['cassandra_time']:.4f}s")
    print("\n")

# Fechar conexões
postgres_conn.close()
cassandra_session.shutdown()
cassandra_cluster.shutdown()

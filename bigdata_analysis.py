import pandas as pd
import sqlalchemy
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BoundStatement, PreparedStatement
import time
from colorama import Fore, Style, init
from numpy import nan

# Inicializar colorama
init(autoreset=True)

# Configurações de conexão e definição de variáveis
database = "linkedin_job_postings"
table = "job_postings"
psql_usr = "postgres"
psql_psw = "postgres"
psql_host = "localhost"

# IP do container Cassandra
# Substitua pelo IP real do container
cassandra_container_ip = "172.20.0.2"

###################################### Helper Functions ##################################################

# Função para medir o tempo de execução


def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        return result, end_time - start_time
    return wrapper

# Função para converter valores para inserir no Cassandra


def safe_convert(value, dtype):
    """
    Safely convert value to the specified dtype, handling NaN and empty values by returning None.
    """
    if pd.isna(value) or value == '':
        return None
    try:
        if dtype == int:
            # Convert to float first to handle string numbers
            return int(float(value))
        elif dtype == float:
            return float(value)
        elif dtype == bool:
            return bool(int(value))
        elif dtype == str:
            return str(value)
    except ValueError:
        return None


###################################### POSTGRESQL ##################################################
try:
    # Começar timer para conexão e inserção de dados no PostgreSQL
    psql_insert_start_time = time.time()

    # Conexão com PostgreSQL
    postgres_conn_str = f"postgresql://{psql_usr}:{psql_psw}@{psql_host}/{database}"
    postgresql_engine = sqlalchemy.create_engine(postgres_conn_str)
    postgres_conn = postgresql_engine.connect()

    # Ler o arquivo CSV
    # Lendo apenas as primeiras 100 linhas para testes
    job_postings = pd.read_csv('datasets/postings.csv', nrows=100)

    # Importar dados para PostgreSQL
    job_postings.to_sql(table, postgresql_engine,
                        if_exists='replace', index=False)

    # Calcular tempo decorrido
    psql_insert_time = time.time() - psql_insert_start_time

    print(Fore.GREEN +
          f"Dados importados para o PostgreSQL com sucesso. Tempo decorrido: {psql_insert_time:.4f}s")
except Exception as e:
    print(Fore.RED + f"Erro ao importar dados para o PostgreSQL: {e}")

###################################### CASSANDRA ##################################################

try:
    # Começar timer para conexão e criação de tabela no Cassandra
    cassandra_create_start_time = time.time()

    # Configurações do Cassandra
    keyspace = database
    cassandra_cluster = Cluster([cassandra_container_ip])
    cassandra_session = cassandra_cluster.connect()

    # Criar keyspace e tabela
    cassandra_session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")

    # Criar keyspace
    # Correct way to create keyspace with f-string
    cassandra_session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}
    """)

    # Usar o keyspace
    cassandra_session.set_keyspace(keyspace)

    # Criar tabela
    cassandra_session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            job_id text PRIMARY KEY,
            company_name text,
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

    # Calcular tempo decorrido desde o inicio
    cassandra_create_time = time.time() - cassandra_create_start_time

    print(Fore.GREEN +
          f"Keyspace e tabela criados com sucesso no Cassandra. Tempo decorrido: {cassandra_create_time:.4f}s")
except Exception as e:
    print(
        Fore.RED + f"Erro ao criar keyspace ou tabela no Cassandra: {e}")

# Importar dados para Cassandra
try:
    # Começar timer para inserção de dados no Cassandra
    cassandra_insert_start_time = time.time()

    # Prepare the CQL statement
    insert_statement = f"""
        INSERT INTO {table} (
            job_id,
            company_name,
            company_id,
            title,
            description,
            max_salary,
            med_salary,
            min_salary,
            pay_period,
            formatted_work_type,
            location,
            applies,
            original_listed_time,
            remote_allowed,
            views,
            job_posting_url,
            application_url,
            application_type,
            expiry,
            closed_time,
            formatted_experience_level,
            skills_desc,
            listed_time,
            posting_domain,
            sponsored,
            work_type,
            currency,
            compensation_type
        ) VALUES (
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?
        )
    """

    # Prepare the statement
    prepared_statement = cassandra_session.prepare(insert_statement)

    # Insert data
    for _, row in job_postings.iterrows():
        # Example usage in the execute call
        cassandra_session.execute(prepared_statement, [
            safe_convert(row['job_id'], str),  # job_id
            safe_convert(row['company_name'], str),  # company_name
            safe_convert(row['company_id'], str),  # company_id
            safe_convert(row['title'], str),  # title
            safe_convert(row['description'], str),  # description
            safe_convert(row['max_salary'], float),  # max_salary
            safe_convert(row['med_salary'], float),  # med_salary
            safe_convert(row['min_salary'], float),  # min_salary
            safe_convert(row['pay_period'], str),  # pay_period
            safe_convert(row['formatted_work_type'],
                         str),  # formatted_work_type
            safe_convert(row['location'], str),  # location
            safe_convert(row['applies'], int),  # applies
            safe_convert(row['original_listed_time'],
                         str),  # original_listed_time
            safe_convert(row['remote_allowed'], bool),  # remote_allowed
            safe_convert(row['views'], int),  # views
            safe_convert(row['job_posting_url'], str),  # job_posting_url
            safe_convert(row['application_url'], str),  # application_url
            safe_convert(row['application_type'], str),  # application_type
            safe_convert(row['expiry'], str),  # expiry
            safe_convert(row['closed_time'], str),  # closed_time
            safe_convert(row['formatted_experience_level'],
                         str),  # formatted_experience_level
            safe_convert(row['skills_desc'], str),  # skills_desc
            safe_convert(row['listed_time'], str),  # listed_time
            safe_convert(row['posting_domain'], str),  # posting_domain
            safe_convert(row['sponsored'], bool),  # sponsored
            safe_convert(row['work_type'], str),  # work_type
            safe_convert(row['currency'], str),  # currency
            safe_convert(row['compensation_type'], str)  # compensation_type
        ])

    # Calcular tempo decorrido desde o inicio
    cassandra_insert_time = time.time() - cassandra_insert_start_time

    print(Fore.GREEN +
          f"Dados importados para o Cassandra com sucesso. Tempo decorrido: {cassandra_insert_time:.4f}s")
except Exception as e:
    print(Fore.RED + f"Erro ao importar dados para o Cassandra: {e}")

###################################### FUNÇÕES ##################################################

# Funções para realizar as consultas e medir o tempo


@measure_time
def postgres_query(query):
    try:
        result = postgres_conn.execute(query)
        if str(query).strip().upper().startswith("UPDATE"):
            postgres_conn.commit()
            return [], 0  # No rows to return for UPDATE queries
        return result.fetchall()
    except Exception as e:
        print(Fore.RED + f"Erro ao executar a consulta no PostgreSQL: {e}")
        return [], 0


@measure_time
def cassandra_query(query):
    try:
        return cassandra_session.execute(SimpleStatement(query)).all()
    except Exception as e:
        print(Fore.RED + f"Erro ao executar a consulta no Cassandra: {e}")
        return [], 0

###################################### QUERIES ##################################################


# Definir queries
queries = [
    # {
    #     "description": "Salário máximo, mínimo e médio por tipo de trabalho",
    #     "sql": """
    #         SELECT formatted_work_type, MAX(max_salary) AS max_salary, MIN(min_salary) AS min_salary, AVG(med_salary) AS avg_salary
    #         FROM job_postings
    #         GROUP BY formatted_work_type
    #     """,
    #     "cql": """
    #         SELECT formatted_work_type, MAX(max_salary) AS max_salary, MIN(min_salary) AS min_salary, AVG(med_salary) AS avg_salary
    #         FROM job_postings
    #         GROUP BY formatted_work_type
    #     """
    # },
    # {
    #     "description": "Buscas por títulos contendo 'Engineer'",
    #     "sql": "SELECT * FROM job_postings WHERE title ILIKE '%Engineer%'",
    #     "cql": "SELECT * FROM job_postings WHERE title LIKE '%Engineer%' ALLOW FILTERING"
    # },
    # {
    #     "description": "Atualizar o salário mínimo de todas as vagas para 4000",
    #     "sql": "UPDATE job_postings SET min_salary = 4000",
    #     "cql": "UPDATE job_postings SET min_salary = 4000"
    # }
    {
        "description": "Função agregada",
        "sql": "SELECT COUNT(*) FROM job_postings",
        "cql": "SELECT COUNT(*) FROM job_postings"
    },
    {
        "description": "Leitura simples por chave primária",
        "sql": "SELECT * FROM job_postings WHERE job_id = '2147609816'",
        "cql": "SELECT * FROM job_postings WHERE job_id = '2147609816'"
    },
    {
        "description": "Atualização por chave primária",
        "sql": "UPDATE job_postings SET title = 'Updated Job' WHERE job_id = '2974397965'",
        "cql": "UPDATE job_postings SET title = 'Updated Job' WHERE job_id = '2974397965'"
    },
    {
        "description": "Filtragem por índice secundário",
        "sql": "SELECT * FROM job_postings WHERE formatted_work_type = 'Full-time'",
        "cql": "SELECT * FROM job_postings WHERE formatted_work_type = 'Full-time' ALLOW FILTERING"
    }
]

###################################### EXECUÇÃO ##################################################

# Executar e medir o tempo das consultas no PostgreSQL e no Cassandra

for query in queries:
    print(Fore.YELLOW + f"Executando: {query['description']}")
    psql_result, psql_time = postgres_query(sqlalchemy.text(query['sql']))
    print(Fore.CYAN +
          f"PostgreSQL: {psql_time:.4f}s | Resultados: {len(psql_result)}")

    cass_result, cass_time = cassandra_query(query['cql'])
    print(Fore.MAGENTA +
          f"Cassandra: {cass_time:.4f}s | Resultados: {len(cass_result)}")

print(Fore.YELLOW + "Comparação finalizada.")

# Fechar conexões
postgres_conn.close()
cassandra_session.shutdown()
cassandra_cluster.shutdown()

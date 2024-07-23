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
file = 'datasets/postings.csv'

try:
    print(Fore.YELLOW + f"Lendo arquivo {file}")
    job_postings = pd.read_csv(file, nrows=100)
    print(Fore.LIGHTGREEN_EX +
          f"{len(job_postings)} linhas lidas com sucesso!")
except Exception as e:
    print(Fore.RED + "Falha ao ler o arquivo CSV: " + str(e))
    exit()

# Configuração do PostgreSQL
postgres_conn_str = f"postgresql://{psql_usr}:{psql_psw}@{psql_host}/{database}"
postgresql_engine = sqlalchemy.create_engine(postgres_conn_str)

# Configurações do Cassandra
cassandra_container_ip = "172.20.0.2"  # Substitua pelo IP real do container
keyspace = database

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

    with postgresql_engine.connect() as postgres_conn:
        # Importar dados para PostgreSQL
        job_postings.to_sql(table, postgresql_engine,
                            if_exists='replace', index=False)

        # Calcular tempo decorrido
        psql_insert_time = time.time() - psql_insert_start_time

        print(
            Fore.GREEN + f"Dados importados para o PostgreSQL com sucesso. Tempo decorrido: {psql_insert_time:.4f}s")
except Exception as e:
    print(Fore.RED + f"Erro ao importar dados para o PostgreSQL: {e}")

###################################### CASSANDRA ##################################################
try:
    # Começar timer para conexão e criação de tabela no Cassandra
    cassandra_create_start_time = time.time()

    with Cluster([cassandra_container_ip]) as cassandra_cluster:
        with cassandra_cluster.connect() as cassandra_session:
            # Criar keyspace e tabela
            cassandra_session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")

            # Criar keyspace
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

            print(
                Fore.GREEN + f"Keyspace e tabela criados com sucesso no Cassandra. Tempo decorrido: {cassandra_create_time:.4f}s")

            ###################################### Importar dados para Cassandra ######################################
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
                prepared_statement = cassandra_session.prepare(
                    insert_statement)

                # Insert data
                for _, row in job_postings.iterrows():
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
                        # formatted_work_type
                        safe_convert(row['formatted_work_type'], str),
                        safe_convert(row['location'], str),  # location
                        safe_convert(row['applies'], int),  # applies
                        # original_listed_time
                        safe_convert(row['original_listed_time'], str),
                        safe_convert(row['remote_allowed'],
                                     bool),  # remote_allowed
                        safe_convert(row['views'], int),  # views
                        safe_convert(row['job_posting_url'],
                                     str),  # job_posting_url
                        safe_convert(row['application_url'],
                                     str),  # application_url
                        safe_convert(row['application_type'],
                                     str),  # application_type
                        safe_convert(row['expiry'], str),  # expiry
                        safe_convert(row['closed_time'], str),  # closed_time
                        # formatted_experience_level
                        safe_convert(row['formatted_experience_level'], str),
                        safe_convert(row['skills_desc'], str),  # skills_desc
                        safe_convert(row['listed_time'], str),  # listed_time
                        safe_convert(row['posting_domain'],
                                     str),  # posting_domain
                        safe_convert(row['sponsored'], bool),  # sponsored
                        safe_convert(row['work_type'], str),  # work_type
                        safe_convert(row['currency'], str),  # currency
                        safe_convert(row['compensation_type'],
                                     str)  # compensation_type
                    ])

                # Calcular tempo decorrido desde o inicio
                cassandra_insert_time = time.time() - cassandra_insert_start_time

                print(
                    Fore.GREEN + f"Dados importados para o Cassandra com sucesso. Tempo decorrido: {cassandra_insert_time:.4f}s")
            except Exception as e:
                print(
                    Fore.RED + f"Erro ao importar dados para o Cassandra: {e}")

except Exception as e:
    print(Fore.RED + f"Erro ao criar keyspace ou tabela no Cassandra: {e}")

###################################### FUNÇÕES ##################################################
# Funções para realizar as consultas e medir o tempo


@measure_time
def postgres_query(query):
    try:
        with postgresql_engine.connect() as postgres_conn:
            result = postgres_conn.execute(sqlalchemy.text(query))
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
        with Cluster([cassandra_container_ip]) as cassandra_cluster:
            with cassandra_cluster.connect() as cassandra_session:
                cassandra_session.set_keyspace(keyspace)
                return cassandra_session.execute(SimpleStatement(query)).all()
    except Exception as e:
        print(Fore.RED + f"Erro ao executar a consulta no Cassandra: {e}")
        return []

# Função para medir o tempo das consultas


def run_and_print_results(label, query):
    postgres_results, postgres_time = postgres_query(query)

    # Erro ao executar a consulta no Cassandra: Error from server: code=2200 [Invalid query] message="Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"
    if (label == "Filtragem por índice secundário"):
        query += " ALLOW FILTERING"

    cassandra_results, cassandra_time = cassandra_query(query)

    print(Fore.YELLOW + f"\n{label}")
    print(Fore.CYAN +
          f"PostgreSQL - Tempo: {postgres_time:.4f}s, Resultados: {len(postgres_results)}")
    print(Fore.MAGENTA +
          f"Cassandra - Tempo: {cassandra_time:.4f}s, Resultados: {len(cassandra_results)}")


# Consultas
queries = [
    {
        "description": "Função agregada - Salário máximo",
        "query": f"SELECT MAX(max_salary) FROM {table}",
    },
    {
        "description": "Leitura simples por chave primária",
        "query": f"SELECT * FROM {table} WHERE job_id = '2147609816'",
    },
    {
        "description": "Atualização por chave primária",
        "query": f"UPDATE {table} SET title = 'Updated Job' WHERE job_id = '2974397965'",
    },
    {
        "description": "Filtragem por índice secundário",
        "query": f"SELECT * FROM {table} WHERE formatted_work_type = 'Full-time'",
    }
]

for query in queries:
    run_and_print_results(query['description'],
                          query['query'])

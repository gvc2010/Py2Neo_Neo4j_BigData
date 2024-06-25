"""
iNSTALAÇÃO DO PY2NEO(NEO4J)
"""

!pip install py2neo

from py2neo import Graph

# Informações de conexão
server_address = "ee6de833.databases.neo4j.io:7687"  # Endereço IP ou hostname da sua instância do Neo4j
username = "neo4j"            # Nome de usuário do Neo4j
password = "your_password"               # Senha do Neo4j

# Construir a URI de conexão
uri = f"neo4j+s://{'ee6de833.databases.neo4j.io'}:7687"

# Conectar ao Neo4j
graph = Graph(uri, auth=(username, password))

import pandas as pd

# Carregar arquivos CSV
city_df = pd.read_csv('city.csv', sep=';')
customer_df = pd.read_csv('customer.csv', sep=';')
language_df = pd.read_csv('language.csv', sep=';')

# Lista para armazenar os DataFrames de cada linha lida com sucesso
film_dfs = []

# Tentar ler o arquivo CSV linha por linha
with open('film.csv', 'r', encoding='utf-8') as file:
    for line in file:
        try:
            # Tentar criar um DataFrame com a linha atual
            film_df = pd.read_csv(StringIO(line), sep=',')
            film_dfs.append(film_df)
        except pd.errors.ParserError:
            # Se ocorrer um erro ao criar o DataFrame, ignorar a linha
            continue

# Concatenar os DataFrames individuais em um único DataFrame
film_df = pd.concat(film_dfs, ignore_index=True)

# Iterar sobre os DataFrames e criar nodos no Neo4j para cada um deles
for _, row in city_df.iterrows():
    query = f"CREATE (:City {{name: '{row['city']}'}})"
    graph.run(query)

for _, row in customer_df.iterrows():
    query = f"CREATE (:Customer {{name: '{row['first_name']} {row['last_name']}'}})"
    graph.run(query)

for _, row in film_df.iterrows():
    query = f"CREATE (:Film {{title: '{row['title']}', language: '{row['language_id']}', release_year: {row['release_year']}}})"
    graph.run(query)

for _, row in language_df.iterrows():
    query = f"CREATE (:Language {{name: '{row['name']}'}})"
    graph.run(query)

# Criar relacionamentos entre os nodos
# Relacionar clientes às cidades onde moram
query = """
MATCH (c:Customer), (city:City {name: c.city})
CREATE (c)-[:LIVES_IN]->(city)
"""
graph.run(query)

# Relacionar filmes ao idioma em que foram produzidos
query = """
MATCH (f:Film), (language:Language {name: f.language})
CREATE (f)-[:IN_LANGUAGE]->(language)
"""
graph.run(query)

# Consulta 1: Quantos filmes existem em cada língua/idioma?
query = """
MATCH (l:Language)<-[:IN_LANGUAGE]-(f:Film)
RETURN l.name AS Language, COUNT(f) AS Number_of_Films
ORDER BY Number_of_Films DESC
"""
result = graph.run(query)
print("Consulta 1: Quantos filmes existem em cada língua/idioma?")
for record in result:
    print(record)

# Consulta 2: Quantos clientes moram na mesma cidade?
query = """
MATCH (c:Customer)-[:LIVES_IN]->(city)
WITH city, COUNT(c) AS Number_of_Customers
RETURN city.name AS City, Number_of_Customers
ORDER BY Number_of_Customers DESC
"""
result = graph.run(query)
print("\nConsulta 2: Quantos clientes moram na mesma cidade?")
for record in result:
    print(record)

from py2neo import Graph
import pandas as pd
from io import StringIO

# Informações de conexão
server_address = "ee6de833.databases.neo4j.io:7687"  # Endereço IP ou hostname da sua instância do Neo4j
username = "neo4j"            # Nome de usuário do Neo4j
password = "ObOlvmi9Ad5nBl9dzywrhbuhK0bTnpGVjvA4-2iZZP4"  # Senha do Neo4j

# Construir a URI de conexão
uri = f"neo4j+s://{server_address}"

# Conectar ao Neo4j
graph = Graph(uri, auth=(username, password))

# Carregar arquivos CSV
city_df = pd.read_csv('city.csv', sep=';')
customer_df = pd.read_csv('customer.csv', sep=';')
language_df = pd.read_csv('language.csv', sep=';')

# Lista para armazenar os DataFrames de cada linha lida com sucesso
film_dfs = []

# Tentar ler o arquivo CSV linha por linha
with open('film.csv', 'r', encoding='utf-8') as file:
    for line in file:
        try:
            # Tentar criar um DataFrame com a linha atual
            film_df = pd.read_csv(StringIO(line), sep=',')
            film_dfs.append(film_df)
        except pd.errors.ParserError:
            # Se ocorrer um erro ao criar o DataFrame, ignorar a linha
            continue

# Concatenar os DataFrames individuais em um único DataFrame
film_df = pd.concat(film_dfs, ignore_index=True)

# Iterar sobre os DataFrames e criar nodos no Neo4j para cada um deles
for _, row in city_df.iterrows():
    query = f"CREATE (:City {{name: '{row['city']}'}})"
    graph.run(query)

for _, row in customer_df.iterrows():
    query = f"CREATE (:Customer {{name: '{row['first_name']} {row['last_name']}'}})"
    graph.run(query)

for _, row in film_df.iterrows():
    query = f"CREATE (:Film {{title: '{row['title']}', language: '{row['language_id']}', release_year: {row['release_year']}}})"
    graph.run(query)

for _, row in language_df.iterrows():
    query = f"CREATE (:Language {{name: '{row['name']}'}})"
    graph.run(query)

''''
# Criar relacionamentos entre os nodos

# Relacionar clientes às cidades onde moram
query = """
MATCH (c:Customer), (city:City {name: c.city})
CREATE (c)-[:LIVES_IN]->(city)
"""
graph.run(query)

# Relacionar filmes ao idioma em que foram produzidos
query = """
MATCH (f:Film), (language:Language {name: f.language})
CREATE (f)-[:IN_LANGUAGE]->(language)
"""
graph.run(query)

# Consulta 1: Quantos filmes existem em cada língua/idioma?
query = """
MATCH (l:Language)<-[:IN_LANGUAGE]-(f:Film)
RETURN l.name AS Language, COUNT(f) AS Number_of_Films
ORDER BY Number_of_Films DESC
"""
result = graph.run(query)
print("Consulta 1: Quantos filmes existem em cada língua/idioma?")
for record in result:
    print(record)

# Consulta 2: Quantos clientes moram na mesma cidade?
query = """
MATCH (c:Customer)-[:LIVES_IN]->(city)
WITH city, COUNT(c) AS Number_of_Customers
RETURN city.name AS City, Number_of_Customers
ORDER BY Number_of_Customers DESC
"""
result = graph.run(query)
print("\nConsulta 2: Quantos clientes moram na mesma cidade?")
for record in result:
    print(record)

# Consulta 1: Quantos filmes existem em cada língua/idioma?
query_1 = """
MATCH (l:Language)<-[:IN_LANGUAGE]-(f:Film)
RETURN l.name AS Language, COUNT(f) AS Number_of_Films
ORDER BY Number_of_Films DESC
"""
result_1 = graph.run(query_1)

print("Consulta 1: Quantos filmes existem em cada língua/idioma?")
for record in result_1:
    print(record["Language"], "|", record["Number_of_Films"])

# Consulta 2: Quantos clientes moram na mesma cidade?
query_2 = """
MATCH (c:Customer)-[:LIVES_IN]->(city)
WITH city, COUNT(c) AS Number_of_Customers
RETURN city.name AS City, Number_of_Customers
ORDER BY Number_of_Customers DESC
"""
result_2 = graph.run(query_2)

print("\nConsulta 2: Quantos clientes moram na mesma cidade?")
for record in result_2:
    print(record["City"], "|", record["Number_of_Customers"])

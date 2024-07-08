## Descrição Geral

Este script conecta-se a uma base de dados Neo4j e cria nodos (nós) e relacionamentos a partir de dados contidos em arquivos CSV. Em seguida, ele executa algumas consultas para obter informações úteis sobre os dados.

## Etapas do Código
Instalação do Py2Neo:

O script começa instalando a biblioteca py2neo, que permite interagir com o banco de dados Neo4j usando Python.
Configuração da Conexão com Neo4j:

Define as informações de conexão, como o endereço do servidor Neo4j, o nome de usuário e a senha.
Cria uma URI de conexão e conecta ao banco de dados Neo4j usando estas credenciais.
Carregamento dos Arquivos CSV:

Utiliza a biblioteca pandas para carregar dados dos arquivos CSV (city.csv, customer.csv e language.csv) em DataFrames.
Leitura e Tratamento do Arquivo film.csv:

Tenta ler o arquivo film.csv linha por linha e armazena cada linha lida com sucesso em um DataFrame. Isso é útil para tratar possíveis erros de leitura em arquivos grandes ou mal formatados.
Criação de Nodos no Neo4j:

Itera sobre cada linha dos DataFrames e cria nodos no banco de dados Neo4j para City (cidade), Customer (cliente), Film (filme) e Language (idioma).
Criação de Relacionamentos entre Nodos:

## Cria relacionamentos entre os nodos, como:
Relacionar clientes às cidades onde moram.
Relacionar filmes ao idioma em que foram produzidos.
Execução de Consultas:

## Executa duas consultas para extrair informações úteis:
Quantidade de filmes existentes em cada idioma.
Quantidade de clientes que moram na mesma cidade.

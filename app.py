import streamlit as st
import pandas as pd
from ydata_profiling import ProfileReport
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import requests
import os
from datetime import datetime

# Configuração da página
st.set_page_config(page_title="Análise Exploratória", layout="wide")

# Título do App
st.title("Análise Exploratória de Dados")

# Função para carregar dados de um arquivo CSV
def load_csv(file):
    try:
        return pd.read_csv(file)
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo CSV: {e}")
        return None

# Função para carregar dados de uma planilha pública do Google Sheets
def load_public_google_sheet(sheet_url):
    try:
        csv_url = sheet_url.replace("/edit#gid=", "/export?format=csv&gid=")
        response = requests.get(csv_url)
        response.raise_for_status()
        data = pd.read_csv(pd.compat.StringIO(response.text))
        return data
    except Exception as e:
        st.error(f"Erro ao carregar a planilha pública: {e}")
        return None

# Função para conectar ao banco PostgreSQL e listar schemas
def connect_postgresql_with_schemas(username, password, host, port, database):
    try:
        # Criando o engine de conexão
        engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")
        
        # Consultando os schemas disponíveis
        schema_query = text("SELECT schema_name FROM information_schema.schemata;")
        with engine.connect() as connection:
            schemas_result = connection.execute(schema_query)
            schemas = [row[0] for row in schemas_result]

        return engine, schemas
    except Exception as e:
        st.error(f"Erro ao conectar ao banco PostgreSQL: {e}")
        return None, []

# Função para listar tabelas do schema selecionado
def get_tables_from_schema(engine, schema_name):
    try:
        query = text(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = :schema;
        """)
        with engine.connect() as connection:
            result = connection.execute(query, {"schema": schema_name})
            tables = [row[0] for row in result]
        return tables
    except Exception as e:
        st.error(f"Erro ao listar tabelas do schema {schema_name}: {e}")
        return []

# Função para carregar dados de uma tabela selecionada do PostgreSQL
def load_table_data(engine, schema_name, table_name):
    try:
        df = pd.read_sql_table(table_name, con=engine, schema=schema_name)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados da tabela {table_name} no schema {schema_name}: {e}")
        return None

# Função para gerar o relatório e salvá-lo como HTML
def generate_report(dataframe, table_name):
    try:
        # Criando o perfil dos dados
        profile = ProfileReport(dataframe, explorative=True)
        
        # Configurando a pasta de saída
        reports_dir = "reports"
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)
        
        # Gerando nome do arquivo com base na tabela, data e hora
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        relative_path = os.path.join(reports_dir, f"{table_name}_relatorio_{timestamp}.html")
        absolute_path = os.path.abspath(relative_path)  # Obtendo o caminho absoluto
        
        # Salvando o relatório
        profile.to_file(absolute_path)
        return absolute_path
    except Exception as e:
        raise Exception(f"Erro ao gerar relatório: {e}")


# Função para obter o nome do arquivo CSV
def get_csv_name(uploaded_file):
    try:
        return uploaded_file.name.split(".")[0]  # Retorna o nome do arquivo sem a extensão
    except Exception as e:
        st.error(f"Erro ao obter o nome do arquivo CSV: {e}")
        return "csv_desconhecido"

# Função para obter o nome do Google Sheet (extrai do link)
def get_sheet_name(sheet_url):
    try:
        return sheet_url.split("/")[-1] or "planilha_desconhecida"
    except Exception as e:
        st.error(f"Erro ao obter o nome da planilha: {e}")
        return "planilha_desconhecida"

# Interface para seleção da fonte de dados
st.header("Escolha a fonte de dados")
data_source = st.radio("Fonte de Dados", ["CSV", "Google Sheets", "PostgreSQL"])

df = None
table_name = None

# Limpar o estado ao mudar a fonte de dados
if "last_data_source" not in st.session_state:
    st.session_state.last_data_source = None

if st.session_state.last_data_source != data_source:
    st.session_state.report_path = None  # Limpa o relatório gerado
    st.session_state.last_data_source = data_source  # Atualiza o estado

if data_source == "CSV":
    uploaded_file = st.file_uploader("Faça upload do arquivo CSV", type=["csv"])
    
    if uploaded_file is not None:
        table_name = get_csv_name(uploaded_file)
        
        # Botão para carregar dados do CSV
        if st.button("Analisar Dados do CSV"):
            with st.spinner("Carregando dados do CSV..."):
                df = load_csv(uploaded_file)
                st.session_state.report_path = None  # Resetar o relatório

elif data_source == "Google Sheets":
    sheet_url = st.text_input("Insira o link da Planilha Google")
    
    if sheet_url:
        # Botão para carregar dados da planilha
        if st.button("Analisar Dados da Planilha Google Sheets"):
            with st.spinner("Carregando dados da planilha..."):
                df = load_public_google_sheet(sheet_url)
                table_name = get_sheet_name(sheet_url)
                st.session_state.report_path = None  # Resetar o relatório

elif data_source == "PostgreSQL":
    st.subheader("Credenciais do Banco de Dados PostgreSQL")
    username = st.text_input("Usuário")
    password = st.text_input("Senha", type="password")
    host = st.text_input("Host")
    port = st.text_input("Porta", value="5432")
    database = st.text_input("Nome do Banco de Dados")

    # Persistindo o estado da conexão
    if "engine" not in st.session_state:
        st.session_state.engine = None
        st.session_state.schemas = []
        st.session_state.tables = []

    if st.button("Conectar"):
        engine, schemas = connect_postgresql_with_schemas(username, password, host, port, database)
        if engine and schemas:
            st.success("Conexão bem-sucedida!")
            st.session_state.engine = engine
            st.session_state.schemas = schemas

    # Se já estivermos conectados, exibir a lista de schemas
    if st.session_state.engine is not None and st.session_state.schemas:
        selected_schema = st.selectbox("Selecione um schema", st.session_state.schemas)

        if "selected_schema" not in st.session_state or st.session_state.selected_schema != selected_schema:
            st.session_state.selected_schema = selected_schema
            st.session_state.tables = []  # Limpa as tabelas ao alterar o schema

        if st.session_state.selected_schema:
            # Listar tabelas do schema selecionado
            if not st.session_state.tables:
                tables = get_tables_from_schema(st.session_state.engine, selected_schema)
                st.session_state.tables = tables if tables else []

            # Exibir selectbox para tabelas
            if st.session_state.tables:
                selected_table = st.selectbox("Selecione uma tabela", st.session_state.tables)

                if st.button("Carregar Dados da Tabela"):
                    with st.spinner("Carregando a tabela..."):
                        df = load_table_data(st.session_state.engine, selected_schema, selected_table)
                        table_name = selected_table
                        st.session_state.report_path = None  # Resetar o relatório para regenerar
            else:
                st.warning("Nenhuma tabela encontrada no schema selecionado.")
    else:
        st.warning("Não há schemas disponíveis. Verifique as credenciais do banco ou a conexão.")

# Gerar relatório se os dados forem carregados
if df is not None and table_name is not None:
    st.write("Dados carregados com sucesso!")
    st.write(df.head())  # Exibe uma amostra dos dados

    # Gerar o relatório apenas se ainda não estiver no estado
    if "report_path" not in st.session_state or st.session_state.report_path is None:
        with st.spinner("Gerando o relatório..."):
            report_path = generate_report(df, table_name)
            st.session_state.report_path = report_path  # Armazenar no estado

    # Usar o relatório armazenado no estado
    report_path = st.session_state.report_path

    # Exibindo mensagem de sucesso
    st.success(f"Relatório HTML gerado com sucesso para '{table_name}'!")

    # Exibir um campo de texto somente leitura com o caminho do relatório
    st.text_input("Caminho do Relatório", value=f"file://{report_path}", disabled=False)

    # Exibindo o relatório diretamente na página com largura total
    st.markdown("### Visualização do Relatório")
    with open(report_path, "r", encoding="utf-8") as file:
        html_content = file.read()

    # Mostrando o HTML dentro de um iframe
    st.components.v1.html(html_content, height=1000, scrolling=True)
else:
    st.info("Por favor, selecione uma fonte de dados para começar.")

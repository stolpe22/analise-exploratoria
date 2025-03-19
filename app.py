import streamlit as st
import pandas as pd
from ydata_profiling import ProfileReport

# Configuração da página
st.set_page_config(page_title="Análise Exploratória", layout="wide")

# Título do App
st.title("Análise Exploratória de Dados com YData Profiling (CSV)")

# Função para carregar dados de um arquivo CSV
def load_data(file):
    try:
        return pd.read_csv(file)
    except Exception as e:
        st.error(f"Erro ao carregar o arquivo CSV: {e}")
        return None

# Função para gerar o relatório e salvá-lo como HTML
def generate_report(dataframe):
    profile = ProfileReport(dataframe, explorative=True)
    output_path = "relatorio.html"
    profile.to_file(output_path)
    return output_path

# Interface: Upload do arquivo CSV
st.header("Faça upload do seu arquivo CSV")
uploaded_file = st.file_uploader("Upload do arquivo", type=["csv"])

# Lógica Principal
if uploaded_file:
    # Carregando os dados
    df = load_data(uploaded_file)
    if df is not None:
        st.write("Dados carregados com sucesso!")
        st.write(df.head())  # Exibe uma amostra dos dados

        # Gerando o relatório
        with st.spinner("Gerando o relatório..."):
            report_path = generate_report(df)
        
        # Exibindo mensagem de sucesso
        st.success("Relatório HTML gerado com sucesso!")

        # Exibindo o relatório diretamente na página com largura total
        st.markdown("### Visualização do Relatório")
        with open(report_path, "r", encoding="utf-8") as file:
            html_content = file.read()

        # Mostrando o HTML dentro de um iframe
        st.components.v1.html(html_content, height=1000, scrolling=True)
else:
    st.info("Por favor, faça o upload de um arquivo CSV para começar.")

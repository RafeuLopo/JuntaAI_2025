from flask import Flask, jsonify, request
import pandas as pd
from openai import OpenAI
import os
import numpy as np

app = Flask(__name__)

df = pd.read_parquet(r'df_churn_cliente.parquet')

df = df.where(pd.notnull(df), None)

df_cluster = pd.read_parquet(r'df_churn_cluster.parquet')

client = OpenAI(api_key="API_KEY")

def serializar_dados(dados):
    """Converte tipos de dados do NumPy para tipos nativos do Python."""
    if isinstance(dados, list):
        return [serializar_dados(item) for item in dados]
    if isinstance(dados, dict):
        return {key: serializar_dados(value) for key, value in dados.items()}
    if isinstance(dados, (np.integer, np.int64)):
        return int(dados)
    if isinstance(dados, (np.floating, np.float64)):
        return float(dados)
    if isinstance(dados, np.ndarray):
        return dados.tolist()
    if pd.isna(dados):
        return None
    return dados

# --- Rotas da API ---
@app.route("/")
def home():
    return "API de Clientes e Clusters - Dados dos DataFrames"

@app.route("/clientes", methods=["GET"])
def listar_clientes():
    """Lista os primeiros N clientes."""
    limit = int(request.args.get("limit", 10))
    dados_brutos = df.head(limit).to_dict(orient="records")
    dados_serializados = serializar_dados(dados_brutos)
    return jsonify(dados_serializados)

@app.route("/cliente/", methods=["GET"])
def cliente_por_codigo(cd_cliente):
    """Retorna os dados de um cliente específico."""
    cliente = df[df["CD_CLIENTE"] == cd_cliente]
    if cliente.empty:
        return jsonify({"erro": "Cliente não encontrado"}), 404

    dados_brutos = cliente.iloc[0].to_dict()
    dados_serializados = serializar_dados(dados_brutos)
    return jsonify(dados_serializados)

@app.route("/clusters", methods=["GET"])
def get_all_clusters():
    '''Retorna todos os clusters.'''
    limit = int(request.args.get("limit", len(df_cluster)))
    dados_brutos = df_cluster.head(limit).to_dict(orient="records")
    dados_serializados = serializar_dados(dados_brutos)
    return jsonify(dados_serializados)

@app.route('/cluster/', methods=['GET'])
def get_cluster_data(id_cluster):
    '''Retorna os dados de um cluster específico.'''
    cluster = df_cluster[df_cluster["rotulos40"] == id_cluster]
    if cluster.empty:
        return jsonify({"erro": "Cluster não encontrado"}), 404

    dados_brutos = cluster.iloc[0].to_dict()
    dados_serializados = serializar_dados(dados_brutos)
    return jsonify(dados_serializados)

@app.route("/cliente//genai", methods=["GET"])
def cliente_genai(cd_cliente):
    '''Gera a saída, utilizando a API da OpenAI para os clientes'''
    cliente = df[df["CD_CLIENTE"] == cd_cliente]
    if cliente.empty:
        return jsonify({"erro": "Cliente não encontrado"}), 404

    dados_cliente_brutos = cliente.iloc[0].to_dict()
    dados_cliente = serializar_dados(dados_cliente_brutos)

    prompt = f"""
Você é um analista de dados explicando os dados de um cliente para um stakeholder não técnico.
Com base nas informações abaixo, forneça um resumo compreensível em linguagem natural, com insights sobre:
- Produtos que o cliente possui, Tempo de cliente, Segmento, Quantidade de contratos, Valor dos contratos, MRR,
- Contratações nos últimos 12 meses, Probabilidade de churn, Cluster do cliente, Qualidade percebida (baseado nas notas)
Formato sugerido: título, parágrafos curtos, listas se útil.
Dados do cliente:
{dados_cliente}"""

    try:
        response = client.chat.completions.create(model="gpt-4", messages=[{"role": "system", "content": "Você é um assistente de análise de dados para stakeholders."}, {"role": "user", "content": prompt}], temperature=0.7, max_tokens=800)
        interpretacao = response.choices[0].message.content
        return jsonify({"interpretacao": interpretacao})
    except Exception as e:
        return jsonify({"erro": f"Erro ao acessar OpenAI: {str(e)}"}), 500

@app.route("/cluster//genai", methods=["GET"])
def cluster_genai(id_cluster):
    '''Gera a saída, utilizando a API da OpenAI para os clusters'''
    cluster = df_cluster[df_cluster["rotulos40"] == id_cluster]
    if cluster.empty:
        return jsonify({"erro": "Cluster não encontrado"}), 404

    dados_cluster_brutos = cluster.iloc[0].to_dict()
    dados_cluster = serializar_dados(dados_cluster_brutos)

    prompt = f"""
Você é um analista de dados explicando os dados de um cluster de clientes para um stakeholder não técnico.
Com base nas informações abaixo, forneça um resumo claro, com insights sobre:
- Pontuações de NPS por etapa, Engajamento baseado na quantidade de clientes que deram notas,
- Distribuição de chamados por tipo e por status, Implicações práticas (ex: onde há problemas, oportunidades, recomendações)
- Probabilidade de Churn geral do cluster, quantidade de clientes com probabilidade de churn maior que 50%
Formato sugerido: título, parágrafos curtos, listas se necessário.
Dados do cluster:
{dados_cluster}"""

    try:
        response = client.chat.completions.create(model="gpt-4", messages=[{"role": "system", "content": "Você é um assistente de análise de dados para stakeholders."}, {"role": "user", "content": prompt}], temperature=0.7, max_tokens=1000)
        interpretacao = response.choices[0].message.content
        return jsonify({"interpretacao": interpretacao})
    except Exception as e:
        return jsonify({"erro": f"Erro ao acessar OpenAI: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(debug=True)
     
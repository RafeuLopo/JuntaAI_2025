import logging
import os
import pandas as pd
from io import BytesIO
import azure.functions as func
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# --- Função utilitária que processa a telemetria ---
def processar_telemetria():
    # --- ETAPA 1: CONEXÃO E LISTAGEM ---
    logging.info("--- INICIANDO PROCESSAMENTO COMPLETO ---")
    connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    container_silver = "silver"
    logging.info(f"A procurar por blobs no contentor '{container_silver}'...")
    blobs = blob_service_client.get_container_client(container_silver).list_blobs()
    telemetria_blobs = [b.name for b in blobs if b.name.startswith('telemetria_')]

    if not telemetria_blobs:
        logging.warning("Nenhum arquivo de telemetria encontrado para processar.")
        return "Nenhum arquivo de telemetria encontrado."

    logging.info(f"Encontrados {len(telemetria_blobs)} arquivos para processar.")

    # --- ETAPA 2: CARREGAR E CONCATENAR ---
    colunas_necessarias = ['CD_CLIENTE', 'STATUS_LICENCA', 'EVENTDURATION_S']
    lista_dfs = []
    for blob_name in telemetria_blobs:
        logging.info(f"A ler o blob: {blob_name}")
        blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
        blob_data = blob_client.download_blob().readall()
        # Otimização: Ler apenas as colunas necessárias
        df = pd.read_parquet(BytesIO(blob_data), columns=colunas_necessarias)
        lista_dfs.append(df)

    telemetria = pd.concat(lista_dfs, ignore_index=True)
    logging.info("Concatenação de todos os dataframes concluída.")

    # --- ETAPA 3: AGREGAÇÃO (AGORA ATIVO) ---
    contagem_licenca = telemetria.groupby(['CD_CLIENTE', 'STATUS_LICENCA']).size().unstack(fill_value=0)
    contagem_licenca.columns = ['LICENCA_' + str(col) for col in contagem_licenca.columns]

    df_agg = telemetria.groupby('CD_CLIENTE').agg(
        EVENTDURATION_S=('EVENTDURATION_S', 'median'),
    )

    df_final = pd.merge(df_agg, contagem_licenca, on='CD_CLIENTE', how='left')
    df_final.reset_index(inplace=True)
    logging.info("Agregação dos dados concluída.")

    # --- ETAPA 4: SALVAR NA GOLD (AGORA ATIVO) ---
    container_gold = "gold"
    output_blob_name = "df_telemetria.parquet"
    output_blob_client = blob_service_client.get_blob_client(container=container_gold, blob=output_blob_name)

    buffer = BytesIO()
    df_final.to_parquet(buffer, index=False)
    buffer.seek(0)
    output_blob_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo '{output_blob_name}' salvo no contentor '{container_gold}' com sucesso.")
    return "Processamento concluído com sucesso."


# --- Timer Trigger (todo dia às 23h) ---
@app.timer_trigger(schedule="0 23 * * *", arg_name="mytimer")
def ProcessarTelemetriaDiario(mytimer: func.TimerRequest):
    logging.info("Iniciando processamento diário da telemetria Silver → Gold via Timer Trigger.")
    try:
        resultado = processar_telemetria()
        logging.info(f"Resultado do processamento diário: {resultado}")
    except Exception as e:
        logging.error("--- ERRO NO PROCESSAMENTO DIÁRIO ---", exc_info=True)


# --- HTTP Trigger (para teste manual) ---
@app.route(route="processar_telemetria_test")
def ProcessarTelemetriaHTTP(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP Trigger acionado para processar telemetria.")
    try:
        resultado = processar_telemetria()
        return func.HttpResponse(resultado, status_code=200)
    except Exception as e:
        logging.error("--- ERRO NO PROCESSAMENTO MANUAL HTTP ---", exc_info=True)
        return func.HttpResponse(f"Erro: {e}", status_code=500)
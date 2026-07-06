import logging
import pandas as pd
from io import BytesIO, StringIO
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os

# --- INICIALIZAÇÃO DO APP DE FUNÇÃO (PADRÃO V2) ---
app = func.FunctionApp()

@app.event_grid_trigger(arg_name="event")
def ProcessarTelemetriaPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('telemetria_'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de telemetria.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        df_telemetria = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=',')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                df_telemetria = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if df_telemetria is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        df_telemetria.rename(columns={
            'eventduration': 'eventduration_segundos',
            'clienteid': 'CD_CLIENTE_ORIGINAL',
            'referencedatestart': 'DT_INICIO_EVENTO',
            'statuslicenca': 'STATUS_LICENCA',
        }, inplace=True)

        df_telemetria['eventduration_horas'] = df_telemetria['eventduration_segundos'] / 3600
        df_telemetria['eventduration_dias'] = df_telemetria['eventduration_segundos'] / (3600 * 24)
        df_telemetria['CD_CLIENTE'] = df_telemetria['CD_CLIENTE_ORIGINAL'].str.removesuffix('00')

        df_telemetria.rename(columns={'eventduration_segundos': 'EVENTDURATION_S',}, inplace=True)
        df_telemetria.rename(columns={'eventduration_horas': 'EVENTDURATION_H',}, inplace=True)
        df_telemetria.rename(columns={'eventduration_dias': 'EVENTDURATION_D',}, inplace=True)

        df_telemetria = df_telemetria[df_telemetria['EVENTDURATION_S'] >= 0]
        df_telemetria = df_telemetria[df_telemetria['EVENTDURATION_D'] <= 15330]

        df_telemetria.drop(columns=['EVENTDURATION_D', 'EVENTDURATION_H'], inplace=True)

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        df_telemetria.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarHistoricoPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('historico'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de historico.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        historico = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                historico = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if historico is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        historico.rename(columns={'CD_CLI': 'CD_CLIENTE'}, inplace=True)
        historico['QTD'] = historico['QTD'].str.replace(',', '.').astype(float)
        historico['MESES_BONIF'] = historico['MESES_BONIF'].astype(float)
        historico['VL_PCT_DESC_TEMP'] = historico['VL_PCT_DESC_TEMP'].str.replace(',', '.').astype(float)
        historico['VL_PCT_DESCONTO'] = historico['VL_PCT_DESCONTO'].str.replace(',', '.').astype(float)
        historico['PRC_UNITARIO'] = historico['PRC_UNITARIO'].str.replace(',', '.').astype(float)
        historico['VL_DESCONTO_TEMPORARIO'] = historico['VL_DESCONTO_TEMPORARIO'].str.replace(',', '.').astype(float)
        historico['VL_TOTAL'] = historico['VL_TOTAL'].str.replace(',', '.').astype(float)
        historico['VL_FULL'] = historico['VL_FULL'].str.replace(',', '.').astype(float)
        historico['VL_DESCONTO'] = historico['VL_DESCONTO'].str.replace(',', '.').astype(float)

        historico['VL_DESCONTO'] = historico['PRC_UNITARIO'] * historico['QTD'] * (historico['VL_PCT_DESCONTO']/100)

        historico['PRC_UNITARIO_TAB'] = historico['PRC_UNITARIO'] + historico['VL_DESCONTO']

        historico['VL_DESCONTO_TEMPORARIO'] = historico['PRC_UNITARIO'] * historico['QTD'] * (historico['VL_PCT_DESC_TEMP']/100)

        historico['VL_TOTAL'] = historico['PRC_UNITARIO'] * historico['QTD']

        historico['VL_FULL'] = historico['PRC_UNITARIO_TAB'] * historico['QTD']

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        historico.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarDadosClientesPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('dados_clientes'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de dados_clientes.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        dados_clientes = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                dados_clientes = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if dados_clientes is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        dados_clientes['VL_TOTAL_CONTRATO'] = dados_clientes['VL_TOTAL_CONTRATO'].str.replace(',', '.').astype(float)
        dados_clientes['DT_ASSINATURA_CONTRATO'] = pd.to_datetime(dados_clientes['DT_ASSINATURA_CONTRATO'])
        dados_clientes.drop(columns=['PAIS', 'CIDADE'], inplace=True)

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        dados_clientes.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarClientesDesdePorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('clientes_desde'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de clientes_desde.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        clientes_desde = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                clientes_desde = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if clientes_desde is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        clientes_desde = clientes_desde.rename(columns={
            'CLIENTE': 'CD_CLIENTE'
        })

        clientes_desde['CLIENTE_DESDE'] = pd.to_datetime(clientes_desde['CLIENTE_DESDE'])
        data_referencia = pd.to_datetime('2025-04-23')

        clientes_desde['IDADE_DIAS'] = (data_referencia - clientes_desde['CLIENTE_DESDE']).dt.days
        clientes_desde['IDADE_ANOS'] = clientes_desde['IDADE_DIAS'] / 365.25

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        clientes_desde.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarContratacoes12MesesPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('contratacoes_ultimos_12'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de contratacoes_ultimos_12_meses.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        contratacoes_12meses = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                contratacoes_12meses = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if contratacoes_12meses is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        contratacoes_12meses['VLR_CONTRATACOES_12M'] = contratacoes_12meses['VLR_CONTRATACOES_12M'].str.replace(',', '.').astype(float)

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        contratacoes_12meses.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarMRRPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('mrr'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de mrr.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        mrr = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                mrr = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if mrr is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        mrr = mrr.rename(columns={'CLIENTE': 'CD_CLIENTE'})

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        mrr.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSRelacionalPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('nps_relacional'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de nps_relacional.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        nps_relacional = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                nps_relacional = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if nps_relacional is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        nps_relacional = nps_relacional.rename(columns={
            'respondedAt': 'DT_RESPOSTA',
            'metadata_codcliente': 'CD_CLIENTE',
            'resposta_NPS': 'RESPOSTA_NPS',
            'resposta_unidade': 'RESPOSTA_UNIDADE',
            'Nota_SupTec_Agilidade': 'NOTA_SUPTEC_AGI',
            'Nota_SupTec_Atendimento': 'NOTA_SUPTEC_ATEN',
            'Nota_Comercial': 'NOTA_COMERCIAL',
            'Nota_Custos': 'NOTA_CUSTOS',
            'Nota_AdmFin_Atendimento': 'NOTA_ADMFIN_ATEN',
            'Nota_Software': 'NOTA_SOFTWARE',
            'Nota_Software_Atualizacao': 'NOTA_SOFTWARE_ATT'
        })

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        nps_relacional.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSAquisicaoPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('nps_transacional_aquisicao'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de nps_transacional_aquisicao.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        nps_transacional_aquisicao = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                nps_transacional_aquisicao = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if nps_transacional_aquisicao is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        nps_transacional_aquisicao = nps_transacional_aquisicao.rename(columns={
            'Cód. Cliente': 'CD_CLIENTE',
            'Data da Resposta': 'DT_RESPOSTA',
            'Nota NPS': 'NOTA_NPS',
            'Nota Agilidade': 'NOTA_AGILIDADE',
            'Nota Conhecimento': 'NOTA_CONHECIMENTO',
            'Nota Custo': 'NOTA_CUSTO',
            'Nota Facilidade': 'NOTA_FACILIDADE',
            'Nota Flexibilidade': 'NOTA_FLEXIBILIDADE'
        })

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        nps_transacional_aquisicao.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSImplantacaoPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('nps_transacional_implantacao'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de nps_transacional_implantacao.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        nps_transacional_implantacao = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                nps_transacional_implantacao = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if nps_transacional_implantacao is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        nps_transacional_implantacao = nps_transacional_implantacao.rename(columns={
            'Cód. Cliente': 'CD_CLIENTE',
            'Data da Resposta': 'DT_RESPOSTA',
            'Nota NPS': 'NOTA_NPS',
            'Nota Metodologia': 'NOTA_METODOLOGIA',
            'Nota Gestao': 'NOTA_GESTAO',
            'Nota Conhecimento': 'NOTA_CONHECIMENTO',
            'Nota Qualidade': 'NOTA_QUALIDADE',
            'Nota Comunicacao': 'NOTA_COMUNICACAO',
            'Nota Prazos': 'NOTA_PRAZOS'
        })

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        nps_transacional_implantacao.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSOnboardingPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('nps_transacional_onboarding'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de nps_transacional_onboarding.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        nps_transacional_onboarding = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                nps_transacional_onboarding = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if nps_transacional_onboarding is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        nps_transacional_onboarding = nps_transacional_onboarding.rename(columns={
            'Cod Cliente': 'CD_CLIENTE',
            'Data de Resposta': 'DT_RESPOSTA',
            'Em uma escala de 0 a 10, quanto você recomenda o Onboarding da TOTVS para um amigo ou colega?.': 'NOTA_RECOMENDACAO',
            'Em uma escala de 0 a 10, o quanto você acredita que o atendimento CS Onboarding ajudou no início da sua trajetória com a TOTVS?': 'NOTA_AJUDA',
            '- Duração do tempo na realização da reunião de Onboarding;': 'NOTA_TEMPO',
            '- Clareza no acesso aos canais de comunicação da TOTVS;': 'NOTA_CLAREZA_CANAL',
            '- Clareza nas informações em geral transmitidas pelo CS que lhe atendeu no Onboarding;': 'NOTA_CLAREZA_GERAL',
            '- Expectativas atendidas no Onboarding da TOTVS.': 'NOTA_EXPECTATIVA'
        })

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        nps_transacional_onboarding.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSProdutoPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('nps_transacional_produto'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de nps_transacional_produto.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        nps_transacional_produto = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                nps_transacional_produto = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if nps_transacional_produto is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        nps_transacional_produto = nps_transacional_produto.rename(columns={
            'Cód. T': 'CD_CLIENTE',
            'Data da Resposta': 'DT_RESPOSTA',
            'Linha de Produto': 'LINHA_PRODUTO',
            'Nome do Produto': 'NM_PRODUTO',
            'Nota': 'NOTA_PRODUTO'
        })

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        nps_transacional_produto.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSSuportePorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('nps_transacional_suporte'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de nps_transacional_suporte.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        nps_transacional_suporte = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                nps_transacional_suporte = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if nps_transacional_suporte is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        nps_transacional_suporte = nps_transacional_suporte.rename(columns={
            'cliente': 'CD_CLIENTE',
            'ticket': 'TICKET',
            'resposta_NPS': 'NOTA_NPS',
            'grupo_NPS': 'GRUPO_NPS',
            'Nota_ConhecimentoAgente': 'NOTA_CONHECIMENTO_AG',
            'Nota_Solucao': 'NOTA_SOLUCAO',
            'Nota_TempoRetorno': 'NOTA_TEMPO_RETORNO',
            'Nota_Facilidade': 'NOTA_FACILIDADE',
            'Nota_Satisfacao': 'NOTA_SATISFACAO'
        })

        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        nps_transacional_suporte.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")


@app.event_grid_trigger(arg_name="event")
def ProcessarTicketsPorEvento(event: func.EventGridEvent):
    logging.info(f"Gatilho de Event Grid acionado. ID do Evento: {event.id}")
    logging.info(f"Tipo de Evento: {event.event_type}")
    logging.info(f"Assunto do Evento: {event.subject}")

    try:
        # --- PASSO 1: EXTRAIR INFORMAÇÕES DO BLOB A PARTIR DO EVENTO ---
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            raise ValueError(f"O formato do 'subject' do evento é inesperado: {subject}")

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        logging.info(f"Evento detectado para o Container: '{container_name}', Blob: '{blob_name}'")

        if container_name != 'bronze':
            logging.info(f"Evento ignorado pois não pertence ao container 'bronze'.")
            return

        if not blob_name.startswith('tickets'):
            logging.info(f"Evento ignorado pois o ficheiro '{blob_name}' não corresponde ao padrão de tickets.")
            return

        # --- PASSO 2: CONECTAR AO STORAGE E FAZER O DOWNLOAD DO BLOB ---
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        downloader = blob_client.download_blob()
        blob_data = downloader.readall()

        # --- PASSO 3: PROCESSAR OS DADOS () ---
        tickets = None
        for encoding in ['utf-8', 'latin1']:
            try:
                s = str(blob_data, encoding)
                data = StringIO(s)
                df = pd.read_csv(data, sep=';')

                if len(df.columns) == 1:
                    data.seek(0)
                    df = pd.read_csv(data, sep=None, engine='python')

                tickets = df
                logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
                break
            except Exception as e:
                logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
                continue

        if tickets is None:
            raise ValueError(f'Não foi possível ler o ficheiro do blob: {blob_name}')

        tickets = tickets.rename(columns={
            'CODIGO_ORGANIZACAO': 'CD_CLIENTE',
        })

        tickets['DT_ATUALIZACAO'] = pd.to_datetime(tickets['DT_ATUALIZACAO'])
        tickets['DT_CRIACAO'] = pd.to_datetime(tickets['DT_CRIACAO'])

        tickets['DIAS_DESDE_ATUALIZACAO'] = (tickets['DT_ATUALIZACAO'] - tickets['DT_CRIACAO']).dt.days


        logging.info(f"Tratamento concluído para o ficheiro: {blob_name}")

        # --- PASSO 4: FAZER O UPLOAD DO ARQUIVO PROCESSADO PARA O 'SILVER' ---
        output_container_name = "silver"
        output_blob_name = blob_name.replace('.csv', '.parquet')

        output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

        parquet_buffer = BytesIO()
        tickets.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        output_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}")
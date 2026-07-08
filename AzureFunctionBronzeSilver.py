import logging
import pandas as pd
from io import BytesIO, StringIO
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os

app = func.FunctionApp()


def ler_csv_blob(blob_data, blob_name, sep_inicial=';'):
    """Tenta ler o CSV com dois encodings diferentes para evitar erros."""
    for encoding in ['utf-8', 'latin1']:
        try:
            s = str(blob_data, encoding)
            data = StringIO(s)
            df = pd.read_csv(data, sep=sep_inicial)

            if len(df.columns) == 1:
                data.seek(0)
                df = pd.read_csv(data, sep=None, engine='python')

            logging.info(f"Ficheiro {blob_name} lido com sucesso usando encoding {encoding}.")
            return df
        except Exception as e:
            logging.warning(f"Falha ao tentar ler {blob_name} com encoding {encoding}: {e}")
            continue
    return None


def salvar_no_silver(df, blob_name, blob_service_client):
    """Converte o DataFrame para Parquet e faz o upload no container Silver."""
    output_container_name = "silver"
    output_blob_name = blob_name.replace('.csv', '.parquet')
    output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    output_blob_client.upload_blob(parquet_buffer, overwrite=True)
    logging.info(f"Ficheiro {output_blob_name} salvo com sucesso no container {output_container_name}.")


def processar_telemetria(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name, sep_inicial=',')
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df.rename(columns={
        'eventduration': 'eventduration_segundos',
        'clienteid': 'CD_CLIENTE_ORIGINAL',
        'referencedatestart': 'DT_INICIO_EVENTO',
        'statuslicenca': 'STATUS_LICENCA',
    }, inplace=True)

    df['eventduration_horas'] = df['eventduration_segundos'] / 3600
    df['eventduration_dias'] = df['eventduration_segundos'] / (3600 * 24)
    df['CD_CLIENTE'] = df['CD_CLIENTE_ORIGINAL'].str.removesuffix('00')

    df.rename(columns={'eventduration_segundos': 'EVENTDURATION_S'}, inplace=True)
    df.rename(columns={'eventduration_horas': 'EVENTDURATION_H'}, inplace=True)
    df.rename(columns={'eventduration_dias': 'EVENTDURATION_D'}, inplace=True)

    df = df[df['EVENTDURATION_S'] >= 0]
    df = df[df['EVENTDURATION_D'] <= 15330]
    df.drop(columns=['EVENTDURATION_D', 'EVENTDURATION_H'], inplace=True)
    
    return df


def processar_historico(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df.rename(columns={'CD_CLI': 'CD_CLIENTE'}, inplace=True)
    
    colunas_virgula = ['QTD', 'VL_PCT_DESC_TEMP', 'VL_PCT_DESCONTO', 'PRC_UNITARIO', 'VL_DESCONTO_TEMPORARIO', 'VL_TOTAL', 'VL_FULL', 'VL_DESCONTO']
    for col in colunas_virgula:
        df[col] = df[col].astype(str).str.replace(',', '.').astype(float)

    df['MESES_BONIF'] = df['MESES_BONIF'].astype(float)
    df['VL_DESCONTO'] = df['PRC_UNITARIO'] * df['QTD'] * (df['VL_PCT_DESCONTO']/100)
    df['PRC_UNITARIO_TAB'] = df['PRC_UNITARIO'] + df['VL_DESCONTO']
    df['VL_DESCONTO_TEMPORARIO'] = df['PRC_UNITARIO'] * df['QTD'] * (df['VL_PCT_DESC_TEMP']/100)
    df['VL_TOTAL'] = df['PRC_UNITARIO'] * df['QTD']
    df['VL_FULL'] = df['PRC_UNITARIO_TAB'] * df['QTD']
    
    return df


def processar_dados_clientes(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df['VL_TOTAL_CONTRATO'] = df['VL_TOTAL_CONTRATO'].str.replace(',', '.').astype(float)
    df['DT_ASSINATURA_CONTRATO'] = pd.to_datetime(df['DT_ASSINATURA_CONTRATO'])
    df.drop(columns=['PAIS', 'CIDADE'], inplace=True)
    return df


def processar_clientes_desde(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df.rename(columns={'CLIENTE': 'CD_CLIENTE'}, inplace=True)
    df['CLIENTE_DESDE'] = pd.to_datetime(df['CLIENTE_DESDE'])
    data_referencia = pd.to_datetime('2025-04-23')
    df['IDADE_DIAS'] = (data_referencia - df['CLIENTE_DESDE']).dt.days
    df['IDADE_ANOS'] = df['IDADE_DIAS'] / 365.25
    return df


def processar_contratacoes_12meses(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df['VLR_CONTRATACOES_12M'] = df['VLR_CONTRATACOES_12M'].str.replace(',', '.').astype(float)
    return df


def processar_mrr(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df.rename(columns={'CLIENTE': 'CD_CLIENTE'}, inplace=True)
    return df


def processar_nps_relacional(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df.rename(columns={
        'respondedAt': 'DT_RESPOSTA', 'metadata_codcliente': 'CD_CLIENTE',
        'resposta_NPS': 'RESPOSTA_NPS', 'resposta_unidade': 'RESPOSTA_UNIDADE',
        'Nota_SupTec_Agilidade': 'NOTA_SUPTEC_AGI', 'Nota_SupTec_Atendimento': 'NOTA_SUPTEC_ATEN',
        'Nota_Comercial': 'NOTA_COMERCIAL', 'Nota_Custos': 'NOTA_CUSTOS',
        'Nota_AdmFin_Atendimento': 'NOTA_ADMFIN_ATEN', 'Nota_Software': 'NOTA_SOFTWARE',
        'Nota_Software_Atualizacao': 'NOTA_SOFTWARE_ATT'
    }, inplace=True)
    return df


def processar_nps_aquisicao(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df.rename(columns={
        'Cód. Cliente': 'CD_CLIENTE', 'Data da Resposta': 'DT_RESPOSTA',
        'Nota NPS': 'NOTA_NPS', 'Nota Agilidade': 'NOTA_AGILIDADE',
        'Nota Conhecimento': 'NOTA_CONHECIMENTO', 'Nota Custo': 'NOTA_CUSTO',
        'Nota Facilidade': 'NOTA_FACILIDADE', 'Nota Flexibilidade': 'NOTA_FLEXIBILIDADE'
    }, inplace=True)
    return df


def processar_nps_implantacao(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df.rename(columns={
        'Cód. Cliente': 'CD_CLIENTE', 'Data da Resposta': 'DT_RESPOSTA',
        'Nota NPS': 'NOTA_NPS', 'Nota Metodologia': 'NOTA_METODOLOGIA',
        'Nota Gestao': 'NOTA_GESTAO', 'Nota Conhecimento': 'NOTA_CONHECIMENTO',
        'Nota Qualidade': 'NOTA_QUALIDADE', 'Nota Comunicacao': 'NOTA_COMUNICACAO',
        'Nota Prazos': 'NOTA_PRAZOS'
    }, inplace=True)
    return df


def processar_nps_onboarding(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df.rename(columns={
        'Cod Cliente': 'CD_CLIENTE', 'Data de Resposta': 'DT_RESPOSTA',
        'Em uma escala de 0 a 10, quanto você recomenda o Onboarding da TOTVS para um amigo ou colega?.': 'NOTA_RECOMENDACAO',
        'Em uma escala de 0 a 10, o quanto você acredita que o atendimento CS Onboarding ajudou no início da sua trajetória com a TOTVS?': 'NOTA_AJUDA',
        '- Duração do tempo na realização da reunião de Onboarding;': 'NOTA_TEMPO',
        '- Clareza no acesso aos canais de comunicação da TOTVS;': 'NOTA_CLAREZA_CANAL',
        '- Clareza nas informações em geral transmitidas pelo CS que lhe atendeu no Onboarding;': 'NOTA_CLAREZA_GERAL',
        '- Expectativas atendidas no Onboarding da TOTVS.': 'NOTA_EXPECTATIVA'
    }, inplace=True)
    return df


def processar_nps_produto(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df.rename(columns={
        'Cód. T': 'CD_CLIENTE', 'Data da Resposta': 'DT_RESPOSTA',
        'Linha de Produto': 'LINHA_PRODUTO', 'Nome do Produto': 'NM_PRODUTO',
        'Nota': 'NOTA_PRODUTO'
    }, inplace=True)
    return df


def processar_nps_suporte(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    df.rename(columns={
        'cliente': 'CD_CLIENTE', 'ticket': 'TICKET', 'resposta_NPS': 'NOTA_NPS',
        'grupo_NPS': 'GRUPO_NPS', 'Nota_ConhecimentoAgente': 'NOTA_CONHECIMENTO_AG',
        'Nota_Solucao': 'NOTA_SOLUCAO', 'Nota_TempoRetorno': 'NOTA_TEMPO_RETORNO',
        'Nota_Facilidade': 'NOTA_FACILIDADE', 'Nota_Satisfacao': 'NOTA_SATISFACAO'
    }, inplace=True)
    return df


def processar_tickets(blob_data, blob_name):
    df = ler_csv_blob(blob_data, blob_name)
    if df is None: raise ValueError(f'Não foi possível ler o ficheiro: {blob_name}')

    tickets = tickets.rename(columns={
        'CODIGO_ORGANIZACAO': 'CD_CLIENTE',
    })

    tickets['DT_ATUALIZACAO'] = pd.to_datetime(tickets['DT_ATUALIZACAO'])
    tickets['DT_CRIACAO'] = pd.to_datetime(tickets['DT_CRIACAO'])

    tickets['DIAS_DESDE_ATUALIZACAO'] = (tickets['DT_ATUALIZACAO'] - tickets['DT_CRIACAO']).dt.days
    return df


@app.event_grid_trigger(arg_name="event")
def ProcessarBronzeUnificado(event: func.EventGridEvent):
    logging.info(f"Evento recebido. Assunto: {event.subject}")

    try:
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            return

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        if container_name != 'bronze':
            return

        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_data = blob_client.download_blob().readall()

        df_processado = None

        if blob_name.startswith('telemetria_'):
            df_processado = processar_telemetria(blob_data, blob_name)
        elif blob_name.startswith('historico'):
            df_processado = processar_historico(blob_data, blob_name)
        elif blob_name.startswith('dados_clientes'):
            df_processado = processar_dados_clientes(blob_data, blob_name)
        elif blob_name.startswith('clientes_desde'):
            df_processado = processar_clientes_desde(blob_data, blob_name)
        elif blob_name.startswith('contratacoes_ultimos_12'):
            df_processado = processar_contratacoes_12meses(blob_data, blob_name)
        elif blob_name.startswith('mrr'):
            df_processado = processar_mrr(blob_data, blob_name)
        elif blob_name.startswith('nps_relacional'):
            df_processado = processar_nps_relacional(blob_data, blob_name)
        elif blob_name.startswith('nps_transacional_aquisicao'):
            df_processado = processar_nps_aquisicao(blob_data, blob_name)
        elif blob_name.startswith('nps_transacional_implantacao'):
            df_processado = processar_nps_implantacao(blob_data, blob_name)
        elif blob_name.startswith('nps_transacional_onboarding'):
            df_processado = processar_nps_onboarding(blob_data, blob_name)
        elif blob_name.startswith('nps_transacional_produto'):
            df_processado = processar_nps_produto(blob_data, blob_name)
        elif blob_name.startswith('nps_transacional_suporte'):
            df_processado = processar_nps_suporte(blob_data, blob_name)
        elif blob_name.startswith('tickets'):
            df_processado = processar_tickets(blob_data, blob_name)
        else:
            logging.info(f"Arquivo '{blob_name}' ignorado. Padrão não reconhecido.")
            return

        if df_processado is not None:
            logging.info(f"Tratamento concluído para o ficheiro: {blob_name}. Iniciando salvamento...")
            salvar_no_silver(df_processado, blob_name, blob_service_client)

    except Exception as e:
        # Usei exec_info=True para logar o stack trace completo APENAS em caso de erro real, 
        # evitando poluir o Log Analytics com logs de "arquivo ignorado".
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}", exc_info=True)

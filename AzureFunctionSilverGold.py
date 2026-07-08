import logging
import os
import pandas as pd
from io import BytesIO
import azure.functions as func
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()


def download_parquet_silver(blob_service_client, blob_name):
    """Faz o download de um arquivo parquet do container Silver."""
    blob_client = blob_service_client.get_blob_client(container="silver", blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    return pd.read_parquet(BytesIO(blob_data))


def upload_parquet_gold(blob_service_client, df, output_name):
    """Salva um DataFrame como Parquet no container Gold."""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    gold_client = blob_service_client.get_blob_client(container="gold", blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)
    logging.info(f"Arquivo processado e salvo em gold/{output_name}")


def processar_tickets_silver(blob_service_client):
    tickets = download_parquet_silver(blob_service_client, "tickets.parquet")

    agrupado = tickets.groupby('CD_CLIENTE').agg(
        QTD_TICKETS=('BK_TICKET', 'count'),
        PRIMEIRA_CRIACAO=('DT_CRIACAO', 'min'),
        ULTIMA_ATUALIZACAO=('DT_ATUALIZACAO', 'max'),
        DIAS_DESDE_ULTIMA_ATUALIZACAO_MEAN=('DIAS_DESDE_ATUALIZACAO', 'mean'),
        DIAS_DESDE_ULTIMA_ATUALIZACAO_MIN=('DIAS_DESDE_ATUALIZACAO', 'min'),
        DIAS_DESDE_ULTIMA_ATUALIZACAO_MAX=('DIAS_DESDE_ATUALIZACAO', 'max'),
    )

    tipo_ticket = pd.crosstab(tickets['CD_CLIENTE'], tickets['TIPO_TICKET'])
    tipo_ticket.columns = ['QTD_TIPO_' + str(col).upper() for col in tipo_ticket.columns]

    status_ticket = pd.crosstab(tickets['CD_CLIENTE'], tickets['STATUS_TICKET'])
    status_ticket.columns = ['QTD_STATUS_' + str(col).upper() for col in status_ticket.columns]

    prioridade_ticket = pd.crosstab(tickets['CD_CLIENTE'], tickets['PRIORIDADE_TICKET'])
    prioridade_ticket.columns = ['QTD_PRIORIDADE_' + str(col).upper() for col in prioridade_ticket.columns]

    df_tickets = agrupado.join([tipo_ticket, status_ticket, prioridade_ticket]).reset_index()
    upload_parquet_gold(blob_service_client, df_tickets, "df_tickets.parquet")


def processar_historico_silver(blob_service_client):
    historico = download_parquet_silver(blob_service_client, "historico.parquet")

    df_proposta_agrupado = historico.groupby(['CD_CLIENTE', 'NR_PROPOSTA']).agg(
        QTD_ITEMS_DISTINTOS_PROPOSTA=('ITEM_PROPOSTA', 'max'),
        QTD_UNIDADES_TOTAIS=('QTD', 'sum'),
        MEDIAN_MESES_BONIF=('MESES_BONIF', 'median'),
        MEDIAN_VL_PCT_DESCONTO=('VL_PCT_DESCONTO', 'median'),
        MEDIAN_VL_PCT_DESC_TEMP=('VL_PCT_DESC_TEMP', 'median'),
        MEDIAN_PRC_UNITARIO_POR_PROPOSTA=('PRC_UNITARIO', 'median'),
        VALOR_TOTAL_PROPOSTA=('PRC_UNITARIO', 'sum'),
    ).reset_index()

    historico_unique = historico[['CD_CLIENTE', 'FAT_FAIXA', 'NR_PROPOSTA']]
    historico_merged = historico_unique.merge(df_proposta_agrupado, on=['CD_CLIENTE', 'NR_PROPOSTA'], how='left')

    df_historico_agrupado_cliente = historico_merged.groupby('CD_CLIENTE').agg(
        QTD_PROPOSTAS_DISTINTAS=('NR_PROPOSTA', 'nunique'),
        QTD_ITEMS_DISTINTOS_PROPOSTA=('QTD_ITEMS_DISTINTOS_PROPOSTA', 'median'),
        QTD_UNIDADES_TOTAIS=('QTD_UNIDADES_TOTAIS', 'sum'),
        MEDIAN_MESES_BONIF=('MEDIAN_MESES_BONIF', 'median'),
        MEDIAN_VL_PCT_DESCONTO=('MEDIAN_VL_PCT_DESCONTO', 'median'),
        MEDIAN_VL_PCT_DESC_TEMP=('MEDIAN_VL_PCT_DESC_TEMP', 'median'),
        MEDIAN_PRC_UNITARIO_POR_PROPOSTA=('MEDIAN_PRC_UNITARIO_POR_PROPOSTA', 'median'),
        MEDIAN_VALOR_TOTAL_PROPOSTA=('VALOR_TOTAL_PROPOSTA', 'median'),
    ).reset_index()

    df_historico_agrupado_cliente['MEDIAN_UNIDADES_POR_PROPOSTA'] = df_historico_agrupado_cliente['QTD_UNIDADES_TOTAIS'] / df_historico_agrupado_cliente['QTD_PROPOSTAS_DISTINTAS']
    upload_parquet_gold(blob_service_client, df_historico_agrupado_cliente, "df_historico.parquet")


def processar_dados_clientes_silver(blob_service_client):
    dados_clientes = download_parquet_silver(blob_service_client, "dados_clientes.parquet")

    contagem_situacoes = dados_clientes.groupby(['CD_CLIENTE', 'SITUACAO_CONTRATO']).size().unstack(fill_value=0)
    contagem_situacoes.columns = ['CONTRATOS_' + col for col in contagem_situacoes.columns]

    valor_situacoes = dados_clientes.groupby(['CD_CLIENTE', 'SITUACAO_CONTRATO'])['VL_TOTAL_CONTRATO'].sum().unstack(fill_value=0)
    valor_situacoes.columns = ['VALOR_CONTRATOS_' + col for col in valor_situacoes.columns]

    valor_periodicidades = dados_clientes.groupby(['CD_CLIENTE', 'PERIODICIDADE']).size().unstack(fill_value=0)
    valor_periodicidades.columns = ['PERIODICIDADE_' + col for col in valor_periodicidades.columns]

    df_dados_clientes_agregado = dados_clientes.groupby('CD_CLIENTE').agg(
        QTD_PRODUTOS_DISTINTOS=('DS_PROD', 'nunique'),
        QTD_TOTAL_PRODUTOS=('DS_PROD', 'count'),
        SOMA_VL_CONTRATOS=('VL_TOTAL_CONTRATO', 'sum'),
        LISTA_PRODUTOS=('DS_PROD', lambda x: list(x))
    )

    df_dados_clientes_agregado = pd.merge(df_dados_clientes_agregado, contagem_situacoes, on='CD_CLIENTE', how='left')
    df_dados_clientes_agregado = pd.merge(df_dados_clientes_agregado, valor_situacoes, on='CD_CLIENTE', how='left')
    df_dados_clientes_agregado = pd.merge(df_dados_clientes_agregado, valor_periodicidades, on='CD_CLIENTE', how='left')

    df_dados_clientes_final = pd.merge(
        df_dados_clientes_agregado,
        dados_clientes[['CD_CLIENTE', 'DS_SEGMENTO', 'DS_SUBSEGMENTO', 'FAT_FAIXA', 'UF']].drop_duplicates(),
        on='CD_CLIENTE', how='left'
    )
    upload_parquet_gold(blob_service_client, df_dados_clientes_final, "df_dados_clientes.parquet")


def processar_clientes_desde_silver(blob_service_client):
    clientes_desde = download_parquet_silver(blob_service_client, "clientes_desde.parquet")
    upload_parquet_gold(blob_service_client, clientes_desde, "df_clientes_desde.parquet")


def processar_contratacoes_12_silver(blob_service_client):
    contratacoes = download_parquet_silver(blob_service_client, "contratacoes_ultimos_12_meses.parquet")
    upload_parquet_gold(blob_service_client, contratacoes, "df_contratacoes_ultimos_12_meses.parquet")


def processar_mrr_silver(blob_service_client):
    mrr = download_parquet_silver(blob_service_client, "mrr.parquet")
    upload_parquet_gold(blob_service_client, mrr, "df_mrr.parquet")


def processar_nps_relacional_silver(blob_service_client):
    nps = download_parquet_silver(blob_service_client, "nps_relacional.parquet")
    nps_agregado = nps.groupby('CD_CLIENTE').agg(
        RELACIONAL_MEDIA_RESPOSTA_NPS=('RESPOSTA_NPS', 'mean'),
        RELACIONAL_MEDIA_RESPOSTA_UNIDADE=('RESPOSTA_UNIDADE', 'mean'),
        RELACIONAL_MEDIA_NOTA_SUPTEC_AGI=('NOTA_SUPTEC_AGI', 'mean'),
        RELACIONAL_MEDIA_NOTA_SUPTEC_ATEN=('NOTA_SUPTEC_ATEN', 'mean'),
        RELACIONAL_MEDIA_NOTA_COMERCIAL=('NOTA_COMERCIAL', 'mean'),
        RELACIONAL_MEDIA_NOTA_CUSTOS=('NOTA_CUSTOS', 'mean'),
        RELACIONAL_MEDIA_NOTA_ADMFIN_ATEN=('NOTA_ADMFIN_ATEN', 'mean'),
        RELACIONAL_MEDIA_NOTA_SOFTWARE=('NOTA_SOFTWARE', 'mean'),
        RELACIONAL_MEDIA_NOTA_SOFTWARE_ATT=('NOTA_SOFTWARE_ATT', 'mean')
    ).reset_index()
    upload_parquet_gold(blob_service_client, nps_agregado, "df_nps_relacional.parquet")


def processar_nps_aquisicao_silver(blob_service_client):
    nps = download_parquet_silver(blob_service_client, "nps_transacional_aquisicao.parquet")
    nps_agregado = nps.groupby('CD_CLIENTE').agg(
        AQUISICAO_MEDIA_NOTA_NPS=('NOTA_NPS', 'mean'),
        AQUISICAO_MEDIA_NOTA_AGILIDADE=('NOTA_AGILIDADE', 'mean'),
        AQUISICAO_MEDIA_NOTA_CONHECIMENTO=('NOTA_CONHECIMENTO', 'mean'),
        AQUISICAO_MEDIA_NOTA_CUSTO=('NOTA_CUSTO', 'mean'),
        AQUISICAO_MEDIA_NOTA_FACILIDADE=('NOTA_FACILIDADE', 'mean'),
        AQUISICAO_MEDIA_NOTA_FLEXIBILIDADE=('NOTA_FLEXIBILIDADE', 'mean')
    ).reset_index()
    upload_parquet_gold(blob_service_client, nps_agregado, "df_nps_transacional_aquisicao.parquet")


def processar_nps_implantacao_silver(blob_service_client):
    nps = download_parquet_silver(blob_service_client, "nps_transacional_implantacao.parquet")
    nps_agregado = nps.groupby('CD_CLIENTE').agg(
        IMPLANTACAO_MEDIA_NOTA_NPS=('NOTA_NPS', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_METODOLOGIA=('NOTA_METODOLOGIA', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_GESTAO=('NOTA_GESTAO', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_CONHECIMENTO=('NOTA_CONHECIMENTO', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_QUALIDADE=('NOTA_QUALIDADE', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_COMUNICACAO=('NOTA_COMUNICACAO', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_PRAZOS=('NOTA_PRAZOS', 'mean')
    ).reset_index()
    upload_parquet_gold(blob_service_client, nps_agregado, "df_nps_transacional_implantacao.parquet")


def processar_nps_onboarding_silver(blob_service_client):
    nps = download_parquet_silver(blob_service_client, "nps_transacional_onboarding.parquet")
    nps_agregado = nps.groupby('CD_CLIENTE').agg(
        ONBOARDING_MEDIA_NOTA_RECOMENDACAO=('NOTA_RECOMENDACAO', 'mean'),
        ONBOARDING_MEDIA_NOTA_AJUDA=('NOTA_AJUDA', 'mean'),
        ONBOARDING_MEDIA_NOTA_TEMPO=('NOTA_TEMPO', 'mean'),
        ONBOARDING_MEDIA_NOTA_CLAREZA_CANAL=('NOTA_CLAREZA_CANAL', 'mean'),
        ONBOARDING_MEDIA_NOTA_CLAREZA_GERAL=('NOTA_CLAREZA_GERAL', 'mean'),
        ONBOARDING_MEDIA_NOTA_EXPECTATIVA=('NOTA_EXPECTATIVA', 'mean')
    ).reset_index()
    upload_parquet_gold(blob_service_client, nps_agregado, "df_nps_transacional_onboarding.parquet")


def processar_nps_suporte_silver(blob_service_client):
    nps = download_parquet_silver(blob_service_client, "nps_transacional_suporte.parquet")
    
    valor_grupo = nps.groupby('CD_CLIENTE')['GRUPO_NPS'].value_counts().unstack(fill_value=0)
    valor_grupo.columns = ['GRUPO_NPS_' + str(col) for col in valor_grupo.columns]

    nps_agregado = nps.groupby('CD_CLIENTE').agg(
        SUPORTE_MEDIA_NOTA_NPS=('NOTA_NPS', 'mean'),
        SUPORTE_MEDIA_NOTA_CONHECIMENTO_AG=('NOTA_CONHECIMENTO_AG', 'mean'),
        SUPORTE_MEDIA_NOTA_SOLUCAO=('NOTA_SOLUCAO', 'mean'),
        SUPORTE_MEDIA_NOTA_TEMPO_RETORNO=('NOTA_TEMPO_RETORNO', 'mean'),
        SUPORTE_MEDIA_NOTA_FACILIDADE=('NOTA_FACILIDADE', 'mean'),
        SUPORTE_MEDIA_NOTA_SATISFACAO=('NOTA_SATISFACAO', 'mean'),
        QTD_TICKET_PARA_SUPORTE=('TICKET', 'nunique'),
    ).reset_index()

    nps_final = pd.merge(nps_agregado, valor_grupo, on='CD_CLIENTE', how='left')
    upload_parquet_gold(blob_service_client, nps_final, "df_nps_transacional_suporte.parquet")


def processar_nps_produto_silver(blob_service_client):
    nps = download_parquet_silver(blob_service_client, "nps_transacional_produto.parquet")
    
    nota_por_produto = nps.groupby(['CD_CLIENTE', 'LINHA_PRODUTO'])['NOTA_PRODUTO'].mean().reset_index()
    matriz_nota = nota_por_produto.pivot(index='CD_CLIENTE', columns='LINHA_PRODUTO', values='NOTA_PRODUTO').fillna(0)
    matriz_nota.columns = ['LINHA_PRODUTO_' + str(col) + '_MEDIA_NOTA' for col in matriz_nota.columns]
    matriz_nota.reset_index(inplace=True)

    matriz_qtd = nps.groupby('CD_CLIENTE')['LINHA_PRODUTO'].value_counts().unstack(fill_value=0)
    matriz_qtd.columns = ['LINHA_PRODUTO_' + str(col) + '_QTD' for col in matriz_qtd.columns]
    matriz_qtd.reset_index(inplace=True)

    nps_final = pd.merge(matriz_qtd, matriz_nota, on='CD_CLIENTE', how='left')
    upload_parquet_gold(blob_service_client, nps_final, "df_nps_transacional_produto.parquet")


@app.event_grid_trigger(arg_name="event")
def ProcessarSilverUnificado(event: func.EventGridEvent):
    logging.info(f"Evento Silver recebido. Assunto: {event.subject}")

    try:
        subject = event.subject
        parts = subject.split('/')

        if len(parts) < 7 or parts[3] != 'containers':
            return

        container_name = parts[4]
        blob_name = '/'.join(parts[6:])

        if container_name != 'silver':
            return

        connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if not connect_str:
            logging.error("Variável de ambiente AZURE_STORAGE_CONNECTION_STRING não encontrada.")
            return

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        if blob_name == 'tickets.parquet':
            processar_tickets_silver(blob_service_client)
        elif blob_name == 'historico.parquet':
            processar_historico_silver(blob_service_client)
        elif blob_name == 'dados_clientes.parquet':
            processar_dados_clientes_silver(blob_service_client)
        elif blob_name == 'clientes_desde.parquet':
            processar_clientes_desde_silver(blob_service_client)
        elif blob_name == 'contratacoes_ultimos_12_meses.parquet':
            processar_contratacoes_12_silver(blob_service_client)
        elif blob_name == 'mrr.parquet':
            processar_mrr_silver(blob_service_client)
        elif blob_name == 'nps_relacional.parquet':
            processar_nps_relacional_silver(blob_service_client)
        elif blob_name == 'nps_transacional_aquisicao.parquet':
            processar_nps_aquisicao_silver(blob_service_client)
        elif blob_name == 'nps_transacional_implantacao.parquet':
            processar_nps_implantacao_silver(blob_service_client)
        elif blob_name == 'nps_transacional_onboarding.parquet':
            processar_nps_onboarding_silver(blob_service_client)
        elif blob_name == 'nps_transacional_suporte.parquet':
            processar_nps_suporte_silver(blob_service_client)
        elif blob_name == 'nps_transacional_produto.parquet':
            processar_nps_produto_silver(blob_service_client)
        else:
            logging.info(f"Arquivo '{blob_name}' ignorado. Padrão não reconhecido para a camada Silver.")
            return

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o processamento do evento {event.id}: {e}", exc_info=True)

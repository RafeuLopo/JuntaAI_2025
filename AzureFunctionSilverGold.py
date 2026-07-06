import logging
import os
import pandas as pd
from io import BytesIO
import azure.functions as func
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.event_grid_trigger(arg_name="event")
def ProcessarTicketsSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO tickets.parquet ---
    container_silver = "silver"
    blob_name = "tickets.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    tickets = pd.read_parquet(BytesIO(blob_data))

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

    # --- SALVAR NA GOLD COMO df_tickets.parquet ---
    container_gold = "gold"
    output_name = "df_tickets.parquet"

    buffer = BytesIO()
    df_tickets.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarHistoricoSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO historico.parquet ---
    container_silver = "silver"
    blob_name = "historico.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    historico = pd.read_parquet(BytesIO(blob_data))

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

    # --- SALVAR NA GOLD COMO df_historico.parquet ---
    container_gold = "gold"
    output_name = "df_historico.parquet"

    buffer = BytesIO()
    df_historico_agrupado_cliente.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarDadosClientesSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO dados_clientes.parquet ---
    container_silver = "silver"
    blob_name = "dados_clientes.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    dados_clientes = pd.read_parquet(BytesIO(blob_data))

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

    df_dados_clientes_agregado = pd.merge(
        df_dados_clientes_agregado,
        contagem_situacoes,
        on='CD_CLIENTE',
        how='left'
    )

    df_dados_clientes_agregado = pd.merge(
        df_dados_clientes_agregado,
        valor_situacoes,
        on='CD_CLIENTE',
        how='left'
    )

    df_dados_clientes_agregado = pd.merge(
        df_dados_clientes_agregado,
        valor_periodicidades,
        on='CD_CLIENTE',
        how='left'
    )

    df_dados_clientes_final = pd.merge(
        df_dados_clientes_agregado,
        dados_clientes[['CD_CLIENTE', 'DS_SEGMENTO', 'DS_SUBSEGMENTO', 'FAT_FAIXA', 'UF']].drop_duplicates(),
        on='CD_CLIENTE',
        how='left'
    )
    # --- SALVAR NA GOLD COMO df_dados_clientes.parquet ---
    container_gold = "gold"
    output_name = "df_dados_clientes.parquet"

    buffer = BytesIO()
    df_dados_clientes_final.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarClientesDesdeSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO clientes_desde.parquet ---
    container_silver = "silver"
    blob_name = "clientes_desde.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    clientes_desde = pd.read_parquet(BytesIO(blob_data))

    # --- SALVAR NA GOLD COMO df_clientes_desde.parquet ---
    container_gold = "gold"
    output_name = "df_clientes_desde.parquet"

    buffer = BytesIO()
    clientes_desde.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarContratacoes12Silver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO contratacoes_ultimos_12_meses.parquet ---
    container_silver = "silver"
    blob_name = "contratacoes_ultimos_12_meses.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    contratacoes_12meses = pd.read_parquet(BytesIO(blob_data))

    # --- SALVAR NA GOLD COMO df_contratacoes_ultimos_12_meses.parquet ---
    container_gold = "gold"
    output_name = "df_contratacoes_ultimos_12_meses.parquet"

    buffer = BytesIO()
    contratacoes_12meses.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarMRRSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO mrr.parquet ---
    container_silver = "silver"
    blob_name = "mrr.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    mrr = pd.read_parquet(BytesIO(blob_data))

    # --- SALVAR NA GOLD COMO mrr.parquet ---
    container_gold = "gold"
    output_name = "df_mrr.parquet"

    buffer = BytesIO()
    mrr.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSRelacionalSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO nps_relacional.parquet ---
    container_silver = "silver"
    blob_name = "nps_relacional.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    nps_relacional = pd.read_parquet(BytesIO(blob_data))

    nps_relacional_agregado = nps_relacional.groupby('CD_CLIENTE').agg(
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

    # --- SALVAR NA GOLD COMO df_nps_relacional.parquet ---
    container_gold = "gold"
    output_name = "df_nps_relacional.parquet"

    buffer = BytesIO()
    nps_relacional_agregado.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSAquisicaoSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO nps_transacional_aquisicao.parquet ---
    container_silver = "silver"
    blob_name = "nps_transacional_aquisicao.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    nps_transacional_aquisicao = pd.read_parquet(BytesIO(blob_data))

    nps_transacional_aquisicao_agregado = nps_transacional_aquisicao.groupby('CD_CLIENTE').agg(
        AQUISICAO_MEDIA_NOTA_NPS=('NOTA_NPS', 'mean'),
        AQUISICAO_MEDIA_NOTA_AGILIDADE=('NOTA_AGILIDADE', 'mean'),
        AQUISICAO_MEDIA_NOTA_CONHECIMENTO=('NOTA_CONHECIMENTO', 'mean'),
        AQUISICAO_MEDIA_NOTA_CUSTO=('NOTA_CUSTO', 'mean'),
        AQUISICAO_MEDIA_NOTA_FACILIDADE=('NOTA_FACILIDADE', 'mean'),
        AQUISICAO_MEDIA_NOTA_FLEXIBILIDADE=('NOTA_FLEXIBILIDADE', 'mean')
    ).reset_index()

    # --- SALVAR NA GOLD COMO df_nps_transacional_aquisicao.parquet ---
    container_gold = "gold"
    output_name = "df_nps_transacional_aquisicao.parquet"

    buffer = BytesIO()
    nps_transacional_aquisicao_agregado.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSImplantacaoSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO nps_transacional_implantacao.parquet ---
    container_silver = "silver"
    blob_name = "nps_transacional_implantacao.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    nps_transacional_implantacao = pd.read_parquet(BytesIO(blob_data))

    nps_transacional_implantacao_agregado = nps_transacional_implantacao.groupby('CD_CLIENTE').agg(
        IMPLANTACAO_MEDIA_NOTA_NPS=('NOTA_NPS', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_METODOLOGIA=('NOTA_METODOLOGIA', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_GESTAO=('NOTA_GESTAO', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_CONHECIMENTO=('NOTA_CONHECIMENTO', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_QUALIDADE=('NOTA_QUALIDADE', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_COMUNICACAO=('NOTA_COMUNICACAO', 'mean'),
        IMPLANTACAO_MEDIA_NOTA_PRAZOS=('NOTA_PRAZOS', 'mean')
    ).reset_index()

    # --- SALVAR NA GOLD COMO df_nps_transacional_implantacao.parquet ---
    container_gold = "gold"
    output_name = "df_nps_transacional_implantacao.parquet"

    buffer = BytesIO()
    nps_transacional_implantacao_agregado.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSOnboardingSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO nps_transacional_onboarding.parquet ---
    container_silver = "silver"
    blob_name = "nps_transacional_onboarding.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    nps_transacional_onboarding = pd.read_parquet(BytesIO(blob_data))

    nps_transacional_onboarding_agregado = nps_transacional_onboarding.groupby('CD_CLIENTE').agg(
        ONBOARDING_MEDIA_NOTA_RECOMENDACAO=('NOTA_RECOMENDACAO', 'mean'),
        ONBOARDING_MEDIA_NOTA_AJUDA=('NOTA_AJUDA', 'mean'),
        ONBOARDING_MEDIA_NOTA_TEMPO=('NOTA_TEMPO', 'mean'),
        ONBOARDING_MEDIA_NOTA_CLAREZA_CANAL=('NOTA_CLAREZA_CANAL', 'mean'),
        ONBOARDING_MEDIA_NOTA_CLAREZA_GERAL=('NOTA_CLAREZA_GERAL', 'mean'),
        ONBOARDING_MEDIA_NOTA_EXPECTATIVA=('NOTA_EXPECTATIVA', 'mean')
    ).reset_index()

    # --- SALVAR NA GOLD COMO df_nps_transacional_onboarding.parquet ---
    container_gold = "gold"
    output_name = "df_nps_transacional_onboarding.parquet"

    buffer = BytesIO()
    nps_transacional_onboarding_agregado.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSSuporteSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO nps_transacional_suporte.parquet ---
    container_silver = "silver"
    blob_name = "nps_transacional_suporte.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    nps_transacional_suporte = pd.read_parquet(BytesIO(blob_data))

    valor_grupo = (
        nps_transacional_suporte
        .groupby('CD_CLIENTE')['GRUPO_NPS']
        .value_counts()
        .unstack(fill_value=0)
    )

    valor_grupo.columns = ['GRUPO_NPS_' + str(col) for col in valor_grupo.columns]

    nps_transacional_suporte_agregado = nps_transacional_suporte.groupby('CD_CLIENTE').agg(
        SUPORTE_MEDIA_NOTA_NPS=('NOTA_NPS', 'mean'),
        SUPORTE_MEDIA_NOTA_CONHECIMENTO_AG=('NOTA_CONHECIMENTO_AG', 'mean'),
        SUPORTE_MEDIA_NOTA_SOLUCAO=('NOTA_SOLUCAO', 'mean'),
        SUPORTE_MEDIA_NOTA_TEMPO_RETORNO=('NOTA_TEMPO_RETORNO', 'mean'),
        SUPORTE_MEDIA_NOTA_FACILIDADE=('NOTA_FACILIDADE', 'mean'),
        SUPORTE_MEDIA_NOTA_SATISFACAO=('NOTA_SATISFACAO', 'mean'),

        QTD_TICKET_PARA_SUPORTE=('TICKET', 'nunique'),
    ).reset_index()

    nps_transacional_suporte_final = pd.merge(
        nps_transacional_suporte_agregado,
        valor_grupo,
        on='CD_CLIENTE',
        how='left'
    )

    # --- SALVAR NA GOLD COMO df_nps_transacional_suporte.parquet ---
    container_gold = "gold"
    output_name = "df_nps_transacional_suporte.parquet"

    buffer = BytesIO()
    nps_transacional_suporte_final.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")


@app.event_grid_trigger(arg_name="event")
def ProcessarNPSProdutoSilver(event: func.EventGridEvent):
    # --- DADOS DO EVENTO ---
    data = event.get_json()
    url = data["url"]  # URL do blob que chegou
    logging.info(f"Novo arquivo detectado: {url}")

    # --- CONEXÃO AO STORAGE ---
    connect_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # --- LER O BLOB FIXO nps_transacional_produto.parquet ---
    container_silver = "silver"
    blob_name = "nps_transacional_produto.parquet"

    blob_client = blob_service_client.get_blob_client(container=container_silver, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    nps_transacional_produto = pd.read_parquet(BytesIO(blob_data))

    matriz_linha_produto = (
        nps_transacional_produto
        .groupby('CD_CLIENTE')['LINHA_PRODUTO']
        .value_counts()
        .unstack(fill_value=0)
    )

    matriz_linha_produto.columns = ['LINHA_PRODUTO_' + str(col) for col in matriz_linha_produto.columns]

    nota_por_produto = (
        nps_transacional_produto
        .groupby(['CD_CLIENTE', 'LINHA_PRODUTO'])['NOTA_PRODUTO']
        .mean()
        .reset_index()
    )

    matriz_nota_linha_produto = nota_por_produto.pivot(index='CD_CLIENTE', columns='LINHA_PRODUTO', values='NOTA_PRODUTO').fillna(0)
    matriz_nota_linha_produto.columns = ['LINHA_PRODUTO_' + str(col) + '_MEDIA_NOTA' for col in matriz_nota_linha_produto.columns]
    matriz_nota_linha_produto.reset_index(inplace=True)

    matriz_linha_produto = (
        nps_transacional_produto
        .groupby('CD_CLIENTE')['LINHA_PRODUTO']
        .value_counts()
        .unstack(fill_value=0)
    )

    matriz_linha_produto.columns = ['LINHA_PRODUTO_' + str(col) + '_QTD' for col in matriz_linha_produto.columns]
    matriz_linha_produto.reset_index(inplace=True)

    nps_transacional_produto_agregado_final = pd.merge(
        matriz_linha_produto,
        matriz_nota_linha_produto,
        on='CD_CLIENTE',
        how='left'
    )

    # --- SALVAR NA GOLD COMO df_nps_transacional_produto.parquet ---
    container_gold = "gold"
    output_name = "df_nps_transacional_produto.parquet"

    buffer = BytesIO()
    nps_transacional_produto_agregado_final.to_parquet(buffer, index=False)
    buffer.seek(0)

    gold_client = blob_service_client.get_blob_client(container=container_gold, blob=output_name)
    gold_client.upload_blob(buffer, overwrite=True)

    logging.info(f"Arquivo processado e salvo em {container_gold}/{output_name}")
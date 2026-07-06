# Junta.AI - Transformando a Jornada do Cliente TOTVS

Este repositório contém a solução desenvolvida pela equipe **Junta.AI** para o **Enterprise Challenge 2025 da FIAP**, em parceria com a **TOTVS**. O objetivo central do projeto é entender, agrupar e extrair insights acionáveis da vasta base de clientes da empresa, visando otimizar a retenção e o Customer Lifetime Value (CLV).

---

## Contextualização e Problema
A TOTVS possui uma base de clientes diversificada em portes e segmentos, gerando um alto volume de dados (telemetria, contratos, tickets de suporte, NPS, etc.). O desafio proposto foi desenvolver um **modelo de clusterização** para responder a perguntas estratégicas:
- Quais padrões de comportamento existem?
- O que influencia o NPS, churn e expansão?
- Qual o impacto financeiro de perdas e retenção?
- Onde estão os gargalos na jornada do cliente?

---

## Proposta de Solução
A solução foi dividida em múltiplas frentes integradas, cobrindo desde a engenharia de dados até a disponibilização dos insights:

1. **Pipeline de Dados Cloud (Azure):** Arquitetura escalável utilizando Azure Blob Storage (camadas Bronze, Silver e Gold), Azure Functions (orientadas a eventos) e Azure Container Apps (ACA) com KEDA para o tratamento e agregação do alto volume de dados.
2. **Modelo de Clusterização (HDBSCAN):** Após testes com KMeans e GMM, optamos pelo **HDBSCAN**. Por ser baseado em densidade, ele permite a formação de grupos coesos sem forçar o agrupamento de *outliers*, que são classificados como "ruído" para análises individuais de anomalias ou clientes singulares.
3. **API REST com IA Generativa:** Desenvolvida em Flask e integrada à API da OpenAI (GPT-4), essa camada traduz os dados analíticos e matemáticos de cada cluster/cliente para resumos em linguagem natural, democratizando o acesso aos dados para *stakeholders* não técnicos.
4. **Dashboard Estratégico (Power BI):** Visualização interativa da jornada do cliente, exibindo métricas agregadas por cluster (NPS por etapa, probabilidade de churn, engajamento, CLV, etc.).
5. **Modelo de Churn Informativo:** Criação de uma regra de risco focada em retenção (priorizando o *Recall*), auxiliando na análise do impacto financeiro.

---

## Perfis de Clientes Identificados
O modelo HDBSCAN segmentou os clientes em dois macrogrupos (Pagantes e Não Pagantes), revelando os seguintes perfis estatisticamente validados (Kruskal-Wallis e Mann-Whitney U):

### Clientes Pagantes
* **P_C0 (Alto Valor e Alta Complexidade):** Clientes *Premium* (R$ ~39k ativos), diversificados e com alta demanda de suporte. Altíssimo custo em caso de churn.
* **P_C1 (Novos Clientes de Valor Moderado):** Base jovem (~3,7 anos), boas contratações recentes, mas altíssima abertura de tickets.
* **P_C2 (Estáveis e Satisfeitos):** A "base sólida". Bons valores, baixo cancelamento, notas altas de suporte e estabilidade.
* **P_C3 (Baixo Custo e Engajamento):** Menor receita recorrente média, utilizam soluções de entrada e demandam pouca interação.
* **P_C4 (Pequeno Porte e Cautelosos):** Menor valor absoluto em contratos ativos e receita, pouca atividade recente.
* **P_C5 (Padrão e Diversificados):** O **coração do negócio** (>6000 clientes). Estáveis, geram receita consistente e demandam pouco suporte.
* **P_Ruido (Risco Elevado / VIPs):** Clientes singulares. Representam o maior volume financeiro, portfólio complexo, mas com o maior histórico de cancelamentos.

---

## Análise de CLV e Estratégia de Retenção
A análise focada nos clusters bem definidos demonstrou que os clientes que cancelaram possuíam um valor histórico médio mais de duas vezes superior aos ativos. A estratégia sugerida divide-se em:

1. **Contenção em Massa (P_C5):** Sendo a base majoritária, é o cluster responsável pela maior perda absoluta de valor (R$ >15 milhões em histórico perdido). Estratégias sistêmicas são cruciais aqui.
2. **Proteção Premium (P_C0):** O impacto de perder um único cliente deste grupo é massivo (perda de > R$ 771 mil com apenas 9 clientes). Exige retenção altamente personalizada e proativa.

---

## Tecnologias Utilizadas
* **Linguagem & Dados:** Python, Pandas, Scikit-Learn, SciPy
* **Machine Learning:** HDBSCAN, Gradient Boosting (Modelo de Churn preditivo/informativo)
* **Cloud (Microsoft Azure):** Blob Storage, Azure Functions, Azure Container Apps, KEDA
* **Disponibilização & GenAI:** Flask, OpenAI API (GPT-4)
* **Visualização:** Power BI, Matplotlib, Seaborn, Plotly (t-SNE/PCA)
* **Gestão:** ClickUp

---

## Estrutura do Repositório

O repositório está organizado de forma a separar a exploração de dados, modelagem, funções Cloud e a API:

* `Agrupamento1_HDBSCAN.ipynb` e `Agrupamento2_HDBSCAN.ipynb`: Notebooks de pré-processamento, engenharia de *features* e agregações dos arquivos .csv originais.
* `HDBSCAN_MODELO.ipynb`: Construção, treinamento e validação do modelo de clusterização final.
* `ComparacaoEstatistica.ipynb`: Validação estatística dos clusters (Kruskal-Wallis, Mann-Whitney U, Correção de Bonferroni).
* `Churn_Model.ipynb`: Definição e treinamento do modelo informativo de Churn.
* `API_Rest_OpenAI.ipynb`: Código-fonte da API Flask integrada ao GPT-4 para tradução dos dados.
* `AzureFunctionBronzeSilver.ipynb`, `AzureFunctionSilverGold.ipynb`, `AzureFunctionSilverGoldDocker.ipynb`: *Scripts* (em formato notebook para documentação) das arquiteturas *Serverless* e de contêineres na Azure para ingestão e transformação de dados.

---
*Projeto desenvolvido para fins acadêmicos - FIAP e TOTVS Enterprise Challenge 2025.*

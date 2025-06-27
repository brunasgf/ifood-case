# NYC Taxi – iFood Case 

### Visão geral
Este projeto demonstra, passo a passo, como baixar, transformar e analisar os dados públicos de corridas de táxi de Nova York (TLC Trip Data) usando PySpark em Google Colab e armazenando tudo no Google Drive.
Foram contruídas três classes reutilizáveis que cobrem do raw à análise, mantendo o fluxo claro mesmo sem Delta Lake ou DBFS.

### Objetivos
Camada	Objetivo	Ferramenta‑chave
Raw	Baixar Parquets originais (Yellow & Green) direto da CloudFront da TLC e salvá‑los em Drive, particionando por year/month.	TripDataDownloader (requests + Spark)
Silver	Normalizar nomes de colunas, garantir tipos consistentes, remover registros corrompidos, e gerar dois diretórios limpos (silver_layer/yellow, silver_layer/green).	SilverLayerBuilder
Gold / Analytics	Responder às perguntas do case com Pyspark e exibir gráficos — tudo a partir da Silver.	TaxiAnalytics + matplotlib

### Tecnologias utilizadas
Apache Spark (Python API) – processamento distribuído.

Google Colab + Google Drive – ambiente gratuito & armazenamento persistente.

Parquet – formato coluna eficiente; sem Delta Lake.

matplotlib – visualização simples dos resultados.

requests – download dos Parquets originais.

### Justificativas técnicas
Google Drive + Parquet substitui Delta Lake para quem não possui cluster Databricks premium, mantendo baixo custo.

Arquitetura em camadas (Raw → Silver) facilita reprocessos e auditoria.

Classes coesas isolam responsabilidades e tornam o código portátil para outros ambientes Spark.

Particionamento por year/month nos diretórios de destino reduz custo de leitura.

Deduplicação manual na Gold (uso de window + row_number) garante idempotência sem depender de MERGE Delta.

### Resultados das análises
Pergunta	Resultado (exemplo com dados 2023)	
1. Média do total_amount por mês 
![download123](https://github.com/user-attachments/assets/ecfddea5-de2b-4b17-8ab0-0771205ed3df)


2. Média de passenger_count por hora – Maio (Yellow + Green)	
![download1236](https://github.com/user-attachments/assets/797743c7-df3d-4fd8-8108-eb4b64fce5b8)


## Como executar 
Abra os notebooks no Google Colab.

Monte seu Drive:

Configure o caminho‑base (ex.: /content/drive/MyDrive/ifood/teste_2).

Execute célula‑a‑célula:

TripDataDownloader baixa Janeiro‑Maio / 2023 (ou o intervalo que você definir).

SilverLayerBuilder cria silver_layer/yellow e silver_layer/green.

TaxiAnalytics calcula métricas e exibe os gráficos.

Tempo total ~15 min em runtime Colab padrão (vCPU + RAM 12 GB).

### Estrutura do repositório
![image](https://github.com/user-attachments/assets/43b66bbe-2db7-437b-b8bf-6f7e43e2e8b2)

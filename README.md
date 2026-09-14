# Azure Databricks Governed Lakehouse

Dados podem atravessar um pipeline sem critérios claros para chegar ao consumo. Este projeto implementa um Lakehouse governado em PySpark e Delta Lake: regras de qualidade, integridade referencial, unicidade e um Publication Gate controlam a publicação na Silver, enquanto registros inválidos podem ser separados em Quarantine. Na validação real, os seis datasets foram aprovados e publicados sem perda de registros, com 99 testes automatizados passando para proteger os comportamentos do pipeline.

## O problema

Ingerir um arquivo não garante que seus dados sejam confiáveis. Um pedido pode referenciar um produto inexistente, uma chave pode estar duplicada ou um campo pode conter um valor fora do intervalo esperado. Se essas situações seguirem automaticamente para o consumo, análises e decisões podem partir de informações inconsistentes.

A publicação precisa de critérios objetivos: quais dados podem avançar, quais exigem revisão e quais devem ser bloqueados. Neste projeto, qualidade e governança fazem parte do processamento, antes de disponibilizar os dados na Silver.

## A solução

O fluxo implementado preserva a origem, estrutura os dados em Delta Lake e avalia sua qualidade antes da publicação:

```text
Instacart CSV
    ↓
Raw
    ↓
Bronze — Delta Lake
    ↓
Data Quality
    ├── regras de linha
    ├── integridade referencial
    └── unicidade
    ↓
Publication Gate
    ├── APPROVED
    ├── NEEDS_REVIEW
    └── BLOCKED
    ↓ somente APPROVED
Silver — Delta Lake

inválidos → Quarantine
```

NEEDS_REVIEW e BLOCKED impedem a escrita Silver. A persistência de inválidos de linha em Quarantine é explícita; um check de dataset com falha não envia automaticamente toda a tabela para esse destino.

As fases Bronze e Silver estão concluídas e validadas localmente em Windows. **Azure Databricks é a plataforma-alvo**, com execução ainda prevista. **Gold é a próxima fase**, sem implementação concluída.

## Resultado comprovado

A execução real e a validação do projeto comprovaram:

- **6 datasets reais processados e 6/6 aprovados** pelo Publication Gate.
- **0 registros inválidos** e **0 registros enviados para Quarantine**.
- **0 registros perdidos na publicação Silver**: Bronze Rows = Silver Rows nos seis datasets.
- **99 testes automatizados passando**.
- **35 regras de qualidade de linha**, **8 checks de integridade referencial** e **6 checks de unicidade** implementados.

| Dataset | Registros Bronze | Registros Silver | Gate |
|---|---:|---:|---|
| orders | 3.421.083 | 3.421.083 | APPROVED |
| products | 49.688 | 49.688 | APPROVED |
| aisles | 134 | 134 | APPROVED |
| departments | 21 | 21 | APPROVED |
| order_products_prior | 32.434.489 | 32.434.489 | APPROVED |
| order_products_train | 1.384.617 | 1.384.617 | APPROVED |

Além das contagens, foram verificadas a leitura Delta, a presença de `_delta_log` e a preservação integral das colunas e valores Bronze/Data Quality na Silver. Raw e Bronze permaneceram intactos, conforme comparação de hashes antes e depois da publicação.

Esses resultados se referem à execução validada e aos critérios implementados. [Detalhes da publicação e da validação real](docs/silver_publication.md).

## Como funciona

### Raw

Mantém os arquivos originais preservados. A ingestão utiliza esses arquivos somente como origem de leitura.

### Bronze

Lê os CSVs com PySpark e schemas explícitos, persiste em Delta Lake e acrescenta metadados de rastreabilidade:

- `_ingested_at`: instante de ingestão.
- `_source_file`: arquivo de origem.
- `_batch_id`: identificador do lote.

### Data Quality

Aplica 35 regras de linha e executa checks de integridade referencial e de unicidade. Exemplos do catálogo:

- IDs positivos.
- `order_dow` entre 0 e 6.
- `order_hour_of_day` entre 0 e 23.
- `reordered` apenas 0 ou 1.
- `product_id` dos itens de pedido deve existir em `products`.
- `aisle_id` de `products` deve existir em `aisles`.
- Chaves simples ou compostas devem ser únicas.

Uma falha de linha **ERROR** torna o registro inválido. **WARNING** registra um alerta e, isoladamente, mantém a linha em `valid_df`, mas pode exigir revisão do dataset no Gate. As 35 regras atuais e os 14 checks são ERROR; o suporte a WARNING está implementado e é testado com dados sintéticos.

O motor preserva as colunas existentes e adiciona `_dq_failed_rules`, `_dq_error_count`, `_dq_warning_count` e `_dq_checked_at`. A separação entre `valid_df` e `invalid_df` não descarta registros.

### Publication Gate

O Gate recebe o resumo de qualidade e os checks aplicáveis, registra o motivo da decisão e retorna um dos três estados:

| Estado | Decisão |
|---|---|
| APPROVED | Dataset pode seguir para Silver. |
| NEEDS_REVIEW | Dataset precisa de revisão antes da publicação. |
| BLOCKED | Falhas críticas impedem a publicação. |

Critérios implementados:

- Falha ERROR de integridade referencial → **BLOCKED**.
- Falha ERROR de unicidade → **BLOCKED**.
- `error_rate > 1%` → **BLOCKED**.
- `error_rate > 0` e `<= 1%` → **NEEDS_REVIEW**, na ausência de bloqueio.
- Warnings podem gerar **NEEDS_REVIEW**.
- Ausência de erros, warnings e checks com falha, em dataset não vazio → **APPROVED**.

`error_rate` corresponde a registros inválidos divididos pelo total. Dataset vazio exige revisão; BLOCKED prevalece sobre NEEDS_REVIEW. [Regras e política de publicação](docs/data_quality.md).

### Quarantine

Registros inválidos podem ser persistidos separadamente em Delta Lake para investigação, preservando dados de negócio e metadados Bronze/Data Quality. A persistência recebe explicitamente os registros selecionados pelo chamador.

**Na execução real atual, nenhum registro precisou ser enviado para Quarantine.** A função foi implementada e testada com dados sintéticos; sua persistência não foi chamada na publicação real.

### Silver

Publica somente datasets APPROVED, usando `valid_df` como entrada. Preserva todas as colunas de negócio e os metadados Bronze/Data Quality, acrescentando `_published_at`, `_publication_status` e `_publication_reason`.

A publicação em Delta Lake foi validada nos seis datasets reais. Entrada vazia, mesmo com APPROVED fornecido externamente, não escreve nem apaga uma tabela existente.

## Implementado x próximo passo

### Implementado

- [x] Raw
- [x] Bronze em Delta Lake
- [x] Schemas explícitos
- [x] Data Quality
- [x] Integridade referencial
- [x] Unicidade
- [x] Quarantine
- [x] Publication Gate
- [x] Silver em Delta Lake
- [x] Testes automatizados
- [x] Execução real local

### Próximos passos

- [ ] Gold
- [ ] Modelagem analítica
- [ ] Métricas de negócio
- [ ] Execução em Azure Databricks
- [ ] CI/CD completo
- [ ] Observabilidade e lineage integrados, além dos metadados e da auditoria local já existentes

## Stack

- Python 3.11 — execução validada com 3.11.9.
- PySpark 3.5.7 e Apache Spark.
- Delta Lake / `delta-spark` 3.3.2.
- Java 17.
- pytest.
- Ruff.
- Git/GitHub.
- Azure Databricks como plataforma-alvo; execução ainda não realizada.

## Dataset

O projeto utiliza o **Instacart Market Basket Analysis**, com os arquivos:

- `orders.csv`
- `products.csv`
- `aisles.csv`
- `departments.csv`
- `order_products__prior.csv`
- `order_products__train.csv`

Os datasets e as tabelas geradas não são versionados no GitHub. Os seis CSVs devem ser disponibilizados localmente em `data/raw/`.

## Estrutura do projeto

```text
src/
├── bronze/
├── common/
├── quality/
└── silver/

tests/
├── bronze/
├── quality/
└── silver/

data/
├── raw/
├── bronze/
├── silver/
├── gold/
└── quarantine/

docs/
```

O diretório `data/gold/` é reservado para a próxima fase.

## Testes

**99 testes automatizados passando.** Os testes protegem os comportamentos e as regras do pipeline: interpretação dos arquivos, preservação dos dados, classificação de falhas e recusa de publicação quando o Gate não aprova.

A cobertura inclui schemas, ingestão Bronze, parsing CSV, caminhos Windows com espaços, leitura/escrita Delta Lake, Data Quality, integridade referencial, unicidade, Quarantine, Publication Gate e publicação Silver.

A suíte usa dados sintéticos e diretórios temporários, sem depender do dataset real. Uma fixture Spark compartilhada mantém uma única sessão durante os testes.

## Como executar

Requisitos: **Python 3.11** e **Java 17**, com `JAVA_HOME` configurado e Java disponível no `PATH`. As versões das dependências Python estão fixadas em `requirements.txt`.

Na raiz do projeto, em PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

A execução Windows validada utiliza os utilitários Hadoop em `C:\hadoop`, incluindo `bin\winutils.exe` e `bin\hadoop.dll`. Com esses componentes instalados, configure a sessão:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
```

Copie os seis CSVs diretamente para `data/raw/`, sem adicioná-los ao Git. Os testes não precisam desses arquivos.

Execute as verificações:

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest -q
```

Para executar Bronze, a função existente `run_bronze_ingestion` lê Raw e substitui as tabelas Bronze:

```powershell
.\.venv\Scripts\python.exe -c "from src.bronze.ingest import run_bronze_ingestion; run_bronze_ingestion()"
```

Com Bronze disponível e os testes aprovados, o módulo `src.silver.run_local` reavalia os seis Gates antes da primeira escrita Silver. A execução real usa 8 GiB de heap JVM e 64 partições de shuffle, configuradas apenas na sessão:

```powershell
$env:PYSPARK_SUBMIT_ARGS = "--driver-memory 8g pyspark-shell"
.\.venv\Scripts\python.exe -m src.silver.run_local
```

Esse comando substitui as tabelas Silver aprovadas, valida o resultado e gera uma auditoria local. Não altera permanentemente `src/common/spark.py`.

## Decisões técnicas

- **Schemas explícitos, com `inferSchema=False`:** tornam os tipos esperados parte do contrato de ingestão.
- **FAILFAST e validação de cabeçalho:** fazem a leitura falhar diante de incompatibilidades detectadas, sem continuar silenciosamente com registros malformados.
- **Dialeto CSV do Instacart:** aspas delimitam campos e aspas duplicadas representam escape; esse comportamento é configurado e testado.
- **Paths compatíveis com Windows e espaços:** caminhos locais são resolvidos e normalizados antes do uso pelo Spark.
- **Overwrite nesta fase:** permite reexecução local sem acumular duplicações. A idempotência se refere aos registros de negócio; metadados de ingestão e publicação são renovados. Cada tabela tem sua própria transação Delta.
- **Fixture Spark única:** compartilha a sessão na suíte e mantém os destinos de teste em diretórios temporários.
- **Publication Gate antes da Silver:** exige decisão APPROVED antes da escrita; no fluxo real, todos os seis Gates são avaliados antes de publicar a primeira tabela.

## Autor

**Samuel de Andrade Maia**

- [LinkedIn](https://linkedin.com/in/samuelmaia-analytics)
- [GitHub](https://github.com/samuelmaia-analytics)
- [Portfólio](https://samuelmaia-analytics.github.io/samuelmaia-analytics/)

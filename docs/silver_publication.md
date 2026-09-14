# Publicação Silver — Fase 2

`publish_silver(approved_df, dataset_name, destination_path, gate_result)` recebe
somente o `valid_df` selecionado pelo chamador e a decisão do Publication Gate.
APPROVED permite a escrita Delta em modo overwrite no caminho local informado.
NEEDS_REVIEW e BLOCKED retornam `written=False`, status e motivo do Gate, sem
executar ações sobre o DataFrame nem modificar o destino. `row_count` informa
quantos registros foram publicados pela chamada, não o tamanho de uma tabela anterior.

Todas as colunas e valores de entrada são preservados, incluindo metadados Bronze
e DQ. São adicionados `_published_at` (UTC), `_publication_status` (APPROVED) e
`_publication_reason` (motivo exato do Gate). Metadados obrigatórios ausentes,
colunas duplicadas, colisões com metadados Silver e decisão de outro dataset
geram erro antes da escrita. Falhas de armazenamento propagam a exceção.

Entrada vazia com APPROVED é explicitamente uma operação sem escrita: retorna
zero, `written=False` e motivo `Empty input: publication skipped`. Não cria
diretórios nem apaga uma tabela existente. O Gate normal continua retornando
NEEDS_REVIEW para datasets vazios; suas regras e thresholds não foram alterados.

## Execução local e auditoria

Após Ruff, testes Silver e suíte completa aprovados:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
$env:PYSPARK_SUBMIT_ARGS = "--driver-memory 8g pyspark-shell"
.\.venv\Scripts\python.exe -m src.silver.run_local
```

A execução configura 64 partições de shuffle somente na sessão. Lê as seis
tabelas Bronze em Delta, aplica as regras existentes e os 14 checks existentes,
exige APPROVED nos seis Gates e só então publica os respectivos `valid_df`.
Não chama persistência de Quarantine. Cada destino tem sua própria transação
Delta; não há atomicidade conjunta entre os seis destinos. Em falha operacional,
as tabelas já publicadas permanecem e a execução pode ser repetida com overwrite.

`docs/silver_publication_audit.json` é o resumo público da execução validada:
contagens Bronze/Silver, estados do Gate, resultados de qualidade e testes.
Hashes, UUIDs, timestamps técnicos e detalhes de sessão foram removidos para
versionamento, sem alterar os resultados comprovados. O executor local ainda
gera a auditoria detalhada nesse mesmo caminho; uma nova execução exige revisar
e simplificar novamente o arquivo antes de versioná-lo. A validação usa
leitura Delta, igualdade exata de multiconjuntos em ambas as direções sobre todas
as colunas de entrada e verifica os metadados Silver em todas as linhas.
Hashes SHA-256 de todos os arquivos de Raw e Bronze são comparados antes/depois;
Quarantine deve continuar contendo somente `.gitkeep` ou estar vazia.

Os testes usam exclusivamente dados sintéticos e destinos em `tmp_path`.
Nenhuma publicação Gold, agregação analítica, commit ou push faz parte desta etapa.

## Resultado real — 14/09/2026

| Dataset | Bronze Rows | Silver Rows | Gate | Delta log | Status |
|---|---:|---:|---|---|---|
| orders | 3421083 | 3421083 | APPROVED | Sim | OK |
| products | 49688 | 49688 | APPROVED | Sim | OK |
| aisles | 134 | 134 | APPROVED | Sim | OK |
| departments | 21 | 21 | APPROVED | Sim | OK |
| order_products_prior | 32434489 | 32434489 | APPROVED | Sim | OK |
| order_products_train | 1384617 | 1384617 | APPROVED | Sim | OK |

6/6 APPROVED, zero registros perdidos e zero enviados para Quarantine.
Igualdade integral dos valores Bronze/DQ confirmada. Todas as colunas Silver
validadas na execução; o resumo público mantém as contagens de registros. Raw e Bronze intactos
por comparação SHA-256; Quarantine contém somente `.gitkeep`.

Após registrar a conclusão e todas as verificações, o encerramento do Spark
reportou falha ao remover um JAR temporário (`antlr4-runtime`) no Windows.
O comando retornou código 1; o log `silver-real.log` registra `COMPLETE` antes
dos erros de limpeza. Não houve falha de escrita ou de validação das tabelas.

Validação de código: Ruff OK; testes Silver 17/17; suíte completa antes da
publicação 99/99; Ruff final OK e suíte final `pytest -q` 99/99 (código 0).

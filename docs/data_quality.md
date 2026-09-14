# Data Quality e Quarantine

> Este documento descreve a etapa inicial de Quality. A publicação Silver da
> Fase 2 está documentada em [silver_publication.md](silver_publication.md), com
> evidências da execução real em `silver_publication_audit.json`. As referências
> abaixo à ausência de Silver real descrevem o estado anterior à publicação.

A base da Fase 2 aplica regras de linha aos DataFrames Bronze sem ler nem gravar
arquivos. O objetivo é identificar violações preservando os dados originais e sua
rastreabilidade. Não há persistência real em Silver ou Quarantine nesta etapa.

```text
Bronze
  ↓
Data Quality
  ↓
Publication Gate
  ├── APPROVED → futuro Silver
  ├── NEEDS_REVIEW → revisão
  └── BLOCKED → Quarantine (registros inválidos disponíveis)
```

## Regras e severidades

Cada `QualityRule` declara nome, dataset, descrição, condição Spark SQL e severidade.
As expressões são avaliadas como colunas PySpark, sem UDF Python. Importar os módulos
não inicia Spark. Todas as 35 regras iniciais são ERROR; WARNING está disponível
para regras personalizadas e é coberta por testes.

- **ERROR:** uma ou mais falhas tornam a linha inválida.
- **WARNING:** registra o alerta, mas sozinha não torna a linha inválida.
- Predicados com resultado NULL contam como falha. A exceção explícita é o intervalo
  `days_since_prior_order`, que aceita NULL. Um ID nulo falha tanto na regra de
  obrigatoriedade quanto na regra de positividade; os contadores medem regras.

| Dataset | Regras ERROR |
|---|---|
| orders (9) | order_id e user_id não nulos e positivos; eval_set em prior/train/test; order_number ≥ 1; order_dow entre 0 e 6; order_hour_of_day entre 0 e 23; days_since_prior_order nulo ou ≥ 0 |
| products (6) | product_id não nulo e positivo; product_name não nulo e não vazio após trim; aisle_id e department_id positivos |
| aisles (4) | aisle_id não nulo e positivo; aisle não nulo e não vazio após trim |
| departments (4) | department_id não nulo e positivo; department não nulo e não vazio após trim |
| order_products_prior (6) | order_id e product_id não nulos e positivos; add_to_cart_order ≥ 1; reordered em 0/1 |
| order_products_train (6) | mesmas regras de order_products_prior |

## Motor e resultados

```python
checked_df = apply_quality_rules(bronze_df, "orders")
valid_df, invalid_df = split_valid_invalid(checked_df)
summary = build_quality_summary(checked_df, "orders")
```

As três funções recebem DataFrames existentes e não criam sessões nem persistem
resultados. `apply_quality_rules` aceita uma sequência opcional de regras que
substitui a seleção do catálogo. Dataset sem regras, nomes duplicados ou regras de
outro dataset geram erro claro. Colunas DQ preexistentes são rejeitadas para evitar
sobrescrita silenciosa.

Todos os registros recebem `_dq_failed_rules` (array de nomes, na ordem declarada),
`_dq_error_count`, `_dq_warning_count` e `_dq_checked_at` (timestamp UTC fixado por
chamada). Todas as colunas Bronze, inclusive `_ingested_at`, `_source_file` e
`_batch_id`, permanecem intactas em ambos os resultados.

`summary` é um DataFrame de uma linha com dataset, total_rows, valid_rows,
invalid_rows, error_rate, warning_rows e checked_at. A taxa é invalid_rows/total_rows.
warning_rows conta qualquer linha com aviso, inclusive inválidas. Entrada vazia
produz contagens e taxa zero, com timestamp da construção do resumo.

Quarantine será o destino das linhas com ERROR para análise e tratamento posterior;
nenhuma linha é descartada pelo motor. WARNING sozinha permanece no conjunto válido.
O Publication Gate avalia as métricas conforme os critérios abaixo. Os testes usam somente dados
sintéticos, sem acesso aos diretórios reais Raw, Bronze, Silver ou Quarantine.

## Integridade referencial

O catálogo `REFERENTIAL_CHECKS` contém oito checks ERROR:

- products: `aisle_id` deve existir em aisles e `department_id` em departments.
- order_products_prior: `product_id` deve existir em products, `order_id` em orders
  e deve existir um pedido com esse ID e `eval_set = 'prior'`.
- order_products_train: as mesmas referências, com `eval_set = 'train'`.

Cada referência é avaliada por `left_anti` join. `failed_rows` conta linhas da
tabela de origem sem correspondência; referências repetidas não multiplicam as
linhas. Chaves nulas não encontram correspondência e falham. O check de eval_set
usa orders filtrado pelo conjunto esperado: um pedido inexistente falha tanto no
check de existência quanto no de conjunto. Portanto, não se deve somar falhas de
checks diferentes para obter o total de registros inválidos.

## Unicidade

O catálogo `UNIQUENESS_CHECKS` contém seis checks ERROR:

| Dataset | Chave esperada |
|---|---|
| orders | order_id |
| products | product_id |
| aisles | aisle_id |
| departments | department_id |
| order_products_prior | (order_id, product_id) |
| order_products_train | (order_id, product_id) |

As chaves são agrupadas com `groupBy/count`. `failed_rows` soma **todas** as linhas
dos grupos com mais de uma ocorrência: duas cópias de uma chave contam como duas
linhas com falha, não apenas uma excedente. Nulos são agrupados pelo Spark; grupos
nulos repetidos também falham. A obrigatoriedade continua nas regras de linha.
Nenhuma deduplicação ou remoção é executada.

## Checks de dataset e resultado padronizado

Regras `ROW_RULE` verificam atributos de uma linha. Checks
`REFERENTIAL_INTEGRITY` comparam tabelas, e `UNIQUENESS` compara chaves entre linhas.
As 35 regras de linha e seu motor permanecem inalterados. Os novos checks não
reescrevem `_dq_failed_rules` nem repartem registros em Silver/Quarantine.

`run_dataset_checks(datasets)` recebe um mapeamento dos seis nomes para DataFrames
existentes e retorna um DataFrame com 14 resultados. Também é possível chamar
`check_referential_integrity` ou `check_uniqueness` individualmente. Não há criação
de SparkSession, leitura de caminhos, persistência ou coleta de datasets no driver.
Os joins e agregações permanecem distribuídos e são executados quando o chamador
solicitar uma ação Spark.

Cada resultado contém `dataset`, `check_name`, `check_type`, `status` (PASS/FAIL),
`failed_rows`, `total_rows`, `failure_rate`, `severity` e `checked_at` (UTC).
`total_rows` é sempre o tamanho da origem; `failure_rate = failed_rows / total_rows`.
Origem vazia retorna PASS, contagens zero e taxa zero. Entradas ausentes geram erro
claro, não um PASS artificial. A existência de uma referência de conjunto válida
não substitui a unicidade de orders: ambos os checks devem ser considerados.

Essas verificações devem anteceder a Silver: IDs positivos ainda podem apontar
para entidades inexistentes, e chaves repetidas podem multiplicar resultados de
joins e distorcer métricas. Esta etapa apenas diagnostica essas situações; não
publica Silver nem executa gravações sobre os dados reais.

## Publication Gate

`evaluate_publication_gate(summary, checks, policy)` recebe o resumo de linha e os
resultados agregados dos checks do mesmo dataset como dicionários. É possível usar
`Row.asDict()` após obter somente as pequenas linhas de métricas do Spark. O gate
não lê tabelas, não coleta datasets e não escreve arquivos. O chamador deve fornecer
todos os checks relevantes; uma lista omitida significa que nenhum check foi informado,
não uma garantia de que integridade e unicidade foram avaliadas.

O limite inicial está centralizado em `GatePolicy.max_error_rate = 0.01`:

- **BLOCKED:** falha ERROR referencial ou de unicidade, independentemente da taxa;
  ou taxa de linhas inválidas maior que 1%.
- **NEEDS_REVIEW:** taxa maior que zero e até 1%, registros com WARNING,
  check WARNING com falha ou check ROW_RULE ERROR com falha abaixo do limite.
- **APPROVED:** entrada não vazia, zero inválidos e nenhum check falho ou aviso relevante.
- Dataset vazio retorna **NEEDS_REVIEW**, pois ausência de dados não comprova aptidão
  para publicação. Um check bloqueante continua tendo precedência.

Todos os motivos são preservados em `reason`; BLOCKED prevalece sobre NEEDS_REVIEW.
O resultado contém dataset, status, reason, total_rows, valid_rows, invalid_rows,
error_rate, warning_rows, failed_error_checks, failed_warning_checks e checked_at UTC.
Contagens inconsistentes, taxa incompatível, checks duplicados, de outro dataset ou
incompletos geram erro explícito. Uma falha operacional não é convertida em aprovação.

## Persistência lógica de Quarantine

`persist_quarantine(invalid_df, dataset_name, destination_path, batch_id)` recebe o
DataFrame de inválidos selecionado pelo chamador e escreve Delta com overwrite no
caminho exato informado (conceitualmente `data/quarantine/<dataset>`). Não acrescenta
o nome do dataset ao caminho automaticamente. Retorna `QuarantineResult` com dataset,
destination_path, batch_id, row_count e written.

Todas as colunas de entrada são preservadas, incluindo os metadados Bronze e DQ.
São adicionados `_quarantined_at` (UTC), `_quarantine_dataset` e
`_quarantine_batch_id`; este último não substitui o `_batch_id` de origem.
Colunas obrigatórias ausentes ou metadados de quarantine preexistentes são rejeitados.
O chamador deve manter a entrada estável durante a contagem e escrita.

Entrada vazia retorna zero e `written=False`, sem criar diretórios ou tabela.
Se já houver uma tabela no destino, ela fica intacta: o resultado zero se refere
à chamada atual, não ao conteúdo histórico existente. Entrada não vazia substitui
os registros do destino sem acumular cópias; o histórico transacional Delta permanece.

O gate opera no nível do dataset, enquanto a quarantine recebe linhas concretas.
Um bloqueio por FK ou unicidade não produz automaticamente linhas em `invalid_df`:
os checks atuais retornam métricas, não anotam os registros envolvidos. Portanto,
o diagrama expressa o fluxo pretendido; não há orquestrador automático que grave
todo dataset bloqueado, nem suposição de que todo bloqueio tem inválidos de linha.
Inválidos de linha também podem ser separados para quarantine quando o gate exige revisão.
Nesta etapa somente testes sintéticos em `tmp_path` exercitam a persistência;
Silver real e Quarantine real não são escritas.

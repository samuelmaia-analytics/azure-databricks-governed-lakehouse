# Modelo Gold — transformações e publicação real validada

A Gold organiza o catálogo e as compras em granularidades explícitas para
responder perguntas sobre presença de produtos nos pedidos, recompra, composição
de cestas e comportamento de usuários. As transformações PySpark e seus testes
sintéticos foram complementados por execução local explícita sobre a Silver real,
com validação antes da escrita e releitura das quatro tabelas Gold em Delta.
Dashboards e implantação no Azure Databricks continuam fora desta etapa.

## Modelo e dependências

A base dimensional combina uma dimensão produto enriquecida, dois fatos e um
agregado por usuário/conjunto. A dimensão mantém definições conformadas de produto,
aisle e departamento para consumidores e futuros agregados. Não há necessidade
inicial de surrogate keys, SCD2, dimensão usuário contendo apenas um ID ou dimensão
calendário sem datas reais. Indicadores por produto/departamento podem ser calculados
sobre os fatos, sem novas tabelas físicas nesta etapa.

```text
Silver (DataFrames fornecidos pelo chamador)
  ├── products ────────┐
  ├── aisles ──────────┼──→ gold_dim_product
  ├── departments ────┘          ↑ relacionamento por product_id
  │                             │ (join no consumo)
  ├── order_products_prior ─┐    │
  ├── order_products_train ┼──→ gold_fact_order_items ───────────┐
  └── orders ─────────────┘             │                      │
       │                               ↓                      │
       └──────────────────────→ gold_fact_orders              │
                                       │                      │
                                       └──→ gold_user_behavior ←┘
```

As funções não criam SparkSession, leem caminhos ou escrevem tabelas. Recebem
DataFrames e retornam DataFrames. As validações de contratos executam ações
Spark de existência (`isEmpty`) e podem levantar `ValueError` antes do retorno.
Não há coleta de registros para lógica Python, deduplicação ou persistência
automática de intermediários. A projeção remove metadados das fontes antes dos joins.

## Interfaces, granularidades e fontes

| Função / tabela lógica | Entradas | Granularidade / chave |
|---|---|---|
| `build_dim_product` / `gold_dim_product` | products, aisles, departments | product_id |
| `build_fact_order_items` / `gold_fact_order_items` | order_products_prior, order_products_train, orders | (order_id, product_id) |
| `build_fact_orders` / `gold_fact_orders` | orders, saída de build_fact_order_items | order_id |
| `build_user_behavior` / `gold_user_behavior` | saídas de build_fact_orders e build_fact_order_items | (user_id, eval_set) |

As fontes devem cumprir os contratos Silver. As validações Gold reforçam chaves,
associações, domínio de reordered e cobertura de cestas; não substituem todas as
regras do Publication Gate. `build_user_behavior` recebe os fatos canônicos
produzidos pelas funções anteriores a partir do mesmo snapshot. Não reconcilia
fatos arbitrários construídos externamente ou snapshots diferentes.

### gold_dim_product

Colunas: product_id, product_name, aisle_id, aisle, department_id, department.
Products é o lado principal de dois left joins: por aisle_id com aisles e por
department_id com departments. Todo o catálogo é preservado, inclusive produtos
sem compras observadas. Chaves nulas/duplicadas e referências ausentes provocam
erro explícito. Unicidade das referências evita multiplicação; o total de linhas
é o total de products. Não se assume relação direta aisle → department.

Não possui medidas transacionais. Permite analisar produtos cadastrados por
categoria e enriquecer KPIs de itens. Consumidores: BI, Databricks SQL e analistas
de categoria. Integridade product_id entre catálogo e itens é pré-condição Silver;
o executor local também validou essa cobertura nas saídas Gold.

### gold_fact_order_items

Colunas: order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day,
days_since_prior_order, product_id, add_to_cart_order, reordered, item_count.

Une prior/train com unionByName, preservando temporariamente a origem para validar
a associação ao eval_set de orders. O eval_set publicado vem de orders. Pedidos
inexistentes, associação ao conjunto errado (inclusive test), chave composta
duplicada na união ou orders duplicados provocam erro. Não há perda silenciosa
por inner join: referências são verificadas antes dele.

item_count é long com valor 1. reordered e add_to_cart_order são preservados.
KPIs calculáveis no consumo: ocorrências por produto, aisle e departamento,
pedidos distintos contendo cada produto, compradores distintos e taxa de reorder.
Consumidores: análise de cesta, BI e Analytics Engineering.

### gold_fact_orders

Preserva todas as sete colunas de negócio de orders. Acrescenta item_count
(long), reordered_item_count (long), has_reordered_item (boolean) e
items_available (boolean). Os itens são agregados por order_id antes do left join
com orders, evitando multiplicação dos pedidos.

- item_count: contagem de ocorrências de produtos no pedido.
- reordered_item_count: soma de reordered.
- has_reordered_item: reordered_item_count > 0.
- prior/train com conteúdo: items_available = true.
- test: items_available = false; as três medidas de cesta são NULL.
- prior/train sem itens: ValueError; nunca uma cesta vazia artificial.

KPIs: pedidos por hora/dia, distribuição de tamanho de cesta, média de itens por
pedido elegível e proporção de pedidos com algum item recomprado. Contagens de
pedidos usam este fato, evitando o fanout do fato de itens. Consumidores: dashboards
de comportamento e consultas SQL.

### gold_user_behavior

Agrupa pedidos e itens separadamente por (user_id, eval_set) antes de uni-los:

| Medida | Definição |
|---|---|
| orders_observed | Contagem de pedidos no grupo |
| orders_with_items | Contagem de pedidos com items_available = true |
| total_items | Soma de item_count dos pedidos |
| reordered_items | Soma de reordered_item_count |
| distinct_products | Contagem distinta de product_id nos itens do grupo |
| days_since_prior_sum | Soma dos intervalos não nulos |
| days_since_prior_count | Contagem dos intervalos não nulos |
| reorder_rate | reordered_items / total_items |
| avg_items_per_eligible_order | total_items / orders_with_items |
| avg_days_since_prior_order | days_since_prior_sum / days_since_prior_count |

Denominador zero ou desconhecido produz NULL, inclusive com modo ANSI habilitado.
Se todos os intervalos são nulos, a soma permanece NULL, a contagem é zero e a
média é NULL. Um intervalo registrado como zero é válido e entra na contagem.

Em test, orders_observed continua conhecido e orders_with_items = 0 é uma medida
de cobertura conhecida. Total de itens, recompras, produtos distintos e médias/taxas
de cesta permanecem NULL. Intervalos entre pedidos continuam calculáveis quando
informados. NULL nunca é convertido em zero para sugerir ausência de compras.

Consumidores: analistas de comportamento e dashboards de recorrência. KPIs:
distribuição de usuários por número de pedidos, variedade de produtos e taxa de
reorder. Para comparações de histórico entre usuários, prior é o recorte recomendado;
train/test têm coberturas distintas e não representam períodos de calendário.

## Semântica e agregação correta

- Item é uma ocorrência de produto em um pedido, não unidades físicas compradas.
- reordered = 1 significa que o usuário já havia comprado esse produto; não é
  quantidade, indicador de recompra de todo o pedido ou previsão de compra futura.
- Frequência de produto é o número de pedidos distintos que contêm o produto.
- Taxa de reorder é soma de itens recomprados dividida pelo total de ocorrências.
- Média de itens por pedido usa somente pedidos com conteúdo disponível.
- Para combinar grupos, somar numeradores e denominadores antes da divisão.
  Por exemplo, 1/4 e 1/1 produzem taxa global 2/5 = 0,4; a média simples das
  taxas é 0,625 e responde a outra pergunta (usuários com o mesmo peso).
- distinct_products e usuários distintos não são aditivos entre conjuntos;
  recalcular nos fatos quando combinar prior/train. Não somar contagens distintas.
- Produtos com denominadores pequenos exigem contextualização nos rankings;
  nenhum limiar arbitrário é implementado nesta etapa.

## Limitações

Não existem preço, receita, custo, margem, ticket médio monetário, localização ou
demografia nas fontes. Não existe quantidade física por produto. Não existem datas
completas: não calcular retenção mensal, sazonalidade mensal/anual ou crescimento
por calendário. Timestamps técnicos de ingestão/publicação não são datas de compra.
order_dow permanece código 0–6, sem inventar o mapeamento para nomes de dias.

days_since_prior_order é um intervalo registrado limitado a 30 no dataset Instacart;
a média não recupera intervalos reais acima desse limite. O intervalo ausente do
primeiro pedido não é zero. Pedidos test não possuem conteúdo publicado nas fontes.
A amostra não representa toda a operação atual, e reorder não equivale a retenção,
churn, probabilidade futura ou causalidade.

## Reprodutibilidade, performance e publicação futura

As transformações usam funções Spark nativas, projeções antecipadas, joins
explícitos e agregação antes dos joins de pedidos/usuários. Não usam pandas,
Python UDF, collect para transformação, repartition(1) ou partições por IDs.
Os mesmos intermediários podem ser reutilizados pelo chamador; cache e seu
descarte devem ser decididos por ele conforme plano físico/memória disponíveis.
Checks de chaves exigem shuffles; não há promessa de custo nulo de validação.

Lineage completo permanece como próxima evolução. A futura extensão da publicação deve
registrar versões Delta de todas as fontes, commit da transformação e ID da
execução Gold. O chamador deve fornecer snapshots estáveis durante checks e ações.
Não se copia arbitrariamente um único _source_file para saídas com múltiplas fontes.
Reexecuções do mesmo snapshot devem reproduzir os registros de negócio.
Publicações Delta são transações por tabela; o executor local valida e reconcilia
o conjunto, mas não oferece atomicidade conjunta das quatro tabelas.

## Validação sintética

Os testes usam somente valores em memória e a fixture Spark compartilhada do
projeto. Cobrem associações, preservação, chaves, duplicatas, referências ausentes,
cobertura prior/train/test, médias ponderadas, nulos, entradas vazias e divisão
segura. Nenhuma fonte real é necessária ou acessada. Cache é restrito aos fatos
sintéticos da fixture e liberado ao término da sessão.

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest tests/gold -v
.\.venv\Scripts\python.exe -m pytest -q
```

## Execução real validada — 16/09/2026

O executor explícito `src.gold.run_local` lê os seis datasets Silver em Delta,
fixando a versão de cada fonte na leitura. Exige o baseline confirmado, constrói
as quatro transformações existentes e conclui todas as validações antes da primeira
escrita. A configuração de recursos é somente de sessão: driver de 8 GiB e 64
partições de shuffle. Não altera `src/common/spark.py`.

Os intermediários Spark são materializados em cache DISK_ONLY para limitar o uso
de memória; isso não publica tabelas no lakehouse. Somente depois da aprovação
completa os quatro destinos `data/gold/<nome>` são gravados com Delta overwrite.
Não há deduplicação corretiva nem novas tabelas de indicadores.

| Gold | Linhas | Validação e releitura |
|---|---:|---|
| gold_dim_product | 49.688 | PASS |
| gold_fact_order_items | 33.819.106 | PASS |
| gold_fact_orders | 3.421.083 | PASS |
| gold_user_behavior | 412.418 | PASS |

As seis contagens Silver conferiram com o baseline. As chaves das quatro Gold são
únicas. Comparações exatas de multiconjuntos confirmaram a preservação dos produtos,
itens por conjunto de origem e pedidos, além da igualdade pré/pós-persistência.
Os schemas relidos preservam nomes, ordem, tipos e metadados dos campos; a leitura
Delta normaliza sua nulabilidade física, sem alterar os contratos lógicos.

Reconciliações medidas: 33.819.106 itens nas linhas e nas cestas; 19.955.360
ocorrências recompradas em ambos os fatos; 3.421.083 pedidos na soma de
orders_observed. Também foram comparadas medidas por pedido e por usuário/conjunto,
incluindo produtos distintos e fórmulas de taxas/médias.

Cobertura: 3.214.874 pedidos prior, 131.209 train e 75.000 test. Todos os pedidos
test conservaram medidas de cesta NULL; nenhum foi convertido em cesta vazia.
Foram observados 206.209 usuários distintos, taxa global de reorder de
0,5900617242809434 (59,0062% arredondados) e média de 10,10707325550502
itens por pedido elegível (10,1071 arredondados). Foram confirmados 0 registros
perdidos e 0 multiplicados nas reconciliações.
Esses resultados são descritivos da amostra, não previsões ou conclusões causais.

Raw, Bronze, Silver e Quarantine permaneceram inalteradas no inventário de arquivos
(presença, tamanho, modificação e criação), com hash adicional dos arquivos de log
Delta. Não foram calculados hashes dos grandes arquivos de dados. A conferência
ocorreu antes da leitura, antes da publicação e depois das validações persistidas.

O executor registrou `COMPLETE` e a auditoria aprovada antes de falhas de limpeza
de um JAR temporário no encerramento do Spark no Windows. O comando retornou código
1; não houve falha de escrita, validação ou releitura. Essa limitação operacional
não é ocultada nem convertida em erro de qualidade dos dados.

O artefato [gold_publication_audit.json](gold_publication_audit.json) contém
contagens, status, reconciliações, indicadores observados e resultados dos testes finais.
Na revisão documental, os indicadores foram transcritos do resultado já medido
pelo executor, sem nova leitura ou processamento dos dados reais. Nenhuma versão
de commit da Gold ainda não publicada foi inventada; não há cópia arbitrária de
_source_file, UUIDs temporários ou caminhos absolutos no relatório público.
Lineage completo permanece pendente. O README foi atualizado na revisão documental
posterior, sem nova publicação Gold.

Validação final: Ruff OK, testes Gold 31/31 e suíte completa 130/130, todos com
código de saída 0. Nenhum teste foi adicionado nesta execução. O executor regenera
a auditoria de dados; os indicadores adicionais devem ser recuperados da saída
da execução correspondente e os resultados de testes anexados somente depois de
uma nova execução efetivamente concluída, sem reutilizar contagens antigas.

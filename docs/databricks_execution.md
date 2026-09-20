# Fase 4 — preparação para Azure Databricks

**Azure Databricks preparado, mas execução remota ainda não validada**.

Esta fase adapta configuração, paths, aquisição de SparkSession e orquestração.
Não provisiona recursos, configura credenciais, transfere dados ou executa Jobs
remotos. A validação usa testes sintéticos e mocks; a execução real comprovada
continua sendo a local em Windows das fases Bronze, Silver e Gold.

## Inspeção e ajustes

O código anterior resolvia todos os paths com pathlib, verificava arquivos CSV
no filesystem do driver e construía sessões com master local e Delta via pip.
Esses comportamentos não são adequados para URIs distribuídas ou uma sessão
Databricks já existente.

- `src/common/paths.py` normaliza paths locais e preserva dbfs:/, abfss:// e
  /Volumes/. URIs remotas não passam por Path.resolve().
- `src/common/runtime.py` centraliza runtime, defaults e overrides das camadas.
- `src/common/spark.py` mantém o builder local e reutiliza a sessão da plataforma
  em DATABRICKS. Sem sessão ativa/fornecida, falha explicitamente; não cria local[*].
- Bronze mantém verificação local antecipada e usa Spark para verificar acesso
  e cabeçalhos remotos antes da primeira escrita. Esse preflight não garante que
  todo o conteúdo posterior esteja válido; erros de leitura continuam propagados.
- Silver e Quarantine aceitam destinos Spark sem convertê-los para paths locais.
- Os módulos Gold de transformação continuam independentes de armazenamento.

Os executores `src.silver.run_local` e `src.gold.run_local` continuam sendo
ferramentas **exclusivamente locais de auditoria**, com paths do repositório,
baseline fixo, inventário de arquivos, relatórios e recursos locais específicos.
Eles recusam runtime DATABRICKS antes de acessar dados. Não são entrypoints de Jobs.
HADOOP_HOME, winutils e heap do driver pertencem ao ambiente local, não ao runtime
compartilhado. Não foi adicionada configuração Windows ao código compartilhado.

## Configuração

RuntimeConfig é uma dataclass imutável, sem acesso a storage ao ser construída.
Pode ser fornecida diretamente ou criada com RuntimeConfig.from_env().

| Variável | Comportamento |
|---|---|
| RUNTIME_ENV | LOCAL ou DATABRICKS, sem distinção de maiúsculas/minúsculas |
| DATA_BASE_PATH | Base genérica; no LOCAL, default é data/ do repositório |
| DATABRICKS_BASE_PATH | No DATABRICKS, tem precedência sobre DATA_BASE_PATH |
| RAW_PATH | Override do diretório Raw |
| BRONZE_PATH | Override do diretório Bronze |
| SILVER_PATH | Override do diretório Silver |
| GOLD_PATH | Override do diretório Gold |
| QUARANTINE_PATH | Override do diretório Quarantine |

Sem RUNTIME_ENV, DATABRICKS_RUNTIME_VERSION sinaliza DATABRICKS; fora da plataforma,
o default é LOCAL. Recomenda-se configuração explícita nos Jobs. Databricks exige
uma base ou todos os cinco overrides: nunca usa silenciosamente o disco do driver.
Cada override prevalece sobre sua base. Bases derivam raw/bronze/silver/gold/quarantine.
Paths iguais ou aninhados entre camadas são recusados; componentes relativos que
escapem do diretório de um dataset também são recusados. A checagem é lexical:
aliases externos, mounts e permissões precisam ser revisados no workspace real.

Exemplo **ilustrativo**, sem storage configurado:

```text
RUNTIME_ENV=DATABRICKS
DATABRICKS_BASE_PATH=abfss://<container>@<account>.dfs.core.windows.net/<project>
RAW_PATH=/Volumes/<catalog>/<schema>/<volume>/raw
```

Nenhum nome real, secret, chave ou token é definido pelo projeto. Não versionar
.env com credenciais. Permissões, identidades e eventuais secrets devem ser
administrados na plataforma; aceitar um formato de path não concede acesso.

## Storage e Unity Catalog

Unity Catalog Volumes é a opção preferencial para os **arquivos Raw**, quando
disponível. Volumes organizam arquivos não tabulares sob
`/Volumes/<catalog>/<schema>/<volume>/`. O suporte sintático a dbfs:/ serve à
compatibilidade, não representa adoção de DBFS root, cuja utilização não é a
recomendação atual. Referências: [Volumes](https://learn.microsoft.com/en-us/azure/databricks/volumes/)
e [DBFS](https://learn.microsoft.com/en-us/azure/databricks/dbfs/).

Para tabelas Delta, duas opções permanecem abertas:

1. **Managed tables em Unity Catalog:** nomes catalog.schema.table, localização
   e ciclo de vida gerenciados pela plataforma. Exigirá um adaptador de leitura e
   escrita por nome de tabela; saveAsTable ainda não está implementado.
2. **External Delta paths:** diretórios abfss:// autorizados por configuração e
   permissões do workspace. Os entrypoints atuais usam format("delta").load/save
   por path; registro como external table ainda será configurado.

Não usar o diretório Raw de um Volume como localização de tabelas UC. Separar
arquivos e tabelas e decidir armazenamento, permissões e registro com o workspace
real. O projeto está preparado para paths de Volumes, mas não declara catálogos,
schemas, volumes ou external locations e não cria tabelas managed automaticamente.

## Arquitetura e entrypoints

```text
RuntimeConfig → SparkSession fornecida/ativa ou builder local
    ↓
Raw → Bronze → Data Quality + checks → Publication Gate → Silver → Gold
                                         ↓ recusa
                                  falha explícita; Gold não executa
```

`src/pipelines/stages.py` orquestra as funções já existentes. Não duplica regras
de qualidade nem transformações Gold. `src/pipelines/run.py` oferece uma única
CLI e API por etapa, evitando quatro wrappers redundantes:

```powershell
# Na raiz do repositório, após configurar o ambiente e fontes.
# Estes comandos escrevem dados e NÃO foram executados sobre dados reais nesta fase.
.\.venv\Scripts\python.exe -m src.pipelines.run --stage bronze
.\.venv\Scripts\python.exe -m src.pipelines.run --stage silver
.\.venv\Scripts\python.exe -m src.pipelines.run --stage gold
.\.venv\Scripts\python.exe -m src.pipelines.run --stage full
```

Em uma futura task de Databricks Jobs, com o pacote src importável e a sessão da
plataforma disponível, a chamada equivalente é:

```python
from src.common.runtime import RuntimeConfig
from src.pipelines.run import run_pipeline

result = run_pipeline("full", config=RuntimeConfig.from_env(), spark=spark)
print(result)
```

A sessão fornecida ou pertencente ao Databricks nunca é encerrada pelo pipeline.
Uma sessão adquirida internamente no modo LOCAL é encerrada ao terminar, inclusive
em falhas. Não há criação de sessão durante imports. No modo remoto não são
aplicados master local, extensões Delta por pip ou limites de memória locais.

A disponibilização do código no Job (checkout Git e raiz no Python path, ou futuro
wheel) e o modo de compute ainda serão configurados. Não instalar cegamente o
requirements.txt local com versões fixas de pyspark/delta-spark no Databricks
Runtime: a plataforma fornece esses componentes e a compatibilidade será validada
no workspace. Um notebook pode servir como chamada fina, sem mover a lógica para
células; nenhum notebook ou Job remoto foi criado nesta etapa.

## Gates, falhas e limites da orquestração

A etapa Silver lê todos os datasets Bronze, executa regras de linha e os 14 checks
e avalia os seis Gates antes de qualquer escrita Silver. BLOCKED e NEEDS_REVIEW
interrompem toda a etapa. O pipeline completo não segue para Gold nem reutiliza
silenciosamente uma Silver anterior após recusa. As decisões acompanham a exceção.

Gold reutiliza os quatro builders existentes e valida chaves, contagens, cobertura
de produtos e reconciliações de itens/reorders/pedidos antes de escrever qualquer
saída. Não depende de contagens fixas do Instacart. O relatório final contém status
e contagens por etapa; falhas propagam PipelineFailure com o resumo parcial e a
exceção original como causa. A CLI imprime esse resumo e termina com erro.

Quarantine possui path configurável e função de persistência compatível. Sua escrita
continua explícita; o orquestrador não envia automaticamente datasets bloqueados
para Quarantine. Não foi alterada a semântica do Gate.

As escritas continuam em overwrite e as transações Delta são por tabela. Uma falha
de I/O pode deixar tabelas anteriores publicadas; não há rollback entre camadas ou
atomicidade de todo o pipeline. Etapas isoladas pressupõem entradas válidas da
camada anterior. O chamador deve impedir escritas concorrentes nas fontes durante
a execução. Versionamento completo de fontes/lineage, retries, publicação coordenada
e releitura auditável remota permanecem evoluções futuras. Os executores locais
de auditoria têm verificações adicionais e não são chamados pelo fluxo portátil.

## Validação e o que ainda não foi executado

Testes cobrem runtime, precedência de configuração, paths locais/Windows e URIs,
sessão local e reutilização Databricks, dispatch e lifecycle, comportamento de
Gate APPROVED/BLOCKED/NEEDS_REVIEW e falhas antes da escrita. O fluxo sintético
exercita as transformações reais de qualidade e Gold com armazenamento simulado.
As interfaces remotas são mocks: os testes não acessam Azure ou URIs de rede.
Os testes locais existentes continuam usando CSVs sintéticos e Delta temporário.

Não houve provisionamento Azure, upload de dados, execução de notebook/Job remoto,
configuração de Unity Catalog, novo processamento dos dados reais ou publicação
Silver/Gold nesta fase. O README mantém a execução Azure como pendente.

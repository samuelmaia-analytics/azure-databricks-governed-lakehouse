# Recuperação de publicação parcial

Cada tabela Delta possui sua própria transação. A aprovação dos Gates e as
reconciliações antecedem a publicação, mas não há atomicidade entre tabelas.

O executor `src.pipelines.run` interrompe o fluxo ao falhar uma escrita Silver
ou Gold. Seu relatório FAILED contém `publication.completed_datasets`,
`failed_dataset` e `failed_destination_state=UNKNOWN`. Uma falha de comunicação
pode ocorrer depois do commit: não se presume que o destino com erro ficou intacto.

Procedimento para esta implementação de portfólio:

1. Suspender o consumo da camada e impedir execuções concorrentes.
2. Preservar o relatório e inspecionar o histórico Delta de todos os destinos.
3. Manter os arquivos de origem e a camada anterior estáveis durante a recuperação.
4. Corrigir a falha de infraestrutura e reexecutar a etapa inteira com
   `python -m src.pipelines.run --stage silver` ou `--stage gold`.
   A etapa sobrescreve todas as suas tabelas; não retomar só as tabelas restantes.
5. Confirmar SUCCESS, reconciliar/reler os destinos e então liberar o consumo.
   Se a Silver foi reprocessada, reexecutar e validar Gold antes de liberá-la.

A suspensão do consumo é manual. Não existe rollback automático, coordenação
entre escritores nem ponteiro atômico de release. Publicação por versões com
promoção de um manifesto é evolução necessária para consumidores concorrentes.
Não apresentar este fluxo como publicação atômica em produção.

## Quarentena no executor principal

Antes de recusar a Silver, registros com erros de linha são gravados em
`quarantine/<dataset>/<attempt_id>` com identificador único de tentativa. As
execuções não sobrescrevem evidências de tentativas anteriores. Falhas apenas
de unicidade/integridade de dataset não enviam automaticamente toda a tabela
à quarentena. Falha ao persistir evidência também interrompe a publicação.
`GateRejected` inclui os caminhos e contagens de quarentena no relatório.
A política de retenção dessas evidências ainda exige operação/configuração.

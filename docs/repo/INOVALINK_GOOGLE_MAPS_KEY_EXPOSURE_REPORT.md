# Relatorio Conciso de Exposicao de Chave Google Maps

## Resumo

Foi identificada a exposicao de uma chave da Google Maps JavaScript API no conteudo retornado publicamente pelo endpoint Livewire do site Inovalink. A chave aparece no HTML/JavaScript retornado pela funcionalidade de mapa e tambem foi capturada pelo pipeline interno que armazena a resposta bruta para fins de ingestao.

## Evidencias observadas

Durante a validacao, foi confirmado que:

1. A chave e valida.
2. A API JavaScript do Google Maps e carregada com sucesso sem depender exclusivamente do dominio da Inovalink.
3. O mesmo payload da API foi retornado tanto com `Referer: https://www.inovalink.org/` quanto com `Referer: https://example.com/`.
4. Nao foram observados indicadores de bloqueio por referrer, como `RefererNotAllowedMapError`, `InvalidKeyMapError` ou `gm_authFailure`.

Isso indica que a chave esta exposta publicamente e, ao menos para a Maps JavaScript API, aparenta nao estar devidamente restrita por HTTP referrer ao dominio esperado. Embora chaves de navegador possam ser publicas por design, elas devem estar limitadas por referrer e por escopo de APIs autorizadas. Na configuracao observada externamente, ha indicio de restricao ausente ou ampla demais.

## Impacto

A chave pode ser reutilizada por terceiros fora do dominio da aplicacao, o que pode gerar consumo indevido de cota, custos nao autorizados e aumento da superficie de abuso da integracao com Google Maps.

## Recomendacoes

1. Rotacionar a chave exposta.
2. Restringir a chave por HTTP referrer ao(s) dominio(s) estritamente necessarios.
3. Restringir a chave apenas as APIs do Google efetivamente usadas pela aplicacao.
4. Revisar endpoints e respostas publicas para evitar retorno de segredos ou credenciais de terceiros em HTML/JS dinamico.
5. Revisar pipelines internos para nao persistirem ou versionarem respostas brutas contendo chaves de terceiros.
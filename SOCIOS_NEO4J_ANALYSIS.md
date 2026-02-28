# Análise de Deduplicação — Quadro Societário para Neo4j
**Data:** 2026-02-26  
**Base analisada:** `data/cnpj/processed/2026-02/socios_*.parquet` (10 arquivos)  
**Ferramenta:** DuckDB (leitura direta nos Parquets, sem carga em banco)

---

## 1. Visão Geral do Dataset

| Métrica | Valor |
|---|---|
| Total de registros | **27.136.365** |
| Sócio PJ (identificador = 1) | 676.510 (2,5%) |
| Sócio PF (identificador = 2) | 26.447.028 (97,5%) |
| Sócio Estrangeiro (identificador = 3) | 12.827 (0,05%) |

---

## 2. Sócio PJ — CNPJ Inteiro (identificador = 1)

### 2.1 Cardinalidade

| Métrica | Valor |
|---|---|
| Total de linhas PJ | 676.510 |
| CNPJs sócios distintos | 291.269 |
| Pares únicos (CNPJ sócio + CNPJ empresa) | 676.510 |
| Pares duplicados na mesma empresa | **0** |

### 2.2 Interpretação

O campo `cpf_cnpj_socio` para sócios PJ contém o **CNPJ completo com 14 dígitos**. Não há nenhum caso de duplicação do mesmo sócio PJ dentro da mesma empresa — o par `(cpf_cnpj_socio, cnpj_basico)` é sempre único.

A razão entre linhas (676.510) e CNPJs distintos (291.269) revela que um mesmo CNPJ aparece, em média, como sócio em **~2,3 empresas diferentes**. Isso é comportamento esperado e legítimo (holdings, gestoras, fundos).

### 2.3 Top 5 — CNPJs sócios mais recorrentes

| CNPJ | Razão Social | Nº de Empresas como Sócio |
|---|---|---|
| 21609217000173 | MEDICALMAIS SERVICOS EM SAUDE LTDA | 1.610 |
| 31237773000110 | AVDV ESTETICA LTDA | 1.101 |
| 12194903000130 | EBES SISTEMAS DE ENERGIA SA | 1.100 |
| 41833444000195 | LIDER SERVICOS MEDICOS LTDA | 1.072 |
| 12058611000170 | KSB SOLUCOES EM SAUDE | 1.063 |

> **Nota:** Os CNPJs com alta recorrência (>1.000 empresas) são típicos de redes de franquias, cooperativas médicas e grupos empresariais estruturados. São relacionamentos legítimos, não artefatos de dados.

### 2.4 Conclusão — Sócio PJ

**O CNPJ inteiro (14 dígitos) é uma chave natural segura e sem ambiguidade.**

- Estratégia Neo4j: `MERGE (:Empresa {cnpj_basico: substring(cpf_cnpj_socio, 0, 8)})`  
  O sócio PJ é vinculado diretamente ao nó `:Empresa` existente pelo `cnpj_basico` (primeiros 8 dígitos do CNPJ), criando o relacionamento `[:SOCIO_DE]`. Se o nó `:Empresa` ainda não existir (empresa não consta na base), o MERGE o cria com propriedades mínimas.

---

## 3. Sócio PF — CPF Mascarado (identificador = 2 e 3)

### 3.1 Cardinalidade

| Métrica | Valor |
|---|---|
| Total de linhas PF + Estrangeiro | 26.459.855 |
| Nomes distintos | 15.318.319 |
| CPFs mascarados distintos | 999.794 |
| Pares únicos **(nome + cpf_mascarado)** | 17.580.973 |
| Pares duplicados reais (mesmo par na mesma empresa) | **35** |

### 3.2 O Problema do CPF Mascarado

A Receita Federal publica os CPFs no formato `***XXXXXX**`, expondo apenas **6 dos 11 dígitos**. Isso cria um espaço de apenas ~1 milhão de valores distintos para 26 milhões de sócios.

| CPFs mascarados distintos | 999.794 |
|---|---|
| CPFs mascarados c/ **mais de 1 nome diferente** | **998.305 (99,9%)** |

Praticamente **toda máscara de CPF colide com múltiplos nomes distintos**. Usar `cpf_cnpj_socio` sozinho como chave de identidade no Neo4j criaria nós incorretos, fundindo pessoas completamente diferentes num único nó.

**Exemplo de colisão:** a máscara `***042745**` pertence a "ANDERSON COSTA REIS" que aparece como sócio em 3.728 empresas diferentes — mas a mesma máscara pode pertencer a outros indivíduos com CPFs que diferem apenas nos dígitos ocultos.

### 3.3 Homonimia — Mesmo Nome, Múltiplos CPFs Mascarados

| Nome | CPFs mascarados distintos | Empresas |
|---|---|---|
| JOSE CARLOS DA SILVA | 1.937 | 2.549 |
| MARIA APARECIDA DA SILVA | 1.458 | 1.716 |
| JOAO BATISTA DA SILVA | 1.330 | 1.780 |
| LUIZ CARLOS DA SILVA | 1.316 | 1.770 |
| JOSE CARLOS DOS SANTOS | 1.250 | 1.648 |
| ANTONIO CARLOS DA SILVA | 1.175 | 1.653 |
| MARIA JOSE DA SILVA | 1.080 | 1.262 |
| MARCOS ANTONIO DA SILVA | 993 | 1.398 |

Nomes extremamente comuns possuem milhares de CPFs mascarados distintos — **são pessoas físicas homônimas distintas**, não o mesmo indivíduo. Usar apenas o nome como chave seria igualmente errado.

### 3.4 Top 5 — Pares (nome + cpf_mascarado) em mais empresas

| Nome | CPF Mascarado | Nº de Empresas |
|---|---|---|
| ANDERSON COSTA REIS | \*\*\*042745\*\* | 3.728 |
| RICARDO ELIAS RESTUM ANTONIO FILHO | \*\*\*364817\*\* | 2.956 |
| KATIA MARIA BEZERRA SILVA | \*\*\*821354\*\* | 2.204 |
| LUIZ FELIPPE FRANCIS DE LIMA CAMILLO | \*\*\*257087\*\* | 1.638 |
| LEONARDO DE LEMOS LEMER | \*\*\*119427\*\* | 1.238 |

Estes são provavelmente administradores de carteira, gestores de fundos ou sócios de redes de franquias. Os relacionamentos são legítimos.

### 3.5 Duplicados Reais

Apenas **35 pares** com o mesmo `(nome, cpf_mascarado, cnpj_basico)` aparecem mais de uma vez na mesma empresa. São ruídos mínimos da própria base da Receita Federal (provavelmente registros de retificações não consolidadas). Serão naturalmente eliminados via `MERGE` no Neo4j.

### 3.6 Conclusão — Sócio PF

**A chave composta `(nome + cpf_cnpj_socio)` é o identificador mais seguro disponível** dado o mascaramento do CPF.

- 17.580.973 pares únicos para 26.459.855 linhas → fator de compressão de ~1,5x ao criar nós `:Pessoa`
- 4.788.196 pares aparecem em >1 empresa → são os nós mais valiosos para análise de rede (pessoas com participação em múltiplas empresas)
- Apenas 35 duplicatas reais → o MERGE elimina naturalmente

---

## 4. Estratégia de Modelagem Neo4j

### 4.1 Nós

| Label | Chave(s) de MERGE | Fonte |
|---|---|---|
| `:Empresa` | `cnpj_basico` (já existe) | Tabela empresas |
| `:Pessoa` | `nome_socio_razao_social` + `cpf_cnpj_socio` | Sócios PF (id=2,3) |

> Sócio PJ **não gera novo nó** — faz `MERGE` no nó `:Empresa` existente pelo `cnpj_basico = SUBSTRING(cpf_cnpj_socio, 0, 8)`.

### 4.2 Relacionamentos

```
(:Pessoa)-[:SOCIO_DE {qualificacao, data_entrada, faixa_etaria, reference_month}]->(:Empresa)
(:Empresa)-[:SOCIO_DE {qualificacao, data_entrada, reference_month}]->(:Empresa)
```

### 4.3 Constraints e Índices necessários

```cypher
-- Constraint no nó :Pessoa (chave composta via propriedade derivada)
CREATE CONSTRAINT pessoa_id IF NOT EXISTS
FOR (p:Pessoa) REQUIRE p.pessoa_id IS UNIQUE;
-- pessoa_id = SHA256(nome || '|' || cpf_cnpj_socio) ou concatenação direta

-- Índices de busca
CREATE INDEX pessoa_nome IF NOT EXISTS FOR (p:Pessoa) ON (p.nome);
CREATE INDEX pessoa_cpf_mask IF NOT EXISTS FOR (p:Pessoa) ON (p.cpf_cnpj_socio);
CREATE FULLTEXT INDEX pessoa_names IF NOT EXISTS FOR (p:Pessoa) ON EACH [p.nome];
```

### 4.4 Volume estimado no grafo

| Elemento | Estimativa |
|---|---|
| Nós `:Pessoa` | ~17,5M (pares únicos nome+cpfmask) |
| Nós `:Empresa` como sócios (PJ) | ~291K (já existem na base) |
| Relacionamentos `[:SOCIO_DE]` PF | ~26,4M |
| Relacionamentos `[:SOCIO_DE]` PJ | ~676K |
| **Total relacionamentos** | **~27,1M** |

---

## 5. Riscos e Limitações

| Risco | Impacto | Mitigação |
|---|---|---|
| CPF mascarado colide em 99,9% dos casos | Alto — não usar como chave única | Usar par `(nome + cpf_mask)` |
| Homonimia (mesmo nome, CPFs distintos) | Médio — fusão incorreta de pessoas | Par `(nome + cpf_mask)` já distingue |
| Sócio PJ sem `:Empresa` correspondente | Baixo — orphan no grafo | MERGE cria empresa mínima |
| 35 duplicatas reais na mesma empresa | Mínimo | MERGE elimina naturalmente |
| CPF mascarado pode variar entre meses | Médio — mesma pessoa, máscara diferente | Aceitar como limitação da RF; não fazer merge cross-month sem nome |

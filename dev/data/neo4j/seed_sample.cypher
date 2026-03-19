MATCH (n:Pessoa)
DETACH DELETE n;

MATCH (n:Empresa)
DETACH DELETE n;

MERGE (acme:Empresa {cnpj_basico: '12345678'})
SET acme.razao_social = 'ACME INOVACAO S.A.',
    acme.porte_empresa = '05',
    acme.natureza_juridica = 2054,
    acme.capital_social = 5000000.00,
    acme.reference_month = '2026-02';

MERGE (holding:Empresa {cnpj_basico: '87654321'})
SET holding.razao_social = 'HOLDING EXEMPLO LTDA',
    holding.porte_empresa = '03',
    holding.natureza_juridica = 2062,
    holding.capital_social = 1250000.00,
    holding.reference_month = '2026-02';

MERGE (energia:Empresa {cnpj_basico: '11222333'})
SET energia.razao_social = 'NOVA ENERGIA PESQUISA LTDA',
    energia.porte_empresa = '03',
    energia.natureza_juridica = 2062,
    energia.capital_social = 900000.00,
    energia.reference_month = '2026-02';

MERGE (estudio:Empresa {cnpj_basico: '99887766'})
SET estudio.razao_social = 'ESTUDIO AUDIOVISUAL BRASIL LTDA',
    estudio.porte_empresa = '01',
    estudio.natureza_juridica = 2062,
    estudio.capital_social = 750000.00,
    estudio.reference_month = '2026-02';

MERGE (inativa:Empresa {cnpj_basico: '44556677'})
SET inativa.razao_social = 'EMPRESA DESATIVADA SERVICOS LTDA',
    inativa.porte_empresa = '01',
    inativa.natureza_juridica = 2062,
    inativa.capital_social = 150000.00,
    inativa.reference_month = '2026-02';

MERGE (ana:Pessoa {pessoa_id: 'pf:11122233344'})
SET ana.nome = 'ANA PAULA TESTE',
    ana.cpf_cnpj_socio = '11122233344',
    ana.identificador_socio = 2,
    ana.faixa_etaria = 4,
    ana.reference_month = '2026-02';

MERGE (roberto:Pessoa {pessoa_id: 'pf:22233344455'})
SET roberto.nome = 'ROBERTO MENDES',
    roberto.cpf_cnpj_socio = '22233344455',
    roberto.identificador_socio = 2,
    roberto.faixa_etaria = 5,
    roberto.reference_month = '2026-02';

MERGE (carlos:Pessoa {pessoa_id: 'pf:55566677788'})
SET carlos.nome = 'CARLOS LIMA',
    carlos.cpf_cnpj_socio = '55566677788',
    carlos.identificador_socio = 2,
    carlos.faixa_etaria = 6,
    carlos.reference_month = '2026-02';

MERGE (julia:Pessoa {pessoa_id: 'pf:99988877766'})
SET julia.nome = 'JULIA COSTA',
    julia.cpf_cnpj_socio = '99988877766',
    julia.identificador_socio = 2,
    julia.faixa_etaria = 3,
    julia.reference_month = '2026-02';

MERGE (mario:Pessoa {pessoa_id: 'pf:44455566677'})
SET mario.nome = 'MARIO SILVA',
    mario.cpf_cnpj_socio = '44455566677',
    mario.identificador_socio = 2,
    mario.faixa_etaria = 7,
    mario.reference_month = '2026-02';

MATCH (ana:Pessoa {pessoa_id: 'pf:11122233344'})
MATCH (acme:Empresa {cnpj_basico: '12345678'})
MERGE (ana)-[r1:SOCIO_DE]->(acme)
SET r1.qualificacao_socio = '49',
    r1.data_entrada_sociedade = date('2020-05-01'),
    r1.reference_month = '2026-02';

MATCH (roberto:Pessoa {pessoa_id: 'pf:22233344455'})
MATCH (acme:Empresa {cnpj_basico: '12345678'})
MERGE (roberto)-[r2:SOCIO_DE]->(acme)
SET r2.qualificacao_socio = '17',
    r2.data_entrada_sociedade = date('2020-05-01'),
    r2.reference_month = '2026-02';

MATCH (holding:Empresa {cnpj_basico: '87654321'})
MATCH (acme:Empresa {cnpj_basico: '12345678'})
MERGE (holding)-[r3:SOCIO_DE]->(acme)
SET r3.qualificacao_socio = '22',
    r3.data_entrada_sociedade = date('2021-01-01'),
    r3.reference_month = '2026-02';

MATCH (carlos:Pessoa {pessoa_id: 'pf:55566677788'})
MATCH (holding:Empresa {cnpj_basico: '87654321'})
MERGE (carlos)-[r4:SOCIO_DE]->(holding)
SET r4.qualificacao_socio = '49',
    r4.data_entrada_sociedade = date('2018-08-10'),
    r4.reference_month = '2026-02';

MATCH (ana:Pessoa {pessoa_id: 'pf:11122233344'})
MATCH (energia:Empresa {cnpj_basico: '11222333'})
MERGE (ana)-[r5:SOCIO_DE]->(energia)
SET r5.qualificacao_socio = '49',
    r5.data_entrada_sociedade = date('2019-04-22'),
    r5.reference_month = '2026-02';

MATCH (julia:Pessoa {pessoa_id: 'pf:99988877766'})
MATCH (estudio:Empresa {cnpj_basico: '99887766'})
MERGE (julia)-[r6:SOCIO_DE]->(estudio)
SET r6.qualificacao_socio = '49',
    r6.data_entrada_sociedade = date('2017-06-01'),
    r6.reference_month = '2026-02';

MATCH (mario:Pessoa {pessoa_id: 'pf:44455566677'})
MATCH (inativa:Empresa {cnpj_basico: '44556677'})
MERGE (mario)-[r7:SOCIO_DE]->(inativa)
SET r7.qualificacao_socio = '49',
    r7.data_entrada_sociedade = date('2012-09-17'),
    r7.reference_month = '2026-02';

// =====================================================
// FINEP Empresa nodes (real proponents and executors)
// Needed so PROPOSTO_POR / EXECUTADO_POR edges can be
// created when finep_nlp_topic DAG runs in dev mode.
// =====================================================

// Top proponents by project count
MERGE (n:Empresa {cnpj_basico: '18720938'}) SET n.razao_social = 'FUNDACAO DE DESENVOLVIMENTO DA PESQUISA', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '74704008'}) SET n.razao_social = 'FUNDACAO DE APOIO DA UNIVERSIDADE FEDERAL DO RIO GRANDE DO SUL', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '72060999'}) SET n.razao_social = 'FUNDACAO COORDENACAO DE PROJETOS PESQUISAS E ESTUDOS TECNOLOGICOS COPPETEC', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '11735586'}) SET n.razao_social = 'FUNDACAO DE APOIO AO DESENVOLVIMENTO DA UNIVERSIDADE FEDERAL DE PERNAMBUCO', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '51619104'}) SET n.razao_social = 'FUNDACAO DE CIENCIA APLICACOES E TECNOLOGIA ESPACIAIS', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '68314830'}) SET n.razao_social = 'FUNDACAO DE APOIO A UNIVERSIDADE DE SAO PAULO', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '20320503'}) SET n.razao_social = 'FUNDACAO ARTHUR BERNARDES - MATRIZ', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '82895327'}) SET n.razao_social = 'FUNDACAO DE ENSINO E ENGENHARIA DE SANTA CATARINA', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '06220430'}) SET n.razao_social = 'FUNDACAO DE APOIO AO DESENVOLVIMENTO DA COMPUTACAO CIENTIFICA', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '42429480'}) SET n.razao_social = 'FUNDACAO UNIVERSITARIA JOSE BONIFACIO', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '49607336'}) SET n.razao_social = 'FUNDACAO DE DESENVOLVIMENTO DA UNICAMP', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '05572870'}) SET n.razao_social = 'FUNDACAO DE AMPARO E DESENVOLVIMENTO DA PESQUISA', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '08469280'}) SET n.razao_social = 'FUNDACAO NORTE RIO GRANDENSE DE PESQUISA E CULTURA', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '14645162'}) SET n.razao_social = 'FUNDACAO DE APOIO A PESQUISA E A EXTENSAO FAPEX-BA', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '64037492'}) SET n.razao_social = 'FUNDACAO CASIMIRO MONTENEGRO FILHO', n.porte_empresa = '05';

// Top executors (distinct from proponents above)
MERGE (n:Empresa {cnpj_basico: '83899526'}) SET n.razao_social = 'UNIVERSIDADE FEDERAL DE SANTA CATARINA', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '92969856'}) SET n.razao_social = 'UNIVERSIDADE FEDERAL DO RIO GRANDE DO SUL', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '24134488'}) SET n.razao_social = 'UNIVERSIDADE FEDERAL DE PERNAMBUCO', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '33663683'}) SET n.razao_social = 'UNIVERSIDADE FEDERAL DO RIO DE JANEIRO', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '63025530'}) SET n.razao_social = 'UNIVERSIDADE DE SAO PAULO', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '17217985'}) SET n.razao_social = 'UNIVERSIDADE FEDERAL DE MINAS GERAIS', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '24365710'}) SET n.razao_social = 'UNIVERSIDADE FEDERAL DO RIO GRANDE DO NORTE', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '46068425'}) SET n.razao_social = 'UNIVERSIDADE ESTADUAL DE CAMPINAS', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '15180714'}) SET n.razao_social = 'UNIVERSIDADE FEDERAL DA BAHIA', n.porte_empresa = '05';
MERGE (n:Empresa {cnpj_basico: '07272636'}) SET n.razao_social = 'UNIVERSIDADE FEDERAL DO CEARA', n.porte_empresa = '05';

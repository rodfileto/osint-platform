from neo4j import GraphDatabase
d = GraphDatabase.driver('bolt://neo4j:7687', auth=('neo4j','osint_graph_password'))
with d.session() as s:
    s.run('CREATE CONSTRAINT empresa_cnpj_basico IF NOT EXISTS FOR (e:Empresa) REQUIRE e.cnpj_basico IS UNIQUE')
    s.run('CREATE CONSTRAINT pessoa_id IF NOT EXISTS FOR (p:Pessoa) REQUIRE p.pessoa_id IS UNIQUE')
    rows = s.run('SHOW CONSTRAINTS YIELD name, labelsOrTypes, properties').data()
    for r in rows:
        print(r)
d.close()
print("OK")

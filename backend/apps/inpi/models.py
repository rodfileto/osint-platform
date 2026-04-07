from django.db import models


class PatenteDadosBibliograficos(models.Model):
    codigo_interno = models.IntegerField(primary_key=True)
    numero_inpi = models.CharField(max_length=20)
    tipo_patente = models.CharField(max_length=5, null=True, blank=True)
    data_deposito = models.DateField(null=True, blank=True)
    data_protocolo = models.DateField(null=True, blank=True)
    data_publicacao = models.DateField(null=True, blank=True)
    numero_pct = models.CharField(max_length=30, null=True, blank=True)
    numero_wo = models.CharField(max_length=30, null=True, blank=True)
    data_publicacao_wo = models.DateField(null=True, blank=True)
    data_entrada_fase_nacional = models.DateField(null=True, blank=True)
    sigilo = models.BooleanField(default=False)
    snapshot_date = models.DateField()
    loaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."patentes_dados_bibliograficos"'
        verbose_name = 'Patente – Dados Bibliográficos'
        verbose_name_plural = 'Patentes – Dados Bibliográficos'
        ordering = ['-data_deposito', 'numero_inpi']

    def __str__(self):
        return self.numero_inpi


class PatenteConteudo(models.Model):
    codigo_interno = models.IntegerField(primary_key=True)
    numero_inpi = models.CharField(max_length=20, null=True, blank=True)
    titulo = models.TextField(null=True, blank=True)
    resumo = models.TextField(null=True, blank=True)
    snapshot_date = models.DateField()
    loaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."patentes_conteudo"'
        verbose_name = 'Patente – Conteúdo'
        verbose_name_plural = 'Patentes – Conteúdo'

    def __str__(self):
        return f'Conteúdo {self.codigo_interno}'


class PatenteInventor(models.Model):
    # Composite PK (codigo_interno, ordem); codigo_interno used as the Django PK
    # for list-only access — retrieve is not exposed.
    codigo_interno = models.IntegerField(primary_key=True)
    ordem = models.SmallIntegerField()
    autor = models.CharField(max_length=500, null=True, blank=True)
    pais = models.CharField(max_length=5, null=True, blank=True)
    estado = models.CharField(max_length=5, null=True, blank=True)
    snapshot_date = models.DateField()
    loaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."patentes_inventores"'
        verbose_name = 'Patente – Inventor'
        verbose_name_plural = 'Patentes – Inventores'
        ordering = ['codigo_interno', 'ordem']
        unique_together = [('codigo_interno', 'ordem')]

    def __str__(self):
        return f'{self.autor or "Desconhecido"} (patente {self.codigo_interno})'


class PatenteDepositante(models.Model):
    # Composite PK (codigo_interno, ordem); codigo_interno used as the Django PK
    # for list-only access.
    codigo_interno = models.IntegerField(primary_key=True)
    ordem = models.SmallIntegerField()
    pais = models.CharField(max_length=5, null=True, blank=True)
    estado = models.CharField(max_length=5, null=True, blank=True)
    depositante = models.CharField(max_length=500, null=True, blank=True)
    cgccpfdepositante = models.CharField(max_length=30, null=True, blank=True)
    tipopessoadepositante = models.CharField(max_length=30, null=True, blank=True)
    data_inicio = models.DateField(null=True, blank=True)
    data_fim = models.DateField(null=True, blank=True)
    cnpj_basico_resolved = models.CharField(max_length=8, null=True, blank=True)
    snapshot_date = models.DateField()
    loaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."patentes_depositantes"'
        verbose_name = 'Patente – Depositante'
        verbose_name_plural = 'Patentes – Depositantes'
        ordering = ['codigo_interno', 'ordem']
        unique_together = [('codigo_interno', 'ordem')]

    def __str__(self):
        return f'{self.depositante or "Desconhecido"} (patente {self.codigo_interno})'


class PatenteClassificacaoIPC(models.Model):
    # Composite PK (codigo_interno, ordem); codigo_interno used as the Django PK
    # for list-only access.
    codigo_interno = models.IntegerField(primary_key=True)
    ordem = models.SmallIntegerField()
    simbolo = models.CharField(max_length=20, null=True, blank=True)
    versao = models.CharField(max_length=10, null=True, blank=True)
    snapshot_date = models.DateField()
    loaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."patentes_classificacao_ipc"'
        verbose_name = 'Patente – Classificação IPC'
        verbose_name_plural = 'Patentes – Classificações IPC'
        ordering = ['codigo_interno', 'ordem']
        unique_together = [('codigo_interno', 'ordem')]

    def __str__(self):
        return f'{self.simbolo or "?"} (patente {self.codigo_interno})'


class PatenteDespacho(models.Model):
    id = models.BigAutoField(primary_key=True)
    codigo_interno = models.IntegerField()
    numero_rpi = models.IntegerField(null=True, blank=True)
    data_rpi = models.DateField(null=True, blank=True)
    codigo_despacho = models.CharField(max_length=20, null=True, blank=True)
    complemento_despacho = models.TextField(null=True, blank=True)
    snapshot_date = models.DateField()
    loaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."patentes_despachos"'
        verbose_name = 'Patente – Despacho'
        verbose_name_plural = 'Patentes – Despachos'
        ordering = ['codigo_interno', '-data_rpi']

    def __str__(self):
        return f'Despacho {self.codigo_despacho} (patente {self.codigo_interno})'


class PatenteProcurador(models.Model):
    id = models.BigAutoField(primary_key=True)
    codigo_interno = models.IntegerField()
    procurador = models.CharField(max_length=500, null=True, blank=True)
    cgccpfprocurador = models.CharField(max_length=30, null=True, blank=True)
    tipopessoaprocurador = models.CharField(max_length=30, null=True, blank=True)
    data_inicio = models.DateField(null=True, blank=True)
    cnpj_basico_resolved = models.CharField(max_length=8, null=True, blank=True)
    snapshot_date = models.DateField()
    loaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."patentes_procuradores"'
        verbose_name = 'Patente – Procurador'
        verbose_name_plural = 'Patentes – Procuradores'
        ordering = ['codigo_interno', 'id']

    def __str__(self):
        return f'{self.procurador or "Desconhecido"} (patente {self.codigo_interno})'


class PatentePrioridade(models.Model):
    # Composite PK (codigo_interno, pais_prioridade, numero_prioridade)
    # codigo_interno used as the Django PK for list-only access.
    codigo_interno = models.IntegerField(primary_key=True)
    pais_prioridade = models.CharField(max_length=5)
    numero_prioridade = models.CharField(max_length=50)
    data_prioridade = models.DateField(null=True, blank=True)
    snapshot_date = models.DateField()
    loaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."patentes_prioridades"'
        verbose_name = 'Patente – Prioridade'
        verbose_name_plural = 'Patentes – Prioridades'
        ordering = ['codigo_interno', 'pais_prioridade']
        unique_together = [('codigo_interno', 'pais_prioridade', 'numero_prioridade')]

    def __str__(self):
        return f'{self.pais_prioridade}/{self.numero_prioridade} (patente {self.codigo_interno})'


class PatenteVinculo(models.Model):
    # Composite PK (codigo_interno_derivado, codigo_interno_origem, tipo_vinculo)
    # codigo_interno_derivado used as the Django PK for list-only access.
    codigo_interno_derivado = models.IntegerField(primary_key=True)
    codigo_interno_origem = models.IntegerField()
    data_vinculo = models.DateField(null=True, blank=True)
    tipo_vinculo = models.CharField(max_length=5, null=True, blank=True)
    snapshot_date = models.DateField()
    loaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."patentes_vinculos"'
        verbose_name = 'Patente – Vínculo'
        verbose_name_plural = 'Patentes – Vínculos'
        ordering = ['codigo_interno_derivado', 'tipo_vinculo']
        unique_together = [('codigo_interno_derivado', 'codigo_interno_origem', 'tipo_vinculo')]

    def __str__(self):
        return (
            f'Vínculo {self.tipo_vinculo}: {self.codigo_interno_derivado}'
            f' → {self.codigo_interno_origem}'
        )


class PatenteRenumeracao(models.Model):
    # Composite PK (codigo_interno, numero_inpi_original)
    # codigo_interno used as the Django PK for list-only access.
    codigo_interno = models.IntegerField(primary_key=True)
    numero_inpi_original = models.CharField(max_length=20)
    snapshot_date = models.DateField()
    loaded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."patentes_renumeracoes"'
        verbose_name = 'Patente – Renumeração'
        verbose_name_plural = 'Patentes – Renumerações'
        ordering = ['codigo_interno']
        unique_together = [('codigo_interno', 'numero_inpi_original')]

    def __str__(self):
        return f'{self.numero_inpi_original} → {self.codigo_interno}'


class IpcSubclasseRef(models.Model):
    """
    WIPO IPC subclass reference table.
    Seeded from the official IPC XML via scripts/parse_ipc_xml.py.
    Join key: simbolo = mv_patent_search.ipc_secao_classe
    """

    simbolo = models.CharField(max_length=4, primary_key=True)
    secao = models.CharField(max_length=1)
    classe = models.CharField(max_length=3)
    titulo_en = models.TextField()
    edition = models.CharField(max_length=10)

    class Meta:
        managed = False
        db_table = '"inpi"."ipc_subclasse_ref"'
        verbose_name = 'IPC Subclasse (referência)'
        verbose_name_plural = 'IPC Subclasses (referência)'
        ordering = ['simbolo']

    def __str__(self):
        return f'{self.simbolo} – {self.titulo_en[:60]}'


class MvPatenteSearch(models.Model):
    """
    Modelo não-gerenciado que mapeia a materialized view inpi.mv_patent_search.

    Desnormaliza dados bibliográficos + conteúdo + primeiro inventor +
    primeiro depositante + classificação IPC primária.
    Populada/atualizada pelo DAG inpi_load_postgres.
    """

    codigo_interno = models.IntegerField(primary_key=True)
    numero_inpi = models.CharField(max_length=20)
    tipo_patente = models.CharField(max_length=5, null=True, blank=True)
    data_deposito = models.DateField(null=True, blank=True)
    data_publicacao = models.DateField(null=True, blank=True)
    sigilo = models.BooleanField(default=False)
    snapshot_date = models.DateField()
    titulo = models.TextField(null=True, blank=True)
    inventor_principal = models.CharField(max_length=500, null=True, blank=True)
    inventor_pais = models.CharField(max_length=5, null=True, blank=True)
    depositante_principal = models.CharField(max_length=500, null=True, blank=True)
    depositante_tipo = models.CharField(max_length=30, null=True, blank=True)
    depositante_cnpj_basico = models.CharField(max_length=8, null=True, blank=True)
    ipc_principal = models.CharField(max_length=20, null=True, blank=True)
    ipc_secao_classe = models.CharField(max_length=4, null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"inpi"."mv_patent_search"'
        verbose_name = 'Patente (busca)'
        verbose_name_plural = 'Patentes (busca)'
        ordering = ['-data_deposito', 'numero_inpi']

    def __str__(self):
        return f'{self.numero_inpi} – {self.titulo or "Sem título"}'

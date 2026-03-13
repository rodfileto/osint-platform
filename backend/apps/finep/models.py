from django.db import models


class FinepModelBase(models.Model):
    created_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        abstract = True


class FinepTrackedModel(FinepModelBase):
    updated_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        abstract = True


class ProjetoOperacaoDireta(FinepTrackedModel):
    id = models.AutoField(primary_key=True)
    instrumento = models.CharField(max_length=100)
    demanda = models.TextField(null=True, blank=True)
    ref = models.CharField(max_length=20, null=True, blank=True)
    contrato = models.CharField(max_length=20, null=True, blank=True)
    data_assinatura = models.DateField(null=True, blank=True)
    prazo_execucao_original = models.DateField(null=True, blank=True)
    prazo_execucao = models.DateField(null=True, blank=True)
    titulo = models.TextField(null=True, blank=True)
    status = models.CharField(max_length=50, null=True, blank=True)
    proponente = models.TextField(null=True, blank=True)
    cnpj_proponente = models.CharField(max_length=18, null=True, blank=True)
    cnpj_proponente_norm = models.CharField(max_length=14, null=True, blank=True, editable=False)
    executor = models.TextField(null=True, blank=True)
    cnpj_executor = models.CharField(max_length=18, null=True, blank=True)
    cnpj_executor_norm = models.CharField(max_length=14, null=True, blank=True, editable=False)
    municipio = models.CharField(max_length=100, null=True, blank=True)
    uf = models.CharField(max_length=2, null=True, blank=True)
    valor_finep = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    contrapartida_financeira = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    contrapartida_nao_financeira = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    valor_pago = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    intervenientes = models.IntegerField(default=0)
    aporte_financeiro_interv = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    aporte_nao_financeiro_interv = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    resumo_publicavel = models.TextField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"finep"."projetos_operacao_direta"'
        verbose_name = 'Projeto Operacao Direta FINEP'
        verbose_name_plural = 'Projetos Operacao Direta FINEP'
        ordering = ['-data_assinatura', 'contrato']

    def __str__(self):
        return self.contrato or self.ref or f'Projeto {self.pk}'


class ProjetoCreditoDescentralizado(FinepTrackedModel):
    id = models.AutoField(primary_key=True)
    data_assinatura = models.DateField(null=True, blank=True)
    contrato = models.CharField(max_length=100, null=True, blank=True)
    proponente = models.TextField(null=True, blank=True)
    cnpj_proponente = models.CharField(max_length=18, null=True, blank=True)
    cnpj_proponente_norm = models.CharField(max_length=14, null=True, blank=True, editable=False)
    uf = models.CharField(max_length=2, null=True, blank=True)
    valor_financiado = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    valor_liberado = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    contrapartida = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    outros_recursos = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    agente_financeiro = models.TextField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"finep"."projetos_credito_descentralizado"'
        verbose_name = 'Projeto Credito Descentralizado FINEP'
        verbose_name_plural = 'Projetos Credito Descentralizado FINEP'
        ordering = ['-data_assinatura', 'contrato']

    def __str__(self):
        return self.contrato or self.proponente or f'Credito descentralizado {self.pk}'


class ProjetoInvestimento(FinepTrackedModel):
    id = models.AutoField(primary_key=True)
    ref = models.CharField(max_length=20, null=True, blank=True)
    numero_contrato = models.CharField(max_length=20, null=True, blank=True)
    cnpj_proponente = models.CharField(max_length=18, null=True, blank=True)
    cnpj_proponente_norm = models.CharField(max_length=14, null=True, blank=True, editable=False)
    proponente = models.TextField(null=True, blank=True)
    data_assinatura = models.DateField(null=True, blank=True)
    data_follow_on = models.DateField(null=True, blank=True)
    valor_follow_on = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    valor_total_contratado = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    valor_total_liberado = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    opcao_compra_exercida = models.BooleanField(null=True, blank=True)
    opcao_compra_prorrogada = models.BooleanField(null=True, blank=True)
    data_exercicio_opcao = models.DateField(null=True, blank=True)
    valuation_opcao = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    participacao_finep = models.DecimalField(max_digits=7, decimal_places=4, null=True, blank=True)
    desinvestimento_realizado = models.BooleanField(null=True, blank=True)
    data_desinvestimento = models.DateField(null=True, blank=True)
    valor_desinvestimento = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    tir = models.DecimalField(max_digits=10, decimal_places=6, null=True, blank=True)
    moi = models.DecimalField(max_digits=10, decimal_places=4, null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"finep"."projetos_investimento"'
        verbose_name = 'Projeto Investimento FINEP'
        verbose_name_plural = 'Projetos Investimento FINEP'
        ordering = ['-data_assinatura', 'numero_contrato']

    def __str__(self):
        return self.numero_contrato or self.ref or self.proponente or f'Investimento {self.pk}'


class ProjetoAncine(FinepTrackedModel):
    id = models.AutoField(primary_key=True)
    instrumento = models.CharField(max_length=100, null=True, blank=True)
    demanda = models.TextField(null=True, blank=True)
    ref = models.CharField(max_length=20, null=True, blank=True)
    contrato = models.CharField(max_length=20, null=True, blank=True)
    data_assinatura = models.DateField(null=True, blank=True)
    prazo_execucao = models.DateField(null=True, blank=True)
    titulo = models.TextField(null=True, blank=True)
    status = models.CharField(max_length=50, null=True, blank=True)
    proponente = models.TextField(null=True, blank=True)
    cnpj_proponente = models.CharField(max_length=18, null=True, blank=True)
    cnpj_proponente_norm = models.CharField(max_length=14, null=True, blank=True, editable=False)
    executor = models.TextField(null=True, blank=True)
    municipio = models.CharField(max_length=100, null=True, blank=True)
    uf = models.CharField(max_length=2, null=True, blank=True)
    valor_finep = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    contrapartida_financeira = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    contrapartida_nao_financeira = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    valor_pago = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    intervenientes = models.IntegerField(default=0)
    aporte_financeiro_interv = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    aporte_nao_financeiro_interv = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"finep"."projetos_ancine"'
        verbose_name = 'Projeto Ancine FINEP'
        verbose_name_plural = 'Projetos Ancine FINEP'
        ordering = ['-data_assinatura', 'contrato']

    def __str__(self):
        return self.contrato or self.ref or self.titulo or f'Ancine {self.pk}'


class LiberacaoOperacaoDireta(FinepModelBase):
    id = models.AutoField(primary_key=True)
    contrato = models.CharField(max_length=20)
    ref = models.CharField(max_length=20, null=True, blank=True)
    num_parcela = models.SmallIntegerField(null=True, blank=True)
    num_liberacao = models.SmallIntegerField(null=True, blank=True)
    data_liberacao = models.DateField(null=True, blank=True)
    valor_liberado = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"finep"."liberacoes_operacao_direta"'
        verbose_name = 'Liberacao Operacao Direta FINEP'
        verbose_name_plural = 'Liberacoes Operacao Direta FINEP'
        ordering = ['-data_liberacao', 'contrato']

    def __str__(self):
        return f'{self.contrato} - parcela {self.num_parcela or "s/n"}'


class LiberacaoCreditoDescentralizado(FinepModelBase):
    id = models.AutoField(primary_key=True)
    contrato = models.CharField(max_length=100)
    cnpj_proponente = models.CharField(max_length=18, null=True, blank=True)
    cnpj_proponente_norm = models.CharField(max_length=14, null=True, blank=True, editable=False)
    num_parcela = models.SmallIntegerField(null=True, blank=True)
    data_liberacao = models.DateField(null=True, blank=True)
    valor_liberado = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"finep"."liberacoes_credito_descentralizado"'
        verbose_name = 'Liberacao Credito Descentralizado FINEP'
        verbose_name_plural = 'Liberacoes Credito Descentralizado FINEP'
        ordering = ['-data_liberacao', 'contrato']

    def __str__(self):
        return f'{self.contrato} - parcela {self.num_parcela or "s/n"}'


class LiberacaoAncine(FinepModelBase):
    id = models.AutoField(primary_key=True)
    contrato = models.CharField(max_length=20)
    ref = models.CharField(max_length=20, null=True, blank=True)
    num_parcela = models.SmallIntegerField(null=True, blank=True)
    num_liberacao = models.SmallIntegerField(null=True, blank=True)
    data_liberacao = models.DateField(null=True, blank=True)
    valor_liberado = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)

    class Meta:
        managed = False
        db_table = '"finep"."liberacoes_ancine"'
        verbose_name = 'Liberacao Ancine FINEP'
        verbose_name_plural = 'Liberacoes Ancine FINEP'
        ordering = ['-data_liberacao', 'contrato']

    def __str__(self):
        return f'{self.contrato} - parcela {self.num_parcela or "s/n"}'

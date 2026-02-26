from django.db import models


# ============================================================================
# AUXILIARY TABLES (Reference Data)
# ============================================================================

class CNAE(models.Model):
    """Economic activity codes"""
    codigo = models.CharField(max_length=7, primary_key=True)
    descricao = models.CharField(max_length=255)

    class Meta:
        db_table = '"cnpj"."cnae"'
        verbose_name = 'CNAE'
        verbose_name_plural = 'CNAEs'

    def __str__(self):
        return f"{self.codigo} - {self.descricao}"


class MotivoSituacaoCadastral(models.Model):
    """Registration status reason codes"""
    codigo = models.IntegerField(primary_key=True)
    descricao = models.CharField(max_length=255)

    class Meta:
        db_table = '"cnpj"."motivo_situacao_cadastral"'
        verbose_name = 'Motivo Situação Cadastral'
        verbose_name_plural = 'Motivos Situação Cadastral'

    def __str__(self):
        return f"{self.codigo} - {self.descricao}"


class Municipio(models.Model):
    """Municipality codes"""
    codigo = models.IntegerField(primary_key=True)
    descricao = models.CharField(max_length=255)

    class Meta:
        db_table = '"cnpj"."municipio"'
        verbose_name = 'Município'
        verbose_name_plural = 'Municípios'

    def __str__(self):
        return f"{self.codigo} - {self.descricao}"


class NaturezaJuridica(models.Model):
    """Legal nature codes"""
    codigo = models.IntegerField(primary_key=True)
    descricao = models.CharField(max_length=255)

    class Meta:
        db_table = '"cnpj"."natureza_juridica"'
        verbose_name = 'Natureza Jurídica'
        verbose_name_plural = 'Naturezas Jurídicas'

    def __str__(self):
        return f"{self.codigo} - {self.descricao}"


class Pais(models.Model):
    """Country codes"""
    codigo = models.IntegerField(primary_key=True)
    descricao = models.CharField(max_length=255)

    class Meta:
        db_table = '"cnpj"."pais"'
        verbose_name = 'País'
        verbose_name_plural = 'Países'

    def __str__(self):
        return f"{self.codigo} - {self.descricao}"


class QualificacaoSocio(models.Model):
    """Partner qualification codes"""
    codigo = models.IntegerField(primary_key=True)
    descricao = models.CharField(max_length=255)

    class Meta:
        db_table = '"cnpj"."qualificacao_socio"'
        verbose_name = 'Qualificação Sócio'
        verbose_name_plural = 'Qualificações Sócios'

    def __str__(self):
        return f"{self.codigo} - {self.descricao}"


# ============================================================================
# MAIN TABLES (CNPJ Data)
# ============================================================================

class Empresa(models.Model):
    """Base company information (first 8 digits of CNPJ)"""
    
    class PorteEmpresa(models.TextChoices):
        NAO_INFORMADO = '00', 'Não informado'
        MICRO_EMPRESA = '01', 'Micro empresa'
        EMPRESA_PEQUENO_PORTE = '03', 'Empresa de pequeno porte'
        DEMAIS = '05', 'Demais'

    cnpj_basico = models.CharField(
        max_length=8,
        primary_key=True,
        help_text='Base CNPJ number (first 8 digits)'
    )
    razao_social = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text='Company legal name'
    )
    natureza_juridica = models.ForeignKey(
        NaturezaJuridica,
        on_delete=models.SET_NULL,
        null=True,
        db_column='natureza_juridica',
        help_text='Legal nature code'
    )
    qualificacao_responsavel = models.ForeignKey(
        QualificacaoSocio,
        on_delete=models.SET_NULL,
        null=True,
        db_column='qualificacao_responsavel',
        help_text='Responsible person qualification'
    )
    capital_social = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        help_text='Registered capital'
    )
    porte_empresa = models.CharField(
        max_length=2,
        null=True,
        choices=PorteEmpresa.choices,
        help_text='Company size code'
    )
    ente_federativo_responsavel = models.CharField(
        max_length=255,
        null=True,
        help_text='Responsible federal entity'
    )

    class Meta:
        db_table = '"cnpj"."empresa"'
        verbose_name = 'Empresa'
        verbose_name_plural = 'Empresas'
        indexes = [
            models.Index(fields=['razao_social']),
        ]

    def __str__(self):
        return f"{self.cnpj_basico} - {self.razao_social or 'Sem razão social'}"


class Estabelecimento(models.Model):
    """Branch/establishment information (complete 14-digit CNPJ)"""
    
    class MatrizFilial(models.IntegerChoices):
        MATRIZ = 1, 'Matriz'
        FILIAL = 2, 'Filial'

    class SituacaoCadastral(models.IntegerChoices):
        NULA = 1, 'Nula'
        ATIVA = 2, 'Ativa'
        SUSPENSA = 3, 'Suspensa'
        INAPTA = 4, 'Inapta'
        BAIXADA = 8, 'Baixada'

    empresa = models.ForeignKey(
        Empresa,
        on_delete=models.CASCADE,
        related_name='estabelecimentos',
        db_column='cnpj_basico'
    )

    # CNPJ components
    cnpj_ordem = models.CharField(
        max_length=4,
        help_text='Establishment number (digits 9-12 of CNPJ)'
    )
    cnpj_dv = models.CharField(
        max_length=2,
        help_text='Check digit (last 2 digits of CNPJ)'
    )

    # Identification
    identificador_matriz_filial = models.IntegerField(
        choices=MatrizFilial.choices,
        help_text='Headquarters or branch identifier'
    )
    nome_fantasia = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text='Trade name'
    )

    # Status
    situacao_cadastral = models.IntegerField(
        choices=SituacaoCadastral.choices,
        null=True,
        help_text='Registration status'
    )
    data_situacao_cadastral = models.IntegerField(
        null=True,
        help_text='Status event date (YYYYMMDD)'
    )
    motivo_situacao_cadastral = models.ForeignKey(
        MotivoSituacaoCadastral,
        on_delete=models.SET_NULL,
        null=True,
        db_column='motivo_situacao_cadastral',
        help_text='Status reason code'
    )

    # International
    nome_cidade_exterior = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text='Foreign city name'
    )
    pais = models.ForeignKey(
        Pais,
        on_delete=models.SET_NULL,
        null=True,
        db_column='pais',
        help_text='Country code'
    )

    # Activity
    data_inicio_atividade = models.IntegerField(
        null=True,
        help_text='Activity start date (YYYYMMDD)'
    )
    cnae_fiscal_principal = models.ForeignKey(
        CNAE,
        on_delete=models.SET_NULL,
        null=True,
        related_name='estabelecimentos_principal',
        db_column='cnae_fiscal_principal',
        help_text='Main economic activity code'
    )
    cnae_fiscal_secundaria = models.TextField(
        null=True,
        blank=True,
        help_text='Secondary economic activity codes (comma-separated)'
    )

    # Address
    tipo_logradouro = models.CharField(
        max_length=255,
        null=True,
        help_text='Street type description'
    )
    logradouro = models.CharField(
        max_length=255,
        null=True,
        help_text='Street name'
    )
    numero = models.CharField(
        max_length=255,
        null=True,
        help_text='Street number'
    )
    complemento = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text='Address complement'
    )
    bairro = models.CharField(
        max_length=255,
        null=True,
        help_text='Neighborhood'
    )
    cep = models.CharField(
        max_length=8,
        null=True,
        help_text='Postal code'
    )
    uf = models.CharField(
        max_length=2,
        null=True,
        help_text='State code'
    )
    municipio = models.ForeignKey(
        Municipio,
        on_delete=models.SET_NULL,
        null=True,
        db_column='municipio',
        help_text='Municipality code'
    )

    # Contact
    ddd_1 = models.CharField(
        max_length=4,
        null=True,
        blank=True,
        help_text='Primary phone area code'
    )
    telefone_1 = models.CharField(
        max_length=8,
        null=True,
        blank=True,
        help_text='Primary phone number'
    )
    ddd_2 = models.CharField(
        max_length=4,
        null=True,
        blank=True,
        help_text='Secondary phone area code'
    )
    telefone_2 = models.CharField(
        max_length=8,
        null=True,
        blank=True,
        help_text='Secondary phone number'
    )
    ddd_fax = models.CharField(
        max_length=4,
        null=True,
        blank=True,
        help_text='Fax area code'
    )
    fax = models.CharField(
        max_length=8,
        null=True,
        blank=True,
        help_text='Fax number'
    )
    correio_eletronico = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text='Email address'
    )

    # Special situation
    situacao_especial = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text='Special situation description'
    )
    data_situacao_especial = models.IntegerField(
        null=True,
        blank=True,
        help_text='Special situation date (YYYYMMDD)'
    )

    class Meta:
        db_table = '"cnpj"."estabelecimento"'
        verbose_name = 'Estabelecimento'
        verbose_name_plural = 'Estabelecimentos'
        indexes = [
            models.Index(fields=['situacao_cadastral']),
            models.Index(fields=['uf']),
            models.Index(fields=['municipio']),
            models.Index(fields=['cnae_fiscal_principal']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['empresa', 'cnpj_ordem', 'cnpj_dv'],
                name='unique_estabelecimento_cnpj'
            )
        ]

    def __str__(self):
        return f"{self.cnpj_completo} - {self.nome_fantasia or 'Sem nome fantasia'}"

    @property
    def cnpj_completo(self):
        """Returns the complete 14-digit CNPJ number"""
        return f"{self.empresa_id}{self.cnpj_ordem}{self.cnpj_dv}"

    @property
    def telefone_1_completo(self):
        """Returns formatted primary phone number"""
        if self.ddd_1 and self.telefone_1:
            return f"({self.ddd_1}) {self.telefone_1}"
        return None

    @property
    def telefone_2_completo(self):
        """Returns formatted secondary phone number"""
        if self.ddd_2 and self.telefone_2:
            return f"({self.ddd_2}) {self.telefone_2}"
        return None


class Socio(models.Model):
    """Partner/shareholder information"""
    
    empresa = models.ForeignKey(
        Empresa,
        on_delete=models.CASCADE,
        related_name='socios',
        db_column='cnpj_basico'
    )
    identificador_socio = models.IntegerField(
        null=True,
        help_text='Partner type identifier'
    )
    nome_socio_razao_social = models.CharField(
        max_length=255,
        null=True,
        help_text='Partner name or company name'
    )
    cpf_cnpj_socio = models.CharField(
        max_length=14,
        null=True,
        help_text='Partner CPF or CNPJ'
    )
    qualificacao_socio = models.ForeignKey(
        QualificacaoSocio,
        on_delete=models.SET_NULL,
        null=True,
        db_column='qualificacao_socio',
        help_text='Partner qualification code'
    )
    data_entrada_sociedade = models.IntegerField(
        null=True,
        help_text='Date joined company (YYYYMMDD)'
    )
    pais = models.ForeignKey(
        Pais,
        on_delete=models.SET_NULL,
        null=True,
        db_column='pais',
        help_text='Country code'
    )
    representante_legal = models.CharField(
        max_length=14,
        null=True,
        help_text='Legal representative CPF'
    )
    nome_do_representante = models.CharField(
        max_length=255,
        null=True,
        help_text='Legal representative name'
    )
    qualificacao_representante_legal = models.ForeignKey(
        QualificacaoSocio,
        on_delete=models.SET_NULL,
        null=True,
        related_name='representantes',
        db_column='qualificacao_representante_legal',
        help_text='Representative qualification code'
    )
    faixa_etaria = models.IntegerField(
        null=True,
        help_text='Age range code'
    )

    class Meta:
        db_table = '"cnpj"."socio"'
        verbose_name = 'Sócio'
        verbose_name_plural = 'Sócios'
        indexes = [
            models.Index(fields=['empresa']),
            models.Index(fields=['cpf_cnpj_socio']),
        ]

    def __str__(self):
        return f"{self.nome_socio_razao_social} - {self.empresa_id}"


class Simples(models.Model):
    """Simples Nacional tax regime information"""
    
    empresa = models.OneToOneField(
        Empresa,
        on_delete=models.CASCADE,
        primary_key=True,
        db_column='cnpj_basico',
        related_name='simples'
    )
    opcao_simples = models.CharField(
        max_length=1,
        null=True,
        help_text='Simples Nacional option (S/N)'
    )
    data_opcao_simples = models.IntegerField(
        null=True,
        help_text='Simples Nacional option date (YYYYMMDD)'
    )
    data_exclusao_simples = models.IntegerField(
        null=True,
        help_text='Simples Nacional exclusion date (YYYYMMDD)'
    )
    opcao_mei = models.CharField(
        max_length=1,
        null=True,
        help_text='MEI option (S/N)'
    )
    data_opcao_mei = models.IntegerField(
        null=True,
        help_text='MEI option date (YYYYMMDD)'
    )
    data_exclusao_mei = models.IntegerField(
        null=True,
        help_text='MEI exclusion date (YYYYMMDD)'
    )

    class Meta:
        db_table = '"cnpj"."simples"'
        verbose_name = 'Simples Nacional'
        verbose_name_plural = 'Simples Nacional'

    def __str__(self):
        return f"{self.empresa_id} - Simples: {self.opcao_simples}"


# ============================================================================
# MATERIALIZED VIEW — busca (não gerenciada pelo Django)
# ============================================================================

class MvCompanySearch(models.Model):
    """
    Modelo não-gerenciado que mapeia a materialized view cnpj.mv_company_search.

    A view é criada/atualizada pelo DAG cnpj_matview_refresh no Airflow.
    O Django nunca cria, altera ou dropa esta tabela (managed = False).

    Filtra apenas empresas ativas (situacao_cadastral = 2) e reside no
    tablespace fast_ssd para leituras rápidas via índices GIN (pg_trgm).
    """

    cnpj_14 = models.CharField(
        max_length=14,
        primary_key=True,
        help_text='CNPJ completo com 14 dígitos (cnpj_basico + ordem + dv)',
    )
    cnpj_basico = models.CharField(max_length=8)
    cnpj_ordem = models.CharField(max_length=4)
    cnpj_dv = models.CharField(max_length=2)
    razao_social = models.TextField()
    nome_fantasia = models.TextField(null=True, blank=True)
    situacao_cadastral = models.IntegerField(null=True)
    municipio = models.TextField(null=True, blank=True)
    uf = models.CharField(max_length=2, null=True, blank=True)
    cnae_fiscal_principal = models.IntegerField(null=True)
    porte_empresa = models.CharField(max_length=2, null=True, blank=True)
    natureza_juridica = models.IntegerField(null=True)
    capital_social = models.DecimalField(max_digits=15, decimal_places=2, null=True)

    class Meta:
        managed = False
        db_table = '"cnpj"."mv_company_search"'
        verbose_name = 'Empresa (busca)'
        verbose_name_plural = 'Empresas (busca)'

    def __str__(self):
        return f"{self.cnpj_14} - {self.razao_social}"

from rest_framework import serializers
from django.db.models import Count, Q
from .models import (
    MvCompanySearch,
    Empresa,
    Estabelecimento,
    NaturezaJuridica,
    QualificacaoSocio,
)


class MvCompanySearchSerializer(serializers.ModelSerializer):
    class Meta:
        model = MvCompanySearch
        fields = [
            'cnpj_14',
            'cnpj_basico',
            'razao_social',
            'nome_fantasia',
            'situacao_cadastral',
            'codigo_municipio',
            'municipio_nome',
            'uf',
            'cnae_fiscal_principal',
            'cnae_descricao',
            'porte_empresa',
            'natureza_juridica',
            'natureza_juridica_descricao',
            'capital_social',
            'data_inicio_atividade',
            'correio_eletronico',
            'reference_month',
        ]


class EstabelecimentoSerializer(serializers.ModelSerializer):
    """Serializer for Estabelecimento with readable labels"""
    cnpj_completo = serializers.SerializerMethodField()
    ddd_telefone_1 = serializers.SerializerMethodField()
    ddd_telefone_2 = serializers.SerializerMethodField()
    ddd_fax = serializers.SerializerMethodField()
    cnae_fiscal_principal_descricao = serializers.CharField(
        source='cnae_fiscal_principal.descricao',
        read_only=True
    )
    municipio_nome = serializers.CharField(
        source='municipio.descricao',
        read_only=True
    )
    pais_nome = serializers.CharField(
        source='pais.descricao',
        read_only=True
    )
    motivo_situacao_cadastral_descricao = serializers.CharField(
        source='motivo_situacao_cadastral.descricao',
        read_only=True
    )
    situacao_cadastral_display = serializers.CharField(
        source='get_situacao_cadastral_display',
        read_only=True
    )
    matriz_filial_display = serializers.CharField(
        source='get_identificador_matriz_filial_display',
        read_only=True
    )
    
    class Meta:
        model = Estabelecimento
        fields = [
            'cnpj_completo',
            'cnpj_ordem',
            'cnpj_dv',
            'identificador_matriz_filial',
            'matriz_filial_display',
            'nome_fantasia',
            'situacao_cadastral',
            'situacao_cadastral_display',
            'data_situacao_cadastral',
                        'motivo_situacao_cadastral',
                        'motivo_situacao_cadastral_descricao',
            'data_inicio_atividade',
                        'cnae_fiscal_principal',
                        'cnae_fiscal_principal_descricao',
            'cnae_fiscal_secundaria',
                        'pais',
                        'pais_nome',
            'tipo_logradouro',
            'logradouro',
            'numero',
            'complemento',
            'bairro',
            'cep',
            'uf',
            'municipio',
                        'municipio_nome',
            'ddd_telefone_1',
            'ddd_telefone_2',
            'ddd_fax',
            'correio_eletronico',
        ]
    
    def get_cnpj_completo(self, obj):
        """Retorna CNPJ completo formatado: XX.XXX.XXX/XXXX-XX"""
        cnpj_basico = obj.empresa.cnpj_basico
        cnpj_completo = f"{cnpj_basico}{obj.cnpj_ordem}{obj.cnpj_dv}"
        return f"{cnpj_completo[:2]}.{cnpj_completo[2:5]}.{cnpj_completo[5:8]}/{cnpj_completo[8:12]}-{cnpj_completo[12:14]}"

    def get_ddd_telefone_1(self, obj):
        return obj.telefone_1_completo

    def get_ddd_telefone_2(self, obj):
        return obj.telefone_2_completo

    def get_ddd_fax(self, obj):
        if obj.ddd_fax and obj.fax is not None:
            numero = str(obj.fax)
            if len(numero) == 9:
                return f"({obj.ddd_fax}) {numero[:5]}-{numero[5:]}"
            if len(numero) == 8:
                return f"({obj.ddd_fax}) {numero[:4]}-{numero[4:]}"
            return f"({obj.ddd_fax}) {numero}"
        return None


class EmpresaDetailSerializer(serializers.ModelSerializer):
    """Serializer for Empresa with all estabelecimentos"""
    estabelecimentos = serializers.SerializerMethodField()
    natureza_juridica_descricao = serializers.SerializerMethodField()
    qualificacao_responsavel_descricao = serializers.SerializerMethodField()
    porte_empresa_display = serializers.CharField(
        source='get_porte_empresa_display',
        read_only=True
    )
    total_estabelecimentos = serializers.SerializerMethodField()
    estabelecimentos_ativos = serializers.SerializerMethodField()
    
    class Meta:
        model = Empresa
        fields = [
            'cnpj_basico',
            'razao_social',
            'natureza_juridica',
            'natureza_juridica_descricao',
            'qualificacao_responsavel',
            'qualificacao_responsavel_descricao',
            'capital_social',
            'porte_empresa',
            'porte_empresa_display',
            'ente_federativo_responsavel',
            'total_estabelecimentos',
            'estabelecimentos_ativos',
            'estabelecimentos',
        ]

    def _get_lookup_cache(self):
        return self.context.setdefault('_lookup_cache', {})

    def _get_natureza_juridica_map(self):
        cache = self._get_lookup_cache()
        natureza_map = cache.get('natureza_juridica')
        if natureza_map is None:
            natureza_map = dict(NaturezaJuridica.objects.values_list('codigo', 'descricao'))
            cache['natureza_juridica'] = natureza_map
        return natureza_map

    def _get_qualificacao_responsavel_map(self):
        cache = self._get_lookup_cache()
        qualificacao_map = cache.get('qualificacao_responsavel')
        if qualificacao_map is None:
            qualificacao_map = dict(QualificacaoSocio.objects.values_list('codigo', 'descricao'))
            cache['qualificacao_responsavel'] = qualificacao_map
        return qualificacao_map

    def get_natureza_juridica_descricao(self, obj):
        if obj.natureza_juridica is None:
            return None
        return self._get_natureza_juridica_map().get(obj.natureza_juridica)

    def get_qualificacao_responsavel_descricao(self, obj):
        if not obj.qualificacao_responsavel:
            return None
        return self._get_qualificacao_responsavel_map().get(obj.qualificacao_responsavel)
    
    def get_total_estabelecimentos(self, obj):
        return self._get_estabelecimentos_counts(obj)['total']
    
    def get_estabelecimentos_ativos(self, obj):
        return self._get_estabelecimentos_counts(obj)['ativos']

    def _get_estabelecimentos_counts(self, obj):
        cache = self._get_lookup_cache()
        counts_cache = cache.setdefault('estabelecimentos_counts', {})
        if obj.cnpj_basico not in counts_cache:
            counts = obj.estabelecimentos.aggregate(
                total=Count('cnpj_ordem'),
                ativos=Count('cnpj_ordem', filter=Q(situacao_cadastral='02')),
            )
            counts_cache[obj.cnpj_basico] = {
                'total': counts.get('total', 0) or 0,
                'ativos': counts.get('ativos', 0) or 0,
            }
        return counts_cache[obj.cnpj_basico]
    
    def get_estabelecimentos(self, obj):
        """Return estabelecimentos as dict to avoid composite PK issues"""
        # Use .values() to avoid Django trying to fetch 'id' column
        estabelecimentos = obj.estabelecimentos.select_related(
            'cnae_fiscal_principal',
            'municipio',
            'pais',
            'motivo_situacao_cadastral',
        ).values(
            'cnpj_ordem',
            'cnpj_dv',
            'identificador_matriz_filial',
            'nome_fantasia',
            'situacao_cadastral',
            'data_situacao_cadastral',
            'motivo_situacao_cadastral_id',
            'motivo_situacao_cadastral__descricao',
            'data_inicio_atividade',
            'cnae_fiscal_principal_id',
            'cnae_fiscal_principal__descricao',
            'cnae_fiscal_secundaria',
            'pais_id',
            'pais__descricao',
            'tipo_logradouro',
            'logradouro',
            'numero',
            'complemento',
            'bairro',
            'cep',
            'uf',
            'municipio_id',
            'municipio__descricao',
            'ddd_telefone_1',
            'telefone_1',
            'ddd_telefone_2',
            'telefone_2',
            'ddd_fax',
            'fax',
            'correio_eletronico',
        )
        
        result = []
        for est in estabelecimentos:
            cnpj_completo = f"{obj.cnpj_basico}{est['cnpj_ordem']}{est['cnpj_dv']}"
            cnpj_formatado = f"{cnpj_completo[:2]}.{cnpj_completo[2:5]}.{cnpj_completo[5:8]}/{cnpj_completo[8:12]}-{cnpj_completo[12:14]}"
            
            result.append({
                'cnpj_completo': cnpj_formatado,
                'cnpj_ordem': est['cnpj_ordem'],
                'cnpj_dv': est['cnpj_dv'],
                'identificador_matriz_filial': est['identificador_matriz_filial'],
                'tipo_estabelecimento': 'Matriz' if est['identificador_matriz_filial'] == 1 else 'Filial',
                'nome_fantasia': est['nome_fantasia'],
                'situacao_cadastral': est['situacao_cadastral'],
                'situacao_cadastral_display': dict(Estabelecimento.SituacaoCadastral.choices).get(est['situacao_cadastral']),
                'data_situacao_cadastral': est['data_situacao_cadastral'],
                'motivo_situacao_cadastral': est.get('motivo_situacao_cadastral_id'),
                'motivo_situacao_cadastral_descricao': est.get('motivo_situacao_cadastral__descricao'),
                'data_inicio_atividade': est['data_inicio_atividade'],
                'cnae_fiscal_principal': est['cnae_fiscal_principal_id'],
                'cnae_fiscal_principal_descricao': est.get('cnae_fiscal_principal__descricao'),
                'cnae_fiscal_secundaria': est['cnae_fiscal_secundaria'],
                'pais': est.get('pais_id'),
                'pais_nome': est.get('pais__descricao'),
                'endereco': {
                    'tipo_logradouro': est['tipo_logradouro'],
                    'logradouro': est['logradouro'],
                    'numero': est['numero'],
                    'complemento': est['complemento'],
                    'bairro': est['bairro'],
                    'cep': est['cep'],
                    'uf': est['uf'],
                    'municipio': est['municipio_id'],
                    'municipio_nome': est.get('municipio__descricao'),
                },
                'contato': {
                    'ddd_telefone_1': _format_phone(est.get('ddd_telefone_1'), est.get('telefone_1')),
                    'ddd_telefone_2': _format_phone(est.get('ddd_telefone_2'), est.get('telefone_2')),
                    'ddd_fax': _format_phone(est.get('ddd_fax'), est.get('fax')),
                    'email': est['correio_eletronico'],
                }
            })
        return result


def _format_phone(ddd, number):
    if not ddd or number is None:
        return None
    numero = str(number)
    if len(numero) == 9:
        return f"({ddd}) {numero[:5]}-{numero[5:]}"
    if len(numero) == 8:
        return f"({ddd}) {numero[:4]}-{numero[4:]}"
    return f"({ddd}) {numero}"


class CompanyNetworkMetadataSerializer(serializers.Serializer):
    core_cnpj_basico = serializers.CharField()
    depth = serializers.IntegerField()
    total_nodes = serializers.IntegerField()
    total_edges = serializers.IntegerField()
    total_relationships = serializers.IntegerField()
    truncated = serializers.BooleanField()
    max_edges = serializers.IntegerField()


class CompanyNetworkResponseSerializer(serializers.Serializer):
    nodes = serializers.ListField(child=serializers.DictField())
    edges = serializers.ListField(child=serializers.DictField())
    metadata = CompanyNetworkMetadataSerializer()


# ============================================================================
# PESSOA (Person) Serializers
# ============================================================================

class PessoaSearchResultSerializer(serializers.Serializer):
    """Resultado de busca de pessoa — um par (nome + cpf_mascarado) distinto."""
    nome = serializers.CharField()
    cpf_cnpj_socio = serializers.CharField()
    faixa_etaria = serializers.IntegerField(allow_null=True)
    total_empresas = serializers.IntegerField()


class SocioEmpresaSerializer(serializers.Serializer):
    """Uma empresa em que a pessoa é/foi sócia."""
    cnpj_basico = serializers.CharField()
    razao_social = serializers.CharField()
    qualificacao_socio = serializers.CharField(allow_null=True)
    qualificacao_socio_descricao = serializers.CharField(allow_null=True)
    data_entrada_sociedade = serializers.DateField(allow_null=True)
    situacao_cadastral = serializers.CharField(allow_null=True)
    uf = serializers.CharField(allow_null=True)
    municipio_nome = serializers.CharField(allow_null=True)
    reference_month = serializers.CharField()


class PessoaDetailSerializer(serializers.Serializer):
    """Detalhes de uma pessoa física: empresas do quadro societário."""
    cpf_mascarado = serializers.CharField()
    nome = serializers.CharField(allow_null=True)
    faixa_etaria = serializers.IntegerField(allow_null=True)
    total_empresas = serializers.IntegerField()
    empresas = SocioEmpresaSerializer(many=True)


class PersonNetworkMetadataSerializer(serializers.Serializer):
    core_cpf_mascarado = serializers.CharField()
    core_nome = serializers.CharField(allow_null=True)
    depth = serializers.IntegerField()
    total_nodes = serializers.IntegerField()
    total_edges = serializers.IntegerField()
    total_relationships = serializers.IntegerField()
    truncated = serializers.BooleanField()
    max_edges = serializers.IntegerField()


class PersonNetworkResponseSerializer(serializers.Serializer):
    nodes = serializers.ListField(child=serializers.DictField())
    edges = serializers.ListField(child=serializers.DictField())
    metadata = PersonNetworkMetadataSerializer()

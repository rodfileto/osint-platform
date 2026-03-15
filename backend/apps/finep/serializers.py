from rest_framework import serializers
from typing import Any, cast

from cnpj.models import CNAE, MvCompanySearch, MvCompanySearchInactive

from .models import (
    LiberacaoAncine,
    LiberacaoCreditoDescentralizado,
    LiberacaoOperacaoDireta,
    ProjetoAncine,
    ProjetoCreditoDescentralizado,
    ProjetoInvestimento,
    ProjetoOperacaoDireta,
)


class FinepLookupSerializer(serializers.ModelSerializer):
    company_join_fields = ()
    company_lookup_fields = (
        'cnpj_14',
        'cnpj_basico',
        'razao_social',
        'nome_fantasia',
        'situacao_cadastral',
        'municipio_nome',
        'uf',
        'cnae_fiscal_principal',
        'cnae_descricao',
        'porte_empresa',
        'natureza_juridica',
        'natureza_juridica_descricao',
        'capital_social',
        'data_inicio_atividade',
        'reference_month',
    )

    def _fill_missing_cnae_descriptions(self, rows):
        missing_codes = {
            row['cnae_fiscal_principal']
            for row in rows
            if row.get('cnae_fiscal_principal') and not row.get('cnae_descricao')
        }
        if not missing_codes:
            return rows

        cnae_map = dict(CNAE.objects.filter(codigo__in=missing_codes).values_list('codigo', 'descricao'))
        for row in rows:
            if row.get('cnae_fiscal_principal') and not row.get('cnae_descricao'):
                row['cnae_descricao'] = cnae_map.get(row['cnae_fiscal_principal'])

        return rows

    def _build_company_lookup_from_cnpjs(self, cnpjs, cache_key):
        cache = self.context.setdefault('_finep_company_lookup_cache', {})
        if cache_key in cache:
            return cache[cache_key]

        if not cnpjs:
            cache[cache_key] = {}
            return cache[cache_key]

        lookup = {}
        active_rows = list(
            MvCompanySearch.objects.filter(cnpj_14__in=cnpjs).values(*self.company_lookup_fields)
        )
        self._fill_missing_cnae_descriptions(active_rows)
        for row in active_rows:
            lookup[row['cnpj_14']] = {
                **row,
                'match_source': 'active',
            }

        missing_cnpjs = cnpjs.difference(lookup.keys())
        if missing_cnpjs:
            inactive_rows = list(
                MvCompanySearchInactive.objects.filter(cnpj_14__in=missing_cnpjs).values(*self.company_lookup_fields)
            )
            self._fill_missing_cnae_descriptions(inactive_rows)
            for row in inactive_rows:
                lookup[row['cnpj_14']] = {
                    **row,
                    'match_source': 'inactive',
                }

        cache[cache_key] = lookup
        return lookup

    def _normalize_cnpj(self, value):
        if not value:
            return None

        digits_only = ''.join(character for character in str(value) if character.isdigit())
        if len(digits_only) != 14:
            return None

        return digits_only

    def _get_instances_for_lookup(self):
        parent_instance = getattr(self.parent, 'instance', None)
        if parent_instance is not None:
            return parent_instance

        instance = getattr(self, 'instance', None)
        if instance is None:
            return []

        return [instance]

    def _iter_company_cnpjs(self):
        for instance in self._get_instances_for_lookup():
            for field_name in self.company_join_fields:
                normalized = self._normalize_cnpj(getattr(instance, field_name, None))
                if normalized:
                    yield normalized

    def _build_company_lookup(self):
        cache_key = tuple(sorted(self.company_join_fields))
        cnpjs = set(self._iter_company_cnpjs())
        return self._build_company_lookup_from_cnpjs(cnpjs, cache_key)

    def _get_company_payload(self, instance, field_name):
        lookup = self._build_company_lookup()
        cnpj = self._normalize_cnpj(getattr(instance, field_name, None))
        if not cnpj:
            return None

        return lookup.get(cnpj)


class FinepEmpresaCnpjJoinSerializer(FinepLookupSerializer):
    pass


class FinepProjectJoinSerializer(FinepLookupSerializer):
    related_project_model = None
    related_project_key_fields = ()
    related_project_value_fields = ()

    def _normalize_key_value(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    def _get_project_lookup_key(self, instance):
        values = []
        for field_name in self.related_project_key_fields:
            values.append(self._normalize_key_value(getattr(instance, field_name, None)))
        return tuple(values)

    def _get_project_lookup(self):
        if self.related_project_model is None:
            return {}

        cache = self.context.setdefault('_finep_project_lookup_cache', {})
        related_project_model = cast(Any, self.related_project_model)
        cache_key = related_project_model.__name__
        if cache_key in cache:
            return cache[cache_key]

        instances = self._get_instances_for_lookup()
        contract_values = set()
        for instance in instances:
            key = self._get_project_lookup_key(instance)
            if key and key[0]:
                contract_values.add(key[0])

        if not contract_values:
            cache[cache_key] = {}
            return cache[cache_key]

        rows = list(
            related_project_model.objects.filter(contrato__in=contract_values).values(*self.related_project_value_fields)
        )
        company_cnpjs = {
            normalized for normalized in (
                self._normalize_cnpj(row.get('cnpj_proponente_norm')) for row in rows
            ) if normalized
        }
        company_lookup = self._build_company_lookup_from_cnpjs(company_cnpjs, ('project-company', cache_key))

        lookup = {}
        for row in rows:
            project_key = tuple(self._normalize_key_value(row.get(field_name)) for field_name in self.related_project_key_fields)
            lookup[project_key] = {
                **row,
                'proponente_empresa': company_lookup.get(self._normalize_cnpj(row.get('cnpj_proponente_norm'))),
            }

        cache[cache_key] = lookup
        return lookup

    def _get_related_project_payload(self, instance):
        lookup = self._get_project_lookup()
        return lookup.get(self._get_project_lookup_key(instance))


class ProjetoOperacaoDiretaSerializer(FinepEmpresaCnpjJoinSerializer):
    proponente_empresa = serializers.SerializerMethodField()
    executor_empresa = serializers.SerializerMethodField()
    company_join_fields = ('cnpj_proponente_norm', 'cnpj_executor_norm')

    class Meta:
        model = ProjetoOperacaoDireta
        fields = '__all__'

    def get_proponente_empresa(self, obj):
        return self._get_company_payload(obj, 'cnpj_proponente_norm')

    def get_executor_empresa(self, obj):
        return self._get_company_payload(obj, 'cnpj_executor_norm')


class ProjetoCreditoDescentralizadoSerializer(FinepEmpresaCnpjJoinSerializer):
    proponente_empresa = serializers.SerializerMethodField()
    company_join_fields = ('cnpj_proponente_norm',)

    class Meta:
        model = ProjetoCreditoDescentralizado
        fields = '__all__'

    def get_proponente_empresa(self, obj):
        return self._get_company_payload(obj, 'cnpj_proponente_norm')


class ProjetoInvestimentoSerializer(FinepEmpresaCnpjJoinSerializer):
    proponente_empresa = serializers.SerializerMethodField()
    company_join_fields = ('cnpj_proponente_norm',)

    class Meta:
        model = ProjetoInvestimento
        fields = '__all__'

    def get_proponente_empresa(self, obj):
        return self._get_company_payload(obj, 'cnpj_proponente_norm')


class ProjetoAncineSerializer(FinepEmpresaCnpjJoinSerializer):
    proponente_empresa = serializers.SerializerMethodField()
    company_join_fields = ('cnpj_proponente_norm',)

    class Meta:
        model = ProjetoAncine
        fields = '__all__'

    def get_proponente_empresa(self, obj):
        return self._get_company_payload(obj, 'cnpj_proponente_norm')


class LiberacaoOperacaoDiretaSerializer(FinepProjectJoinSerializer):
    projeto = serializers.SerializerMethodField()
    related_project_model = ProjetoOperacaoDireta
    related_project_key_fields = ('contrato', 'ref')
    related_project_value_fields = (
        'id',
        'contrato',
        'ref',
        'titulo',
        'proponente',
        'cnpj_proponente_norm',
        'executor',
        'status',
        'instrumento',
        'uf',
        'data_assinatura',
        'valor_finep',
        'valor_pago',
    )

    class Meta:
        model = LiberacaoOperacaoDireta
        fields = '__all__'

    def get_projeto(self, obj):
        return self._get_related_project_payload(obj)


class LiberacaoCreditoDescentralizadoSerializer(FinepEmpresaCnpjJoinSerializer):
    proponente_empresa = serializers.SerializerMethodField()
    company_join_fields = ('cnpj_proponente_norm',)

    class Meta:
        model = LiberacaoCreditoDescentralizado
        fields = '__all__'

    def get_proponente_empresa(self, obj):
        return self._get_company_payload(obj, 'cnpj_proponente_norm')


class LiberacaoAncineSerializer(FinepProjectJoinSerializer):
    projeto = serializers.SerializerMethodField()
    related_project_model = ProjetoAncine
    related_project_key_fields = ('contrato', 'ref')
    related_project_value_fields = (
        'id',
        'contrato',
        'ref',
        'titulo',
        'proponente',
        'cnpj_proponente_norm',
        'executor',
        'status',
        'instrumento',
        'uf',
        'data_assinatura',
        'valor_finep',
        'valor_pago',
    )

    class Meta:
        model = LiberacaoAncine
        fields = '__all__'

    def get_projeto(self, obj):
        return self._get_related_project_payload(obj)


class FinepResumoEmpresaSerializer(serializers.Serializer):
    cnpj_14 = serializers.CharField()
    razao_social = serializers.CharField(allow_null=True)
    nome_fantasia = serializers.CharField(allow_null=True)
    uf = serializers.CharField(allow_null=True)
    municipio_nome = serializers.CharField(allow_null=True)
    cnae_fiscal_principal = serializers.CharField(allow_null=True)
    cnae_descricao = serializers.CharField(allow_null=True)
    porte_empresa = serializers.CharField(allow_null=True)
    natureza_juridica = serializers.IntegerField(allow_null=True)
    natureza_juridica_descricao = serializers.CharField(allow_null=True)
    capital_social = serializers.DecimalField(max_digits=15, decimal_places=2, allow_null=True)
    match_source = serializers.CharField(allow_null=True)
    total_projetos = serializers.IntegerField()
    total_aprovado_finep = serializers.DecimalField(max_digits=20, decimal_places=2)
    total_liberado = serializers.DecimalField(max_digits=20, decimal_places=2)
    fontes = serializers.ListField(child=serializers.CharField())


class FinepGiniUfSerializer(serializers.Serializer):
    uf = serializers.CharField()
    total_municipios = serializers.IntegerField()
    gini_liberado = serializers.FloatField()


class FinepResumoGeralSerializer(serializers.Serializer):
    total_empresas = serializers.IntegerField()
    total_municipios = serializers.IntegerField()
    total_ufs = serializers.IntegerField()
    total_projetos = serializers.IntegerField()
    total_aprovado_finep = serializers.DecimalField(max_digits=20, decimal_places=2)
    total_liberado = serializers.DecimalField(max_digits=20, decimal_places=2)
    ticket_medio_projeto = serializers.DecimalField(max_digits=20, decimal_places=2)
    gini_liberado = serializers.FloatField()
    gini_por_uf = FinepGiniUfSerializer(many=True)
    fontes = serializers.ListField(child=serializers.CharField())
    projetos_por_fonte = serializers.DictField(child=serializers.IntegerField())


class FinepResumoUfSerializer(serializers.Serializer):
    uf = serializers.CharField()
    total_empresas = serializers.IntegerField()
    total_projetos = serializers.IntegerField()
    total_aprovado_finep = serializers.DecimalField(max_digits=20, decimal_places=2)
    total_liberado = serializers.DecimalField(max_digits=20, decimal_places=2)
    fontes = serializers.ListField(child=serializers.CharField())


class FinepResumoCnaeSerializer(serializers.Serializer):
    cnae_fiscal_principal = serializers.CharField(allow_null=True)
    cnae_descricao = serializers.CharField(allow_null=True)
    total_empresas = serializers.IntegerField()
    total_projetos = serializers.IntegerField()
    total_aprovado_finep = serializers.DecimalField(max_digits=20, decimal_places=2)
    total_liberado = serializers.DecimalField(max_digits=20, decimal_places=2)
    fontes = serializers.ListField(child=serializers.CharField())


class FinepResumoMunicipioSerializer(serializers.Serializer):
    municipio_nome = serializers.CharField()
    uf = serializers.CharField(allow_null=True)
    municipality_ibge_code = serializers.CharField(allow_null=True)
    latitude = serializers.FloatField(allow_null=True)
    longitude = serializers.FloatField(allow_null=True)
    total_empresas = serializers.IntegerField()
    total_projetos = serializers.IntegerField()
    total_aprovado_finep = serializers.DecimalField(max_digits=20, decimal_places=2)
    total_liberado = serializers.DecimalField(max_digits=20, decimal_places=2)
    fontes = serializers.ListField(child=serializers.CharField())

from rest_framework import serializers

from cnpj.models import CNAE, MvCompanySearch, MvCompanySearchInactive

from .models import (
    IpcSubclasseRef,
    MvPatenteSearch,
    PatenteDadosBibliograficos,
    PatenteClassificacaoIPC,
    PatenteConteudo,
    PatenteDespacho,
    PatenteDepositante,
    PatenteInventor,
    PatentePrioridade,
    PatenteProcurador,
    PatenteRenumeracao,
    PatenteVinculo,
)

# ---------------------------------------------------------------------------
# Company lookup helpers (lookup by cnpj_basico — 8 digits)
# ---------------------------------------------------------------------------

_COMPANY_LOOKUP_FIELDS = (
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


def _fill_cnae_descriptions(rows):
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


def _build_company_lookup_by_cnpj_basico(cnpj_basicos: set) -> dict:
    """
    Returns a dict {cnpj_basico: company_payload}.

    Prefers the matriz (cnpj_ordem='0001') when multiple establishments share
    the same cnpj_basico.  Falls back to active → inactive ordering.
    """
    if not cnpj_basicos:
        return {}

    lookup: dict = {}

    active_rows = list(
        MvCompanySearch.objects.filter(cnpj_basico__in=cnpj_basicos).values(*_COMPANY_LOOKUP_FIELDS)
    )
    _fill_cnae_descriptions(active_rows)
    for row in active_rows:
        basico = row['cnpj_basico']
        existing = lookup.get(basico)
        # Prefer matriz (cnpj_14 ends with '000100') over other establishments
        is_matriz = row['cnpj_14'][8:12] == '0001'
        if existing is None or is_matriz:
            lookup[basico] = {**row, 'match_source': 'active'}

    missing = cnpj_basicos.difference(lookup.keys())
    if missing:
        inactive_rows = list(
            MvCompanySearchInactive.objects.filter(cnpj_basico__in=missing).values(*_COMPANY_LOOKUP_FIELDS)
        )
        _fill_cnae_descriptions(inactive_rows)
        for row in inactive_rows:
            basico = row['cnpj_basico']
            existing = lookup.get(basico)
            is_matriz = row['cnpj_14'][8:12] == '0001'
            if existing is None or is_matriz:
                lookup[basico] = {**row, 'match_source': 'inactive'}

    return lookup


# ---------------------------------------------------------------------------
# Base serializer with CNPJ-basico company join
# ---------------------------------------------------------------------------

class InpiCompanyJoinSerializer(serializers.ModelSerializer):
    """
    Base for serializers that need to look up a company by cnpj_basico_resolved.
    Subclasses declare `company_join_field` (the model field holding the 8-digit
    cnpj_basico) and call _get_company_payload(obj) in a SerializerMethodField.
    """

    company_join_field: str = ''

    def _get_instances(self):
        parent_instance = getattr(self.parent, 'instance', None)
        if parent_instance is not None:
            return parent_instance
        instance = getattr(self, 'instance', None)
        return [instance] if instance is not None else []

    def _build_company_lookup(self):
        cache = self.context.setdefault('_inpi_company_lookup_cache', {})
        field = self.company_join_field
        if field in cache:
            return cache[field]

        cnpj_basicos = {
            getattr(inst, field)
            for inst in self._get_instances()
            if getattr(inst, field, None)
        }
        lookup = _build_company_lookup_by_cnpj_basico(cnpj_basicos)
        cache[field] = lookup
        return lookup

    def _get_company_payload(self, obj):
        cnpj_basico = getattr(obj, self.company_join_field, None)
        if not cnpj_basico:
            return None
        return self._build_company_lookup().get(cnpj_basico)


# ---------------------------------------------------------------------------
# Sub-table serializers (no company join needed)
# ---------------------------------------------------------------------------

class PatenteConteudoSerializer(serializers.ModelSerializer):
    class Meta:
        model = PatenteConteudo
        fields = '__all__'


class PatenteInventorSerializer(serializers.ModelSerializer):
    class Meta:
        model = PatenteInventor
        fields = '__all__'


class PatenteClassificacaoIPCSerializer(serializers.ModelSerializer):
    class Meta:
        model = PatenteClassificacaoIPC
        fields = '__all__'


class PatenteDespachoSerializer(serializers.ModelSerializer):
    class Meta:
        model = PatenteDespacho
        fields = '__all__'


class PatentePrioridadeSerializer(serializers.ModelSerializer):
    class Meta:
        model = PatentePrioridade
        fields = '__all__'


class PatenteVinculoSerializer(serializers.ModelSerializer):
    class Meta:
        model = PatenteVinculo
        fields = '__all__'


class PatenteRenumeracaoSerializer(serializers.ModelSerializer):
    class Meta:
        model = PatenteRenumeracao
        fields = '__all__'


# ---------------------------------------------------------------------------
# Serializers with company join
# ---------------------------------------------------------------------------

class PatenteDepositanteSerializer(InpiCompanyJoinSerializer):
    empresa = serializers.SerializerMethodField()
    company_join_field = 'cnpj_basico_resolved'

    class Meta:
        model = PatenteDepositante
        fields = '__all__'

    def get_empresa(self, obj):
        return self._get_company_payload(obj)


class PatenteProcuradorSerializer(InpiCompanyJoinSerializer):
    empresa = serializers.SerializerMethodField()
    company_join_field = 'cnpj_basico_resolved'

    class Meta:
        model = PatenteProcurador
        fields = '__all__'

    def get_empresa(self, obj):
        return self._get_company_payload(obj)


# ---------------------------------------------------------------------------
# Main patent serializers
# ---------------------------------------------------------------------------

class PatenteDadosBibliograficosSerializer(serializers.ModelSerializer):
    class Meta:
        model = PatenteDadosBibliograficos
        fields = '__all__'


class MvPatenteSearchSerializer(InpiCompanyJoinSerializer):
    """
    Serializer for the mv_patent_search materialized view.
    Enriches records that have a depositante_cnpj_basico with company data.
    """

    depositante_empresa = serializers.SerializerMethodField()
    company_join_field = 'depositante_cnpj_basico'

    class Meta:
        model = MvPatenteSearch
        fields = '__all__'

    def get_depositante_empresa(self, obj):
        return self._get_company_payload(obj)


# ---------------------------------------------------------------------------
# Analytics serializers
# ---------------------------------------------------------------------------

class InpiResumoTipoSerializer(serializers.Serializer):
    tipo_patente = serializers.CharField(allow_null=True)
    total = serializers.IntegerField()


class IpcSubclasseRefSerializer(serializers.ModelSerializer):
    class Meta:
        model = IpcSubclasseRef
        fields = '__all__'


class InpiResumoIPCSerializer(serializers.Serializer):
    ipc_secao_classe = serializers.CharField(allow_null=True)
    total = serializers.IntegerField()
    titulo_en = serializers.CharField(allow_null=True, required=False)


class InpiResumoAnoSerializer(serializers.Serializer):
    ano = serializers.IntegerField(allow_null=True)
    total = serializers.IntegerField()


class InpiResumoGeralSerializer(serializers.Serializer):
    total_patentes = serializers.IntegerField()
    por_tipo = InpiResumoTipoSerializer(many=True)
    por_secao_ipc = InpiResumoIPCSerializer(many=True)
    por_ano_deposito = InpiResumoAnoSerializer(many=True)
    snapshot_date = serializers.CharField(allow_null=True)

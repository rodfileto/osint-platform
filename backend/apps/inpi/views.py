from collections import defaultdict

from django.db.models import Count, Q
from rest_framework import mixins, viewsets
from rest_framework.response import Response

from .models import (
    IpcSubclasseRef,
    MvPatenteSearch,
    PatenteClassificacaoIPC,
    PatenteDespacho,
    PatenteDepositante,
    PatenteInventor,
    PatentePrioridade,
    PatenteProcurador,
    PatenteRenumeracao,
    PatenteVinculo,
)
from .serializers import (
    InpiResumoGeralSerializer,
    IpcSubclasseRefSerializer,
    MvPatenteSearchSerializer,
    PatenteClassificacaoIPCSerializer,
    PatenteDespachoSerializer,
    PatenteDepositanteSerializer,
    PatenteInventorSerializer,
    PatentePrioridadeSerializer,
    PatenteProcuradorSerializer,
    PatenteRenumeracaoSerializer,
    PatenteVinculoSerializer,
)


def _digits_only(value: str) -> str:
    return ''.join(ch for ch in value if ch.isdigit())


# ---------------------------------------------------------------------------
# Main patent search viewset (uses mv_patent_search)
# ---------------------------------------------------------------------------

class MvPatenteSearchViewSet(viewsets.ReadOnlyModelViewSet):
    """
    List and retrieve patents from the mv_patent_search materialized view.

    Filters (GET params):
      q             — free-text against titulo, depositante_principal, inventor_principal
      tipo_patente  — PI | MU | PP | MI | DI
      ipc_secao     — 4-char IPC section/class prefix, e.g. F41A
      cnpj_basico   — 8 or 14-digit CNPJ (matched against depositante_cnpj_basico)
      numero_inpi   — exact match on numero_inpi
      data_deposito_min / data_deposito_max — date range (YYYY-MM-DD)
      sigilo        — true | false
    """

    queryset = MvPatenteSearch.objects.all()
    serializer_class = MvPatenteSearchSerializer

    def get_queryset(self):
        qs = super().get_queryset()
        params = self.request.GET

        q = params.get('q', '').strip()
        tipo_patente = params.get('tipo_patente', '').strip().upper()
        ipc_secao = params.get('ipc_secao', '').strip().upper()
        cnpj_raw = params.get('cnpj', '').strip()
        numero_inpi = params.get('numero_inpi', '').strip()
        data_min = params.get('data_deposito_min', '').strip()
        data_max = params.get('data_deposito_max', '').strip()
        sigilo = params.get('sigilo', '').strip().lower()

        if q:
            predicate = (
                Q(titulo__icontains=q)
                | Q(depositante_principal__icontains=q)
                | Q(inventor_principal__icontains=q)
            )
            qs = qs.filter(predicate)

        if tipo_patente:
            qs = qs.filter(tipo_patente=tipo_patente)

        if ipc_secao:
            qs = qs.filter(ipc_secao_classe__istartswith=ipc_secao)

        if cnpj_raw:
            cnpj_digits = _digits_only(cnpj_raw)
            if len(cnpj_digits) >= 8:
                qs = qs.filter(depositante_cnpj_basico=cnpj_digits[:8])

        if numero_inpi:
            qs = qs.filter(numero_inpi=numero_inpi)

        if data_min:
            qs = qs.filter(data_deposito__gte=data_min)

        if data_max:
            qs = qs.filter(data_deposito__lte=data_max)

        if sigilo in ('true', '1', 'yes'):
            qs = qs.filter(sigilo=True)
        elif sigilo in ('false', '0', 'no'):
            qs = qs.filter(sigilo=False)

        return qs


# ---------------------------------------------------------------------------
# Sub-table viewsets (list-only, filterable by codigo_interno)
# ---------------------------------------------------------------------------

class InpiSubtableBaseViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    """
    Base for list-only sub-table viewsets.
    All sub-tables support ?codigo_interno=<int> to filter by patent.
    """

    codigo_interno_field = 'codigo_interno'

    def get_queryset(self):
        qs = super().get_queryset()
        codigo_interno = self.request.GET.get('codigo_interno', '').strip()
        if codigo_interno.isdigit():
            qs = qs.filter(**{self.codigo_interno_field: int(codigo_interno)})
        return qs


class PatenteInventorViewSet(InpiSubtableBaseViewSet):
    """
    List inventors. Filter: ?codigo_interno=, ?pais=, ?q= (autor name).
    """

    queryset = PatenteInventor.objects.all()
    serializer_class = PatenteInventorSerializer

    def get_queryset(self):
        qs = super().get_queryset()
        q = self.request.GET.get('q', '').strip()
        pais = self.request.GET.get('pais', '').strip().upper()
        if q:
            qs = qs.filter(autor__icontains=q)
        if pais:
            qs = qs.filter(pais=pais)
        return qs


class PatenteDepositanteViewSet(InpiSubtableBaseViewSet):
    """
    List depositors. Filter: ?codigo_interno=, ?cnpj_basico=, ?q= (depositante name).
    Enriches with company data when cnpj_basico_resolved is present.
    """

    queryset = PatenteDepositante.objects.all()
    serializer_class = PatenteDepositanteSerializer

    def get_queryset(self):
        qs = super().get_queryset()
        q = self.request.GET.get('q', '').strip()
        cnpj_raw = self.request.GET.get('cnpj_basico', '').strip()
        if q:
            qs = qs.filter(depositante__icontains=q)
        if cnpj_raw:
            cnpj_digits = _digits_only(cnpj_raw)
            if len(cnpj_digits) >= 8:
                qs = qs.filter(cnpj_basico_resolved=cnpj_digits[:8])
        return qs


class PatenteClassificacaoIPCViewSet(InpiSubtableBaseViewSet):
    """
    List IPC classifications.
    Filter: ?codigo_interno=, ?simbolo= (exact or prefix), ?versao=.
    """

    queryset = PatenteClassificacaoIPC.objects.all()
    serializer_class = PatenteClassificacaoIPCSerializer

    def get_queryset(self):
        qs = super().get_queryset()
        simbolo = self.request.GET.get('simbolo', '').strip().upper()
        versao = self.request.GET.get('versao', '').strip()
        if simbolo:
            qs = qs.filter(simbolo__istartswith=simbolo)
        if versao:
            qs = qs.filter(versao=versao)
        return qs


class PatenteDespachoViewSet(InpiSubtableBaseViewSet):
    """
    List administrative decisions.
    Filter: ?codigo_interno=, ?codigo_despacho=, ?data_rpi_min=, ?data_rpi_max=.
    """

    queryset = PatenteDespacho.objects.all()
    serializer_class = PatenteDespachoSerializer

    def get_queryset(self):
        qs = super().get_queryset()
        codigo_despacho = self.request.GET.get('codigo_despacho', '').strip()
        data_min = self.request.GET.get('data_rpi_min', '').strip()
        data_max = self.request.GET.get('data_rpi_max', '').strip()
        if codigo_despacho:
            qs = qs.filter(codigo_despacho=codigo_despacho)
        if data_min:
            qs = qs.filter(data_rpi__gte=data_min)
        if data_max:
            qs = qs.filter(data_rpi__lte=data_max)
        return qs


class PatenteProcuradorViewSet(InpiSubtableBaseViewSet):
    """
    List attorneys.
    Filter: ?codigo_interno=, ?cnpj_basico=, ?q= (procurador name).
    Enriches with company data when cnpj_basico_resolved is present.
    """

    queryset = PatenteProcurador.objects.all()
    serializer_class = PatenteProcuradorSerializer

    def get_queryset(self):
        qs = super().get_queryset()
        q = self.request.GET.get('q', '').strip()
        cnpj_raw = self.request.GET.get('cnpj_basico', '').strip()
        if q:
            qs = qs.filter(procurador__icontains=q)
        if cnpj_raw:
            cnpj_digits = _digits_only(cnpj_raw)
            if len(cnpj_digits) >= 8:
                qs = qs.filter(cnpj_basico_resolved=cnpj_digits[:8])
        return qs


class PatentePrioridadeViewSet(InpiSubtableBaseViewSet):
    """
    List priority claims.
    Filter: ?codigo_interno=, ?pais_prioridade=.
    """

    queryset = PatentePrioridade.objects.all()
    serializer_class = PatentePrioridadeSerializer

    def get_queryset(self):
        qs = super().get_queryset()
        pais = self.request.GET.get('pais_prioridade', '').strip().upper()
        if pais:
            qs = qs.filter(pais_prioridade=pais)
        return qs


class PatenteVinculoViewSet(InpiSubtableBaseViewSet):
    """
    List patent links (derivation / incorporation).
    Filter: ?codigo_interno= (matches both derivado and origem), ?tipo_vinculo=.
    """

    queryset = PatenteVinculo.objects.all()
    serializer_class = PatenteVinculoSerializer
    codigo_interno_field = 'codigo_interno_derivado'

    def get_queryset(self):
        qs = super().get_queryset()
        tipo_vinculo = self.request.GET.get('tipo_vinculo', '').strip().upper()
        if tipo_vinculo:
            qs = qs.filter(tipo_vinculo=tipo_vinculo)
        return qs


class PatenteRenumeracaoViewSet(InpiSubtableBaseViewSet):
    """
    List renumbering records.
    Filter: ?codigo_interno=, ?numero_inpi_original=.
    """

    queryset = PatenteRenumeracao.objects.all()
    serializer_class = PatenteRenumeracaoSerializer

    def get_queryset(self):
        qs = super().get_queryset()
        numero = self.request.GET.get('numero_inpi_original', '').strip()
        if numero:
            qs = qs.filter(numero_inpi_original=numero)
        return qs


# ---------------------------------------------------------------------------
# Analytics viewset
# ---------------------------------------------------------------------------

class InpiResumoGeralViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    """
    Aggregate stats from mv_patent_search.

    Returns totals by tipo_patente, IPC section/class, and year of deposit.
    Supports the same filters as MvPatenteSearchViewSet.
    """

    serializer_class = InpiResumoGeralSerializer

    def _base_queryset(self):
        qs = MvPatenteSearch.objects.all()
        params = self.request.GET

        tipo_patente = params.get('tipo_patente', '').strip().upper()
        ipc_secao = params.get('ipc_secao', '').strip().upper()
        cnpj_raw = params.get('cnpj', '').strip()

        if tipo_patente:
            qs = qs.filter(tipo_patente=tipo_patente)

        if ipc_secao:
            qs = qs.filter(ipc_secao_classe__istartswith=ipc_secao)

        if cnpj_raw:
            cnpj_digits = _digits_only(cnpj_raw)
            if len(cnpj_digits) >= 8:
                qs = qs.filter(depositante_cnpj_basico=cnpj_digits[:8])

        return qs

    def list(self, request, *args, **kwargs):
        qs = self._base_queryset()

        total_patentes = qs.count()

        por_tipo = list(
            qs.values('tipo_patente')
            .annotate(total=Count('codigo_interno'))
            .order_by('-total')
        )

        por_secao_ipc = list(
            qs.exclude(ipc_secao_classe__isnull=True)
            .values('ipc_secao_classe')
            .annotate(total=Count('codigo_interno'))
            .order_by('-total')[:50]
        )

        # Enrich IPC rows with descriptions from the reference table
        ipc_codes = {row['ipc_secao_classe'] for row in por_secao_ipc}
        ipc_lookup = dict(
            IpcSubclasseRef.objects.filter(simbolo__in=ipc_codes).values_list('simbolo', 'titulo_en')
        )
        for row in por_secao_ipc:
            row['titulo_en'] = ipc_lookup.get(row['ipc_secao_classe'])

        por_ano_deposito = list(
            qs.exclude(data_deposito__isnull=True)
            .values('data_deposito__year')
            .annotate(total=Count('codigo_interno'))
            .order_by('data_deposito__year')
        )
        por_ano_serialized = [
            {'ano': item['data_deposito__year'], 'total': item['total']}
            for item in por_ano_deposito
        ]

        snapshot_date = None
        latest = qs.order_by('-snapshot_date').values('snapshot_date').first()
        if latest:
            snapshot_date = str(latest['snapshot_date'])

        payload = {
            'total_patentes': total_patentes,
            'por_tipo': por_tipo,
            'por_secao_ipc': por_secao_ipc,
            'por_ano_deposito': por_ano_serialized,
            'snapshot_date': snapshot_date,
        }

        serializer = self.get_serializer(payload)
        return Response(serializer.data)


class IpcSubclasseRefViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    """
    List all IPC subclass reference entries.
    Useful for building filter dropdowns and autocomplete.
    Filter: ?secao= (single letter A-H), ?q= (search in titulo_en or simbolo).
    """

    queryset = IpcSubclasseRef.objects.all()
    serializer_class = IpcSubclasseRefSerializer

    def get_queryset(self):
        qs = super().get_queryset()
        secao = self.request.GET.get('secao', '').strip().upper()
        q = self.request.GET.get('q', '').strip()
        if secao:
            qs = qs.filter(secao=secao)
        if q:
            qs = qs.filter(titulo_en__icontains=q) | qs.filter(simbolo__istartswith=q.upper())
        return qs

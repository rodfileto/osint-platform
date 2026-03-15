from pathlib import Path
from urllib.parse import urlencode
from django.http import FileResponse
from django.db.models import Q
from django.conf import settings
from rest_framework import viewsets, mixins
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError, NotFound, APIException
from django.urls import reverse
from django.utils import timezone
import unicodedata

from .models import MvCompanySearch, MvCompanySearchInactive, Empresa
from .serializers import (
    MvCompanySearchSerializer,
    EmpresaDetailSerializer,
    CompanyNetworkResponseSerializer,
)
from .graph_service import CompanyNetworkService
from .export_utils import build_export_bytes, open_export, persist_export


def _strip_accents(text: str) -> str:
    """Remove acentos e converte para maiúsculo para bater com os dados da Receita Federal."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', text.upper())
        if unicodedata.category(c) != 'Mn'
    )


class CnpjSearchViewSet(mixins.ListModelMixin, mixins.RetrieveModelMixin, viewsets.GenericViewSet):
    """
    ViewSet de busca de empresas sobre a materialized view cnpj.mv_company_search.

    Endpoints:
        GET /api/cnpj/search/              — lista paginada
        GET /api/cnpj/search/{cnpj_14}/   — detalhe por CNPJ de 14 dígitos

    Filtros:
        ?q=<texto|cnpj>   busca por razao_social / nome_fantasia (texto, aceita acentos)
                          ou por cnpj_14 / cnpj_basico (numérico, aceita formatação)
        ?uf=<UF>          filtra por UF (ex: SP, RJ)
        ?municipio=<nome> filtra por município (case-insensitive)
        ?cnae=<código>    filtra por CNAE fiscal principal (match exato, ex: 4713004)
    
    Escopo:
        ?scope=active    usa mv_company_search (padrão)
        ?scope=inactive  usa mv_company_search_inactive
    """

    serializer_class = MvCompanySearchSerializer
    lookup_field = 'cnpj_14'
    export_prefix = 'cnpj-search'

    def _get_export_limit(self, request) -> int:
        raw_limit = str(request.data.get('limit', request.query_params.get('limit', ''))).strip()
        configured_limit = int(getattr(settings, 'CNPJ_EXPORT_MAX_ROWS', 5000))
        if not raw_limit:
            return configured_limit
        if not raw_limit.isdigit():
            raise ValidationError({'limit': 'Informe um limite inteiro positivo.'})
        limit = int(raw_limit)
        if limit < 1:
            raise ValidationError({'limit': 'Informe um limite maior que zero.'})
        return min(limit, configured_limit)

    def _get_export_format(self, request) -> str:
        export_format = str(request.data.get('format', request.query_params.get('format', 'csv'))).strip().lower()
        if export_format not in {'csv', 'xlsx'}:
            raise ValidationError({'format': 'Use `csv` ou `xlsx`.'})
        return export_format

    @action(detail=False, methods=['post'], url_path='export')
    def export(self, request, *args, **kwargs):
        export_limit = self._get_export_limit(request)
        export_format = self._get_export_format(request)

        queryset = self.filter_queryset(self.get_queryset())[:export_limit]
        serializer = self.get_serializer(queryset, many=True)
        rows = list(serializer.data)
        if not rows:
            raise ValidationError({'detail': 'Nenhum registro encontrado para exportacao.'})

        timestamp = timezone.now().strftime('%Y%m%dT%H%M%SZ')
        extension = 'csv' if export_format == 'csv' else 'xlsx'
        object_key = f'{self.export_prefix}/{timestamp}.{extension}'
        file_name = f'cnpj-search-{timestamp}.{extension}'

        content, content_type = build_export_bytes(
            rows,
            list(self.serializer_class.Meta.fields),
            export_format,
        )
        stored_key = persist_export('exports', object_key, content)

        download_query = urlencode({'file': stored_key})
        download_url = request.build_absolute_uri(
            f"{reverse('cnpj-search-export-download')}?{download_query}"
        )

        return Response(
            {
                'file_name': file_name,
                'storage_key': stored_key,
                'download_url': download_url,
                'content_type': content_type,
                'row_count': len(rows),
                'truncated': len(rows) == export_limit,
                'limit': export_limit,
            }
        )

    @action(detail=False, methods=['get'], url_path='export-download')
    def export_download(self, request, *args, **kwargs):
        file_name = request.query_params.get('file', '').strip()
        if not file_name:
            raise ValidationError({'file': 'Informe o caminho do arquivo exportado.'})
        if '..' in file_name or not file_name.startswith(f'{self.export_prefix}/'):
            raise ValidationError({'file': 'Arquivo de exportacao invalido.'})

        try:
            exported_file = open_export('exports', file_name)
        except FileNotFoundError as exc:
            raise NotFound('Arquivo de exportacao nao encontrado.') from exc

        suffix = Path(file_name).suffix.lower()
        content_type = 'text/csv' if suffix == '.csv' else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return FileResponse(
            exported_file,
            as_attachment=True,
            filename=Path(file_name).name,
            content_type=content_type,
        )

    def _apply_filters(self, qs):
        """Apply common search filters to active/inactive querysets."""
        q = self.request.GET.get('q', '').strip()
        uf = self.request.GET.get('uf', '').strip().upper()
        municipio = self.request.GET.get('municipio', '').strip()
        cnae = self.request.GET.get('cnae', '').strip()

        if q:
            # Remove formatação do CNPJ (pontos, barras, traços e espaços)
            digits_only = q.replace('.', '').replace('/', '').replace('-', '').replace(' ', '')

            if digits_only.isdigit():
                n = len(digits_only)
                if n == 14:
                    # CNPJ completo — match exato
                    qs = qs.filter(cnpj_14=digits_only)
                elif n >= 9:
                    # CNPJ parcial com ordem/dv — prefixo sobre cnpj_14 completo
                    qs = qs.filter(cnpj_14__startswith=digits_only)
                else:
                    # Apenas cnpj_basico ou menos — prefixo sobre os 8 dígitos base
                    qs = qs.filter(cnpj_basico__startswith=digits_only[:8])
            else:
                # Busca por nome — normaliza acentos antes do ILIKE
                q_normalized = _strip_accents(q)
                qs = qs.filter(
                    Q(razao_social__icontains=q_normalized) |
                    Q(nome_fantasia__icontains=q_normalized)
                )

        if uf:
            qs = qs.filter(uf=uf)

        if municipio:
            qs = qs.filter(municipio_nome__icontains=municipio)

        if cnae:
            # CNAE é CharField na matview
            if cnae.isdigit():
                qs = qs.filter(cnae_fiscal_principal=cnae)

        return qs.order_by('razao_social')

    def get_queryset(self):
        scope = self.request.GET.get('scope', 'active').strip().lower()
        if scope == 'inactive':
            qs = MvCompanySearchInactive.objects.all()
        else:
            qs = MvCompanySearch.objects.all()

        return self._apply_filters(qs)


class CnpjSearchInactiveViewSet(CnpjSearchViewSet):
    """
    ViewSet dedicado para busca de empresas inativas.

    Endpoints:
        GET /api/cnpj/search-inactive/
        GET /api/cnpj/search-inactive/{cnpj_14}/
    """

    def get_queryset(self):
        qs = MvCompanySearchInactive.objects.all()
        return self._apply_filters(qs)


class EmpresaViewSet(mixins.RetrieveModelMixin, viewsets.GenericViewSet):
    """
    ViewSet para detalhes completos de uma empresa (cnpj_basico).
    
    Endpoints:
        GET /api/cnpj/empresa/{cnpj_basico}/   — detalhes da empresa + todos estabelecimentos
    
    Retorna:
        - Dados da empresa (razão social, capital, natureza jurídica, etc)
        - Lista de todos os estabelecimentos (matriz + filiais)
        - Contadores: total de estabelecimentos e estabelecimentos ativos
    
    Exemplo:
        GET /api/cnpj/empresa/47960950/
        
        Retorna Magazine Luiza com todos os seus estabelecimentos (matriz + filiais)
    """
    
    serializer_class = EmpresaDetailSerializer
    lookup_field = 'cnpj_basico'
    
    def get_queryset(self):
        # Don't use prefetch_related - Estabelecimento has composite PK without id field
        # The serializer will handle fetching estabelecimentos
        return Empresa.objects.all()


class CompanyNetworkViewSet(mixins.RetrieveModelMixin, viewsets.GenericViewSet):
    """
    ViewSet para visualizar rede societária de uma empresa no Neo4j.

    Endpoints:
        GET /api/cnpj/network/{cnpj_basico}/
    """

    lookup_field = 'cnpj_basico'
    serializer_class = CompanyNetworkResponseSerializer
    network_service = CompanyNetworkService()

    def get_queryset(self):
        return Empresa.objects.all()

    def retrieve(self, request, *args, **kwargs):
        cnpj_basico = kwargs.get(self.lookup_field, '').strip()
        depth_raw = request.GET.get('depth', '1').strip()

        if not (cnpj_basico.isdigit() and len(cnpj_basico) == 8):
            raise ValidationError({
                'cnpj_basico': 'Informe um CNPJ básico válido com 8 dígitos numéricos.'
            })

        if not depth_raw.isdigit():
            raise ValidationError({
                'depth': 'Informe uma profundidade válida com valor inteiro positivo.'
            })

        depth = int(depth_raw)
        max_depth = max(1, settings.NEO4J_MAX_NETWORK_DEPTH)
        if depth < 1 or depth > max_depth:
            raise ValidationError({
                'depth': f'Informe uma profundidade entre 1 e {max_depth}.'
            })

        try:
            payload = self.network_service.get_company_network(cnpj_basico, depth=depth)
        except RuntimeError as exc:
            raise APIException('Falha ao consultar rede no Neo4j.') from exc

        if payload is None:
            raise NotFound('Empresa não encontrada na base Neo4j para o CNPJ informado.')

        serializer = self.get_serializer(payload)
        return Response(serializer.data)

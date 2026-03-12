from django.db.models import Q
from rest_framework import viewsets, mixins
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError, NotFound, APIException
import unicodedata

from .models import MvCompanySearch, MvCompanySearchInactive, Empresa
from .serializers import (
    MvCompanySearchSerializer,
    EmpresaDetailSerializer,
    CompanyNetworkResponseSerializer,
)
from .graph_service import CompanyNetworkService


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

        if not (cnpj_basico.isdigit() and len(cnpj_basico) == 8):
            raise ValidationError({
                'cnpj_basico': 'Informe um CNPJ básico válido com 8 dígitos numéricos.'
            })

        try:
            payload = self.network_service.get_company_network(cnpj_basico)
        except RuntimeError as exc:
            raise APIException('Falha ao consultar rede no Neo4j.') from exc

        if payload is None:
            raise NotFound('Empresa não encontrada na base Neo4j para o CNPJ informado.')

        serializer = self.get_serializer(payload)
        return Response(serializer.data)

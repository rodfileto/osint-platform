from django.db.models import Q
from rest_framework import viewsets, mixins
from rest_framework.decorators import action
from rest_framework.response import Response
import unicodedata

from .models import MvCompanySearch, Empresa
from .serializers import MvCompanySearchSerializer, EmpresaDetailSerializer


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
    
    Nota: Busca apenas empresas ATIVAS (situacao_cadastral=2) via mv_company_search.
          Para buscar empresas inativas, seria necessária MatView adicional.
    """

    serializer_class = MvCompanySearchSerializer
    lookup_field = 'cnpj_14'

    def get_queryset(self):
        qs = MvCompanySearch.objects.all()

        q = self.request.query_params.get('q', '').strip()
        uf = self.request.query_params.get('uf', '').strip().upper()
        municipio = self.request.query_params.get('municipio', '').strip()
        cnae = self.request.query_params.get('cnae', '').strip()

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
            qs = qs.filter(municipio__icontains=municipio)

        if cnae:
            # CNAE é IntegerField — match exato
            if cnae.isdigit():
                qs = qs.filter(cnae_fiscal_principal=int(cnae))

        return qs.order_by('razao_social')


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

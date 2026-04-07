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

from .models import MvCompanySearch, MvCompanySearchInactive, Empresa, Socio, Estabelecimento, QualificacaoSocio
from .serializers import (
    MvCompanySearchSerializer,
    EmpresaDetailSerializer,
    CompanyNetworkResponseSerializer,
    PessoaSearchResultSerializer,
    PessoaDetailSerializer,
    PersonNetworkResponseSerializer,
)
from .graph_service import CompanyNetworkService
from .export_utils import build_export_bytes, open_export, persist_export


def _strip_accents(text: str) -> str:
    """Remove acentos e converte para maiúsculo para bater com os dados da Receita Federal."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', text.upper())
        if unicodedata.category(c) != 'Mn'
    )


def _mask_cpf(cpf_raw: str) -> str | None:
    """Converte CPF completo (11 dígitos) para o formato mascarado da Receita Federal: ***XXXXXX**"""
    digits = cpf_raw.replace('.', '').replace('-', '').replace(' ', '')
    if len(digits) != 11 or not digits.isdigit():
        return None
    return f"***{digits[3:9]}**"


def _normalize_masked_cpf(cpf_masked: str) -> str | None:
    """Valida CPF mascarado no formato Receita Federal: ***XXXXXX**."""
    normalized = cpf_masked.strip().replace(' ', '')
    if len(normalized) != 11:
        return None
    if normalized[:3] != '***' or normalized[-2:] != '**':
        return None
    middle = normalized[3:9]
    if not middle.isdigit():
        return None
    return normalized


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


# ============================================================================
# PESSOA (Person Search & Detail)
# ============================================================================

class PessoaSearchViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    """
    Busca de pessoas físicas (sócios PF) no quadro societário.

    Endpoints:
        GET /api/cnpj/pessoa/search/

    Filtros:
        ?q=<nome>  busca por nome (normaliza acentos, mínimo 3 chars)

    A busca retorna pares distintos (nome + cpf_mascarado), que são usados
    para drill-down nos detalhes e na rede da pessoa.
    """

    serializer_class = PessoaSearchResultSerializer

    def get_queryset(self):
        from django.db.models import Count

        q = self.request.GET.get('q', '').strip()

        if len(q) < 3:
            raise ValidationError({'q': 'Informe ao menos 3 caracteres para busca por nome.'})

        # Somente sócios PF/Estrangeiro (identificador 2 e 3); PJ são empresas, não pessoas
        qs = Socio.objects.filter(identificador_socio__in=[2, 3])

        q_normalized = _strip_accents(q)
        qs = qs.filter(nome_socio_razao_social__icontains=q_normalized)

        return (
            qs
            .values('nome_socio_razao_social', 'cpf_cnpj_socio', 'faixa_etaria')
            .annotate(total_empresas=Count('empresa', distinct=True))
            .order_by('nome_socio_razao_social')
        )

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)
        rows = page if page is not None else queryset
        data = [
            {
                'nome': row['nome_socio_razao_social'],
                'cpf_cnpj_socio': row['cpf_cnpj_socio'],
                'faixa_etaria': row['faixa_etaria'],
                'total_empresas': row['total_empresas'],
            }
            for row in rows
        ]
        if page is not None:
            return self.get_paginated_response(data)
        return Response(data)


class PessoaDetailViewSet(viewsets.GenericViewSet):
    """
    Detalhes de pessoa física por nome + CPF mascarado.

    Endpoints:
        GET /api/cnpj/pessoa/detail/?nome=<nome>&cpf_mascarado=***123456**
        GET /api/cnpj/pessoa/{cpf}/  (compatibilidade com lookup legado)

    Parâmetros:
        nome            obrigatório no endpoint `detail`
        cpf_mascarado   obrigatório no endpoint `detail`

    Retorna as empresas em que a pessoa é/foi sócia com dados do quadro societário.
    """

    serializer_class = PessoaDetailSerializer
    lookup_field = 'cpf'
    lookup_value_regex = r'[0-9]{11}'

    def _build_response(self, nome: str, cpf_masked: str):
        nome_normalized = _strip_accents(nome)

        socios = list(
            Socio.objects.filter(
                cpf_cnpj_socio=cpf_masked,
                identificador_socio__in=[2, 3],
                nome_socio_razao_social=nome_normalized,
            )
            .select_related('empresa', 'qualificacao_socio')
            .order_by('empresa__razao_social', '-reference_month')
        )

        if not socios:
            raise NotFound('Pessoa não encontrada para o par nome + CPF mascarado informado.')

        cnpj_basicos = list({s.empresa.cnpj_basico for s in socios})
        matrizes = {
            e['empresa_id']: e
            for e in Estabelecimento.objects.filter(
                empresa_id__in=cnpj_basicos,
                identificador_matriz_filial=1,
            ).values(
                'empresa_id',
                'situacao_cadastral',
                'uf',
                'municipio__descricao',
            )
        }

        qualificacao_map = dict(QualificacaoSocio.objects.values_list('codigo', 'descricao'))

        seen_companies = {}
        for socio in socios:
            cnpj = socio.empresa.cnpj_basico
            if cnpj in seen_companies:
                continue
            matriz = matrizes.get(cnpj, {})
            qualificacao_codigo = (
                socio.qualificacao_socio.codigo
                if socio.qualificacao_socio is not None
                else None
            )
            seen_companies[cnpj] = {
                'cnpj_basico': cnpj,
                'razao_social': socio.empresa.razao_social,
                'qualificacao_socio': qualificacao_codigo,
                'qualificacao_socio_descricao': qualificacao_map.get(qualificacao_codigo),
                'data_entrada_sociedade': socio.data_entrada_sociedade,
                'situacao_cadastral': matriz.get('situacao_cadastral'),
                'uf': matriz.get('uf'),
                'municipio_nome': matriz.get('municipio__descricao'),
                'reference_month': socio.reference_month,
            }

        first = socios[0]
        return Response({
            'cpf_mascarado': cpf_masked,
            'nome': first.nome_socio_razao_social,
            'faixa_etaria': first.faixa_etaria,
            'total_empresas': len(seen_companies),
            'empresas': list(seen_companies.values()),
        })

    @action(detail=False, methods=['get'], url_path='detail')
    def detail_lookup(self, request, *args, **kwargs):
        nome = request.GET.get('nome', '').strip()
        cpf_masked = _normalize_masked_cpf(request.GET.get('cpf_mascarado', ''))

        if len(nome) < 3:
            raise ValidationError({'nome': 'Informe o nome da pessoa com ao menos 3 caracteres.'})
        if cpf_masked is None:
            raise ValidationError({'cpf_mascarado': 'Informe um CPF mascarado válido no formato ***123456**.'})

        return self._build_response(nome, cpf_masked)

    def retrieve(self, request, *args, **kwargs):
        cpf_raw = kwargs['cpf']
        cpf_masked = _mask_cpf(cpf_raw)
        if cpf_masked is None:
            raise ValidationError({'cpf': 'Informe um CPF válido com 11 dígitos numéricos.'})

        nome_param = request.GET.get('nome', '').strip()
        if len(nome_param) < 3:
            raise ValidationError({'nome': 'Informe o nome da pessoa para desambiguar o CPF mascarado.'})

        return self._build_response(nome_param, cpf_masked)


class PessoaNetworkViewSet(viewsets.GenericViewSet):
    """
    Rede de co-propriedade de uma pessoa física via Neo4j.

    Endpoints:
        GET /api/cnpj/pessoa-network/detail/?nome=<nome>&cpf_mascarado=***123456**
        GET /api/cnpj/pessoa-network/{cpf}/  (compatibilidade com lookup legado)

    Parâmetros:
        nome            obrigatório no endpoint `detail`
        cpf_mascarado   obrigatório no endpoint `detail`
        depth           profundidade da rede (1 = empresas diretas; 2 = co-sócios)
    """

    serializer_class = PersonNetworkResponseSerializer
    lookup_field = 'cpf'
    lookup_value_regex = r'[0-9]{11}'
    network_service = CompanyNetworkService()

    def _retrieve_network(self, nome: str, cpf_masked: str, depth_raw: str):
        nome_normalized = _strip_accents(nome)

        if not depth_raw.isdigit():
            raise ValidationError({'depth': 'Informe uma profundidade válida com valor inteiro positivo.'})

        depth = int(depth_raw)
        max_depth = max(1, settings.NEO4J_MAX_NETWORK_DEPTH)
        if depth < 1 or depth > max_depth:
            raise ValidationError({'depth': f'Informe uma profundidade entre 1 e {max_depth}.'})

        try:
            payload = self.network_service.get_person_network(cpf_masked, nome=nome_normalized, depth=depth)
        except RuntimeError as exc:
            raise APIException('Falha ao consultar rede no Neo4j.') from exc

        if payload is None:
            raise NotFound('Pessoa não encontrada na base Neo4j para o par nome + CPF mascarado informado.')

        serializer = self.get_serializer(payload)
        return Response(serializer.data)

    @action(detail=False, methods=['get'], url_path='detail')
    def detail_lookup(self, request, *args, **kwargs):
        nome = request.GET.get('nome', '').strip()
        cpf_masked = _normalize_masked_cpf(request.GET.get('cpf_mascarado', ''))
        depth_raw = request.GET.get('depth', '1').strip()

        if len(nome) < 3:
            raise ValidationError({'nome': 'Informe o nome da pessoa com ao menos 3 caracteres.'})
        if cpf_masked is None:
            raise ValidationError({'cpf_mascarado': 'Informe um CPF mascarado válido no formato ***123456**.'})

        return self._retrieve_network(nome, cpf_masked, depth_raw)

    def retrieve(self, request, *args, **kwargs):
        cpf_raw = kwargs['cpf']
        cpf_masked = _mask_cpf(cpf_raw)
        if cpf_masked is None:
            raise ValidationError({'cpf': 'Informe um CPF válido com 11 dígitos numéricos.'})

        nome_param = request.GET.get('nome', '').strip()
        depth_raw = request.GET.get('depth', '1').strip()
        if len(nome_param) < 3:
            raise ValidationError({'nome': 'Informe o nome da pessoa para desambiguar o CPF mascarado.'})

        return self._retrieve_network(nome_param, cpf_masked, depth_raw)


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

from collections import defaultdict
from decimal import Decimal
from typing import cast
import json
import unicodedata

from django.db import connection
from django.db.models import Q
from rest_framework import mixins, viewsets
from rest_framework.response import Response

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
from .serializers import (
    FinepResumoGeralSerializer,
    FinepResumoCnaeSerializer,
    FinepResumoEmpresaSerializer,
    FinepResumoMunicipioSerializer,
    FinepResumoUfSerializer,
    LiberacaoAncineSerializer,
    LiberacaoCreditoDescentralizadoSerializer,
    LiberacaoOperacaoDiretaSerializer,
    ProjetoAncineSerializer,
    ProjetoCreditoDescentralizadoSerializer,
    ProjetoInvestimentoSerializer,
    ProjetoOperacaoDiretaSerializer,
)


def _digits_only(value: str) -> str:
    return ''.join(character for character in value if character.isdigit())


def _decimal_or_zero(value):
    if value is None:
        return Decimal('0.00')
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _normalize_municipio(value):
    if value is None:
        return None

    normalized = ' '.join(str(value).strip().split())
    return normalized or None


def _municipio_match_key(value):
    normalized = _normalize_municipio(value)
    if normalized is None:
        return None

    ascii_value = unicodedata.normalize('NFKD', normalized)
    ascii_value = ''.join(character for character in ascii_value if not unicodedata.combining(character))
    return ascii_value.upper()


def _calculate_gini(values):
    non_negative_values = [float(value) for value in values if value is not None and float(value) >= 0]
    if not non_negative_values:
        return 0.0

    sorted_values = sorted(non_negative_values)
    total = sum(sorted_values)
    if total == 0:
        return 0.0

    weighted_sum = 0.0
    item_count = len(sorted_values)
    for index, value in enumerate(sorted_values, start=1):
        weighted_sum += ((2 * index) - item_count - 1) * value

    return round(weighted_sum / (item_count * total), 6)


def _safe_non_negative_float(value, default=0.0):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return max(parsed, 0.0)


def _fill_missing_cnae_descriptions(rows):
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


def _load_geo_municipality_lookup(records):
    company_codes = {
        company['codigo_municipio']
        for record in records
        for company in [record.get('company') or {}]
        if company.get('codigo_municipio')
    }
    ufs = {
        record.get('uf')
        for record in records
        if record.get('uf')
    }

    by_cnpj_code = {}
    by_name = {}

    with connection.cursor() as cursor:
        if company_codes:
            cursor.execute(
                """
                SELECT
                    cnpj_municipality_code,
                    municipality_ibge_code,
                    municipality_name,
                    state_abbrev,
                    ST_Y(centroid::geometry) AS latitude,
                    ST_X(centroid::geometry) AS longitude
                FROM geo.v_cnpj_br_municipality
                WHERE cnpj_municipality_code = ANY(%s)
                  AND municipality_ibge_code IS NOT NULL
                  AND centroid IS NOT NULL
                """,
                [list(company_codes)],
            )
            for code, ibge_code, municipality_name, state_abbrev, latitude, longitude in cursor.fetchall():
                by_cnpj_code[code] = {
                    'municipality_ibge_code': ibge_code,
                    'municipio_nome': municipality_name,
                    'uf': state_abbrev,
                    'latitude': float(latitude) if latitude is not None else None,
                    'longitude': float(longitude) if longitude is not None else None,
                }

        if ufs:
            cursor.execute(
                """
                SELECT
                    municipality_ibge_code,
                    municipality_name,
                    state_abbrev,
                    ST_Y(centroid::geometry) AS latitude,
                    ST_X(centroid::geometry) AS longitude
                FROM geo.br_municipality
                WHERE state_abbrev = ANY(%s)
                  AND centroid IS NOT NULL
                """,
                [list(ufs)],
            )
            for ibge_code, municipality_name, state_abbrev, latitude, longitude in cursor.fetchall():
                key = (state_abbrev, _municipio_match_key(municipality_name))
                by_name[key] = {
                    'municipality_ibge_code': ibge_code,
                    'municipio_nome': municipality_name,
                    'uf': state_abbrev,
                    'latitude': float(latitude) if latitude is not None else None,
                    'longitude': float(longitude) if longitude is not None else None,
                }

    return by_cnpj_code, by_name


def _resolve_geo_municipality(record, by_cnpj_code, by_name):
    company = record.get('company') or {}
    company_code = company.get('codigo_municipio')
    if company_code and company_code in by_cnpj_code:
        return by_cnpj_code[company_code]

    uf = record.get('uf')
    municipio_key = _municipio_match_key(record.get('municipio_nome'))
    if uf and municipio_key:
        return by_name.get((uf, municipio_key))

    return None


def _company_lookup(company_cnpjs):
    if not company_cnpjs:
        return {}

    lookup = {}
    active_rows = list(
        MvCompanySearch.objects.filter(cnpj_14__in=company_cnpjs).values(
            'cnpj_14',
            'razao_social',
            'nome_fantasia',
            'uf',
            'codigo_municipio',
            'municipio_nome',
            'cnae_fiscal_principal',
            'cnae_descricao',
            'porte_empresa',
            'natureza_juridica',
            'natureza_juridica_descricao',
            'capital_social',
        )
    )
    _fill_missing_cnae_descriptions(active_rows)
    for row in active_rows:
        lookup[row['cnpj_14']] = {**row, 'match_source': 'active'}

    missing = set(company_cnpjs).difference(lookup.keys())
    if missing:
        inactive_rows = list(
            MvCompanySearchInactive.objects.filter(cnpj_14__in=missing).values(
                'cnpj_14',
                'razao_social',
                'nome_fantasia',
                'uf',
                'codigo_municipio',
                'municipio_nome',
                'cnae_fiscal_principal',
                'cnae_descricao',
                'porte_empresa',
                'natureza_juridica',
                'natureza_juridica_descricao',
                'capital_social',
            )
        )
        _fill_missing_cnae_descriptions(inactive_rows)
        for row in inactive_rows:
            lookup[row['cnpj_14']] = {**row, 'match_source': 'inactive'}

    return lookup


def _collect_project_records():
    records = []

    for row in ProjetoOperacaoDireta.objects.values(
        'id', 'cnpj_proponente_norm', 'municipio', 'uf', 'valor_finep', 'valor_pago', 'instrumento'
    ):
        records.append({
            'source': 'operacao_direta',
            'source_id': row['id'],
            'cnpj_14': row['cnpj_proponente_norm'],
            'municipio_nome': _normalize_municipio(row['municipio']),
            'uf': row['uf'],
            'total_aprovado_finep': _decimal_or_zero(row['valor_finep']),
            'total_liberado': _decimal_or_zero(row['valor_pago']),
            'instrumento': row['instrumento'],
        })

    for row in ProjetoCreditoDescentralizado.objects.values(
        'id', 'cnpj_proponente_norm', 'uf', 'valor_financiado', 'valor_liberado'
    ):
        records.append({
            'source': 'credito_descentralizado',
            'source_id': row['id'],
            'cnpj_14': row['cnpj_proponente_norm'],
            'municipio_nome': None,
            'uf': row['uf'],
            'total_aprovado_finep': _decimal_or_zero(row['valor_financiado']),
            'total_liberado': _decimal_or_zero(row['valor_liberado']),
            'instrumento': 'Credito Descentralizado',
        })

    for row in ProjetoInvestimento.objects.values(
        'id', 'cnpj_proponente_norm', 'valor_total_contratado', 'valor_total_liberado'
    ):
        records.append({
            'source': 'investimento',
            'source_id': row['id'],
            'cnpj_14': row['cnpj_proponente_norm'],
            'municipio_nome': None,
            'uf': None,
            'total_aprovado_finep': _decimal_or_zero(row['valor_total_contratado']),
            'total_liberado': _decimal_or_zero(row['valor_total_liberado']),
            'instrumento': 'Investimento',
        })

    for row in ProjetoAncine.objects.values(
        'id', 'cnpj_proponente_norm', 'municipio', 'uf', 'valor_finep', 'valor_pago', 'instrumento'
    ):
        records.append({
            'source': 'ancine',
            'source_id': row['id'],
            'cnpj_14': row['cnpj_proponente_norm'],
            'municipio_nome': _normalize_municipio(row['municipio']),
            'uf': row['uf'],
            'total_aprovado_finep': _decimal_or_zero(row['valor_finep']),
            'total_liberado': _decimal_or_zero(row['valor_pago']),
            'instrumento': row['instrumento'] or 'Ancine',
        })

    company_cnpjs = {
        record['cnpj_14'] for record in records
        if record['cnpj_14'] and len(record['cnpj_14']) == 14
    }
    companies = _company_lookup(company_cnpjs)

    for record in records:
        company = companies.get(record['cnpj_14'])
        if company:
            record['company'] = company
            if not record['uf']:
                record['uf'] = company.get('uf')
            if not record.get('municipio_nome'):
                record['municipio_nome'] = _normalize_municipio(company.get('municipio_nome'))
        else:
            record['company'] = None

    return records


def _build_municipio_summary(records):
    groups = {}
    geo_by_cnpj_code, geo_by_name = _load_geo_municipality_lookup(records)

    for record in records:
        municipio_nome = _normalize_municipio(record.get('municipio_nome'))
        if not municipio_nome:
            continue

        uf = record.get('uf') or None
        group_key = (uf, municipio_nome.upper())
        geo_municipality = _resolve_geo_municipality(record, geo_by_cnpj_code, geo_by_name)
        resolved_name = geo_municipality['municipio_nome'] if geo_municipality else municipio_nome
        if group_key not in groups:
            groups[group_key] = {
                'municipio_nome': resolved_name,
                'uf': (geo_municipality['uf'] if geo_municipality else uf),
                'municipality_ibge_code': geo_municipality['municipality_ibge_code'] if geo_municipality else None,
                'latitude': geo_municipality['latitude'] if geo_municipality else None,
                'longitude': geo_municipality['longitude'] if geo_municipality else None,
                'total_empresas': set(),
                'total_projetos': 0,
                'total_aprovado_finep': Decimal('0.00'),
                'total_liberado': Decimal('0.00'),
                'fontes': set(),
            }

        group = groups[group_key]
        if geo_municipality and not group.get('municipality_ibge_code'):
            group['municipio_nome'] = geo_municipality['municipio_nome']
            group['uf'] = geo_municipality['uf']
            group['municipality_ibge_code'] = geo_municipality['municipality_ibge_code']
            group['latitude'] = geo_municipality['latitude']
            group['longitude'] = geo_municipality['longitude']
        if record.get('cnpj_14'):
            cast(set[str], group['total_empresas']).add(record['cnpj_14'])
        group['total_projetos'] = cast(int, group['total_projetos']) + 1
        group['total_aprovado_finep'] = cast(Decimal, group['total_aprovado_finep']) + record['total_aprovado_finep']
        group['total_liberado'] = cast(Decimal, group['total_liberado']) + record['total_liberado']
        cast(set[str], group['fontes']).add(record['source'])

    payload = []
    for row in groups.values():
        payload.append({
            'municipio_nome': row['municipio_nome'],
            'uf': row['uf'],
            'municipality_ibge_code': row['municipality_ibge_code'],
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'total_empresas': len(cast(set[str], row['total_empresas'])),
            'total_projetos': cast(int, row['total_projetos']),
            'total_aprovado_finep': cast(Decimal, row['total_aprovado_finep']),
            'total_liberado': cast(Decimal, row['total_liberado']),
            'fontes': sorted(cast(set[str], row['fontes'])),
        })

    payload.sort(key=lambda item: (item['total_aprovado_finep'], item['total_projetos']), reverse=True)
    return payload


def _load_geo_municipality_features(municipio_summary, simplify_tolerance=0.0, uf=None):
    numeric_tolerance = max(float(simplify_tolerance or 0.0), 0.0)
    summary_by_code = {
        item['municipality_ibge_code']: item
        for item in municipio_summary
        if item.get('municipality_ibge_code')
    }

    query = """
        SELECT
            municipality_ibge_code,
            municipality_name,
            state_abbrev,
            ST_Y(centroid::geometry) AS latitude,
            ST_X(centroid::geometry) AS longitude,
            ST_AsGeoJSON(
                ST_Transform(
                    CASE
                        WHEN %s > 0 THEN ST_SimplifyPreserveTopology(geom, %s)
                        ELSE geom
                    END,
                    4326
                ),
                5
            )::jsonb AS geometry
        FROM geo.br_municipality
    """
    params = [numeric_tolerance, numeric_tolerance]

    if uf:
        query += " WHERE state_abbrev = %s"
        params.append(uf)

    query += " ORDER BY municipality_name"

    features = []
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        for municipality_ibge_code, municipality_name, state_abbrev, latitude, longitude, geometry in cursor.fetchall():
            if isinstance(geometry, str):
                geometry = json.loads(geometry)
            item = summary_by_code.get(municipality_ibge_code)
            approved_value = float(item['total_aprovado_finep']) if item else 0.0
            released_value = float(item['total_liberado']) if item else 0.0

            features.append({
                'type': 'Feature',
                'id': municipality_ibge_code,
                'geometry': geometry,
                'properties': {
                    'municipality_ibge_code': municipality_ibge_code,
                    'municipio_nome': municipality_name,
                    'uf': state_abbrev,
                    'latitude': item.get('latitude') if item else (float(latitude) if latitude is not None else None),
                    'longitude': item.get('longitude') if item else (float(longitude) if longitude is not None else None),
                    'total_empresas': item['total_empresas'] if item else 0,
                    'total_projetos': item['total_projetos'] if item else 0,
                    'total_aprovado_finep': approved_value,
                    'total_liberado': released_value,
                    'fontes': item['fontes'] if item else [],
                },
            })

    return features


class FinepAnalyticsBaseViewSet(mixins.ListModelMixin, viewsets.GenericViewSet):
    serializer_class = None

    def _base_records(self):
        records = _collect_project_records()
        params = self.request.GET
        cnpj = _digits_only(params.get('cnpj', '').strip())
        uf = params.get('uf', '').strip().upper()
        q = params.get('q', '').strip().lower()

        filtered = []
        for record in records:
            company = record.get('company') or {}
            if cnpj:
                record_cnpj = record.get('cnpj_14') or ''
                if len(cnpj) == 14:
                    if record_cnpj != cnpj:
                        continue
                elif not record_cnpj.startswith(cnpj):
                    continue

            if uf and (record.get('uf') or '') != uf:
                continue

            if q:
                haystacks = [
                    company.get('razao_social') or '',
                    company.get('nome_fantasia') or '',
                    company.get('cnae_descricao') or '',
                    record.get('municipio_nome') or '',
                    record.get('instrumento') or '',
                ]
                if not any(q in haystack.lower() for haystack in haystacks):
                    continue

            filtered.append(record)

        return filtered


class FinepResumoGeralViewSet(FinepAnalyticsBaseViewSet):
    serializer_class = FinepResumoGeralSerializer

    def list(self, request, *args, **kwargs):
        records = self._base_records()
        municipio_summary = _build_municipio_summary(records)
        total_projetos = len(records)
        total_aprovado = Decimal('0.00')
        total_liberado = Decimal('0.00')
        empresas = set()
        ufs = set()
        fontes = set()
        projetos_por_fonte = defaultdict(int)

        for record in records:
            cnpj_14 = record.get('cnpj_14')
            if cnpj_14:
                empresas.add(cnpj_14)

            uf = record.get('uf')
            if uf:
                ufs.add(uf)

            source = record['source']
            fontes.add(source)
            projetos_por_fonte[source] += 1
            total_aprovado += record['total_aprovado_finep']
            total_liberado += record['total_liberado']

        payload = {
            'total_empresas': len(empresas),
            'total_municipios': len(municipio_summary),
            'total_ufs': len(ufs),
            'total_projetos': total_projetos,
            'total_aprovado_finep': total_aprovado,
            'total_liberado': total_liberado,
            'ticket_medio_projeto': (
                total_aprovado / total_projetos if total_projetos else Decimal('0.00')
            ),
            'gini_liberado': _calculate_gini([
                item['total_liberado'] for item in municipio_summary
            ]),
            'gini_por_uf': [],
            'fontes': sorted(fontes),
            'projetos_por_fonte': dict(sorted(projetos_por_fonte.items())),
        }

        municipios_por_uf = defaultdict(list)
        for item in municipio_summary:
            if item['uf']:
                municipios_por_uf[item['uf']].append(item)

        payload['gini_por_uf'] = sorted(
            [
                {
                    'uf': uf,
                    'total_municipios': len(items),
                    'gini_liberado': _calculate_gini([
                        municipality['total_liberado'] for municipality in items
                    ]),
                }
                for uf, items in municipios_por_uf.items()
            ],
            key=lambda item: item['gini_liberado'],
            reverse=True,
        )

        serializer = self.get_serializer(payload)
        return Response(serializer.data)


class FinepResumoEmpresaViewSet(FinepAnalyticsBaseViewSet):
    serializer_class = FinepResumoEmpresaSerializer

    def list(self, request, *args, **kwargs):
        groups = {}
        for record in self._base_records():
            cnpj_14 = record.get('cnpj_14')
            if not cnpj_14:
                continue

            company = record.get('company') or {}
            if cnpj_14 not in groups:
                groups[cnpj_14] = {
                    'cnpj_14': cnpj_14,
                    'razao_social': company.get('razao_social'),
                    'nome_fantasia': company.get('nome_fantasia'),
                    'uf': record.get('uf') or company.get('uf'),
                    'municipio_nome': company.get('municipio_nome'),
                    'cnae_fiscal_principal': company.get('cnae_fiscal_principal'),
                    'cnae_descricao': company.get('cnae_descricao'),
                    'porte_empresa': company.get('porte_empresa'),
                    'natureza_juridica': company.get('natureza_juridica'),
                    'natureza_juridica_descricao': company.get('natureza_juridica_descricao'),
                    'capital_social': company.get('capital_social'),
                    'match_source': company.get('match_source'),
                    'total_projetos': 0,
                    'total_aprovado_finep': Decimal('0.00'),
                    'total_liberado': Decimal('0.00'),
                    'fontes': set(),
                }

            group = groups[cnpj_14]
            group['total_projetos'] += 1
            group['total_aprovado_finep'] += record['total_aprovado_finep']
            group['total_liberado'] += record['total_liberado']
            group['fontes'].add(record['source'])

        payload = []
        for row in groups.values():
            row['fontes'] = sorted(row['fontes'])
            payload.append(row)

        payload.sort(key=lambda item: (item['total_aprovado_finep'], item['total_projetos']), reverse=True)
        page = self.paginate_queryset(payload)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(payload, many=True)
        return Response(serializer.data)


class FinepResumoUfViewSet(FinepAnalyticsBaseViewSet):
    serializer_class = FinepResumoUfSerializer

    def list(self, request, *args, **kwargs):
        groups = defaultdict(
            lambda: {
                'total_empresas': set(),
                'total_projetos': 0,
                'total_aprovado_finep': Decimal('0.00'),
                'total_liberado': Decimal('0.00'),
                'fontes': set(),
            }
        )

        for record in self._base_records():
            uf = record.get('uf') or 'NA'
            group = groups[uf]
            company_ids = cast(set[str], group['total_empresas'])
            source_names = cast(set[str], group['fontes'])
            total_projetos = cast(int, group['total_projetos'])
            total_aprovado = cast(Decimal, group['total_aprovado_finep'])
            total_liberado = cast(Decimal, group['total_liberado'])
            if record.get('cnpj_14'):
                company_ids.add(record['cnpj_14'])
            group['total_projetos'] = total_projetos + 1
            group['total_aprovado_finep'] = total_aprovado + record['total_aprovado_finep']
            group['total_liberado'] = total_liberado + record['total_liberado']
            source_names.add(record['source'])

        payload = []
        for uf, row in groups.items():
            company_ids = cast(set[str], row['total_empresas'])
            source_names = cast(set[str], row['fontes'])
            payload.append({
                'uf': uf,
                'total_empresas': len(company_ids),
                'total_projetos': cast(int, row['total_projetos']),
                'total_aprovado_finep': cast(Decimal, row['total_aprovado_finep']),
                'total_liberado': cast(Decimal, row['total_liberado']),
                'fontes': sorted(source_names),
            })

        payload.sort(key=lambda item: (item['total_aprovado_finep'], item['total_projetos']), reverse=True)
        page = self.paginate_queryset(payload)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(payload, many=True)
        return Response(serializer.data)


class FinepResumoCnaeViewSet(FinepAnalyticsBaseViewSet):
    serializer_class = FinepResumoCnaeSerializer

    def list(self, request, *args, **kwargs):
        groups = defaultdict(
            lambda: {
                'total_empresas': set(),
                'total_projetos': 0,
                'total_aprovado_finep': Decimal('0.00'),
                'total_liberado': Decimal('0.00'),
                'fontes': set(),
            }
        )

        for record in self._base_records():
            company = record.get('company') or {}
            cnae_key = (
                company.get('cnae_fiscal_principal'),
                company.get('cnae_descricao'),
            )
            group = groups[cnae_key]
            company_ids = cast(set[str], group['total_empresas'])
            source_names = cast(set[str], group['fontes'])
            total_projetos = cast(int, group['total_projetos'])
            total_aprovado = cast(Decimal, group['total_aprovado_finep'])
            total_liberado = cast(Decimal, group['total_liberado'])
            if record.get('cnpj_14'):
                company_ids.add(record['cnpj_14'])
            group['total_projetos'] = total_projetos + 1
            group['total_aprovado_finep'] = total_aprovado + record['total_aprovado_finep']
            group['total_liberado'] = total_liberado + record['total_liberado']
            source_names.add(record['source'])

        payload = []
        for cnae_key, row in groups.items():
            company_ids = cast(set[str], row['total_empresas'])
            source_names = cast(set[str], row['fontes'])
            payload.append({
                'cnae_fiscal_principal': cnae_key[0],
                'cnae_descricao': cnae_key[1],
                'total_empresas': len(company_ids),
                'total_projetos': cast(int, row['total_projetos']),
                'total_aprovado_finep': cast(Decimal, row['total_aprovado_finep']),
                'total_liberado': cast(Decimal, row['total_liberado']),
                'fontes': sorted(source_names),
            })

        payload.sort(key=lambda item: (item['total_aprovado_finep'], item['total_projetos']), reverse=True)
        page = self.paginate_queryset(payload)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(payload, many=True)
        return Response(serializer.data)


class FinepResumoMunicipioViewSet(FinepAnalyticsBaseViewSet):
    serializer_class = FinepResumoMunicipioSerializer

    def list(self, request, *args, **kwargs):
        payload = _build_municipio_summary(self._base_records())

        if request.GET.get('all', '').strip().lower() in {'1', 'true', 'yes'}:
            serializer = self.get_serializer(payload, many=True)
            return Response(serializer.data)

        page = self.paginate_queryset(payload)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(payload, many=True)
        return Response(serializer.data)


class FinepMunicipioMapaViewSet(FinepAnalyticsBaseViewSet):
    serializer_class = None

    def list(self, request, *args, **kwargs):
        records = self._base_records()
        municipio_summary = _build_municipio_summary(records)
        simplify_tolerance = _safe_non_negative_float(
            request.GET.get('simplify_tolerance', '0.0'),
            default=0.0,
        )
        features = _load_geo_municipality_features(
            municipio_summary,
            simplify_tolerance=simplify_tolerance,
            uf=request.GET.get('uf', '').strip().upper() or None,
        )

        max_approved_value = max(
            (feature['properties']['total_aprovado_finep'] for feature in features),
            default=0.0,
        )
        municipalities_with_value = sum(
            1 for feature in features if feature['properties']['total_aprovado_finep'] > 0
        )

        return Response({
            'type': 'FeatureCollection',
            'features': features,
            'metadata': {
                'feature_count': len(features),
                'municipalities_with_value': municipalities_with_value,
                'max_total_aprovado_finep': max_approved_value,
                'simplify_tolerance': simplify_tolerance,
            },
        })


class FinepBaseReadOnlyViewSet(viewsets.ReadOnlyModelViewSet):
    search_fields = ()
    cnpj_field = None

    def get_queryset(self):
        queryset = super().get_queryset()
        params = self.request.GET

        q = params.get('q', '').strip()
        contrato = params.get('contrato', '').strip()
        ref = params.get('ref', '').strip()
        uf = params.get('uf', '').strip().upper()
        cnpj = _digits_only(params.get('cnpj', '').strip())

        if q and self.search_fields:
            predicate = Q()
            for field_name in self.search_fields:
                predicate |= Q(**{f'{field_name}__icontains': q})
            queryset = queryset.filter(predicate)

        if contrato:
            queryset = queryset.filter(contrato__icontains=contrato)

        if ref and hasattr(queryset.model, 'ref'):
            queryset = queryset.filter(ref__icontains=ref)

        if uf and hasattr(queryset.model, 'uf'):
            queryset = queryset.filter(uf=uf)

        if cnpj and self.cnpj_field:
            lookup = self.cnpj_field
            if len(cnpj) == 14:
                queryset = queryset.filter(**{lookup: cnpj})
            else:
                queryset = queryset.filter(**{f'{lookup}__startswith': cnpj})

        return queryset


class ProjetoOperacaoDiretaViewSet(FinepBaseReadOnlyViewSet):
    queryset = ProjetoOperacaoDireta.objects.all()
    serializer_class = ProjetoOperacaoDiretaSerializer
    search_fields = ('titulo', 'proponente', 'executor', 'demanda', 'status', 'instrumento')
    cnpj_field = 'cnpj_proponente_norm'


class ProjetoCreditoDescentralizadoViewSet(FinepBaseReadOnlyViewSet):
    queryset = ProjetoCreditoDescentralizado.objects.all()
    serializer_class = ProjetoCreditoDescentralizadoSerializer
    search_fields = ('proponente', 'agente_financeiro')
    cnpj_field = 'cnpj_proponente_norm'


class ProjetoInvestimentoViewSet(FinepBaseReadOnlyViewSet):
    queryset = ProjetoInvestimento.objects.all()
    serializer_class = ProjetoInvestimentoSerializer
    search_fields = ('proponente', 'numero_contrato', 'ref')
    cnpj_field = 'cnpj_proponente_norm'


class ProjetoAncineViewSet(FinepBaseReadOnlyViewSet):
    queryset = ProjetoAncine.objects.all()
    serializer_class = ProjetoAncineSerializer
    search_fields = ('titulo', 'proponente', 'executor', 'demanda', 'status', 'instrumento')
    cnpj_field = 'cnpj_proponente_norm'


class LiberacaoOperacaoDiretaViewSet(FinepBaseReadOnlyViewSet):
    queryset = LiberacaoOperacaoDireta.objects.all()
    serializer_class = LiberacaoOperacaoDiretaSerializer
    search_fields = ('contrato', 'ref')


class LiberacaoCreditoDescentralizadoViewSet(FinepBaseReadOnlyViewSet):
    queryset = LiberacaoCreditoDescentralizado.objects.all()
    serializer_class = LiberacaoCreditoDescentralizadoSerializer
    search_fields = ('contrato',)
    cnpj_field = 'cnpj_proponente_norm'


class LiberacaoAncineViewSet(FinepBaseReadOnlyViewSet):
    queryset = LiberacaoAncine.objects.all()
    serializer_class = LiberacaoAncineSerializer
    search_fields = ('contrato', 'ref')

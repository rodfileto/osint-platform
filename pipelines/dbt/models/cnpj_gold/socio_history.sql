{{ config(
    materialized='external',
    location=gold_location('socio_history'),
    options={'compression': 'zstd'}
) }}

-- One row per continuous period of a partnership, over all silver months.
-- The partnership key is cnpj_basico + identificador_socio + cpf_cnpj_socio +
-- nome_socio_razao_social + qualificacao_socio: a qualification change opens a
-- new period, and a partner who leaves and comes back gets two rows.

-- Periods are counted over the months present in silver, not calendar months:
-- a month missing from silver (never downloaded) does not split a period.

with available_months as (
    select
        reference_month,
        strptime(reference_month || '-01', '%Y-%m-%d') as month_date,
        row_number() over (order by reference_month) as month_ordinal
    from (select distinct reference_month from {{ source('cnpj_silver', 'socios') }})
),

months as (
    select
        s.*,
        m.month_date,
        m.month_ordinal
    from {{ source('cnpj_silver', 'socios') }} s
    join available_months m using (reference_month)
),

islands as (
    select
        *,
        (select max(month_date) from available_months) as latest_month,
        month_ordinal - dense_rank() over (
            partition by cnpj_basico, identificador_socio, cpf_cnpj_socio,
                         nome_socio_razao_social, qualificacao_socio
            order by month_ordinal
        ) as island_id
    from months
)

select
    cnpj_basico,
    identificador_socio,
    cpf_cnpj_socio,
    nome_socio_razao_social,
    qualificacao_socio,
    min(month_date)::DATE as valid_from,
    max(month_date)::DATE as valid_to,
    max(latest_month) = max(month_date) as is_current,
    arg_max(data_entrada_sociedade, month_ordinal) as data_entrada_sociedade,
    arg_max(faixa_etaria, month_ordinal) as faixa_etaria,
    arg_max(pais, month_ordinal) as pais,
    arg_max(representante_legal, month_ordinal) as representante_legal,
    arg_max(nome_do_representante, month_ordinal) as nome_do_representante,
    arg_max(qualificacao_representante_legal, month_ordinal) as qualificacao_representante_legal
from islands
group by
    cnpj_basico,
    identificador_socio,
    cpf_cnpj_socio,
    nome_socio_razao_social,
    qualificacao_socio,
    island_id,
    latest_month

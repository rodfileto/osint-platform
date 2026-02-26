from rest_framework import serializers
from .models import MvCompanySearch


class MvCompanySearchSerializer(serializers.ModelSerializer):
    class Meta:
        model = MvCompanySearch
        fields = [
            'cnpj_14',
            'cnpj_basico',
            'razao_social',
            'nome_fantasia',
            'situacao_cadastral',
            'municipio',
            'uf',
            'cnae_fiscal_principal',
            'porte_empresa',
            'natureza_juridica',
            'capital_social',
        ]

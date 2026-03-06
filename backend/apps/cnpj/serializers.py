from rest_framework import serializers
from .models import MvCompanySearch, Empresa, Estabelecimento


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


class EstabelecimentoSerializer(serializers.ModelSerializer):
    """Serializer for Estabelecimento with readable labels"""
    cnpj_completo = serializers.SerializerMethodField()
    situacao_cadastral_display = serializers.CharField(
        source='get_situacao_cadastral_display',
        read_only=True
    )
    matriz_filial_display = serializers.CharField(
        source='get_identificador_matriz_filial_display',
        read_only=True
    )
    
    class Meta:
        model = Estabelecimento
        fields = [
            'cnpj_completo',
            'cnpj_ordem',
            'cnpj_dv',
            'identificador_matriz_filial',
            'matriz_filial_display',
            'nome_fantasia',
            'situacao_cadastral',
            'situacao_cadastral_display',
            'data_situacao_cadastral',
            'data_inicio_atividade',
          'cnae_fiscal_principal',
            'cnae_fiscal_secundaria',
            'tipo_logradouro',
            'logradouro',
            'numero',
            'complemento',
            'bairro',
            'cep',
            'uf',
            'municipio',
            'ddd_telefone_1',
            'ddd_telefone_2',
            'ddd_fax',
            'correio_eletronico',
        ]
    
    def get_cnpj_completo(self, obj):
        """Retorna CNPJ completo formatado: XX.XXX.XXX/XXXX-XX"""
        cnpj_basico = obj.empresa.cnpj_basico
        cnpj_completo = f"{cnpj_basico}{obj.cnpj_ordem}{obj.cnpj_dv}"
        return f"{cnpj_completo[:2]}.{cnpj_completo[2:5]}.{cnpj_completo[5:8]}/{cnpj_completo[8:12]}-{cnpj_completo[12:14]}"


class EmpresaDetailSerializer(serializers.ModelSerializer):
    """Serializer for Empresa with all estabelecimentos"""
    estabelecimentos = serializers.SerializerMethodField()
    porte_empresa_display = serializers.CharField(
        source='get_porte_empresa_display',
        read_only=True
    )
    total_estabelecimentos = serializers.SerializerMethodField()
    estabelecimentos_ativos = serializers.SerializerMethodField()
    
    class Meta:
        model = Empresa
        fields = [
            'cnpj_basico',
            'razao_social',
            'natureza_juridica',
            'capital_social',
            'porte_empresa',
            'porte_empresa_display',
            'ente_federativo_responsavel',
            'total_estabelecimentos',
            'estabelecimentos_ativos',
            'estabelecimentos',
        ]
    
    def get_total_estabelecimentos(self, obj):
        return obj.estabelecimentos.count()
    
    def get_estabelecimentos_ativos(self, obj):
        return obj.estabelecimentos.filter(situacao_cadastral=2).count()
    
    def get_estabelecimentos(self, obj):
        """Return estabelecimentos as dict to avoid composite PK issues"""
        # Use .values() to avoid Django trying to fetch 'id' column
        estabelecimentos = obj.estabelecimentos.values(
            'cnpj_ordem',
            'cnpj_dv',
            'identificador_matriz_filial',
            'nome_fantasia',
            'situacao_cadastral',
            'data_situacao_cadastral',
            'data_inicio_atividade',
            'cnae_fiscal_principal_id',
            'cnae_fiscal_secundaria',
            'tipo_logradouro',
            'logradouro',
            'numero',
            'complemento',
            'bairro',
            'cep',
            'uf',
            'municipio_id',
            'ddd_telefone_1',
            'ddd_telefone_2',
            'ddd_fax',
            'correio_eletronico',
        )
        
        result = []
        for est in estabelecimentos:
            cnpj_completo = f"{obj.cnpj_basico}{est['cnpj_ordem']}{est['cnpj_dv']}"
            cnpj_formatado = f"{cnpj_completo[:2]}.{cnpj_completo[2:5]}.{cnpj_completo[5:8]}/{cnpj_completo[8:12]}-{cnpj_completo[12:14]}"
            
            result.append({
                'cnpj_completo': cnpj_formatado,
                'cnpj_ordem': est['cnpj_ordem'],
                'cnpj_dv': est['cnpj_dv'],
                'identificador_matriz_filial': est['identificador_matriz_filial'],
                'tipo_estabelecimento': 'Matriz' if est['identificador_matriz_filial'] == 1 else 'Filial',
                'nome_fantasia': est['nome_fantasia'],
                'situacao_cadastral': est['situacao_cadastral'],
                'situacao_cadastral_display': dict(Estabelecimento.SituacaoCadastral.choices).get(est['situacao_cadastral']),
                'data_situacao_cadastral': est['data_situacao_cadastral'],
                'data_inicio_atividade': est['data_inicio_atividade'],
                'cnae_fiscal_principal': est['cnae_fiscal_principal_id'],
                'cnae_fiscal_secundaria': est['cnae_fiscal_secundaria'],
                'endereco': {
                    'tipo_logradouro': est['tipo_logradouro'],
                    'logradouro': est['logradouro'],
                    'numero': est['numero'],
                    'complemento': est['complemento'],
                    'bairro': est['bairro'],
                    'cep': est['cep'],
                    'uf': est['uf'],
                    'municipio': est['municipio_id'],
                },
                'contato': {
                    'ddd_telefone_1': est.get('ddd_telefone_1'),
                    'ddd_telefone_2': est.get('ddd_telefone_2'),
                    'ddd_fax': est.get('ddd_fax'),
                    'email': est['correio_eletronico'],
                }
            })
        return result

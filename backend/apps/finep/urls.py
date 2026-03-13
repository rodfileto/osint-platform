from rest_framework.routers import DefaultRouter

from .views import (
    FinepResumoGeralViewSet,
    FinepResumoCnaeViewSet,
    FinepResumoEmpresaViewSet,
    FinepResumoMunicipioViewSet,
    FinepResumoUfViewSet,
    LiberacaoAncineViewSet,
    LiberacaoCreditoDescentralizadoViewSet,
    LiberacaoOperacaoDiretaViewSet,
    ProjetoAncineViewSet,
    ProjetoCreditoDescentralizadoViewSet,
    ProjetoInvestimentoViewSet,
    ProjetoOperacaoDiretaViewSet,
)


router = DefaultRouter()
router.register(r'operacao-direta', ProjetoOperacaoDiretaViewSet, basename='finep-operacao-direta')
router.register(r'credito-descentralizado', ProjetoCreditoDescentralizadoViewSet, basename='finep-credito-descentralizado')
router.register(r'investimento', ProjetoInvestimentoViewSet, basename='finep-investimento')
router.register(r'ancine', ProjetoAncineViewSet, basename='finep-ancine')
router.register(r'liberacoes-operacao-direta', LiberacaoOperacaoDiretaViewSet, basename='finep-liberacoes-operacao-direta')
router.register(r'liberacoes-credito-descentralizado', LiberacaoCreditoDescentralizadoViewSet, basename='finep-liberacoes-credito-descentralizado')
router.register(r'liberacoes-ancine', LiberacaoAncineViewSet, basename='finep-liberacoes-ancine')
router.register(r'resumo-geral', FinepResumoGeralViewSet, basename='finep-resumo-geral')
router.register(r'resumo-empresa', FinepResumoEmpresaViewSet, basename='finep-resumo-empresa')
router.register(r'resumo-uf', FinepResumoUfViewSet, basename='finep-resumo-uf')
router.register(r'resumo-cnae', FinepResumoCnaeViewSet, basename='finep-resumo-cnae')
router.register(r'resumo-municipio', FinepResumoMunicipioViewSet, basename='finep-resumo-municipio')

urlpatterns = router.urls

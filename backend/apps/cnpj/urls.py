from rest_framework.routers import DefaultRouter
from .views import (
    CnpjSearchViewSet,
    CnpjSearchInactiveViewSet,
    EmpresaViewSet,
    CompanyNetworkViewSet,
    PessoaSearchViewSet,
    PessoaDetailViewSet,
    PessoaNetworkViewSet,
)

router = DefaultRouter()
router.register(r'search', CnpjSearchViewSet, basename='cnpj-search')
router.register(r'search-inactive', CnpjSearchInactiveViewSet, basename='cnpj-search-inactive')
router.register(r'empresa', EmpresaViewSet, basename='empresa')
router.register(r'network', CompanyNetworkViewSet, basename='company-network')
router.register(r'pessoa/search', PessoaSearchViewSet, basename='pessoa-search')
router.register(r'pessoa', PessoaDetailViewSet, basename='pessoa')
router.register(r'pessoa-network', PessoaNetworkViewSet, basename='pessoa-network')

urlpatterns = router.urls

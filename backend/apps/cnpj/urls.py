from rest_framework.routers import DefaultRouter
from .views import CnpjSearchViewSet, CnpjSearchInactiveViewSet, EmpresaViewSet, CompanyNetworkViewSet

router = DefaultRouter()
router.register(r'search', CnpjSearchViewSet, basename='cnpj-search')
router.register(r'search-inactive', CnpjSearchInactiveViewSet, basename='cnpj-search-inactive')
router.register(r'empresa', EmpresaViewSet, basename='empresa')
router.register(r'network', CompanyNetworkViewSet, basename='company-network')

urlpatterns = router.urls

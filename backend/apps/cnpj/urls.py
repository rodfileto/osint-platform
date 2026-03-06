from rest_framework.routers import DefaultRouter
from .views import CnpjSearchViewSet, EmpresaViewSet

router = DefaultRouter()
router.register(r'search', CnpjSearchViewSet, basename='cnpj-search')
router.register(r'empresa', EmpresaViewSet, basename='empresa')

urlpatterns = router.urls

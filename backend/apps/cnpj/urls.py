from rest_framework.routers import DefaultRouter
from .views import CnpjSearchViewSet

router = DefaultRouter()
router.register(r'search', CnpjSearchViewSet, basename='cnpj-search')

urlpatterns = router.urls

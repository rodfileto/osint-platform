from rest_framework.routers import DefaultRouter

from .views import (
    InpiResumoGeralViewSet,
    IpcSubclasseRefViewSet,
    MvPatenteSearchViewSet,
    PatenteClassificacaoIPCViewSet,
    PatenteDespachoViewSet,
    PatenteDepositanteViewSet,
    PatenteInventorViewSet,
    PatentePrioridadeViewSet,
    PatenteProcuradorViewSet,
    PatenteRenumeracaoViewSet,
    PatenteVinculoViewSet,
)

router = DefaultRouter()
router.register(r'patentes', MvPatenteSearchViewSet, basename='inpi-patentes')
router.register(r'inventores', PatenteInventorViewSet, basename='inpi-inventores')
router.register(r'depositantes', PatenteDepositanteViewSet, basename='inpi-depositantes')
router.register(r'classificacao-ipc', PatenteClassificacaoIPCViewSet, basename='inpi-classificacao-ipc')
router.register(r'despachos', PatenteDespachoViewSet, basename='inpi-despachos')
router.register(r'procuradores', PatenteProcuradorViewSet, basename='inpi-procuradores')
router.register(r'prioridades', PatentePrioridadeViewSet, basename='inpi-prioridades')
router.register(r'vinculos', PatenteVinculoViewSet, basename='inpi-vinculos')
router.register(r'renumeracoes', PatenteRenumeracaoViewSet, basename='inpi-renumeracoes')
router.register(r'resumo', InpiResumoGeralViewSet, basename='inpi-resumo')
router.register(r'ipc-ref', IpcSubclasseRefViewSet, basename='inpi-ipc-ref')

urlpatterns = router.urls

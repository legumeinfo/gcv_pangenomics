from django.apps import AppConfig


# extend AppConfig class to fire signal when pangenomics application is ready
class PangenomicsConfig(AppConfig):
    name = 'pangenomics'

    def ready(self):
        import pangenome
        from signals import app_ready
        app_ready.send(sender=self.__class__)

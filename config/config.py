from dynaconf import Dynaconf

settings = Dynaconf( settings_files=[".secrets_api.yaml", "paths.yaml"])
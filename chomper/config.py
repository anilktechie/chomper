import os
from backports.configparser import ConfigParser as BaseConfigParser


UNSET = object()


class ConfigParser(BaseConfigParser):

    def get(self, section, option, fallback=UNSET, **kwargs):
        if fallback is not UNSET:
            kwargs['fallback'] = fallback
        return super(ConfigParser, self).get(section, option, **kwargs)


config = ConfigParser()
default_project_ini = os.path.join(os.getcwd(), 'chomper.ini')
config.read(default_project_ini)

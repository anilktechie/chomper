import os
from backports.configparser import ConfigParser as BaseConfigParser


UNSET = object()


class ConfigParser(BaseConfigParser):

    def get(self, section, option, fallback=UNSET, **kwargs):
        if fallback is not UNSET:
            kwargs['fallback'] = fallback
        return super(ConfigParser, self).get(section, option, **kwargs)

    def getint(self, section, option, fallback=UNSET, **kwargs):
        if fallback is not UNSET:
            kwargs['fallback'] = fallback
        return super(ConfigParser, self).getint(section, option, **kwargs)

    def getfloat(self, section, option, fallback=UNSET, **kwargs):
        if fallback is not UNSET:
            kwargs['fallback'] = fallback
        return super(ConfigParser, self).getfloat(section, option, **kwargs)

    def getboolean(self, section, option, fallback=UNSET, **kwargs):
        if fallback is not UNSET:
            kwargs['fallback'] = fallback
        return super(ConfigParser, self).getboolean(section, option, **kwargs)


config = ConfigParser()
default_project_ini = os.path.join(os.getcwd(), 'chomper.ini')
config.read(default_project_ini)

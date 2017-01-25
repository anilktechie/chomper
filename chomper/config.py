import os
import six

try:
    import configparser as BaseConfigParser
except ImportError:
    import ConfigParser as BaseConfigParser


class ConfigParser(BaseConfigParser.ConfigParser, object):
    """
    Override ConfigParser to make it more forgiving when getting
    and setting missing sections or options.
    """

    def get(self, *args, **kwargs):
        try:
            return super(ConfigParser, self).get(*args, **kwargs)
        except (BaseConfigParser.NoOptionError, BaseConfigParser.NoSectionError):
            return None

    def set(self, section, option, value=None):
        try:
            return super(ConfigParser, self).set(section, option, value)
        except BaseConfigParser.NoSectionError:
            self.add_section(section)
            return super(ConfigParser, self).set(section, option, value)


config = ConfigParser()


def load_config(path):
    if os.path.isfile(path):
        config.read(path)


def set_config(module, values=None, **kwargs):
    if not values:
        values = kwargs
    for key, value in six.iteritems(values):
        config.set(module, key, value)


default_project_ini = os.path.join(os.getcwd(), 'chomper.ini')
load_config(default_project_ini)

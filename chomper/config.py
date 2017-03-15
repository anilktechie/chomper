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

    def getdict(self, section):
        try:
            return dict(self.items(section))
        except BaseConfigParser.NoSectionError:
            return None

    def get_many(self, *sections):
        _config = dict()
        for section_name in sections:
            section = self.get(section_name, None)
            if section is not None:
                _config[section_name] = section
        return _config

    def set(self, section, option, value=None):
        try:
            return super(ConfigParser, self).set(section, option, value)
        except BaseConfigParser.NoSectionError:
            self.add_section(section)
            return super(ConfigParser, self).set(section, option, value)

    def set_section(self, section, _values=None, **values):
        if isinstance(_values, dict):
            values.update(_values)
        for key, value in six.iteritems(values):
            config.set(section, key, value)


config = ConfigParser()
default_project_ini = os.path.join(os.getcwd(), 'chomper.ini')
config.read(default_project_ini)

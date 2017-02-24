from base_monitor import *
from django.conf import settings
from deepInsight.util import get_logger
from deepInsight.monitor.constants import *


class FluentdPluginManager(BaseMonitor):
    NAME = "loggers"

    def __init__(self, tid):
        super(FluentdPluginManager, self).__init__(FluentdPluginManager.NAME, tid)

        # Initialize logger object
        self.logger = get_logger('FluentdPluginManager')

        # Initialize defaults
        self.plugin_path = os.path.sep + 'etc' + os.path.sep + 'td-agent'
        self.service_name = 'td-agent'

        self.plugins, self.target = [], []
        # self.logger_user_input = template_data

        config_file = os.path.join(settings.PROFILE_DIR, self.name, 'plugin_config.json')
        with open(config_file, 'r') as f:
            self.plugin_config = json.load(f)

        self.plugin_post_data, self.status = [], []

        self.logger.info('Logger Object Successfully Initialized.')
        self.logger.info('Targets Nodes : %s' % str(self.nodelist))
        self.logger.info('User Input : %s' % str(self.sub_template))

    def get_name(self):
        return self.name

    def configure_plugin_data(self):
        # Read template config, merge them with plugin config and generate plugin params
        self.logger.info('Configuring the plugin data.')
        for x_plugin in self.sub_template.get('plugins'):
            temp = dict()
            temp['source'] = {}
            temp['source']['tag'] = x_plugin.get('tag')
            temp['name'] = x_plugin.get('type')

            if x_plugin.get('type') in self.plugin_config.keys():
                temp['source'].update(self.plugin_config.get(x_plugin.get('type')).get('source'))
                temp['filter'] = self.plugin_config.get(x_plugin.get('type')).get('filter')
                temp['match'] = self.plugin_config.get(x_plugin.get('type')).get('match')
            else:
                print'In-Valid plugin type - ', x_plugin.get('type')
                self.logger.warning('In-Valid input plugin type.')
                continue

            filter_lower = [x.lower() for x in x_plugin.get('filter').get('level')]
            filter_upper = [x.upper() for x in x_plugin.get('filter').get('level')]

            if 'WARNING' in filter_upper:
                filter_upper.remove('WARNING')
                filter_upper.append('WARN')

            if 'all' in filter_lower:
                temp['source']['format'] = 'none'
                temp['usr_filter'] = '(.*?)'
            else:
                temp['source']['format'] = 'none'
                temp['usr_filter'] = '(.*(' + '|'.join(filter_upper) + ').*?)'

            self.plugins.append(temp)
        self.logger.info('Plugin data successfully Configured.')
        return True

    def configure_plugin_file(self, data):
        # Add source.
        lines = ['<source>']
        for key, val in data.get('source', {}).iteritems():
            lines.append('\t' + key + ' ' + val)
        lines.append('</source>')

        # Add grep filter.
        lines.append('\n<filter ' + data.get('source').get('tag') + '>')
        lines.append('\t@type grep')
        lines.append('\tregexp1 message ' + data.get('usr_filter'))
        lines.append('</filter>')

        # Add Record-Transformation Filter.
        if data.get('filter').has_key('tag'):
            lines.append('\n<filter ' + data.get('filter').get('tag') + '>')
        else:
            lines.append('\n<filter ' + data.get('source').get('tag') + '>')
        lines.extend(['\t@type record_transformer', '\t<record>'])
        for key, val in data.get('filter', {}).iteritems():
            lines.append('\t\t' + key + ' \"' + val + '\"')

        # lines.append('\t\ttags ' + str(self.tags + [data.get('source').get('tag')]))
        tags = [str(x) for x in self.tags + [data.get('source').get('tag')]]
        lines.append('\t\ttags ' + str(tags).replace('\'', '"'))
        lines.extend(['\t</record>', '</filter>'])

        # Add match.
        if data.get('match').has_key('tag'):
            lines.append('\n<match ' + data.get('match').get('tag') + '>')
            data.get('match').pop('tag')
        else:
            lines.append('\n<match ' + data.get('source').get('tag') + '>')

        for key, val in self.sub_template.get('target')[0].iteritems():
            if key == "type":
                key = "@" + key
            lines.append('\t' + key + ' ' + val)

        for key, val in data.get('match', {}).iteritems():
            lines.append('\t' + key + ' ' + val)
        lines.append('</match>')

        filename = self.plugin_path + os.path.sep + data.get('name')
        self.plugin_post_data.append((filename, '\n'.join(lines)))
        return True

    def generate_plugins(self):
        # Generate the files in the salt dir
        self.configure_plugin_data()

        for x_plugin in self.plugins:
            self.logger.debug('Configuring the plugin: %s' % (str(x_plugin)))
            self.configure_plugin_file(x_plugin)
        return True

    def generate_fluentd_config_file(self):
        self.logger.info('Generating fluentd config file (td-agent.conf).')
        lines = []
        for x_plugin in self.plugins:
            lines.append('@include ' + x_plugin.get('name'))

        lines.append('\n<match *>')
        for key, val in self.sub_template.get('target')[0].iteritems():
            if key == "type":
                key = "@" + key
            lines.append('\t' + key + ' ' + val)

        lines.append('\t' + 'flush_interval' + ' ' + self.plugin_config.get('default_flush_interval'))
        lines.append('\t' + 'include_tag_key' + ' true')
        lines.append('</match>')

        filename = self.plugin_path + os.path.sep + 'td-agent.conf'
        self.plugin_post_data.append((filename, '\n'.join(lines)))
        return True

    def deploy(self, oper):
        self.logger.info('Deployment Started.')
        self.logger.debug('Operation : ' + str(oper))
        self.logger.debug('Enable : ' + str(self.sub_template.get('enable', False)))

        salt_obj = SaltManager()
        status = {}
        if oper == START:
            if self.sub_template.get('enable', False):
                self.generate_plugins()
                self.generate_fluentd_config_file()
                # print '\nPost Data: ', json.dumps(self.plugin_post_data)

                self.logger.info('Pushing the configs to the target node.')
                self.logger.debug('self.plugin_post_data: %s', json.dumps(self.plugin_post_data))
                status = salt_obj.push_config(self.plugin_post_data, self.nodelist)
                self.logger.debug('Push Status: %s', json.dumps(status))
                self.restart()
            else:
                self.stop()

        elif oper == STOP:
            if self.sub_template.get('enable', False):
                disable_files, disable_plugins = [], []
                for x_plugin in self.sub_template.get('plugins'):
                    disable_plugins.append(x_plugin.get('type'))
                    disable_files.append(self.plugin_path + os.path.sep + x_plugin.get('type'))

                self.logger.info('Plugin disable in progress: ' + str(disable_plugins))
                status = salt_obj.delete_files(disable_files, self.nodelist)
                new_config_data = salt_obj.reset_logger_config(disable_plugins, self.nodelist)
                config_post_data = [[self.plugin_path + os.path.sep + 'td-agent.conf', new_config_data]]
                self.logger.debug('self.plugin_post_data: %s', json.dumps(config_post_data))
                config_status = salt_obj.push_config(config_post_data, self.nodelist)
                self.logger.debug('Config Push Status :%s', json.dumps(config_status))
                status.update(config_status)
                self.restart()
            else:
                self.stop()

        elif oper == 'teardown':
            self.logger.info('Teardown Started.')
            self.stop()
            return

        else:
            self.logger.debug('Unknown Operation.')
            pass

        self.change_service_status("status")
        return self.status

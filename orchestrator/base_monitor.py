import abc
import os
from django.conf import settings
from orchestrator.util import get_logger
import salt.client
import json


class BaseMonitor:
    __metaclass__ = abc.ABCMeta
    logger = get_logger('BaseMonitor')

    def __init__(self, name, tid):
        self.name = name
        self.tid = tid

        self.tags, self.nodelist = [], []
        self.template, self.sub_template = {}, {}
        self.read_template_data()

        self.status = []

    @abc.abstractmethod
    def get_name(self):
        return

    @abc.abstractmethod
    def deploy(self):
        raise NotImplementedError()

    def read_template_data(self):
        self.logger.debug('Reading sub-template for specific watch utility.')
        filepath = settings.DATA_DIR + os.path.sep + self.tid
        template = {}
        try:
            with open(filepath, 'r') as f:
                self.template = json.load(f)
                self.tags = self.template.get('tags', [])
                self.nodelist = self.template.get('node_list', [])
                self.sub_template = self.template.get('sub_template')[0].get(self.name)
        except Exception as e:
            self.logger.debug('Exception: %s', str(e))
            self.logger.error('Expected sub-template Not Found.')
        return

    def change_service_status(self, operation):
        try:
            salt_obj = SaltManager()
            salt_status = salt_obj.change_service_status(self.nodelist, [self.service_name], operation)
            self.logger.debug('salt_status :' + str(salt_status))

            self.status = []
            for node in self.nodelist:
                temp = dict()
                temp['node_name'], temp['status'] = node, "Not Running"
                if node in salt_status:
                    if salt_status[node]:
                        temp['status'] = "Running"
                else:
                    temp['status'] = "No Response"
                self.status.append(temp)

        except Exception as e:
            self.logger.error('Exception Caused: %s.', str(e))
            return

    def start(self):
        self.logger.debug('Server - td-agent - start call.')
        self.change_service_status("start")

    def restart(self):
        self.logger.debug('Server - td-agent - restart call.')
        self.change_service_status("restart")

    def stop(self):
        self.logger.debug('Server - td-agent - stop call.')
        self.change_service_status("stop")

    def teardown(self):
        self.logger.debug('Server - td-agent - Teardown call.')
        self.change_service_status("stop")

    def check_status(self):
        self.logger.debug('Server - td-agent - check_status call.')
        self.change_service_status("status")
        return self.status

    '''
    def check_status(self):
        raise NotImplementedError()
    '''


class SaltManager:
    SERVICE_STATUS = {'available': 'service.available',
                      'restart': 'service.restart',
                      'start': 'service.start',
                      'stop': 'service.stop',
                      'status': 'service.status'}

    def __init__(self):
        self.logger = get_logger('BaseMonitor')
        self.local = salt.client.LocalClient()

    def change_service_status(self, nodes, services, status):
        self.logger.info('Changing Service Status')
        self.logger.debug('Nodes: %s, Services: %s, Status: %s'%(str(nodes), str(services), str(status)))

        # Change the service status to user provided value.

        status = self.local.cmd(tgt=nodes, expr_form='list',
                                fun=self.SERVICE_STATUS[status], arg=services)

        self.logger.debug('Status: %s' % (str(status)))
        return status

    def delete_files(self, file_list=[], dest_nodes=[]):
        self.logger.debug('Deleting Files.')
        status_delete = {}
        for node in dest_nodes:
            status_delete[node] = {}
            status_delete[node]["success_list"] = []
            status_delete[node]["failed_list"] = []
        try:
            for file in file_list:
                cmd_string = "rm " + file
                status = self.local.cmd(tgt=dest_nodes, expr_form='list',
                                   fun="cmd.run", arg=[cmd_string])
                for node in dest_nodes:
                    if node in status:
                        if file == "*.conf":
                            status_delete[node]["success_list"].append(file)
                            continue
                        if status[node].startswith("rm"):
                            status_delete[node]["failed_list"].append(file)
                        else:
                            status_delete[node]["success_list"].append(file)
                    else:
                        status_delete[node]["failed_list"].append(file)
        except Exception as e:
            print e
        self.logger.debug('Delete Status: %s', status_delete)
        return status_delete

    def reset_logger_config(self, disable_plugins=[], dest_nodes=[]):
        self.logger.debug('Resetting Logger Configurations.')
        new_config = os.path.sep + 'etc' + os.path.sep + 'td-agent' + os.path.sep + 'td-agent.conf'
        content = self.local.cmd(tgt=dest_nodes, expr_form='list', fun="cp.get_file_str", arg=[new_config])

        existing_plugins = content.values()[0].split('\n\n')[0].split('\n')
        default_match = content.values()[0].split('\n\n')[1].split('\n')

        for x_plugin in disable_plugins:
            if "@include " + x_plugin in existing_plugins:
                existing_plugins.remove("@include " + x_plugin)

        existing_plugins.append('\n')
        all_content = existing_plugins + default_match
        return '\n'.join(all_content).replace('\n\n', '\n')

    def push_config(self, cfg_info_list=[], dest_nodes=[]):
        self.logger.debug('Pushing Configurations.')
        status_push = {}
        for node in dest_nodes:
            status_push[node] = {}
            status_push[node]["success_list"] = []
            status_push[node]["failed_list"] = []
        try:
            local = salt.client.LocalClient()
            for cfg_info in cfg_info_list:
                config = cfg_info[1]
                dest_file = cfg_info[0]
                status = local.cmd(tgt=dest_nodes, expr_form='list',
                                   fun="file.write", arg=[dest_file, config])
                for node in dest_nodes:
                    if node in status:
                        if status[node].startswith("Wrote"):
                            status_push[node]["success_list"].append(dest_file)
                            continue
                    status_push[node]["failed_list"].append(dest_file)
        except Exception as e:
            print e

        self.logger.debug('Push Status: %s', json.dumps(status_push))
        return status_push


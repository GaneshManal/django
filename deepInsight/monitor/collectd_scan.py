import sys
from base_monitor import *
from deepInsight.monitor.constants import *
from deepInsight.util import get_logger
import copy
from mako.template import Template


class CollectdPluginManager(BaseMonitor):
    NAME = "collectors"

    def __init__(self, tid):
        super(CollectdPluginManager, self).__init__(CollectdPluginManager.NAME, tid)

        # Initialize logger object
        self.expanded_template = {}
        self.logger = get_logger('CollectdPluginManager')

        self.collector_type = COLLECTD
        self.config_info, self.node_state, self.nodes_cfg_list = [], [], []
        self.all_conf = CollectdPluginDestDir + ALL_CONF

    def get_name(self):
        return self.name

    @staticmethod
    def get_expanded_profile(sub_template):
        # self.logger.debug("Expanding the input profile.")
        template_prof = dict()
        template_prof[PLUGINS] = []
        plugin_map = {}

        prof_file = settings.PROFILE_DIR + os.path.sep + 'profile'
        with open(prof_file, 'r') as fh:
            plugin_map = json.load(fh)

        for profile in sub_template.get(PLUGINS, []):
            for plugin_name in plugin_map[profile[NAME]]:
                new_plugin = dict(profile)
                new_plugin[NAME] = plugin_name
                template_prof[PLUGINS].append(new_plugin)
        # self.logger.debug("Expanded profile - %s.", json.dumps(template_prof))
        return template_prof

    def deploy(self, oper, plugin_name=""):
        self.logger.debug("Inside Deploy ( Operation = %s )", oper)

        if oper == 'teardown':
            return self.teardown()

        state = True if oper == START else False
        self.logger.info('Template: %s', json.dumps(self.sub_template))
        self.logger.info('List of Plugins: %s', json.dumps(self.sub_template.get(PLUGINS)))
        for item in self.sub_template.get(PLUGINS, []):
            if item.get(NAME) not in [LS, LD]:
                self.logger.error('In-Valid plugin profile: %s', item.get(NAME))
                continue

        # if the operation is to disable just stop collector and return.
        if self.sub_template.get(ENABLE):
            self.logger.info('Enable plugin parameter: %s.', self.sub_template.get(ENABLE))
        else:
            self.logger.warning('Enable plugin parameter: %s.', self.sub_template.get(ENABLE))
            self.logger.info("Stopping Collector Service.")
            return self.stop()

        # self.expanded_template = self.get_expanded_profile()

        status_dict = dict()
        status_dict[CONFIG], status_dict[PUSH] = dict(), dict()
        success = False
        error_msg = ""
        try:
            # if the node_list is empty then just simply return
            if not self.nodelist:
                status_dict[ERROR] = EMPTY_NODE_LIST
                self.logger.error('Node list is Empty.')
                return success, status_dict

            # Generate config
            self.logger.info("Generating config.")
            collector_obj = CollectdManager(self.template, self.sub_template)
            success, conf_dict = collector_obj.generate_config()
            self.logger.debug('Configuration Dictionary: %s', str(conf_dict))
            status_dict[CONFIG][FAILED_LIST] = conf_dict[FAILED_LIST]

            # if config generation is Successful then delete remote conf files.
            # and then push the new conf files and restart service
            if success:
                self.logger.info("Config Generation Completed Successfully.")
                salt_obj = SaltManager()

                # delete remote conf files
                self.logger.info("Deleting remote conf files.")
                status_dict[DELETE] = salt_obj.delete_files(file_list=[self.all_conf], dest_nodes=self.nodelist)
                self.logger.debug(str(status_dict[DELETE]))

                # push conf files on remote node
                self.logger.info("Pushing config files on Remote Machine.")
                status_dict[PUSH] = salt_obj.push_config(conf_dict[SUCCESS_LIST], self.nodelist)
                self.logger.debug(str(status_dict[PUSH]))

                # restart service
                self.logger.info("Restarting collectd service.")
                status_dict[START] = self.start()
                self.logger.debug(str(status_dict[START]))
            else:
                status_dict[CONFIG][ERROR] = conf_dict[ERROR]
                error_msg = conf_dict[ERROR]
                self.logger.info("Configuration generation Failed")
                self.logger.debug(str(error_msg))
        except KeyError, e:
            error_msg = str(e) + KEY_ERROR
            success = False
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = str(e)
            success = False
            self.logger.error(error_msg)
        status_dict[ERROR] = error_msg
        self.logger.info("Deploy COMPLETE.")
        self.logger.debug("Deploy return status: " + str(status_dict))
        return success, status_dict

    def start(self):
        self.change_service_status(RESTART)
        return self.node_state

    def stop(self):
        self.change_service_status(STOP)
        return self.node_state

    def teardown(self):
        error_msg = ""
        # first stop service and then delete remote conf files
        try:
            # Stop Service
            self.logger.info("Tearing down / Stopping service.")
            self.change_service_status(STOP)

            # delete remote conf files
            salt_obj = SaltManager()
            self.logger.info("Deleting Remote configuration Files.")
            status = salt_obj.delete_files(file_list=[self.all_conf], dest_nodes=self.nodelist)
            self.logger.info("Teardown Complete.")
            self.logger.debug("Teardown Return Status: %s", str(status))
        except Exception as e:
            error_msg += str(e)
            self.logger.error('Exception: %s', error_msg)
        return self.node_state

    def check_status(self):
        self.change_service_status(STATUS)
        return self.node_state

    def change_service_status(self, operation):
        state = []
        error_msg = ""
        try:
            salt_obj = SaltManager()
            status = salt_obj.change_service_status(self.nodelist, [self.collector_type], operation)

            # Iterate over Node List and Update the Status.
            for node in self.nodelist:
                # fill default status to "no response"
                status_temp = {NODE_NAME: node, STATUS: NO_RESP}
                if node in status:
                    # status = True means running
                    if status[node]:
                        status_temp[STATUS] = RUNNING
                    else:
                        status_temp[STATUS] = NOT_RUNNING
                state.append(status_temp)
        except KeyError, e:
            error_msg = str(e) + KEY_ERROR
            self.logger.error('Exception: %s', error_msg)
        except Exception as e:
            error_msg = str(e)
            self.logger.error('Exception: %s', error_msg)
        self.logger.info("Changed service status")
        self.logger.debug('Node State: %s', str(state))
        self.node_state = state


class CollectdManager:

    def __init__(self, template, sub_template):
        self.plugin_src_dir = CollectdPluginDestDir
        self.plugin_conf_dir = CollectdPluginConfDir
        self.collectd_conf_dir = CollectdConfDir
        self.interval = 10

        self.template = template
        self.sub_template = sub_template
        self.cfg_list, self.tag_list, self.target_list = [], [], []
        self.tags, self.targets = {}, {}

        self.logger = get_logger(COLLECTD_MGR)
        self.logger.debug('Template: %s', json.dumps(self.template))
        self.logger.debug('SubTemplate: %s', json.dumps(self.sub_template))

    def set_target_and_tag(self, plugin, plugin_targets=[], plugin_tags=[]):
        error_msg = ""
        try:
            self.logger.debug("plugin: " + str(plugin) + " targets: " + str(plugin_targets) + " tags: " + str(plugin_tags))
            for ptarget in plugin_targets:
                for target in self.target_list:
                    if ptarget == target[NAME]:
                        target_type = target[TYPE]
                        if target_type not in self.targets:
                            self.targets[target_type] = {}
                        if ptarget not in self.targets[target_type]:
                            self.targets[target_type][ptarget] = {CONFIG: target, PLUGINS: []}
                        self.targets[target_type][ptarget][PLUGINS].append(plugin)
            if plugin_tags:
                self.tags[plugin] = plugin_tags
            return True
        except Exception as e:
            error_msg += str(e)
        self.logger.error(error_msg)
        return False

    '''
    Input: Plugin name and dictionary of options.
    Output: Returns plugin config.
    '''
    def get_section_cfg(self, section_name, section):
        self.logger.info('Getting Section Configurations.')
        self.logger.info('section_name: %s, section: %s' % (section_name, section))
        section_cfg = ""
        error_msg = ""
        try:
            filename = section_name + EXTENSION
            filename = os.path.join(settings.PROFILE_DIR, COLLECTORS, filename)
            self.logger.debug('Section Configuration Filename: %s', filename)
            mytemplate = Template(filename=filename)
            section_cfg = mytemplate.render(data=section)
            # self.logger.debug('section configuration: %s', str(section_cfg))
            if section_cfg is not None:
                # filters and targets won't have name key
                if NAME in section and TARGETS in section and TAGS in section:
                    if self.set_target_and_tag(section[NAME], section[TARGETS], section[TAGS]):
                        return True, section_cfg
                else:
                    return True, section_cfg
        except KeyError, e:
            error_msg = error_msg + str(e) + KEY_ERROR
        except Exception as e:
            error_msg += str(e)
        self.logger.error('Exception: %s', error_msg)
        return False, None

    '''
    Iterates over the list of plugins generating config for each plugin.
    Then iterates over target list and generates config for targets.
    At the end generates config for filters.
    '''
    def generate(self):
        error_msg = ""
        return_dict = dict()
        return_dict[SUCCESS_LIST] = []
        return_dict[FAILED_LIST] = []
        success_overall = False
        try:
            # generate config for plugins
            for cfg in self.cfg_list:
                filename = os.path.join(self.plugin_src_dir, cfg[NAME] + EXTENSION)
                (success, section_cfg) = self.get_section_cfg(cfg[NAME], section=cfg)
                if success:
                    self.logger.info("%s: Section Configuration DONE.", cfg.get(NAME))
                    return_dict[SUCCESS_LIST].append((filename, section_cfg))
                else:
                    self.logger.debug("%s: Section Configuration FAILED.", cfg.get(NAME))
                    return_dict[FAILED_LIST].append((filename, ERROR_CFG_GEN))
                success_overall = success_overall or success

            self.logger.debug('Plugin Configuration return Status: %s', json.dumps(return_dict))
            self.logger.info("Success Overall: %s", str(success_overall))

            # generate config for targets
            for target, conf in self.targets.items():
                filename = os.path.join(self.plugin_src_dir, target + EXTENSION)
                (success, section_cfg) = self.get_section_cfg(target, section=conf)
                if success:
                    self.logger.info("%s: Target Configuration DONE.", target)
                    return_dict[SUCCESS_LIST].append((filename, section_cfg))
                else:
                    self.logger.info("%s: Target Configuration FAILED.", target)
                    return_dict[FAILED_LIST].append((filename, ERROR_CFG_GEN))
                success_overall = success_overall or success

            self.logger.debug('Target Configuration return Dict: %s', json.dumps(return_dict))
            self.logger.info("Success Overall: %s", str(success_overall))

            # generate config for filters
            filename = os.path.join(self.plugin_src_dir, FILTERS + EXTENSION)
            (success, cfg) = self.get_section_cfg(FILTERS, {TAGS: self.tags, TARGETS: self.targets})
            if success:
                self.logger.info("Filter Configuration DONE.")
                return_dict[SUCCESS_LIST].append((filename, cfg))
            else:
                self.logger.info("Filter Configuration FAILED.")
                return_dict[FAILED_LIST].append((filename, ERROR_CFG_GEN))
            success_overall = success_overall or success

            self.logger.debug('Filter Configuration return Status: %s', json.dumps(return_dict))
            self.logger.info("Success Overall: %s", str(success_overall))

            return success_overall, return_dict
        except KeyError, e:
            error_msg = error_msg + str(e) + KEY_ERROR
        except Exception as e:
            error_msg += str(e)

        return_dict[FAILED_LIST].append((DUMMY, DUMMY))
        self.logger.error('Exception: %s', error_msg)
        self.logger.error('Failed List:', json.dumps(return_dict[FAILED_LIST]))
        return success_overall, return_dict

    '''
    Input: Template in Orchestrator format.
    Functionality: Applies global level options on individual
                   plugins and sets list of plugins and targets.
    '''
    def create_cfg_list(self):
        self.logger.debug('Inside create_cfg_list.')
        error_msg = ""
        try:
            self.tag_list = self.tag_list + self.template.get(TAGS, [])
            self.tag_list = self.tag_list + self.sub_template.get(TAGS, [])
            self.interval = self.sub_template.get(INTERVAL, 30)
            self.target_list = self.sub_template.get(TARGETS, [])

            target_names_list = []
            for target in self.target_list:
                target_names_list.append(target.get(NAME, 'undefined'))

            plugin_cfg_list, plugin_disable_list = [], []
            expanded_template = CollectdPluginManager.get_expanded_profile(self.sub_template)
            for plugin in expanded_template.get(PLUGINS, []):
                plugin_temp = copy.deepcopy(plugin)
                if not plugin_temp.get(ENABLE):
                    plugin_disable_list.append(plugin_temp)
                    continue

                if TARGETS not in plugin_temp:
                    plugin_temp[TARGETS] = target_names_list
                elif not set(plugin_temp.get(TARGETS, [])) <= set(target_names_list):
                    error_msg = "Invalid target specified for plugin " + plugin_temp.get(NAME, 'undefined')
                    self.logger.error('Exception: %s', error_msg)
                    return False, error_msg

                plugin_temp[INTERVAL] = plugin_temp.get('INTERVAL', 0) or self.interval
                plugin_temp[TAGS] = self.tag_list + plugin_temp.get(TAGS, [])
                plugin_cfg_list.append(plugin_temp)

            self.cfg_list = plugin_cfg_list
            self.logger.debug("Disable Plugin List: %s", json.dumps(plugin_disable_list))
            self.logger.debug("Enable Plugin List: %s", json.dumps(self.cfg_list))
            return True, error_msg

        except Exception as e:
            error_msg = str(e)

        self.logger.error('Exception: %s', error_msg)
        return False, error_msg

    def generate_config(self):
        success = False
        return_dict = dict()
        return_dict[SUCCESS_LIST] = []
        return_dict[FAILED_LIST] = []
        try:
            self.logger.debug("Converting orchestrator input template to Collectd Format.")
            success, error_msg = self.create_cfg_list()
            if success:
                self.logger.info("Template conversion SUCCESSFUL.")
                (success, return_dict) = self.generate()
                self.logger.debug("Success: " + str(success) + " generate_config Return Status: " + str(return_dict))
            else:
                return_dict[ERROR] = error_msg
                self.logger.error("Template conversion FAILED.")
        except Exception as e:
            self.logger.error('Exception: %s', str(e))
        return success, return_dict

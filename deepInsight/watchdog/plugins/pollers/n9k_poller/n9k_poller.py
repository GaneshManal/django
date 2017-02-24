"""
n9kpoller
poll give interfaces for stats as per meta data given
"""
import json
import logging
import operator
import os
import re
import sys
import time
from pprint import pformat

sys.path.append("./poller/plugins/n9k_poller")

import n9k_rest_api


class N9k_Poller(object):
    """ N9k poller class"""

    def __init__(self):
        self.logger = None
        self.rest_api = n9k_rest_api.N9k_rest_api()
        try:
            with open(os.path.abspath("poller/plugins/n9k_poller/n9k_poller_config.json")) as fin:
            # with open("n9k_poller_config.json") as fin:
                self.stats = json.load(fin)["stats_interface"]
        except IOError as e:
            print "ERROR - Unable to open config file"
            sys.exit(0)

    def normal_iface_names(self, interfaces):
        """
        create ethernetN/N form passed interfaces
        :param interfaces:
        :return:
        """
        regx_eth = re.compile(r"(e|E)th.*?(\d+\/\d+)$", re.IGNORECASE)
        new_interfaces = dict()
        for interface, compute in interfaces.items():
            result = re.match(regx_eth, interface)
            new_interfaces["ethernet" + result.group(2)] = compute
        self.logger.debug("normalised interfaces: %s", new_interfaces)
        return new_interfaces

    def process_meta(self, meta):
        """
        :param meta:
        :return device_ip, device_credentials, interfaces, corr, disc_proto,
            port, secure_conn, log_level, log_dir:
        """
        device_ip = None
        device_credentials = None
        interfaces = dict()
        corr = "no"
        disc_proto = "cdp"
        port = 80
        secure_conn = "no"
        log_level = "NOTSET"
        log_dir = "."
        reset_counters = "no"

        if "port" in meta.keys():
            port = meta["port"]
        if "secure_connection" in meta.keys():
            secure_conn = meta["secure_connection"]
        if "host" in meta.keys():
            device_ip = meta["host"]
        if "credentials" in meta.keys():
            device_credentials = meta["credentials"]
        if "input_port_mapping" in meta.keys():
            for item in meta["input_port_mapping"]:
                interfaces[item["interface"]] = item["hostname"]
        if "correlation" in meta.keys():
            corr = meta["correlation"]
        if "discovery_protocol" in meta.keys():
            disc_proto = meta["discovery_protocol"]
        if "log_level" in meta.keys():
            log_level = str(meta["log_level"]).upper()
        if "log_dir" in meta.keys():
            log_dir = str(meta["log_dir"])
        if "reset_counters" in meta.keys():
            reset_counters = meta["reset_counters"]

        return device_ip, device_credentials, interfaces, corr, disc_proto, \
            port, secure_conn, log_level, log_dir, reset_counters

    def validate_meta_interfaces(self, meta_interfaces):
        """
        type_1 meta data : {interface, hostname}
        type_2 meta data : {interface, None}
        """
        type_1, type_2 = 0, 0
        for interface, host in meta_interfaces.items():
            if host in ("", None, "None", "none"):
                type_2 += 1
            else:
                type_1 += 1
        if type_1 and type_2:
            return False, "Invalid meta data"
        else:
            if type_1:
                return True, "Valid type_1 meta data"
            elif type_2:
                return True, "Valid type_2 meta data"

    def process_buffers_details(self, buff_details):
        """
        process buff_details to find module_peak, module_total, module_peak_util of buffer
        :param buff_details:
        :return:
        """
        sorted_buff_details = sorted(buff_details, key=operator.itemgetter('instance'))
        module_buffer = {"peak.buffer": list(), "total.buffer": list(), "peak.percent": list()}
        for item in sorted_buff_details:
            mod_instant_peak = int(item["max_cell_usage_drop_pg"])
            mod_instant_total = int(item["total_instant_usage_drop_pg"]) + int(item["rem_instant_usage_drop_pg"])
            mod_instant_peak_util = round((mod_instant_peak * 100.0) / mod_instant_total, 2)
            module_buffer["peak.buffer"].append(mod_instant_peak)
            module_buffer["total.buffer"].append(mod_instant_total)
            module_buffer["peak.percent"].append(mod_instant_peak_util)
        self.logger.info("module buffer: %s", module_buffer)
        return module_buffer

    def poll(self, meta, state):
        """
         poll function
        """
        device_ip, device_credentials, interfaces, corr, disc_proto, \
            port, secure_conn, log_level, log_dir, reset_counters = self.process_meta(meta)

        # setup logging
        log_file_path = '{}{}{}.log'.format(log_dir, os.path.sep, __name__)
        formatter = logging.Formatter('[%(filename)20s: %(lineno)5s '
                                      '- %(funcName)20s()] %(asctime)s '
                                      '[%(levelname)5s] %(message)s')

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.logger = logging.getLogger('')
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        self.logger.setLevel(eval("logging.{}".format(log_level)))
        fhandler = logging.FileHandler(log_file_path)
        fhandler.setFormatter(formatter)
        self.logger.addHandler(fhandler)
        # chandler = logging.StreamHandler()
        # chandler.setFormatter(formatter)
        # self.logger.addHandler(chandler)
        self.logger.info("-" * 100)

        self.logger.info("device_ip: %s", device_ip)
        self.logger.info("interfaces: %s", interfaces)
        self.logger.info("correlation: %s", corr)
        self.logger.info("discovery_protocol: %s", disc_proto)
        self.logger.info("port: %s", port)
        self.logger.info("secure_connection: %s", secure_conn)
        self.logger.info("log_level: %s", log_level)
        self.logger.info("log_dir: %s", log_dir)
        self.logger.info("reset_counters: %s", reset_counters)

        interfaces = self.normal_iface_names(interfaces)

        # return list of dicts
        output = list()

        if not len(interfaces):
            state["status_code"] = 404
            state["error"] = "Invalid"
            state["message"] = "No interfaces specified for polling"
            self.logger.error("\nOutput:\n %s", pformat(output))
            self.logger.error("\nState:\n %s", pformat(state))
            return output, state

        valid_interfaces, msg = self.validate_meta_interfaces(interfaces)
        if not valid_interfaces:
            state["status_code"] = 404
            state["error"] = "Invalid"
            state["message"] = msg
            self.logger.error("\nOutput:\n %s", pformat(output))
            self.logger.error("\nState:\n %s", pformat(state))
            return output, state

        # create a session for given device credentials
        session, device_url = self.rest_api.create_sesion(device_ip,
                                                          device_credentials,
                                                          port,
                                                          secure_conn)
        # test connection by getting switch name
        code, msg, switch_name = self.rest_api.show_device_hostname(session,
                                                                    device_url)
        if int(code) != 200:
            state["status_code"] = code
            state["error"] = msg
            state["message"] = switch_name
            self.rest_api.close_session(session)
            self.logger.error("\nOutput:\n %s", pformat(output))
            self.logger.error("\nState:\n %s", pformat(state))
            return output, state

        # get data for each interface
        for interface, compute in interfaces.items():
            self.logger.info("interface: %s, compute: %s",
                             interface, compute)
            temp_dict = dict()

            # utc timestamp
            temp_dict["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")

            # verify correlation
            if (corr.lower() == "yes") and (compute.lower() == "none"):
                host_name = None
                if disc_proto == "lldp":
                    lldp_neighbors = self.rest_api.show_lldp_neighbors(session,
                                                                       device_url,
                                                                       interface)[2]
                    if lldp_neighbors:
                        host_name = str(lldp_neighbors["sys_name"])
                else:
                    cdp_neighbors = self.rest_api.show_cdp_neighbors(session,
                                                                     device_url,
                                                                     interface)[2]
                    if cdp_neighbors:
                        host_name = str(cdp_neighbors["sys_name"])
                if not host_name:
                    self.logger.warn("couldn't find neighbor_host of %s ... "
                                     "DO NOT SEND STATS INFO.", interface)
                    continue
                else:
                    self.logger.info("found hostname for %s: %s",
                                     interface, host_name)
                    temp_dict["neighbor_host"] = host_name
                    temp_dict["n9k_poller_tag"] = host_name
                    temp_dict["interface"] = interface

            if (corr.lower() == "yes") and (compute.lower() != "none"):
                self.logger.info("correlation is yes tag and host "
                                 "names are from meta data")
                temp_dict["neighbor_host"] = compute
                temp_dict["n9k_poller_tag"] = compute
                temp_dict["interface"] = interface

            if (corr.lower() == "no") and (compute.lower() == "none"):
                host_name = None
                if disc_proto == "lldp":
                    lldp_neighbors = self.rest_api.show_lldp_neighbors(session,
                                                                       device_url,
                                                                       interface)[2]
                    if lldp_neighbors:
                        host_name = str(lldp_neighbors["sys_name"])
                else:
                    cdp_neighbors = self.rest_api.show_cdp_neighbors(session,
                                                                     device_url,
                                                                     interface)[2]
                    if cdp_neighbors:
                        host_name = str(cdp_neighbors["sys_name"])
                if not host_name:
                    self.logger.warn("couldn't find neighbor_host of %s ... "
                                     "DO NOT ADD neighbor_host INFO IN STATS INFO.",
                                     interface)
                    temp_dict["neighbor_host"] = None
                    temp_dict["n9k_poller_tag"] = None
                    temp_dict["interface"] = interface
                else:
                    self.logger.info("found hostname for %s: %s",
                                     interface, host_name)
                    temp_dict["neighbor_host"] = None
                    temp_dict["n9k_poller_tag"] = None
                    temp_dict["interface"] = interface

            if (corr.lower() == "no") and (compute.lower() != "none"):
                self.logger.info("correlation is no skip tag and host nmaes in output ")
                temp_dict["neighbor_host"] = None
                temp_dict["n9k_poller_tag"] = None
                temp_dict["interface"] = interface

            # get stats_interface info
            code, msg, interface_details = self.rest_api.show_interface(session,
                                                                        device_url,
                                                                        interface)
            # get interface stats
            if interface_details:
                for key, value in self.stats.items():
                    temp_dict[str(key)] = int(interface_details[value])
                if interface_details["state"]:
                    temp_dict["operational_status_code"] = str(interface_details["state"])
                if interface_details["admin_state"]:
                    temp_dict["admin_status_code"] = str(interface_details["admin_state"])

            # get vlan info
            code, msg, interface_switchport = \
                self.rest_api.show_interface_switchport(session,
                                                        device_url,
                                                        interface)
            if interface_switchport:
                if interface_switchport["switchport"].lower() != "disabled":
                    temp_dict["vlan_mode"] = str(interface_switchport["oper_mode"])
                    if interface_switchport["oper_mode"] == "access":
                        temp_dict["access_vlan"] = str(interface_switchport["access_vlan"])
                        temp_dict["trunk_vlans"] = None
                    if interface_switchport["oper_mode"] == "trunk":
                        temp_dict["trunk_vlans"] = str(interface_switchport["trunk_vlans"])
                        temp_dict["access_vlan"] = None
                else:
                    temp_dict["vlan_mode"] = None
                    temp_dict["trunk_vlans"] = None
                    temp_dict["access_vlan"] = None

            # get buffers
            code, msg, buffer_details = self.rest_api.get_buffer_details(session,
                                                                         device_url,
                                                                         interface)
            if buffer_details:
                buffer_info = self.process_buffers_details(buffer_details)
                temp_dict.update(buffer_info)
            else:
                temp_dict["peak.buffer"] = None
                temp_dict["total.buffer"] = None
                temp_dict["peak.percent"] = None

            output.append(temp_dict)

        # reset counters if yes
        if reset_counters.lower() == "yes":
            self.rest_api.clear_interface_counters(session, device_url)
            self.rest_api.clear_buffer_counters(session, device_url)
            self.logger.info("counters reset...")

        self.rest_api.close_session(session)

        state["status_code"] = 200
        state["error"] = None
        state["message"] = "stats query successful"

        self.logger.info("\nOutput:\n %s", pformat(output))
        self.logger.info("\nState:\n %s", pformat(state))

        return output, state


def poll(meta, state):
    poller = N9k_Poller()
    result, state = poller.poll(meta, state)
    return result, state


if __name__ == '__main__':
    pass

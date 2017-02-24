#!/usr/bin/env python
# ACI poller is a pluing to which plugs data from Cisco APIC
# and returns the data to the poller framework.

import datetime
import json
import logging
import os
from operator import attrgetter
from pprint import pprint
from acitoolkit import Node, Pod
import acitoolkit.acitoolkit as ACI
from acitoolkit.aciConcreteLib import (ConcreteLLdp, ConcreteLLdpIf,
                                       ConcreteLLdpAdjEp, ConcreteCdp,
                                       ConcreteCdpAdjEp, ConcreteCdpIf)

LOG  = None
HTTP_PORT = "80"
HTTPS_PORT = "443"

def aci_logging(meta):
    logger = logging.getLogger("aci_poller")

    logger.setLevel(eval('logging.' + meta['log_level']))

    log_dir = meta['log_dir']
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)

    logfile = log_dir + os.path.sep + 'aci_poller.log'

    formatter_str = "[*(asctime)s-*(filename)s:*(name)s:*(lineno)s-*(funcName)s()] -*(levelname)s: *(message)s"
    formatter_str = formatter_str.replace('*', '%')
    # Set Logging Handler
    if not len(logger.handlers):
        handler = logging.FileHandler(logfile)
        handler.setLevel(eval('logging.' + str(meta['log_level'])))
        formatter = logging.Formatter(formatter_str)
        handler.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(handler)
        logger.propagate = False

    return logger


def get_vlan_detail(session):
    """
    get_vlans queries all the nodes from APIC and collects VLAN
    details from each of the node.

    :param session:
    :return: List
     Example:      [ {
                      'id': '101',
                      'data': [
                               {
                                 'vlan': 13,
                                 'name': vlan13
                                 'status': 'active',
                                 'ports': ['eth1/41', 'eth1/41']
                               }]
                    }]
    """

    nodes = []
    node_ids = []
    # node_details return the data
    node_details = []

    # URL to get the registered nodes in APIC
    query_url = ('/api/node/class/fabricNode.json?query-'
                 'target-filter=eq(fabricNode.role,"leaf")')
    resp = session.get(query_url)
    if not resp.ok:
        raise Exception('Could not get switch list from APIC.')
    nodes = resp.json()['imdata']
    for node in nodes:
        node_ids.append(str(node['fabricNode']['attributes']['id']))

    pods = Pod.get(session)
    for pod in pods:
        for node_id in node_ids:
            query_url = ('/api/mo/topology/%s/node-%s.json?query-target=subtr'
                         'ee&target-subtree-class=l2BD,l2RsPathDomAtt' %
                         (pod.name, node_id))
            resp = session.get(query_url)
            if resp.ok:
                nodedata = {}
                l2bd_data = []
                port_data = {}
                for obj in resp.json()['imdata']:
                    if 'l2BD' in obj:
                        obj_attr = obj['l2BD']['attributes']
                        l2bd_data.append((int(obj_attr['id']),
                                          str(obj_attr['name']),
                                          str(obj_attr['adminSt']),
                                          str(obj_attr['fabEncap'])))
                    else:
                        dn = obj['l2RsPathDomAtt']['attributes']['dn']
                        port_id = str(
                            dn.rpartition('/path-[')[2].partition(']')[0])
                        port_bd_encap = str(
                            dn.partition('/bd-[')[2].partition(']')[0])
                        if port_bd_encap not in port_data:
                            port_data[port_bd_encap] = port_id
                        port_data[port_bd_encap] += ', ' + port_id
                output_data = []
                for (l2bd_id, l2bd_name,
                     l2bd_admin_state, l2bd_fab_encap) in l2bd_data:
                    try:
                        ports = port_data[str(l2bd_fab_encap)]
                    except KeyError:
                        ports = ''
                    output_data.append((l2bd_id, l2bd_name,
                                        l2bd_admin_state, ports))
                output_data.sort(key=lambda tup: tup[0])

                nodedata['id'] = node_id
                nodedata['data'] = []
                for data in output_data:
                    vlan_detail = {}
                    vlan_detail["ports"] = data[3].split(',')
                    vlan_detail["vlanid"] = data[0]
                    vlan_detail["status"] = data[2]
                    nodedata['data'].append(vlan_detail)

                node_details.append(nodedata)
        return node_details


def get_detailed_data(session, interface):
    """

    :param session:
    :param interface:
    :return:
    """

    (pod, node, module, port) = interface.split('/')
    interface = "%s/%s" % (module, port)

    url = ("/api/node/mo/topology/pod-%s/node-%s/sys/"
           "phys-[eth%s].json?query-target=children" % (pod.strip('eth '), node, interface))

    resp = session.get(url)
    for dn in resp.json()['imdata']:
        if dn.has_key('ethpmPhysIf'):
            return dn['ethpmPhysIf']['attributes']


def show_stats_short(session, meta, interfaces, granularity,
                     epoch, lldp_neigh=None, input_type=None,
                     correlation=None):

    """
    show stats short routine
    :param session: A session to login to APIC
    :param meta: metadata in JSON format
    :param interfaces: list of interfaces
    :param granularity: interval ex: 5min, 15min, 1h etc
    :param epoch: The <epoch> is an integer representing which set of
                  historical stats you want to reference
    :return: list of dictionaries with results
    """

    #resp = get_detailed_data(session, interfaces)
    #return

    counter_list = []
    for counter_family in meta["stats_family"]:
        for counter_name in counter_family["family_sub_type"]:
            temp_family = (counter_family["family_name"], counter_name)
            counter_list.append(temp_family)

    # List to hold the result dictionaries
    rec = []

    # mapping dictionary maps the input stats to the output stats
    mapping = {
        "ingrTotal.bytesCum": "rx.bytes",
        "ingrTotal.bytesRate": "rx.bytesRate",
        "ingrTotal.pktsCum": "rx.pkts",
        "ingrTotal.pktsRate": "rx.pktsRate",
        "egrTotal.bytesCum": "tx.bytes",
        "egrTotal.bytesRate": "tx.bytesRate",
        "egrTotal.pktsCum": "tx.pkts",
        "egrTotal.pktsRate": "tx.pktsRate",
        "ingrDropPkts.bufferCum": "rxDropPkts.buffer",
        "egrDropPkts.bufferCum": "txDropPkts.buffer"
    }

    # Loop over interfaces to add all the details of the individual interface
    for interface in sorted(interfaces, key=attrgetter('if_name')):
        interface.stats.get()

        result = {}
        for (counter_family, counter_name) in counter_list:
            key = str(counter_family+"."+counter_name)
            result[mapping[key]] = interface.stats.retrieve(
                counter_family,
                granularity,
                epoch, counter_name)

        result["interface_name"] = interface.name
        result["admin_status"] = interface.adminstatus
        result["operational_status"] = str(interface.attributes["operSt"])
        result["node"] = interface.node
        result["vlan_ids"] = []
        result['neighbour_host'] = "None"
        result["aci_poller_tag"] = "None"
        result['timestamp'] = str(datetime.datetime.utcnow())

        intf_deep = get_detailed_data(session, interface.name)
        result['vlan_mode'] = str(intf_deep['operMode'])


        if input_type == "type_2":
            for neigh in lldp_neigh:
                if neigh['local_intf'] == interface.name.strip("eth, "):
                    result['neighbour_host'] = str(neigh['sysname'])
                    result["aci_poller_tag"] = str(neigh['sysname'])
        elif input_type == "type_1" and correlation == "yes":
            for intf in meta["input_port_mapping"]:
                if intf["interface"] == interface.name.strip("eth, "):
                    result['neighbour_host'] = str(intf['hostname'])
                    result["aci_poller_tag"] = str(intf['hostname'])

        # Under interfaces loop
        rec.append(result)

    nodes = get_vlan_detail(session)
    for node in nodes:
        for interface in rec:
            if node['id'] == interface['node']:
                for nodedata in node['data']:
                    for port in nodedata['ports']:
                        if port.strip('eth') in interface['interface_name']:
                            interface['vlan_ids'].append(nodedata['vlanid'])

    # rec returns from method scope
    return rec


def is_valid(interface):
    """
    Check if an interface is a valid or not
    :param interface: String example : 1/101/1/2
    :return: boolean
    """
    if len(interface['interface'].split('/')) != 4:
        return False
    else:
        (pod, node, module, port) = interface['interface'].split('/')
        try:
            int(pod)
            int(node)
            if len(node) != 3:
                raise ValueError("Invalid Node")
            int(module)
            int(port)
        except ValueError:
            return False

        return True


def parse_discovery(lldps, proto="cdp"):
    """
    collect lldp details for each node
    :param title:
    :param lldps: list of LLDP instance
    :return list of dict: lldp information
    """
    data = []
    if proto == "lldp":
        if_class = ConcreteLLdpIf
        adj_class = ConcreteLLdpAdjEp
    elif proto == "cdp":
        if_class = ConcreteCdpIf
        adj_class = ConcreteCdpAdjEp

    for lldp in lldps:
        for lldp_if in lldp.get_children(if_class):
            for adj_ep in lldp_if.get_children(adj_class):
                pod = adj_ep.dn.split('/')[1].split('-')[1]
                node_id = adj_ep.dn.split('/')[2].split('-')[1]
                interface = adj_ep.dn.split('[')[1].split(']')[0].strip('eth')
                iface = "%s/%s/%s" % (pod, node_id, interface)
                entry = {
                    "sysname": adj_ep.name,
                    "local_intf": iface
                }
                data.append(entry)
    return data


def neighbours(session, proto="cdp"):
    """
    Get the node list and collect the LLDP instance list
    :param session: APIC session object
    :param proto: string, contains discovery protocol name
    :return: list of dict: containing lldp details
    """

    nodes = Node.get_deep(session, include_concrete=True)
    lldps = []
    LOG.info("Discovery protocol used: %s", proto)
    for node in nodes:
        if proto == "cdp":
            node_concrete = node.get_children(child_type=ConcreteCdp)
        elif proto == "lldp":
            node_concrete = node.get_children(child_type=ConcreteLLdp)
        else:
            LOG.info("No discovery data found!")
            return ["No discovery data found!"]

        for node_concrete_lldp_obj in node_concrete:
            lldps.append(node_concrete_lldp_obj)

    neig = parse_discovery(lldps, proto)
    LOG.debug("Neighbour Data: %s", str(neig))
    return neig


def filter_interface(lldp_neis, input_port_mapping, correlation,
                     input_type):
    """
    Desc: from the input interface list filter the
    interfaces with lldp interface if correlation="yes"
    :param lldp_neis: list of dictionary
    :param input_port_mapping: list of dictionary
    :param correlation: String (yes/no)
    :return: list of dictionary
    """
    if input_type == "type_2":
        final_int_list = []
        if correlation == "yes":
            for intf in [int_f for int_f in input_port_mapping]:
                if intf['interface'] in (
                        [entry['local_intf'] for entry in lldp_neis]):
                    final_int_list.append(intf)
            return final_int_list
        else:
            return input_port_mapping
    elif input_type == "type_1":
        return input_port_mapping


def is_validate_hosts(meta):
    """
    Return of any of the hostname is invalid
    :param meta:
    :return:
    """
    check_list = ["", "None", None]
    hostnames = [host['hostname'] for host in meta['input_port_mapping']]
    for host in hostnames:
        if host.strip(' ') in check_list:
            return False
    return True


def is_all_invalid_hosts(meta):
    """
    Return if all the hosts are in valid
    :param meta:
    :return:
    """
    invalid = True
    check_list = ["", "None", None]
    hostnames = [host['hostname'] for host in meta['input_port_mapping']]
    for host in hostnames:
        if host.strip(' ') in check_list:
            invalid = True
        else:
            invalid = False
            break
    return invalid


def is_valid_interfaces(meta):
    """
    :param meta:
    :return: boolean
    """
    interfaces = meta['input_port_mapping']
    for intf in interfaces:
        if not is_valid(intf):
            return False
    return True


def get_input_type(meta):
    """
    Validate the input based on the meta input and return the input ytpe
    :param meta: input dictionary
    :return: String : type of input
    """

    if not is_valid_interfaces(meta):
        return "invalid"
    elif is_valid_interfaces(meta) and is_validate_hosts(meta):
        return "type_1"
    elif is_valid_interfaces(meta) and is_all_invalid_hosts(meta):
        return "type_2"
    else:
        return "invalid"


def poll(meta, state):

    """
    Parse the JSON and get the information
    """
    global  LOG
    LOG = aci_logging(meta)
    LOG.info("ACI poller starting....")

    with open('./poller/plugins/aci_poller_profile.json') as profile:
        meta_stats = json.load(profile)
        meta["stats_family"] = meta_stats["stats_family"]

    if meta["port"] == HTTPS_PORT:
        url = "https://" + str(meta["host"])
    elif meta["port"] == HTTP_PORT:
        url = "http://" + str(meta["host"])

    username = meta["credentials"]["username"]
    password = meta["credentials"]["password"]

    input_port_mapping = meta['input_port_mapping']
    correlation = meta["correlation"]

    discovery_protocol = meta['discovery_protocol']

    granularity = "5min"
    epoch = 0

    LOG.info("APIC Credentials: username=> %s, URL=> %s" % (username, url))
    LOG.info("Granularity: %s, epoch: %s", str(granularity), str(epoch))

    # Login to APIC
    session = ACI.Session(url, username, password)
    resp = session.login()

    state = {}
    result = []

    input_type = get_input_type(meta)

    if input_type == "type_2":
        lldp_neigh = neighbours(session, discovery_protocol)
    elif input_type == "type_1":
        lldp_neigh = None
        LOG.info("Discovery is not required for the provided input")
    elif input_type == "invalid":
        LOG.error("Invalid input in meta file.")
        state['status'] = resp.status_code
        state['error'] = "Could not collect stats"
        state['message'] = "Invalid input in meta input file"
        LOG.debug("Result: %s, state: %s", str(result), str(state))
        return result, state

    final_intf_list = filter_interface(lldp_neigh, input_port_mapping,
                                       correlation, input_type)
    LOG.info("List of interfaces to fetch the stats for: %s", str(
        final_intf_list))

    state['status_code'] = None
    state['message'] = None
    state['error'] = None

    if not resp.ok:
        LOG.error("Login failed : %s", str(resp.text))
        state['status_code'] = resp.status_code
        state['error'] = "Login failed"
        state['message'] = "Could not login to APIC"
        LOG.debug("Result: %s, state: %s", str(result), str(state))
        return result, state

    if "all" in final_intf_list:
        # TODO (not implemented)
        # collect stats for all the interface
        pass
    else:
        for interface in [intf for intf in final_intf_list]:
            if is_valid(interface):
                LOG.info("Collecting stats for : %s interface", interface['interface'])
                if 'eth ' in interface:
                    interface = interface[4:]
                (pod, node, module, port) = interface['interface'].split('/')

                # convert to string, as get method expects pod,
                # node,module, port in string format

                pod = str(pod)
                node = str(node)
                module = str(module)
                port = str(port)
                iface = ACI.Interface.get(session, pod, node, module, port)
                res = show_stats_short(session, meta,
                                       iface, granularity,
                                       epoch, lldp_neigh,
                                       input_type, correlation)
                for i in res:
                    result.append(i)

                state['message'] = "Successfully pulled interface stats"
                state['error'] = "None"
                state['status_code'] = resp.status_code
                LOG.info("Collected stats for %s", interface['interface'])
                LOG.debug("State info for interface %s: %s", str(
                    interface['interface']), str(state))
            else:
                state['message'] = "Invalid interface name found : %s" % (
                    interface['interface'])

                state['error'] = "Invalid interface name"
                state['status_code'] = resp.status_code
                LOG.error("Result: %s, state: %s", str(result), str(state))
                LOG.error("Exiting aci_poller...!")
                break

    LOG.debug("Result: %s, state %s", str(result), str(state))
    return result, state

'''
def main():
    """

    Main function to invoke the pull() function
    :return:
    """
    with open('./poller/plugins/aci_poller_meta_input.json') as input_meta:
        input_meta = json.load(input_meta)
        state = {}
        result, state = poll(input_meta, state)
        pprint(result)
        pprint(state)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
'''





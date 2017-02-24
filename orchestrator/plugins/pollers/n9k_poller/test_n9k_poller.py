import os
import shutil
import unittest

from n9k_poller import N9k_Poller


class OutcomesTest(unittest.TestCase):
    ip_address = "10.23.248.88"
    credentials = {"username": "admin", "password": "cisco123"}
    host_name_1 = "n9k-poller-2.cisco.com"
    host_name_2 = "compute_1"
    interface_1 = "eth1/1"
    interface_2 = "eth1/7"
    meta_data = dict()
    poller = N9k_Poller()

    def test_Pass_ValidData(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "./log"
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertTrue(data)

    def test_Fail_Invalid_IP(self):
        self.meta_data["host"] = "111.111.0.110"
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1},
                                                {"hostname": "none", "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertFalse(data)

    def test_Fail_Invalid_Credentials(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = {"username": "admin", "password": "cisco"}
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1},
                                                {"hostname": "none", "interface": self.interface_2}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertFalse(data)

    def test_LLDP_neighbor(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertEqual(data[0]["neighbor_host"], self.meta_data["input_port_mapping"][0]["hostname"])

    def test_CDP_neighbor(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "cdp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertEqual(data[0]["neighbor_host"], self.meta_data["input_port_mapping"][0]["hostname"])

    def test_Invalid_metadata(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "cdp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": "compute_3", "interface": "ethernet1/2"},
                                                {"hostname": "none", "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertFalse(data)

    def test_Correlation_yes_neighbor_host(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertEqual(data[0]["neighbor_host"], self.meta_data["input_port_mapping"][0]["hostname"])

    def test_Correlation_yes_n9k_poller_tag(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "cdp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertEqual(data[0]["n9k_poller_tag"], self.meta_data["input_port_mapping"][0]["hostname"])

    def test_Correlation_no_neighbor_host(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "cdp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "no"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertEqual(data[0]["neighbor_host"], None)

    def test_Correlation_no_n9k_poller_tag(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "no"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertEqual(data[0]["n9k_poller_tag"], None)

    def test_HTTPS_Connection(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "443"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "yes"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertTrue(data)

    def test_Correlation_yes_No_neighbor_LLDP(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "80"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": "none", "interface": "eth1/7"}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertFalse(data)

    def test_Correlation_yes_No_neighbor_CDP(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "cdp"
        self.meta_data["secure_connection"] = "80"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": "none", "interface": "eth1/7"}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertFalse(data)

    def test_Correlation_no_No_neighbor_LLDP(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "80"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "no"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": "none", "interface": "eth1/7"}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertEqual(data[0]["n9k_poller_tag"], None)

    def test_Correlation_no_No_neighbor_CDP(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "cdp"
        self.meta_data["secure_connection"] = "80"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "no"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": "none", "interface": "eth1/7"}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertEqual(data[0]["n9k_poller_tag"], None)

    def test_log_dir(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "cdp"
        self.meta_data["secure_connection"] = "80"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "no"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "./log/test"
        self.meta_data["input_port_mapping"] = [{"hostname": "none", "interface": "eth1/7"}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertTrue(os.path.exists(self.meta_data["log_dir"]))

    def test_Output_length(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "cdp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "no"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": "compute_3", "interface": "ethernet1/2"},
                                                {"hostname": "compute_1", "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertEqual(len(data), 2)

    def test_Output_length_Correlation_yes_No_neighbor(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": "none", "interface": "ethernet1/2"},
                                                {"hostname": "none", "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertEqual(len(data), 0)

    def test_Counters_reset(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "no"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": "none", "interface": "ethernet1/2"},
                                                {"hostname": "none", "interface": self.interface_1}]
        self.meta_data["reset_counters"] = "yes"
        data, state = self.poller.poll(self.meta_data, {})

    def test_No_Interfaces(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "yes"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = []
        self.meta_data["reset_counters"] = "no"
        data, state = self.poller.poll(self.meta_data, {})
        self.assertFalse(data)

    def test_Hardware_buffers(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "no"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = [{"hostname": self.host_name_1, "interface": self.interface_1}]
        data, state = self.poller.poll(self.meta_data, {})
        self.assertTrue(data)

    def test_No_interfaces_in_meta(self):
        self.meta_data["host"] = self.ip_address
        self.meta_data["port"] = "80"
        self.meta_data["discovery_protocol"] = "lldp"
        self.meta_data["secure_connection"] = "no"
        self.meta_data["credentials"] = self.credentials
        self.meta_data["correlation"] = "no"
        self.meta_data["log_level"] = "DEBUG"
        self.meta_data["log_dir"] = "."
        self.meta_data["input_port_mapping"] = []
        data, state = self.poller.poll(self.meta_data, {})
        self.assertFalse(data)

if __name__ == '__main__':
    if os.path.exists("./log"):
        shutil.rmtree("./log", ignore_errors=True)
    unittest.main()

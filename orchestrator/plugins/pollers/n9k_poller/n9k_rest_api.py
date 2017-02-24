"""
n9k rest api
REST calls to retrive details from N9K Switch
"""
import json
import re

import requests
from requests.auth import HTTPBasicAuth


# requests.packages.urllib3.disable_warnings()


class N9k_rest_api(object):
    """ initialize """

    def __init__(self):
        self.payload = {
            "ins_api": {
                "version": "1.0",
                "type": "cli_show",
                "chunk": "0",
                "sid": "1",
                "output_format": "json"
            }
        }

    def create_sesion(self, ip_address, credential,
                      port=80, secure="no", verify=False):
        """
        :param ip_address:
        :param credential:
        :param port:
        :param secure:
        :param verify:
        :return session object:
        """
        if secure is "yes":
            api_url = 'https://{}:{}/ins'.format(ip_address, port)
        else:
            api_url = 'http://{}:{}/ins'.format(ip_address, port)
        user = credential["username"]
        password = credential["password"]
        sess = requests.session()
        sess.headers['Content-Type'] = 'application/json'
        sess.auth = HTTPBasicAuth(user, password)
        sess.verify = verify
        return sess, api_url

    def close_session(self, session):
        """
        :param session:
        :return:
        """
        session.close()

    def session_post(self, session, url):
        """
        :param session:
        :param url:
        :return status_code, message, body:
        """
        try:
            resp = session.post(url, data=json.dumps(self.payload), timeout=None)
            result = resp.json()
            if result["ins_api"]["outputs"]["output"]["code"] != "200":
                status_code = result["ins_api"]["outputs"]["output"]["code"]
                message = result["ins_api"]["outputs"]["output"]["msg"]
                body = ""
            else:
                status_code = result["ins_api"]["outputs"]["output"]["code"]
                message = result["ins_api"]["outputs"]["output"]["msg"]
                body = result["ins_api"]["outputs"]["output"]["body"]
        except requests.exceptions.RequestException as ex:
            status_code = 404
            message = "Exception"
            body = str(ex)
        return status_code, message, body

    def show_device_hostname(self, session, url):
        """
        get device hostnmae
        :param session:
        :param url:
        :return status_code, message, body:
        """
        self.payload["type"] = "cli_show"
        self.payload["input"] = "show hostname"
        status_code, message, body = self.session_post(session, url)
        return status_code, message, body

    def show_interface(self, session, url, interface):
        """
        get interface related details including counters
        :param session:
        :param url:
        :param interface:
        :return status_code, message, body:
        """
        self.payload["type"] = "cli_show"
        self.payload["input"] = "show interface {}".format(interface)
        status_code, message, body = self.session_post(session, url)
        if int(status_code) != 200:
            status_code = status_code
            message = message
            body = body
        else:
            status_code = status_code
            message = message
            body = body["TABLE_interface"]["ROW_interface"]
        return status_code, message, body

    def show_interface_switchport(self, session, url, interface):
        """
        get vlan related info
        :param session:
        :param url:
        :param interface:
        :return status_code, message, body:
        """
        self.payload["type"] = "cli_show"
        self.payload["input"] = "show interface {} switchport".format(interface)
        status_code, message, body = self.session_post(session, url)
        if int(status_code) != 200:
            status_code = status_code
            message = message
            body = body
        else:
            status_code = status_code
            message = message
            body = body["TABLE_interface"]["ROW_interface"]
        return status_code, message, body

    def show_lldp_neighbors(self, session, url, interface):
        """
        get neighbor using lldp
        :param session:
        :param url:
        :param interface:
        :return status_code, message, body:
        """
        self.payload["type"] = "cli_show"
        self.payload["input"] = "show lldp neighbors interface {} detail".format(interface)
        status_code, message, body = self.session_post(session, url)
        if int(status_code) != 200:
            status_code = status_code
            message = message
            body = body
        else:
            status_code = status_code
            message = message
            body = body["TABLE_nbor_detail"]["ROW_nbor_detail"]
        return status_code, message, body

    def show_cdp_neighbors(self, session, url, interface):
        """
        get neighbor using cdp
        :param session:
        :param url:
        :param interface:
        :return status_code, message, body:
        """
        self.payload["type"] = "cli_show"
        self.payload["input"] = "show lldp neighbors interface {} detail".format(interface)
        status_code, message, body = self.session_post(session, url)
        if int(status_code) != 200:
            status_code = status_code
            message = message
            body = body
        else:
            status_code = status_code
            message = message
            body = body["TABLE_nbor_detail"]["ROW_nbor_detail"]
        return status_code, message, body

    def clear_interface_counters(self, session, url, interface=None):
        """
        clear interface counters if interface name is passed
        else clear counters of all interfaces
        :param session:
        :param url:
        :param interface:
        :return:
        """
        self.payload["type"] = "cli_show_ascii"
        if not interface:
            self.payload["input"] = "clear counter interface all"
        else:
            self.payload["input"] = "clear counter interface {}".format(interface)
        status_code, message, body = self.session_post(session, url)
        if int(status_code) != 200:
            status_code = status_code
            message = message
            body = body
        else:
            status_code = status_code
            message = message
            body = "clear counters successful"
        return status_code, message, body

    def clear_buffer_counters(self, session, url):
        """
        clear buffer counters of the switch
        :param session:
        :param url:
        :return:
        """
        self.payload["type"] = "cli_show_ascii"
        self.payload["input"] = "clear counter buffers"
        status_code, message, body = self.session_post(session, url)
        if int(status_code) != 200:
            status_code = status_code
            message = message
            body = body
        else:
            status_code = status_code
            message = message
            body = "clear counters buffer successful"
        return status_code, message, body

    def get_buffer_details(self, session, url, interface):
        regx_eth = re.compile(r"(e|E)th.*?(\d+)\/\d+", re.IGNORECASE)
        module = re.match(regx_eth, interface).group(2)
        self.payload["type"] = "cli_show"
        self.payload["input"] = "show hardware internal buffer info pkt-stats module {}".format(module)
        status_code, message, body = self.session_post(session, url)
        if int(status_code) != 200 or not body:
            status_code = status_code
            message = message
            body = body
        else:
            status_code = status_code
            message = message
            body = body["TABLE_module"]["ROW_module"]["TABLE_instance"]["ROW_instance"]
        return status_code, message, body


def main():
    """
    main function
    :return:
    """
    api = N9k_rest_api()

    ip_address = '10.11.0.78'
    credentials = {"username": "admin", "password": "cisco123"}

    sess, url = api.create_sesion(ip_address, credentials)

    print api.show_device_hostname(sess, url)

    print api.show_interface(sess, url, interface="ethernet1/1")

    print api.show_interface_switchport(sess, url, interface="eth1/5")

    print api.show_lldp_neighbors(sess, url, interface="eth1/1")

    print api.show_cdp_neighbors(sess, url, interface="eth1/1")

    print api.clear_interface_counters(sess, url, interface="eth1/1")
    print api.clear_buffer_counters(sess, url)

    print api.get_buffer_details(sess, url, interface="eth1/1")

    api.close_session(sess)


if __name__ == '__main__':
    main()

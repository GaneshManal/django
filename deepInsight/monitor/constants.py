import os
from django.conf import settings

# GAM: Verified
LS = "linux_static"
LD = "linux_dynamic"
ENABLE = "enable"

# operations
START = "start"
STOP = "stop"
RESTART = "restart"
STATUS = "status"
PUSH = "push"

NAME = "name"
PLUGINS = "plugins"

ERROR = "error_msg"
NODES = "nodelist"
TAGS = "tags"
RUN_COUNT = "run_count"
TARGETS = "targets"
CONFIG = "config"
EXTENSION = ".conf"
INTERVAL = "interval"
COLLECTORS = "collectors"

# ################### POLLER Settings ##########################
ORCH_COMM = "orch_comm"
PCON_COMM = "pcon_comm"
KAFKA_CLIENT = "localhost:9092"
CONTROLLER = os.path.join(settings.BASE_DIR, 'deepInsight', 'monitor', 'pcontroller.py')

# ################### POLLER Constants ##########################
POLLERM = "pollerm"
PCONTROLLER = "pcontroller"
POLLER = "poller"

META = "meta"
PLUGIN_TYPE = "plugin_type"
PLUGIN_ID = "plugin_id"
TARGET = "plugin_file"

READER = "reader"
WRITER = "writer"
DEST_LIST = "dest_list"

CMD = "cmd"
MSG_TYPE = "msg_type"
CNTRL = "cntrl"
CNTRL_DATA_MAP = "cntrl_data_map"

STATE = "state"
STOPALL = "stopall"
RECOVER = "recover"
CHECK_POINT = "check_point"
TEARDOWN = "teardown"
TEARDOWNALL = "teardownall"
UPDATE = "update"
RESUME = "resume"

POLL_WAIT_TIME_MS = 15000
STATE_FILE_PATH = os.path.join(settings.BASE_DIR, 'deepInsight', 'monitor', 'plugins', 'state.db')
DEFAULT_INTERVAL = 10
JOB_POLLER = 'mapr_job'

# GAM: Not Verified
RUNNING = "Running"
NOT_RUNNING = "Not Running"
NO_RESPONSE = "No Response"

OFF = "Off"
ON = "On"
NA = "Unknown"
INIT = "init"
NODE_LIST = "node_list"
SUB_TEMPLATE = "sub_template"

# Collectd Constants
CollectdPluginDestDir = '/opt/collectd'
CollectdPluginConfDir = '/opt/collectd'
CollectdConfDir = '/opt/collectd/etc'

# Collector - constants.py
COLLECTOR_MGR = "collector_manager"
COLLECTD_MGR = "collectd_manager"
NODEMGR = "nodemgr_type"
FAILED_LIST = "failed_list"
SUCCESS_LIST = "success_list"
SALT = "salt"
COLLECTD = "collectd"
COLLECTOR = "collector_type"
ALL_CONF = "/*.conf"
DELETE = "delete"
FILTERS = "filters"
PLUGIN = "Plugin"
OPTIONS = "Options"

ELASTICSEARCH = "elasticsearch"
HOST = "host"
PORT = "port"
INDEX = "index"
SERVER_IP = "server_ip"
SERVER_PORT = "server_port"
CLIENT_IP = "client_ip"
CLIENT_PORT = "client_port"
TYPE = "type"
NODE_NAME = "node_name"
NO_RESP = "No Response"
RUNNING = "Running"
NOT_RUNNING = "Not Running"

# ERROR MESSAGES
EMPTY_NODE_LIST = "No nodes specified"
KEY_ERROR = " key not found"
ERROR_CFG_GEN = "Configuration generation failed"
DUMMY = "dummy"

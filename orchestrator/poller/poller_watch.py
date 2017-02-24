import json
import multiprocessing

from kafka import KafkaConsumer, KafkaProducer
from orchestrator.watchdog.constants import *

from orchestrator.base_monitor import *
from orchestrator.util import get_logger


class PollerManager(BaseMonitor):
    NAME = "pollers"
    logger = get_logger(POLLERM)

    controller_id = -1
    template = {}
    orch_comm = ""
    pcon_comm = ""
    orch_comm_producer = None
    pcon_comm_consumer = None
    kafka_client = ""
    pcon_consumer_p = None
    check_point = {}
    plugin_id_map = {}
    plugin_count_map = {}

    def __init__(self, tid):
        super(PollerManager, self).__init__(PollerManager.NAME, tid)
        self.pcontroller = CONTROLLER
        self.controller_id = 0
        self.orch_comm = ORCH_COMM
        self.pcon_comm = PCON_COMM
        self.kafka_client = KAFKA_CLIENT
        self.pcon_consumer_p = None

    @staticmethod
    def get_name(self):
        return self.__NAME__

    def _start_producer(self):
        try:
            producer = KafkaProducer(bootstrap_servers=self.kafka_client, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
            return producer
        except Exception as e:
            self.logger.error('Exception Caused: %s.', str(e))
            self.logger.error("Error: Failed to start kafka producer form client- %s", self.kafka_client)
            exit(0)

    def _start_consumer(self):
        try:
            consumer = KafkaConsumer(self.pcon_comm, bootstrap_servers=[self.kafka_client], value_deserializer=lambda m: json.loads(m.decode("ascii")))
            return consumer
        except Exception as e:
            self.logger.error('Exception Caused: %s.', str(e))
            self.logger.error("Error: Failed to start Consumer: %s, topic: %s" % (self.kafka_client, self.pcon_comm))
            exit(0)

    def deploy(self, oper):
        # Start Producer
        self.orch_comm_producer = self._start_producer()

        # Start Consumer
        self.pcon_consumer_p = multiprocessing.Process(target=pcon_consumer, args=())
        self.pcon_consumer_p.start()

        controller = 'python' + ' ' + self.pcontroller + ' ' + self.kafka_client + ' ' + self.orch_comm + ' ' + self.pcon_comm + ' &'
        self.logger.debug("Controller: %s", controller)
        os.system(controller)

        # GAMGAM: Dirty list ( Yet to Handle )
        dirty_list = []
        # self.logger.debug("Template: %s", json.dumps(self.template))
        self.logger.debug("Sub-Template: %s", json.dumps(self.sub_template))
        self.logger.debug("dirty_list- %s", dirty_list)

        for item in self.sub_template.get(TARGETS, []):
            if (not dirty_list) or (item[NAME] in dirty_list):
                msg = self.build_msg(item, inst_type=TARGET, op=oper)
                self.logger.debug('Target# Message send to controller: %s', msg)
                self.send_to_controller(msg)

        for item in self.sub_template.get(PLUGINS, []):
            if (not dirty_list) or (item[NAME] in dirty_list):
                msg = self.build_msg(item, inst_type=PLUGIN, op=oper)
                self.logger.debug('Plugin# Message send to controller: %s', msg)
                self.send_to_controller(msg)

    def teardownall(self, template):
        # teardown poller
        self.logger.debug("Received teardown-all.")

        msg = self.build_msg(msg, op=TEARDOWNALL)
        self.logger.debug('Teardown# Message built: %s', msg)
        if msg:
            resp = self.send_to_controller(msg)
            if resp:
                if self.pcon_consumer_p:
                    if self.pcon_consumer_p.is_alive():
                        self.pcon_consumer_p.terminate()
                        self.logger.debug("Terminated- pcon_consumer_p")
        else:
            self.logger.debug("Failed to teardown.")

    def teardown_inst(self):
        pass

    def check_state(self, plugin_id=""):
        # get state of poller or plugin
        self.logger.debug("check state- 1# plugin_id- %s", plugin_id)

        state = {}
        with open(STATE_FILE_PATH, 'r') as fh:
            state = json.load(fh)

        self.logger.debug("State File: %s", json.dumps(state))
        if plugin_id:
            if CHECK_POINT in state.keys():
                if STATE in state[CHECK_POINT].keys():
                    if plugin_id in state[CHECK_POINT][STATE].keys():
                        ret = state[CHECK_POINT][STATE][plugin_id]
                        self.logger.debug("Returning: %s", ret)
                        return ret
        else:
            if CHECK_POINT in state.keys():
                if STATE in state[CHECK_POINT].keys():
                    ret = state.get(CHECK_POINT, {}).get(STATE, {})
                    self.logger.debug("Returning: %s", ret)
                    return ret
        return state

    def check_status(self, plugin_id=""):
        # get status of poller or plugin
        self.logger.debug("check state- 1# plugin_id- %s", plugin_id)

        status = {}
        pcon_comm_consumer_t = self.pcon_comm_consumer
        if not pcon_comm_consumer_t:
            self.logger.debug("check status: self.pcon_comm_consumer is None")
            pcon_comm_consumer_t = self._start_consumer()
            self.logger.debug("check status: Initialized a temp consumer- pcon_comm_consumer_t")

        partitions = pcon_comm_consumer_t.poll(POLL_WAIT_TIME_MS)
        if len(partitions) > 0:
            for p in partitions:
                if not plugin_id:
                    try:
                        status = json.loads(partitions[p][-1].value)[CHECK_POINT][STATUS]
                        self.logger.debug("check status- 2: Status: %s", status)
                        return status
                    except Exception as e:
                        self.logger.exception("Exception: %s", str(e))
                else:
                    try:
                        status = json.loads(partitions[p][-1].value)[CHECK_POINT][STATUS][plugin_id]
                        log.debug("check status- 3: Status: %s", status)
                        return status
                    except Exception as e:
                        self.logger.exception("Exception: %s", str(e))
        self.logger.debug("check status- 4: Status: %s", status)
        return status

    def build_msg(self, msg, inst_type=PLUGIN, op=START):
        self.logger.debug("build msg: msg- %s, inst_type- %s, op- %s" % (msg, inst_type, op))
        if op in [TEARDOWNALL, STOPALL]:
            msg = {MSG_TYPE: op}

        if op in [START, UPDATE, STOP, RESUME, TEARDOWN]:
            msg.update({MSG_TYPE: CNTRL})
            msg.update({CMD: op})
            msg.update({PLUGIN_ID: msg[NAME]})

            if inst_type == PLUGIN:
                msg.update({PLUGIN_TYPE: READER})
                msg.update({DEST_LIST: msg[TARGETS]})

            if inst_type == TARGET:
                msg.update({PLUGIN_TYPE: WRITER})

        msg["id"] = self.controller_id
        self.logger.debug("Message Built: %s", msg)
        return msg

    def send_to_controller(self, msg):
        orch_comm_producer_t = self.orch_comm_producer
        if not orch_comm_producer_t:
            self.logger.debug("self.orch_comm_producer is None")
            orch_comm_producer_t = self._start_producer()
            self.logger.debug("Initialized a temp producer- orch_comm_producer_t")
        try:
            orch_comm_producer_t.send(self.orch_comm, json.dumps(msg))
            orch_comm_producer_t.flush()
            return True
        except Exception as e:
            self.logger.error("Failed to send to- orch_comm")
            self.logger.error("Exception: %s", str(e))
            return False

    def generate_id(self, plugin_name):
        return plugin_name + "_" + str(self.plugin_count_map[plugin_name]) + "_" + str(self.controller_id)


def pcon_consumer():
    # self.logger.debug("pcon_consumer: Starting Process.")
    try:
        with open(STATE_FILE_PATH, 'w') as fh:
            fh.write("{}")
    except IOError as e:
        # log.debug("pcon_consumer: Failed to start pcon_consumer- exiting")
        print 'Exception: %s', str(e)

    consumer = []
    try:
        consumer = KafkaConsumer(PCON_COMM, bootstrap_servers=[KAFKA_CLIENT],
                                 value_deserializer=lambda m: json.loads(m.decode("ascii")))
    except Exception as e:
        # log.debug("Error: Failed to start- %s, topic- %s" % (settings.KAFKA_CLIENT, settings.PCON_COM))
        print 'Exception: %s', str(e)
    for item in consumer:
        with open(STATE_FILE_PATH, 'w') as fh:
            fh.write(item.value)


# Poller utility functions for qcomm & putils.
def dispatch_data(data, dest_q_list, tag=""):
    if tag:
        data[TAGS] = tag
    for dest_q in dest_q_list:
        put_msg(dest_q, data)


def extract_tag_interval_count(msg):
    tag = {}
    interval = DEFAULT_INTERVAL
    count = -1

    if TAGS in msg.keys():
        tag = msg[TAGS]
    if INTERVAL in msg.keys():
        interval = msg[INTERVAL]
    if RUN_COUNT in msg.keys():
        count = msg[RUN_COUNT]
    return tag, interval, count


def get_msgs(mgmt_q):
    data = {}
    if not is_queue_empty(mgmt_q):
        data = get_msg(mgmt_q)
    return data


def get_queue():
    return multiprocessing.Queue()


def is_queue_empty(queue):
    return queue.empty()


def get_msg(queue):
    return queue.get()


def put_msg(queue, data):
    return queue.put(data)


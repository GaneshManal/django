from base_monitor import *
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import multiprocessing
from django.conf import settings
from deepInsight.util import get_logger, get_job_time_out
from deepInsight.watchdog.constants import *
import multiprocessing
import thread

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

    mapr_user = 'mapr'
    mapr_home = os.path.sep + os.path.join('home', 'mapr')

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

    def deploy(self, oper, dirty_list=[]):
        # Start Producer
        self.orch_comm_producer = self._start_producer()

        # Start Consumer
        self.pcon_consumer_p = multiprocessing.Process(target=pcon_consumer, args=())
        self.pcon_consumer_p.start()

        controller = 'python' + ' ' + self.pcontroller + ' ' + self.kafka_client + ' ' + self.orch_comm + ' ' + self.pcon_comm + ' &'
        self.logger.debug("Controller: %s", controller)
        os.system(controller)

        # GAMGAM: Dirty list ( Yet to Handle )
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


    def start_yarn_application(self, job_meta):
        job_timeout = get_job_time_out()
        self.logger.info('Job Timeout in seconds: %s', str(job_timeout))
        self.logger.debug("Job Details: %s", json.dumps(job_meta.get('job_details')))

        resource_manager = str()
        if self.template.get('node_list'):
            resource_manager = self.template.get('node_list')[0]
        self.logger.debug("Resource Manager: %s", resource_manager or "None")

        job_tag = job_meta.get('tag', None)
        job_details = job_meta.get("job_details", {})
        job_type = job_details.get('job_type', None)
        job_name = job_type + '_' + os.urandom(4).encode('hex')
        self.logger.info("job_type: %s, job_name: %s" % (job_type, job_name))

        application_id = None
        try:
            job_cmd = str()
            if job_type:
                job_name_option, job_tag_option = str(), str()

                job_name_option = '-Dmapreduce.job.name={}'.format(job_name)
                if job_tag:
                    job_tag_option = '-Dmapreduce.job.tags={}'.format(job_tag)

                # TestDFSIO Read & Write
                jar_name = '/opt/mapr/hadoop/hadoop-0.20.2/hadoop-0.20.2-dev-test.jar'

                if job_type.lower() in ['dfsioread', 'dfsiowrite']:

                    file_count = job_details.get('nrfiles', 0)
                    file_size = job_details.get('filesize', 0)
                    test_name = 'TestDFSIO'

                    if job_tag:
                        if job_type.lower() == "dfsioread":
                            job_cmd = 'yarn jar {} {} {} {} -read -nrFiles {} -fileSize {}'.format(
                            jar_name, test_name,job_name_option, job_tag_option, file_count, file_size)
                        else:
                            job_cmd = 'yarn jar {} {} {} {} -write -nrFiles {} -fileSize {}'.format(
                                jar_name, test_name, job_name_option, job_tag_option, file_count, file_size)

                    else:
                        if job_type.lower() == "dfsioread":
                            job_cmd = 'yarn jar {} {} {} -read -nrFiles {} -fileSize {}'.format(
                            jar_name, test_name, job_name_option, file_count, file_size)
                        else:
                            job_cmd = 'yarn jar {} {} {} -write -nrFiles {} -fileSize {}'.format(
                                jar_name, test_name, job_name_option, file_count, file_size)

                        self.logger.debug("job_cmd: %s", job_cmd)

                # Teragen
                if job_type.lower() == 'teragen':

                    file_size = job_details.get('filesize', 0)
                    jar_name = '/opt/mapr/hadoop/hadoop-0.20.2/hadoop-0.20.2-dev-examples.jar'
                    test_name = 'teragen'
                    if job_tag:
                        job_cmd = 'hadoop fs -rm -r -f -skipTrash /terasort-input; yarn jar {} {} {} {} {} /terasort-input'.format(
                            jar_name, test_name, job_name_option, job_tag_option, file_size)
                    else:
                        job_cmd = 'hadoop fs -rm -r -f -skipTrash /terasort-input; yarn jar {} {} {} {}'.format(
                            jar_name, test_name, job_name_option, file_size)

                # Terasort
                if job_type.lower() == 'terasort':
                    # file_size = job_details.get('file_size')
                    jar_name = '/opt/mapr/hadoop/hadoop-0.20.2/hadoop-0.20.2-dev-examples.jar'
                    test_name = 'terasort'
                    if job_tag:
                        job_cmd = 'hadoop fs -rm -r -f -skipTrash /terasort-output; yarn jar {} {} {} {} /terasort-input /terasort-output'.format(
                            jar_name, test_name, job_name_option, job_tag_option)
                    else:
                        job_cmd = 'hadoop fs -rm -r -f -skipTrash /terasort-output; yarn jar {} {} {} /terasort-input /terasort-output'.format(
                            jar_name, test_name, job_name_option)

                log_file = self.mapr_home + os.path.sep + job_name + '.log'
                yarn_command = 'su -l {} -c "{} &> {}"'.format(self.mapr_user, job_cmd, log_file)
                print "#"*50
                print "YARN Command : ", yarn_command
                print "#" * 50

                if yarn_command:
                    salt_client = salt.client.LocalClient()
                    job_exec_status = salt_client.cmd(resource_manager, fun="cmd.run_bg", arg=[yarn_command])
                    print 'Job exec status :' + json.dumps(job_exec_status)
                    self.logger.debug("Job Execution Status: %s", json.dumps(job_exec_status))

                    counter, result = job_timeout, {}
                    while True:
                        counter -= 1
                        job_started_expr = ' Submitted application application_'
                        result = salt_client.cmd(resource_manager, fun="file.grep",arg=[log_file, job_started_expr])
                        if (not result.get(resource_manager).get('retcode')) or (not counter):
                            break
                        time.sleep(1)
                    print "\nYARN Result: ", result
                    if result:
                        application_id = result.get(resource_manager).get('stdout').split()[-1]
                        self.logger.info("YARN application Started Successfully")
                        return application_id
                    else:
                        raise Exception('Unable to execute yarn application')
                else:
                    raise Exception('Unable to execute yarn application')

        except Exception as e:
            self.logger.exception("Exception while starting job %s, Exception: %s" % (job_name, str(e)))
            raise e

    def monitor_poller_job(self, app_id, test_var):
        self.logger.info("Starting hadoop application {} polling for template {}.".format(app_id, self.tid))
        job_status_flag = 0
        temp, job_plugin_status  = None, None

        while True:
            time.sleep(5)
            temp = self.check_state(plugin_id=JOB_POLLER)

            print "="*50
            print "APP {} STATUS:{} ".format(app_id, temp)
            print "=" * 50

            if temp != {}:
                job_plugin_status = temp
            else:
                print "******LAST STATUS :", str(job_plugin_status)
                continue

            for app in job_plugin_status.get('applications_status'):
                if app['application_id'] == app_id:
                    if app['status'] in ['FINISHED', 'KILLED']:
                        job_meta = self.sub_template.get(PLUGINS).get(JOB_POLLER)
                        if app_id in job_meta.get('application_ids'):
                            job_meta.get('application_ids').remove(app_id)
                            self.sub_template[PLUGINS][JOB_POLLER][META] = job_meta
                            print "APP {} REMOVE".format(app_id)

                            # GAMGAM
                            self.logger.info("Got to set profile to idle here.")
                            # profile_manager.set_idle_profile(template_id)

                            self.logger.info("For template %s, Stopped hadoop application [{}] Polling" % (self.tid, app_id))
                            return
        return None
        # Code ends here.

    def start_job(self, job_meta):
        # Update poller Meta data
        app_ids = []

        self.logger.debug("Template before starting Job: %s" % (self.sub_template))
        self.logger.debug("Updating Poller Job plugin meta data.")

        dirty_list, app_ids = [], []
        print 'subtemplate: ', json.dumps(self.sub_template)
        for item in self.sub_template[PLUGINS]:
            if item[NAME] == JOB_POLLER:
                dirty_list.append(JOB_POLLER)
                # item[META] = job_meta
                app_ids = item.get(META).get("application_names", [])
                self.deploy(START, dirty_list)
                break
        self.logger.debug("Template updated to: %s" % (self.sub_template))

        app_id = self.start_yarn_application(job_meta)
        app_ids.append(app_id)
        self.logger.info("Application Started successfully: %s" % (app_id))

        # Set Active Profile.
        thread.start_new_thread( self.monitor_poller_job, (app_id, 'test'))
        response = dict()
        response["application_id"] = app_id
        response["message"] = "Successfully initiated the yarn application"
        return json.dumps(response)
        # Code Ends here

def pcon_consumer():
    # self.logger.debug("pcon_consumer: Starting Process.")
    print 'Starting Consumer Process.'
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


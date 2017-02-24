import datetime
import imp
import json
from kafka import KafkaConsumer, KafkaProducer
import multiprocessing
import sys
import time
import os

sys.path.append(os.getcwd())

from deepInsight.util import get_logger
# from deepInsight.monitor.constants import *
from deepInsight.monitor.poller_watch import *
from django.conf import settings


# Logger Handle
log = get_logger(PCONTROLLER)

# Take kafka topics as arguments
if len(sys.argv) != 4:
    log.debug("Provide - kafka_client, orch_comm and pcon_comm")
    exit(0)

kafka_client = sys.argv[1]
orch_comm = sys.argv[2]
pcon_comm = sys.argv[3]
'''
#kafka_client = '192.168.101.12:9092'
#orch_comm = 'orch_comm'
#pcon_comm = 'pcon_comm'
'''


def orch_consumer(orch_q, kafka_client, orch_comm):
    orch_comm_consumer = KafkaConsumer(orch_comm, bootstrap_servers=[kafka_client],
                                       value_deserializer=lambda m: json.loads(m.decode("ascii")))

    # key_deserializer=lambda m: json.loads(m.decode("ascii")))
    for item in orch_comm_consumer:
        log.debug("Received from orch_comm: item.value- %s" % item.value)
        put_msg(orch_q, item.value)
        orch_comm_consumer.commit()


def update_worker(cntrl_data, worker_proc_map, worker_target_map,
                  worker_mgmt_queue_map, worker_metric_queue_map, worker_cntrl_list_map):
    worker_id = cntrl_data[PLUGIN_ID]

    if not worker_id in worker_proc_map.keys():
        log.debug("Worker does not  exists -  what shall I update?")
        return False

    meta = dict(cntrl_data[META])
    # tag, interval, run_count = extract_tag_interval_count(cntrl_data)
    # meta.update({TAG: tag})
    # meta.update({INTERVAL: interval})
    # meta.update({RUN_COUNT: run_count})

    put_msg(worker_mgmt_queue_map[worker_id], cntrl_data)
    log.debug("Update worker %s with - %s" % (worker_id, cntrl_data))

    index = 0
    for const in worker_cntrl_list_map:
        if const.keys()[0] == worker_id:
            worker_cntrl_list_map[index][worker_id][META] = meta
        index += 1
    return True


def get_writer_worker_inst(plugin, mgmt_q, state_q, metric_q, cntrl_data, state):
    meta = cntrl_data[META]
    while True:
        update = get_msgs(mgmt_q)
        if update:
            meta = update[META]

        doc = get_msgs(metric_q)
        if doc:
            state = plugin.park(doc, meta, state)
            dispatch_data(state, [state_q])


def get_reader_worker_inst(plugin, mgmt_q, state_q, dest_q_list, cntrl_data, state):

    meta = cntrl_data[META]
    tag, interval, run_count = extract_tag_interval_count(cntrl_data)

    while True:
        update = get_msgs(mgmt_q)
        if update:
            meta = update[META]
            tag, interval, run_count = extract_tag_interval_count(update)

        doc_list, state = plugin.poll(meta, state)
        for doc in doc_list:
            dispatch_data(doc, dest_q_list, tag)

        dispatch_data(state, [state_q])

        if run_count > 0:
            run_count -= 1

        if run_count == 0:
            exit(0)

        time.sleep(interval)


def spin_worker(cntrl_data, worker_list, worker_proc_map, worker_target_map, worker_mgmt_queue_map, worker_queue_map, worker_metric_queue_map, worker_type_map, reader_to_tag_map, worker_to_mode_map, worker_state={}):
    plugin_id = cntrl_data[PLUGIN_ID]

    if plugin_id in worker_proc_map.keys():
        log.debug("%s already exists -  why do you wanna restart?" % (worker_type_map[plugin_id]))
        return False

    uninitialized_writer_metric_q_map = {}

    plugin_type = cntrl_data[PLUGIN_TYPE]
    target = cntrl_data[TARGET]
    dest_list = []

    if plugin_type == READER:
        dest_list = cntrl_data[DEST_LIST]

    fpath = os.path.join(settings.PROFILE_DIR, "pollers", target + ".py")
    log.debug("File Path: %s", fpath)

    try:
        plugin = imp.load_source(target, fpath)
    except IOError:
        log.debug("Could not load plugin file- %s" % (fpath))
        log.debug("Exiting Controller")
        exit(0)

    mgmt_q = get_queue()
    state_q = get_queue()
    meta = cntrl_data[META]

    if plugin_type == READER:
        target_writer = []
        for writer_id in dest_list:
            if writer_id not in worker_metric_queue_map.keys():
                log.debug("Forming a metric queue in reader's call as target writer - %s still not activated" % (writer_id))
                metric_q = get_queue()
                uninitialized_writer_metric_q_map[writer_id] = metric_q
            else:
                metric_q = worker_metric_queue_map[writer_id]
            target_writer.append(metric_q)
        # worker = multiprocessing.Process(target=(target), args=(mgmt_q, state_q, target_writer, meta, worker_state,))
        worker = multiprocessing.Process(target=get_reader_worker_inst, args=(plugin, mgmt_q, state_q, target_writer, cntrl_data, worker_state,))
    elif plugin_type == WRITER:
        if plugin_id not in worker_metric_queue_map.keys():
            metric_q = get_queue()
        else:
            metric_q = worker_metric_queue_map[plugin_id]

        # worker = multiprocessing.Process(target=(target), args=(mgmt_q, state_q, metric_q, meta, worker_state,))
        worker = multiprocessing.Process(target=get_writer_worker_inst, args=(plugin, mgmt_q, state_q, metric_q, cntrl_data, worker_state,))
    else:
        log.debug("spin_worker: Plugin type not recognized ", plugin_type)

    worker.start()
    worker_list.append(plugin_id)

    if plugin_type == READER:
        reader_to_tag_map[plugin_id] = cntrl_data[TAGS]
        worker_target_map[plugin_id] = dest_list
        if uninitialized_writer_metric_q_map:
            worker_metric_queue_map.update(uninitialized_writer_metric_q_map)

    if plugin_type == WRITER:
        worker_metric_queue_map[plugin_id] = metric_q

    worker_mgmt_queue_map[plugin_id] = mgmt_q
    worker_queue_map[plugin_id] = state_q
    worker_proc_map[plugin_id] = worker
    # worker_target_map[plugin_id] = target
    worker_type_map[plugin_id] = plugin_type
    worker_to_mode_map[plugin_id] = cntrl_data["mode"]

    log.debug("spin_worker: worker_proc_map- %s" % worker_proc_map)
    log.debug("spin_worker: worker_mgmt_queue_map- %s" % worker_mgmt_queue_map)
    log.debug("spin_worker: worker_queue_map- %s" % worker_queue_map)
    log.debug("spin_worker: worker_target_map- %s" % worker_target_map)
    return True


def clear_worker_maps(worker_id, worker_proc_map, worker_target_map, worker_mgmt_queue_map,
                      worker_queue_map, worker_metric_queue_map, worker_type_map,
                      worker_state_map, reader_to_tag_map):

    if worker_proc_map.get(worker_id):
        del worker_proc_map[worker_id]

    if worker_mgmt_queue_map.get(worker_id):
        del worker_mgmt_queue_map[worker_id]

    if worker_queue_map.get(worker_id):
        del worker_queue_map[worker_id]

    if worker_state_map.get(worker_id):
        del worker_state_map[worker_id]

    if worker_type_map.get(worker_id) == READER:
        if worker_target_map.get(worker_id):
            del worker_target_map[worker_id]
        if reader_to_tag_map.get(worker_id):
            del reader_to_tag_map[worker_id]

    if worker_type_map.get(worker_id) == WRITER:
        if worker_metric_queue_map.get(worker_id):
            del worker_metric_queue_map[worker_id]

    if worker_type_map.get(worker_id):
        del worker_type_map[worker_id]


def stop_process(proc, worker_id):
    if proc and proc.is_alive():
        proc.terminate()
        log.debug("Terminated- %s" % worker_id)

        # clear_worker_maps(worker_id, worker_proc_map, worker_target_map, worker_mgmt_queue_map,
        # worker_queue_map, worker_metric_queue_map, worker_type_map, worker_state_map, reader_to_tag_map)


def start_orch_comm(kafka_client, orch_comm):
    orch_q = get_queue()
    orch_consumer_p = multiprocessing.Process(target=orch_consumer, args=(orch_q, kafka_client, orch_comm,))
    orch_consumer_p.start()
    log.debug("Started orch_comm -- Yipee!!")
    return orch_consumer_p, orch_q


def controller(kafka_client, orch_comm, pcon_comm):
    beacon_interval = 2
    update_orch = True
    check_point = True
    time = datetime.datetime.now()
    worker_list = []
    # State recovery
    worker_cntrl_list_map = []
    # Mgmt queue for all workers
    worker_mgmt_queue_map = {}
    # Data queue for all workers
    worker_queue_map = {}
    # Write queues on which writers are waiting
    worker_metric_queue_map = {}
    # State update received from workers
    worker_state_map = {}
    # Worker to spwaned process map
    worker_proc_map = {}
    # Wroker to type reader or writer
    worker_type_map = {}
    # For readers what are the target writers
    worker_target_map = {}
    # Writer - Who are all writing to me? Not yet in use -- see if you need it -- I think you will.
    writer_to_reader_map = {}
    # Reader to tag map - Am I a writer or a reader? I dont know.
    reader_to_tag_map = {}
    # worker to level of intrusion map.
    worker_to_mode_map = {}
    # Worker run count
    worker_run_count = {}
    # Workers total run left - no cheat sheet!! (0 > infinite) (0 = kill the worker) (0 < keep count)
    # worker_lifes_left = {}

    # start orch_comm
    orch_consumer_p, orch_q = start_orch_comm(kafka_client, orch_comm)

    pcon_comm_producer = KafkaProducer(bootstrap_servers=kafka_client, value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    log.debug("Started the Controller.")
    while True:
        if not is_queue_empty(orch_q):
            cntrl_data = json.loads(get_msg(orch_q))
            # cntrl_data = get_msg(orch_q)
            log.debug("Received control data: %s", cntrl_data)

            # Recovery -- you recover at the expanse of grace!!
            log.debug("Received control data: msg_type# %s", cntrl_data[MSG_TYPE])
            if cntrl_data[MSG_TYPE] == RECOVER:
                tmp_state_map = cntrl_data[CHECK_POINT][STATE]
                tmp_worker_cntrl_list_map = cntrl_data[CHECK_POINT][CNTRL_DATA_MAP]
                for const in tmp_worker_cntrl_list_map:
                    plugin_id = const.keys()[0]
                    cntrl_data = const[plugin_id]
                    if cntrl_data[PLUGIN_TYPE] == WRITER:
                        state = {}
                        spin_worker(cntrl_data, worker_list, worker_proc_map, worker_target_map, worker_mgmt_queue_map,  worker_queue_map, worker_metric_queue_map, worker_type_map, reader_to_tag_map, worker_to_mode_map, state)
                        worker_state_map[plugin_id] = state
                        worker_cntrl_list_map.append(const)

                for const in tmp_worker_cntrl_list_map:
                    plugin_id = const.keys()[0]
                    cntrl_data = const[plugin_id]
                    if cntrl_data[PLUGIN_TYPE] == READER:
                        state = tmp_state_map[plugin_id]
                        spin_worker(cntrl_data, worker_list, worker_proc_map, worker_target_map, worker_mgmt_queue_map,  worker_queue_map, worker_metric_queue_map, worker_type_map, reader_to_tag_map, worker_to_mode_map, state)
                        worker_state_map[plugin_id] = state
                        worker_cntrl_list_map.append(const)

            # Asked to stop all readers and writers
            if cntrl_data[MSG_TYPE] == STOPALL:
                log.debug("seriously teardown!!")
                for worker_id, proc in worker_proc_map.iteritems():
                    stop_process(proc, worker_id)

            # Asked to bring down controller
            if cntrl_data[MSG_TYPE] == TEARDOWNALL:
                log.debug("seriously teardown all!!")
                for worker_id, proc in worker_proc_map.iteritems():
                    stop_process(proc, worker_id)
                stop_process(orch_consumer_p, "orch_comm")
                exit(0)

            # start/stop/manage each plugin - respect individuality
            if cntrl_data[MSG_TYPE] == CNTRL:
                log.debug("Control data: cntrl_data- %s" % cntrl_data)

                cmd = cntrl_data[CMD]

                if cmd == START:
                    worker_cntrl_list_map.append({cntrl_data[PLUGIN_ID]: cntrl_data})
                    spin_worker(cntrl_data, worker_list, worker_proc_map, worker_target_map, worker_mgmt_queue_map,  worker_queue_map, worker_metric_queue_map, worker_type_map, reader_to_tag_map, worker_to_mode_map)

                if cmd == STOP:
                    worker_id = cntrl_data[PLUGIN_ID]
                    stop_process(worker_proc_map.get(worker_id), worker_id)
                    # if worker_type_map[worker_id] == WRITER:
                    # update_worker()

                if cmd == TEARDOWN:
                    worker_id = cntrl_data[PLUGIN_ID]
                    stop_process(worker_proc_map.get(worker_id), worker_id)
                    clear_worker_maps(worker_id, worker_proc_map, worker_target_map, worker_mgmt_queue_map,  worker_queue_map, worker_metric_queue_map, worker_type_map, worker_state_map, reader_to_tag_map)
                    # if worker_type_map[worker_id] == WRITER:
                    #    update_worker()

                if cmd == RESUME:
                    worker_id = cntrl_data[PLUGIN_ID]
                    restart_worker(worker_id, worker_proc_map, worker_mgmt_queue_map, worker_queue_map, worker_state_map, worker_cntrl_list_map, worker_metric_queue_map, worker_target_map, worker_type_map, worker_to_mode_map)

                if cmd == UPDATE:
                    worker_id = cntrl_data[PLUGIN_ID]
                    worker_type = worker_type_map[worker_id]

                    index = 0
                    for const in worker_cntrl_list_map:
                        if const.keys()[0] == worker_id:
                                worker_cntrl_list_map[index][worker_id][META] = cntrl_data[META]
                                if worker_type == READER:
                                    worker_cntrl_list_map[index][worker_id][INTERVAL] = cntrl_data[INTERVAL]
                                    worker_cntrl_list_map[index][worker_id][TAGS] = cntrl_data[TAGS]
                        index += 1

                    if worker_type == READER:
                        dest_list_new = cntrl_data[DEST_LIST]
                        dest_list = worker_target_map[worker_id]
                        if set(dest_list) != set(dest_list_new):
                            log.debug("Have to restart for update - opps!!")
                            log.debug("Update worker - %s, %s, %s, %s" % (worker_id, worker_type, dest_list_new, dest_list))
                            index = 0
                            for const in worker_cntrl_list_map:
                                if const.keys()[0] == worker_id:
                                    worker_cntrl_list_map[index][worker_id][DEST_LIST] = dest_list_new
                                index += 1

                            worker_proc_map[worker_id].terminate()
                            restart_worker(worker_id, worker_proc_map, worker_mgmt_queue_map, worker_queue_map, worker_state_map, worker_cntrl_list_map, worker_metric_queue_map, worker_target_map, worker_type_map, worker_to_mode_map)
                        else:
                            log.debug("Update worker - %s, %s" % (worker_id, worker_type))
                            update_worker(cntrl_data, worker_proc_map, worker_target_map, worker_mgmt_queue_map, worker_metric_queue_map, worker_cntrl_list_map)
                    else:
                        log.debug("Update worker - %s, %s" % (worker_id, worker_type))
                        update_worker(cntrl_data, worker_proc_map, worker_target_map, worker_mgmt_queue_map, worker_metric_queue_map, worker_cntrl_list_map)

        is_alive_map = []
        for worker_id, worker_proc in worker_proc_map.iteritems():
            is_alive_map.append({NAME: worker_id, STATUS: worker_proc.is_alive()})

        for worker_id, worker_q in worker_queue_map.iteritems():
            if not is_queue_empty(worker_q):
                # update state info
                state = get_msg(worker_q)
                worker_state_map[worker_id] = state
                # log.debug("worker state- %s", % worker_state_map)

        # beacon -- should it be threaded?
        time_delta_sec = (datetime.datetime.now() - time).total_seconds()
        if time_delta_sec >= beacon_interval:
            # Do check-pointing
            if check_point:
                data = {CHECK_POINT: {CNTRL_DATA_MAP: worker_cntrl_list_map, STATE: worker_state_map, STATUS: is_alive_map}}
                pcon_comm_producer.send(pcon_comm, json.dumps(data))
                log.debug("checkpoint using: data- %s" % data)
            log.debug("Its time for revival!! - so much time has elapsed %s", time_delta_sec)

            # check orch_comm.
            if not orch_consumer_p.is_alive():
                orch_consumer_p, orch_q = start_orch_comm(kafka_client, orch_comm)

            # Worry about workers now.
            tmp_worker_proc_map = dict(worker_proc_map)
            for worker_id, proc in tmp_worker_proc_map.iteritems():
                if not proc.is_alive():
                    log.debug("Found %s is quite dead", worker_id)
                    if worker_to_mode_map[worker_id] == "manage":
                        restart_worker(worker_id,
                                       worker_proc_map,
                                       worker_mgmt_queue_map,
                                       worker_queue_map,
                                       worker_state_map,
                                       worker_cntrl_list_map,
                                       worker_metric_queue_map,
                                       worker_target_map,
                                       worker_type_map,
                                       worker_to_mode_map)
            time = datetime.datetime.now()


def restart_worker(worker_id,
                   worker_proc_map,
                   worker_mgmt_queue_map,
                   worker_queue_map,
                   worker_state_map,
                   worker_cntrl_list_map,
                   worker_metric_queue_map,
                   worker_target_map,
                   worker_type_map,
                   worker_to_mode_map):
        mgmt_q = worker_mgmt_queue_map[worker_id]
        state_q = worker_queue_map[worker_id]
        target_writer = []
        state = {}
        plugin_type = worker_type_map[worker_id]

        for const in worker_cntrl_list_map:
            if worker_id == const.keys()[0]:
                cntrl_data = const[worker_id]
        meta = cntrl_data[META]

        target = cntrl_data[TARGET]
        fpath = os.path.join(settings.PROFILE_DIR, "pollers", target + ".py")

        try:
            plugin = imp.load_source(target, fpath)
        except IOError:
            log.debug("Could not load plugin file- %s" % (fpath))
            log.debug("Exiting Controller")
            exit(0)

        # target = eval("%s.%s" % ("plugin",target))
        try:
            worker_state = worker_state_map[worker_id]
        except:
            worker_state = {}

        if plugin_type == READER:
            dest_list = cntrl_data[DEST_LIST]
            for writer_id in dest_list:
                target_writer.append(worker_metric_queue_map[writer_id])
            log.debug("Going to restart worker %s, %s, %s, %s" %
                  (worker_id, plugin_type, dest_list, target_writer))
            worker = multiprocessing.Process(target=get_reader_worker_inst, args=(plugin, mgmt_q, state_q, target_writer, cntrl_data, worker_state,))
        elif plugin_type == WRITER:
            metric_q = worker_metric_queue_map[worker_id]
            worker = multiprocessing.Process(target=get_writer_worker_inst,
                                             args=(plugin, mgmt_q, state_q,
                                             metric_q, cntrl_data,
                                             worker_state,))
            log.debug("Going to restart worker %s, %s, %s" %(worker_id, plugin_type, metric_q))
        worker.start()
        worker_proc_map[worker_id] = worker

        if plugin_type == READER:
            worker_target_map[worker_id] = dest_list

# Start controller
# controller('192.168.101.12:9092', 'orch_comm', 'pcon_comm')
controller(kafka_client, orch_comm, pcon_comm)

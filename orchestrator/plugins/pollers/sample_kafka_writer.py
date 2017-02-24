from kafka import KafkaProducer
import json
import time


def park(doc, meta={}, state={}):
    if not state:
        state["No_Client"] = 0
        state["No_Topic"] = 0
        state["Send_Success"] = 0
        state["Send_Failed"] = 0

    try:
        kafka_client = ("%s:%s" % (meta["host"], meta["port"]))
    except:
        state["No_Client"] += 1
        return state

    try:
        topic = meta["topic"]
    except:
        state["No_Topic"] += 1
        return state

    try:
        producer = KafkaProducer(bootstrap_servers=kafka_client,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        doc.update({"time": time.time()})
        producer.send(topic, doc)
        producer.flush()
        state["Send_Success"] += 1
    except:
        state["Send_Failed"] += 1
    return state

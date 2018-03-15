#!/usr/bin/env python3

import sys
import uavcan
import threading
import time
import datetime
import configparser
import influxdb
import os
from collections import defaultdict

uavcan.load_dsdl(
    os.path.join(os.path.dirname(__file__), 'dsdl_files', 'homeautomation'))


def main(config_filename, *args):

  node_infos = defaultdict(dict(last_seen = None, last_info = None))
  config = read_config(config_filename)

  influxdb_client = influxdb.InfluxDBClient(
      config.get('influxdb', 'host'),
      config.getint('influxdb', 'port'),
      config.get('influxdb', 'username'),
      config.get('influxdb', 'password'),
      config.get('influxdb', 'database'),
  )
  influxdb_client.create_database(config.get('influxdb', 'database'))

  node_info = uavcan.protocol.GetNodeInfo.Response()
  node_info.name = config.get('node', 'name')

  node = uavcan.make_node(
      config.get('canbus', 'ifname'),
      node_id=config.getint('node', 'id'),
      node_info=node_info,
  )

  uavcan_types = [
      uavcan.protocol.NodeStatus,
      uavcan.thirdparty.homeautomation.EventCount,
      uavcan.thirdparty.homeautomation.Environment,
      uavcan.thirdparty.homeautomation.ConductionSensor,
      uavcan.thirdparty.homeautomation.PumpStatus,
  ]

  for uavcan_type in uavcan_types:
    node.add_handler(
        uavcan_type,
        record_event_data,
        influxdb_client=influxdb_client,
    )

  def node_status_cb(event):
    node_infos[event.transfer.source_node_id]['last_seen'] = datetime.datetime.now()

  node.add_handler(uavcan.protocol.NodeStatus, node_status_cb)
  node.add_handler(uavcan.protocol.RestartNode, restart_request)

  node.mode = uavcan.protocol.NodeStatus().MODE_OPERATIONAL
  node.health = uavcan.protocol.NodeStatus().HEALTH_OK

  print('started')

  while True:
    try:
      node.spin(1)
      get_node_infos(node, node_infos, influxdb_client)
      receive_node_info(config.getint('node', 'id'), node_info, influxdb_client)
    except uavcan.UAVCANException as ex:
      print('Node error:', ex)


def restart():
  time.sleep(0.5)  # give it a moment to send the response
  print('restarting')
  sys.stdout.flush()
  os.execv(__file__, sys.argv)


def restart_request(event):

  if event.request.magic_number != event.request.MAGIC_NUMBER:
    return uavcan.protocol.RestartNode.Response(ok=False)

  threading.Thread(target=restart, daemon=True).start()
  return uavcan.protocol.RestartNode.Response(ok=True)


def read_config(filename):
  config = configparser.ConfigParser()
  config.read(os.path.join(os.path.dirname(__file__), 'defaults.ini'))
  config.read(filename)
  return config


def receive_node_info(node_id, node_info, influxdb_client):

  # node info comes with node status too

  fields = extract_fields(node_info.status._fields)

  fields.update({
      'name': str(node_info.name),
      'software_version.major': node_info.software_version.major,
      'software_version.minor': node_info.software_version.minor,
  })

  influxdb_client.write_points([{
      "measurement": 'uavcan.protocol.NodeInfo',
      "tags": {
          "node_id": node_id,
      },
      "fields": fields,
  }])


def get_node_infos(node, node_infos, influxdb_client):

  def handle_event_for(node_id, event):
    if not event:
      return
      
    node_infos[node_id]['info'] = event.transfer.payload
    node_infos[node_id]['last_info'] = datetime.datetime.now()
    return receive_node_info(node_id, event.transfer.payload, influxdb_client),

  now = datetime.datetime.now()
  for node_id in node_infos:
    if (now - node_id['last_seen']).total_seconds() < 5 and (node_id['last_info'] is None or (now - node_id['last_info']).total_seconds() > 3600):
      node.request(
          uavcan.protocol.GetNodeInfo.Request(),
          node_id,
          lambda event, node_id=node_id: handle_event_for(node_id, event),
      )


def extract_fields(message_fields):

  fields = {}

  for key, value in message_fields.items():
    if isinstance(value, uavcan.transport.PrimitiveValue):
      fields[key] = value.value
    elif isinstance(value, uavcan.transport.ArrayValue):
      for idx, array_value in enumerate(value):
        fields['{}{}'.format(key, idx)] = array_value

  return fields


def record_event_data(event, influxdb_client=None):
  if influxdb_client is None:
    raise Exception('please pass in a influxdb_client option')

  fields = extract_fields(event.message._fields)

  if len(fields) > 0:
    influxdb_client.write_points([{
        "measurement": event.message._type.full_name,
        "tags": {
            "node_id": event.transfer.source_node_id,
        },
        "fields": fields,
    }])


if __name__ == '__main__':
  main(*sys.argv[1:])

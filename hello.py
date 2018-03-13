import sys
import uavcan
import threading
import time
import configparser
import influxdb
import os


def main(config_filename):

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
        handle_event,
        influxdb_client=influxdb_client,
    )

  node.mode = uavcan.protocol.NodeStatus().MODE_OPERATIONAL

  print('started')

  while True:
    try:
      node.spin(1)
      get_node_infos(node, influxdb_client)
    except uavcan.UAVCANException as ex:
      print('Node error:', ex)


def read_config(filename):
  config = configparser.ConfigParser()
  config.read(os.path.join(os.path.dirname(__file__), 'defaults.ini'))
  config.read(filename)
  return config


def receive_node_info(event, influxdb_client):

  # node info comes with node status too

  fields = extract_fields(event.transfer.payload.status._fields)
  node_info = event.transfer.payload

  fields.update({
      'name': str(node_info.name),
      'software_version.major': node_info.software_version.major,
      'software_version.minor': node_info.software_version.minor,
  })

  influxdb_client.write_points([{
      "measurement": 'uavcan.protocol.NodeInfo',
      "tags": {
          "node_id": event.transfer.source_node_id,
      },
      "fields": fields,
  }])


def get_node_infos(node, influxdb_client):
  for node_id in [2, 3]:  # TODO: get/keep a list of node ids
    node.request(
        uavcan.protocol.GetNodeInfo.Request(),
        node_id,
        lambda event: receive_node_info(event, influxdb_client),
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


def handle_event(event, influxdb_client=None):
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
  main(sys.argv[1])

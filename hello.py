import sys
import uavcan
import threading
import time
import configparser
import influxdb
import os

# gas meter increments at 0.01 cubic meters


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

  node = uavcan.make_node(config.get('canbus', 'ifname'))

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
        write_to_influxdb,
        influxdb_client=influxdb_client,
    )

  thread = threading.Thread(target=node.spin, daemon=True)
  thread.start()

  node.mode = uavcan.protocol.NodeStatus().MODE_OPERATIONAL

  print('started')

  thread.join()


def read_config(filename):
  config = configparser.ConfigParser()
  config.read(os.path.join(os.path.dirname(__file__), 'defaults.ini'))
  config.read(filename)
  return config


def extract_fields(event):

  fields = {}

  for key, value in event.message._fields.items():
    if isinstance(value, uavcan.transport.PrimitiveValue):
      fields[key] = value.value
    elif isinstance(value, uavcan.transport.ArrayValue):
      for idx, array_value in enumerate(value):
        fields['{}{}'.format(key, idx)] = array_value

  return fields


def write_to_influxdb(event, influxdb_client=None):

  if influxdb_client is None:
    raise Exception('please pass in a influxdb_client option')

  fields = extract_fields(event)

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

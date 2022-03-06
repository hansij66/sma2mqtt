"""
  Rename to config.py

  Configure:
  - MQTT client
  - Debug level
  - Home Assistant
  - InfluxDB

"""

# [ LOGLEVELS ]
# DEBUG, INFO, WARNING, ERROR, CRITICAL
loglevel = "INFO"


# [ MQTT ]
# Using local dns names was not always reliable with PAHO
MQTT_BROKER = "192.168.1.1"
MQTT_PORT = 1883
MQTT_CLIENT_UNIQ = 'mqtt-sma'
MQTT_QOS = 1
MQTT_USERNAME = "ijntema"
MQTT_PASSWORD = "mosquitto0000"

# Max nrof MQTT messages per second
# Set to 0 for unlimited rate
MQTT_RATE = 100
MQTT_TOPIC_PREFIX = "solar/sma/yard"


# [ InfluxDB ]
# Add a influxdb database tag, for Telegraf processing (database:INFLUXDB)
# This is not required for core functionality of this parser
# Set to None if Telegraf is not used
INFLUXDB = "solar"
# INFLUXDB = None


# [ SMA INVERTER ]
# Not sure anymore if  you have to enable modbus on the inverter
# Make sure settings below are same as in inverter or vice versa
# SMACOVERTER = "192.168.189.22"
SMACOVERTER = "sma"

SMAPORT = 502
SMASLAVE = 0x03
SMATIMEOUT = 3

# NROF MQTT updates per hour
SMAMQTTFREQUENCY = 60


# [ Home Assistant ]
# HA has its own SMA integration, this is not required
HA_DISCOVERY = True

# only supports one level of hierarchy
HA_MQTT_DISCOVERY_TOPIC_PREFIX = "sma"

# Default is False, removes the auto config message when this program exits
HA_DELETECONFIG = False

# Discovery messages per hour
# At start-up, always a discovery message is send
# Default is 12 ==> 1 message every 5 minutes. If the MQTT broker is restarted
# it can take up to 5 minutes before the dsmr device re-appears in HA
HA_INTERVAL = 12

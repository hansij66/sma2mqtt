# [ LOGLEVELS ]
# DEBUG, INFO, WARNING, ERROR, CRITICAL
loglevel = "INFO"
# loglevel = "DEBUG"

# [ INVERTERS ]
__SMA1 = {
                "name": "sma_south",
                "ip": "sma1.fritz.box",
                # "ip": "192.168.1.128",
                "smaport": 502,
                "smatimeout": 3,
                "smaunitidregister": 42109,  # modbus register to determine Unit ID
              }

__SMA2 = {
                "name": "sma_east",
                "ip": "sma2.fritz.box",
                "smaport": 502,
                "smatimeout": 3,
                "smaunitidregister": 42109,  # modbus register to determine Unit ID
              }

INVERTERS = [__SMA1, __SMA2]

# Set to True when deployed in production
PRODUCTION = True

# Retry attemps modbus connections before giving up
# With exponantial delay
RETRY = 20

# [ MQTT ]
# Using local dns names was not always reliable with PAHO
MQTT_BROKER = "192.168.1.1"
MQTT_PORT = 1883
MQTT_CLIENT_UNIQ = 'mqtt-sma'
MQTT_QOS = 1
MQTT_USERNAME = "myusername"
MQTT_PASSWORD = "topsecret"
MQTT_TOPIC_PREFIX = "solar/sma"

if not PRODUCTION:
  MQTT_CLIENT_UNIQ = MQTT_CLIENT_UNIQ + "-test"
  MQTT_TOPIC_PREFIX = MQTT_TOPIC_PREFIX + "/test"
  RETRY = 3


# NROF MQTT updates per hour
SMAMQTTFREQUENCY = 60

# Path to SMA MODBUS HTML FILE
# Obtained and extracted from
# https://files.sma.de/downloads/PARAMETER-HTML_SBxx-1AV-41-GG10_V16.zip?
# Use English file
SMA_REGISTER_DEFINITION_FILE = "./modbus/parameterlist_en.html"

# [ MODBUS REGISTERS ]
# Select registers (by address) which will be read
# See modbus/parameterlist_en.html
# See for active attributes: ACTIVEHEADERSDICT in sma_sunnyboy_modbus_config.py

# MODBUS_ADDRESS: mandatory attribute
# All other attributes are optional, to override parameters specified in modbus/parameterlist_en.html
# For example, for IP4 registers, MODBUS_DATATYPE is incorrectly STR32; Can be overriden to STR16

# CHANNEL: optional attribute, key in MQTT key:value pair
# MODBUS_DATATYPE: optional
# REGISTER_SIZE: optional
# MODBUS_DATAFORMAT: optional

MODBUSREGISTERS = [
  {'MODBUS_ADDRESS': 30057, 'CHANNEL': 'serial'},
  {'MODBUS_ADDRESS': 30513, 'CHANNEL': 'yield_total'},
  {'MODBUS_ADDRESS': 30769, 'CHANNEL': 'i_pv1'},
  {'MODBUS_ADDRESS': 30771, 'CHANNEL': 'v_pv1'},
  {'MODBUS_ADDRESS': 30773, 'CHANNEL': 'p_pv1'},
  {'MODBUS_ADDRESS': 30775, 'CHANNEL': 'p_ac1'},
  {'MODBUS_ADDRESS': 30783, 'CHANNEL': 'v_ac1'},
  {'MODBUS_ADDRESS': 30803, 'CHANNEL': 'f_ac'},
  {'MODBUS_ADDRESS': 30953, 'CHANNEL': 'temperature'},
  {'MODBUS_ADDRESS': 30957, 'CHANNEL': 'i_pv2'},
  {'MODBUS_ADDRESS': 30959, 'CHANNEL': 'v_pv2'},
  {'MODBUS_ADDRESS': 30961, 'CHANNEL': 'p_pv2'},
  {'MODBUS_ADDRESS': 30977, 'CHANNEL': 'i_ac1'},
  {'MODBUS_ADDRESS': 41255, 'CHANNEL': 'Pthrottle'},
  {'MODBUS_ADDRESS': 30225, 'CHANNEL': 'r_insulation'},
  {'MODBUS_ADDRESS': 31247, 'CHANNEL': 'i_residual'},
]

"""
  Parses SMA telegrams to MQTT messages
  Queue MQTT messages

        This program is free software: you can redistribute it and/or modify
        it under the terms of the GNU General Public License as published by
        the Free Software Foundation, either version 3 of the License, or
        (at your option) any later version.

        This program is distributed in the hope that it will be useful,
        but WITHOUT ANY WARRANTY; without even the implied warranty of
        MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
        GNU General Public License for more details.

        You should have received a copy of the GNU General Public License
        along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import threading
import copy
import time
import json

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder

import config as cfg
from modbus_sma_sb5 import sma_definition

# Logging
import __main__
import logging
import os
script = os.path.basename(__main__.__file__)
script = os.path.splitext(script)[0]
logger = logging.getLogger(script + "." + __name__)


class ParseTelegrams(threading.Thread):
  """
  """

  def __init__(self, trigger, stopper, mqtt, telegram):
    """
    Args:
      :param threading.Event() trigger: signals that new telegram is available
      :param threading.Event() stopper: stops thread
      :param mqtt.mqttclient() mqtt: reference to mqtt worker
      :param list() telegram: sma telegram
    """
    logger.debug(">>")
    super().__init__()
    self.__trigger = trigger
    self.__stopper = stopper
    self.__telegram = telegram
    self.__mqtt = mqtt

  def __del__(self):
    logger.debug(">>")

  def __publish_telegram(self, json_dict):
    # publish the dictionaries per topic

    # make resilient against double forward slashes in topic
    topic = cfg.MQTT_TOPIC_PREFIX
    topic = topic.replace('//', '/')
    message = json.dumps(json_dict, sort_keys=True, separators=(',', ':'))
    self.__mqtt.do_publish(topic, message, retain=False)

  def __decode_telegram_element(self, register, element, jsonvalues):
    """
    :param register:
    :param element:
    :param jsonvalues:
    :return:
    :raises modbus exceptions
    """

    MIN_SIGNED = (-2147483648 + 1)
    MAX_UNSIGNED = (4294967295 - 1)

    mod_message = BinaryPayloadDecoder.fromRegisters(element.registers, byteorder=Endian.Big, wordorder=Endian.Big)

    # provide the correct result depending on the defined datatype
    datatype = register[sma_definition['DATATYPE']]
    if datatype == 'S32': interpreted = mod_message.decode_32bit_int()
    elif datatype == 'U32': interpreted = mod_message.decode_32bit_uint()
    elif datatype == 'U64': interpreted = mod_message.decode_64bit_uint()
    elif datatype == 'STR16': interpreted = mod_message.decode_string(16)
    elif datatype == 'STR32': interpreted = mod_message.decode_string(32)
    elif datatype == 'S16': interpreted = mod_message.decode_16bit_int()
    elif datatype == 'U16': interpreted = mod_message.decode_16bit_uint()

    # if no data type is defined do raw interpretation of the delivered data
    else: interpreted = mod_message.decode_16bit_uint()

    # check for "None" data before doing anything else
    if (interpreted <= MIN_SIGNED) or (interpreted >= MAX_UNSIGNED):
      json_value = None
    else:
      multiplier = register[sma_definition['FORMAT']]
      # put the data with correct formatting into the data table
      if multiplier == 'FIX3':   json_value = float(interpreted) / 1000.0
      elif multiplier == 'FIX2': json_value = float(interpreted) / 100.0
      elif multiplier == 'FIX1': json_value = float(interpreted) / 10.0
      elif multiplier == 'STRING': json_value = str(interpreted)
      else: json_value = interpreted

    logger.debug(f"mod_message for {register[sma_definition['NAME']]} = {json_value}    {interpreted}")

    try:
      json_key = register[sma_definition['JSONKEY']]
    except IndexError:
      json_key = register[sma_definition['NAME']]

    jsonvalues[json_key] = json_value

  def __decode_telegrams(self, telegram):
    """
    Args:
      :param list telegram:

    Returns:

    """
    logger.debug(f">>")
    json_values = dict()

    # epoch, mqtt timestamp
    ts = int(time.time())

    # get counter and remove from telegram list
    counter = telegram[0]
    telegram.pop(0)

    # Build a dict of key:value, for MQTT JSON
    json_values["timestamp"] = ts
    json_values["counter"] = counter

    if cfg.INFLUXDB:
      json_values["database"] = cfg.INFLUXDB

    for (register, element) in telegram:
      try:
        self.__decode_telegram_element(register, element, json_values)
      except Exception as e:
        logger.warning(f"Exception {e}")
        # ? break

    # Parts of SMA inverter will go in standby at dusk/night
    # Only serial and total_yield can be read
    # Other register values are set to None
    # Skip MQTT broadcast @ night
    if None in json_values.values():
      logger.debug(f"MQTT values = {json_values}...Not broadcasted...SMA Inverter is in night mode/standby mode")
      pass
    else:
      logger.debug(f"MQTT values = {json_values}")
      self.__publish_telegram(json_values)

  def run(self):
    logger.debug(">>")

    while not self.__stopper.is_set():
      # block till event is set, but implement timeout to allow stopper
      self.__trigger.wait(timeout=1)
      if self.__trigger.is_set():
        logger.debug(f"Trigger set")

        # Make copy of the telegram, for further parsing
        telegram = copy.deepcopy(self.__telegram)

        # Clear trigger to allow serial reader to read next telegram
        self.__trigger.clear()

        # Clear telegram list for next capture by ReadSerial class
        self.__telegram.clear()

        self.__decode_telegrams(telegram)

    logger.debug("<<")

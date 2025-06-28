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

from typing import Dict, List, Any
from dataclasses import dataclass
import threading
import copy
import time
import json
import os

# ----------------------------------------------------------------------------------------------------------------------
# Local imports
# ----------------------------------------------------------------------------------------------------------------------
import config as cfg
import mqtt as mqtt
import __main__
import logging

logger = logging.getLogger(f"{os.path.splitext(os.path.basename(__main__.__file__))[0]}.{__name__}")


# ----------------------------------------------------------------------------------------------------------------------
# DataFormat
# --------------------------------------------------------------------------------------------------------------------
@dataclass
class DataFormat:
  """Configuration for different data formats and their processing rules"""

  # Data format conversion factors
  # FIX0 has multiplier 1, and is an integer, hence PASSTHROUGH_FORMATS
  FORMAT_MULTIPLIERS = {
    #     "FIX0": 1,
    "FIX1": 0.1,
    "FIX2": 0.01,
    "FIX3": 0.001,
    "FIX4": 0.0001,
    "TEMP": 0.1
  }

  # Data formats that should be converted to strings
  STRING_FORMATS = {"IP4", "RAW", "UTF8"}

  # Data formats that should be kept as-is
  PASSTHROUGH_FORMATS = {"FIX0", "Duration", "Dauer", "TM", "TAGLIST", "ENUM", "FW", "REV"}


# --------------------------------------------------------------------------------------------------------------------
# class ParseTelegrams
# --------------------------------------------------------------------------------------------------------------------
class TelegramParser(threading.Thread):
  """
  Handles parsing of SMA telegrams, performs data processing, and publishes messages
  to an MQTT broker. Extends the threading.Thread class to utilize threaded execution.

  The class allows for modular decoding of telegram data, supporting various data formats
  with conversion or passthrough capabilities. It enables communication with an MQTT broker
  to publish parsed and structured data in JSON format.
  """

  def __init__(self,
               trigger: threading.Event,
               stopper: threading.Event,
               mqttclient: mqtt.MQTTClient,
               telegram: List,
               invertername: str) -> None:
    """
    Initialize the telegram parser.

    Args:
        trigger: Event signaling that new telegram is available
        stopper: Event to stop the thread
        mqttclient: Reference to MQTT worker
        telegram: SMA telegram data
        invertername: Name of the inverter
    """

    logger.debug(f"{invertername}: >>")
    super().__init__()
    self.__trigger = trigger
    self.__stopper = stopper
    self.__telegram = telegram
    self.__mqtt = mqttclient
    self.__invertername = invertername
    self.__dataformat = DataFormat()

    logger.debug(f"{self.__invertername}: <<")
    return

  def __del__(self) -> None:
    logger.debug(f"{self.__invertername}: >>")
    logger.debug(f"{self.__invertername}: <<")
    return

  def __publish_mqtt_message(self,
                             payload: Dict[str, Any]) -> None:
    """
    Publish telegram data as MQTT message.

    Args:
      payload: JSON Dictionary of key:value pairs to publish as MQTT message.
    """
    logger.debug(f"{self.__invertername}: >>")

    # make resilient against double forward slashes in topic
    topic = f"{cfg.MQTT_TOPIC_PREFIX}/{self.__invertername}".replace('//', '/')
    message = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    self.__mqtt.do_publish(topic=topic, message=message, retain=False)

    return

  # --------------------------------------------------------------------------------------------------------------------
  # __decode_telegram_element
  # --------------------------------------------------------------------------------------------------------------------
  def __decode_telegram_element(self,
                                register_description: Dict[str, Any],
                                register_value: Any,
                                values: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process single telegram element and add it to result dictionary.
    :param register_description: MODBUS register description, including MODBUS_ADDRESS, MODBUS_DATAFORMAT, CHANNEL, ...
    :param register_value: Value of the register, as read from the inverter.
    :param values: json dictionary to store key:value pairs of all registers (constructing 1 by 1 for the MQTT message)
    :return: same as (json) values param, updated with new key:value pairs for the current register_description, if any.

    :raises modbus exceptions
    """

    if register_value is None:
      # None can happen during night (DC side is off)...Inverter returns NAN values, converted None values downstream
      logger.debug(f"{self.__invertername}: register_value is None, ignored")
      return values

    data_format = register_description['MODBUS_DATAFORMAT']

    if data_format in self.__dataformat.FORMAT_MULTIPLIERS:
      json_value = float(register_value) * self.__dataformat.FORMAT_MULTIPLIERS[data_format]
    elif data_format in self.__dataformat.STRING_FORMATS:
      json_value = str(register_value)
    elif data_format in self.__dataformat.PASSTHROUGH_FORMATS:
      json_value = register_value
    else:
      # This will result in dropping specific key:value from mqtt message
      logger.error(f"{self.__invertername}: Unknown format {data_format}")
      json_value = None

    json_key = register_description['CHANNEL']
    if json_value is not None:
      values[json_key] = json_value
    else:
      logger.debug(f"{self.__invertername}: key {json_key} has value None, ignored")

    return values

  # --------------------------------------------------------------------------------------------------------------------
  # __decode_telegrams
  # --------------------------------------------------------------------------------------------------------------------
  def __decode_telegrams(self,
                         telegram: List) -> None:
    """Process complete telegram and publish via MQTT if valid."""

    logger.debug(f"{self.__invertername}: >>")

    # dict of key:value, for MQTT JSON
    json_values = {
      "timestamp": int(time.time()),  # epoch, mqtt timestamp
      "counter": telegram.pop(0)  # get counter and remove from telegram list
    }

    logger.debug(f"TELEGRAM = {telegram}")
    for (register, register_value) in telegram:
      json_values = self.__decode_telegram_element(register_description=register,
                                                   register_value=register_value,
                                                   values=json_values)

    self.__publish_mqtt_message(json_values)
    logger.debug(f"{self.__invertername}: <<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # run
  # --------------------------------------------------------------------------------------------------------------------
  def run(self) -> None:
    """Main thread loop for processing telegrams."""
    logger.debug(f"{self.__invertername}: >>")

    while not self.__stopper.is_set():
      # block till event is set, but implement timeout to allow stopper
      self.__trigger.wait(timeout=1)
      if self.__trigger.is_set():
        logger.debug(f"{self.__invertername}: Trigger set")

        # Make copy of the telegram, for further parsing
        telegram = copy.deepcopy(self.__telegram)

        # Clear trigger to allow serial reader to read next telegram
        self.__trigger.clear()

        # Clear telegram list for next capture by ReadSerial class
        self.__telegram.clear()

        # Catch unexpected exceptions & restart
        try:
          self.__decode_telegrams(telegram=telegram)
        except Exception as e:
          logger.error(f"{self.__invertername}: Unspecified exception: {e}; Restarting")
          self.__stopper.set()

    logger.debug(f"{self.__invertername}: <<")
    return

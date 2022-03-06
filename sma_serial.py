"""
Read SMA telegrams.


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
import time
from pymodbus.client.sync import ModbusTcpClient
import config as cfg
from modbus_sma_sb5 import sma_registers, sma_moddatatype, sma_definition

# Logging
import __main__
import logging
import os
script = os.path.basename(__main__.__file__)
script = os.path.splitext(script)[0]
logger = logging.getLogger(script + "." + __name__)


class TaskReadSerial(threading.Thread):

  def __init__(self, trigger, stopper, telegram):
    """

    Args:
      :param threading.Event() trigger: signals that new telegram is available
      :param threading.Event() stopper: stops thread
      :param list() telegram: dsmr telegram
    """

    logger.debug(">>")
    super().__init__()
    self.__trigger = trigger
    self.__stopper = stopper
    self.__telegram = telegram
    self.__timelastread = 0
    self.__intervaltime = 3600/cfg.SMAMQTTFREQUENCY
    self.__counter = 0

    try:
      self.__client = ModbusTcpClient(cfg.SMACOVERTER, timeout=cfg.SMATIMEOUT, RetryOnEmpty=True, retries=2, port=cfg.SMAPORT)
    except Exception as e:
      logger.error(f"ModbusTcpClient creation problem {type(e).__name__}: {str(e)}")
      self.__stopper.set()
      raise ValueError('Cannot open connection to SMA')

  def __del__(self):
    logger.debug(">>")

  def __connect(self):
    logger.debug(">>")
    while not self.__stopper.is_set():
      try:
        if self.__client.connect():
          return
        else:
          time.sleep(1)
      except Exception as e:
        logger.error(f"ModbusTcpClient connection problem {type(e).__name__}: {str(e)}")
        # Todo and then?

  def __readregister(self, register, unit):
    """

    :param register:
    :param unit:
    :return: modbus_result (when successfull)
             False (one failure)
    :rtype: modbus
    :raises modbus exceptions
    """
    start = register[sma_definition['REGISTER']]
    count = sma_moddatatype[register[sma_definition['DATATYPE']]]

    modbus_result = self.__client.read_input_registers(address=start, count=count, unit=unit)

    return modbus_result

  def __read_serial(self):
    """
      Reads SMA telegrams; stores in global variable (self.__telegram)
      Sets threading event to signal other clients (parser) that
      new telegram is available.
    """
    logger.debug(">>")

    # https://github.com/doopa75/SMA-Inverter-ModbusTCPIP/blob/master/plugin.py
    # https://github.com/jgaalen/modbus2influxdb/blob/main/modbus2influxdb.py

    while not self.__stopper.is_set():

      # wait till parser has copied telegram content
      # ...we need the opposite of trigger.wait()...block when set; not available
      while self.__trigger.is_set():
        time.sleep(0.2)

      # wait specified waiting period
      while (time.time() < (self.__timelastread + self.__intervaltime)) and not self.__stopper.is_set():
        time.sleep(0.5)

      self.__telegram.clear()

      if not self.__client.is_socket_open():
        self.__client.connect()

      # add a counter as first field to the list
      self.__counter += 1
      self.__telegram.append(f"{self.__counter}")

      # Have for loop in try-except; exception will break out of for loop.
      try:
        for register in sma_registers:
          telegram = self.__readregister(register, unit=cfg.SMASLAVE)
          self.__telegram.append((register, telegram))
      except Exception as e:
        logger.warning(f"Connection exception {e}")

        # Wait a bit and the retry
        time.sleep(1.0)

        # start at top in while loop
        continue

      # Trigger that new telegram is available for MQTT
      self.__trigger.set()

      # for calculating the waiting period in next read sequence
      self.__timelastread = time.time()

    logger.debug("<<")

  def run(self):
    logger.debug(">>")
    try:
      self.__read_serial()
    except Exception as e:
      logger.error(f"While calling self.__sock.accept(), exception {e} occurred")

    finally:
      self.__stopper.set()
      self.__client.connect()

    logger.debug("<<")

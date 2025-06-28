#!/usr/bin/python3

# TODO
# MQTT multithreading
# Add NAN to standard headers


"""
 DESCRIPTION
   Read SMA solar converter
   Tested on raspberry pi4

  Worker threads:
  - SMA reader
  - SMA telegram parser to MQTT messages
  - MQTT client

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

  VERSIONS: See README.md
"""
__version__ = "2.0.0"
__author__ = "Hans IJntema"
__license__ = "GPLv3"

import signal
import socket
import time
import sys
import threading
import platform
import os
from typing import List
from dataclasses import dataclass


# ----------------------------------------------------------------------------------------------------------------------
# Local imports
# ----------------------------------------------------------------------------------------------------------------------
import config as cfg
import mqtt as mqtt
import sma_modbus as sma
import sma_parser as convert
import sma_sunnyboy_modbus_config as smamodbusconfig
from log import logger
logger.setLevel(cfg.loglevel)


# ----------------------------------------------------------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------------------------------------------------------
@dataclass
class CONSTANTS:
    LOCK_FILE_PREFIX: str = "\0"
    MQTT_STARTUP_DELAY: int = 1
    STATUS_TOPIC_SUFFIX: str = "/status"
    VERSION_TOPIC_SUFFIX: str = "/sw-version"
    EXIT_SUCCESS: int = 0
    EXIT_FAILURE: int = 1


# ----------------------------------------------------------------------------------------------------------------------
# Globals
# ----------------------------------------------------------------------------------------------------------------------
__exit_code = CONSTANTS.EXIT_FAILURE

# To flag that all worker threads (except mqtt) have to stop
t_threads_stopper = threading.Event()


# ----------------------------------------------------------------------------------------------------------------------
# SMAInverterManager
# ----------------------------------------------------------------------------------------------------------------------
class SMAInverterManager:
  """
  Manage the initialization, configuration, and lifecycle of SMA inverters,
  as well as their integration with MQTT clients and Modbus components.

  This class is responsible for setting up inverters and their components,
  managing the MQTT client, starting all involved threads, and providing
  methods for orderly shutdown and cleanup of resources.

  :ivar mqtt_client: Instance of configured MQTT client for communication.
  :ivar mqtt_stopper: Event object used to stop MQTT client operations.
  :type mqtt_stopper: threading.Event
  :ivar threads_stopper: Event object used to signal thread termination.
  :type threads_stopper: threading.Event
  :ivar list_of_inverters: List of inverter parser instances managed by the class.
  :type list_of_inverters: List[convert.TelegramParser]
  :ivar list_of_modbus_readers: List of Modbus reader instances associated with inverters.
  :type list_of_modbus_readers: List[sma.SMA_ModBus]
  :ivar list_of_telegrams: List of telegrams processed by inverters.
  :type list_of_telegrams: List
  """
  def __init__(self, stopper: threading.Event) -> None:
    logger.debug(">>")

    self.mqtt_client: mqtt.MQTTClient | None = None
    self.mqtt_stopper: threading.Event = threading.Event()
    self.threads_stopper: threading.Event = stopper
    self.list_of_inverters: List[convert.TelegramParser] = []
    self.list_of_modbus_readers: List[sma.SMA_ModBus] = []
    self.list_of_telegrams: List[List] = []

    logger.debug("<<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # __del__
  # --------------------------------------------------------------------------------------------------------------------
  def __del__(self) -> None:
    logger.debug(">>")
    logger.debug("<<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # initialize_mqtt
  # --------------------------------------------------------------------------------------------------------------------
  def initialize_mqtt(self) -> mqtt.MQTTClient:
    """Initialize and configure MQTT client"""
    mqtt_client = mqtt.MQTTClient(
      mqtt_broker=cfg.MQTT_BROKER,
      mqtt_port=cfg.MQTT_PORT,
      mqtt_client_id=cfg.MQTT_CLIENT_UNIQ,
      mqtt_qos=cfg.MQTT_QOS,
      mqtt_cleansession=True,
      mqtt_protocol=mqtt.MQTTv5,
      username=cfg.MQTT_USERNAME,
      password=cfg.MQTT_PASSWORD,
      mqtt_stopper=self.mqtt_stopper,
      worker_threads_stopper=self.threads_stopper
    )

    mqtt_client.will_set(topic=f"{cfg.MQTT_TOPIC_PREFIX}{CONSTANTS.STATUS_TOPIC_SUFFIX}",
                         payload="offline", qos=cfg.MQTT_QOS, retain=True)

    return mqtt_client

  # --------------------------------------------------------------------------------------------------------------------
  # setup_inverters
  # --------------------------------------------------------------------------------------------------------------------
  def setup_inverters(self, register_description: List) -> None:
    """Set up inverter instances and their associated components"""
    logger.debug(">>")

    for inverter_config in cfg.INVERTERS:
      logger.debug(f"inverter {cfg.INVERTERS}")
      logger.debug(f"Setup inverter {inverter_config}")

      trigger = threading.Event()

      telegram_list = []
      self.list_of_telegrams.append(telegram_list)

      modbus_reader = sma.SMA_ModBus(trigger=trigger,
                                     stopper=self.threads_stopper,
                                     inverter=inverter_config,
                                     telegram=telegram_list,
                                     list_of_registers=register_description)

      self.list_of_modbus_readers.append(modbus_reader)

      inverter_parser = convert.TelegramParser(
        trigger=trigger,
        stopper=self.threads_stopper,
        mqttclient=self.mqtt_client,
        telegram=telegram_list,
        invertername=inverter_config['name']
      )
      self.list_of_inverters.append(inverter_parser)

    logger.debug("<<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # start_components
  # --------------------------------------------------------------------------------------------------------------------
  def start_components(self) -> None:
    """Start all threads and set initial MQTT status"""
    logger.debug(">>")

    self.mqtt_client.start()

    for inverter, modbus_reader in zip(self.list_of_inverters, self.list_of_modbus_readers):
      inverter.start()
      modbus_reader.start()

    self.mqtt_client.set_status(f"{cfg.MQTT_TOPIC_PREFIX}{CONSTANTS.STATUS_TOPIC_SUFFIX}",
                                payload="online", retain=True)
    self.mqtt_client.do_publish(f"{cfg.MQTT_TOPIC_PREFIX}{CONSTANTS.VERSION_TOPIC_SUFFIX}",
                                message=f"main={__version__}; mqtt={mqtt.__version__}", retain=True)

    logger.debug("<<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # wait_till_done
  # --------------------------------------------------------------------------------------------------------------------
  def wait_till_done(self) -> None:
    """Wait till last modbus reader thread exits"""
    logger.debug(">>")

    for modbus_reader in self.list_of_modbus_readers:
      modbus_reader.join()

    logger.debug("<<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # shutdown
  # --------------------------------------------------------------------------------------------------------------------
  def shutdown(self) -> None:
    """Gracefully shutdown all components"""
    logger.debug(">>")

    self.threads_stopper.set()
    self.mqtt_client.set_status(f"{cfg.MQTT_TOPIC_PREFIX}{CONSTANTS.STATUS_TOPIC_SUFFIX}", "offline", retain=True)

    # Allow for some time for MQTT to broadcast buffers
    time.sleep(CONSTANTS.MQTT_STARTUP_DELAY)
    self.mqtt_stopper.set()

    logger.debug("<<")
    return


# ----------------------------------------------------------------------------------------------------------------------
# check_single_instance
# ----------------------------------------------------------------------------------------------------------------------
def check_single_instance() -> bool:
  """
  Checks if the script is already running to enforce a single instance of the application.

  The function ensures that only one instance of the script runs at a time for Linux platforms.
  It uses a UNIX socket file as a lock mechanism to identify whether an instance of the script
  is already running. If a lock already exists, the function returns `False`, indicating that
  another instance is active. If no lock exists, the function sets up a lock and allows
  the script to proceed.

  :return: True if no other instance is running, False otherwise
  :rtype: bool
  """
  logger.debug(">>")

  if sys.platform == "linux":
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    lockfile = f"{CONSTANTS.LOCK_FILE_PREFIX}{script_name}_lockfile"
    try:
      s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
      s.bind(lockfile)
      logger.info(f"Starting {__file__}; version = {__version__}")
    except OSError as err:
      logger.info(f"{lockfile} already running. Exiting; {err}")
      return False

  logger.debug("<<")
  return True


# ----------------------------------------------------------------------------------------------------------------------
# exit_gracefully
# ----------------------------------------------------------------------------------------------------------------------
def exit_gracefully(signum, _stackframe) -> None:
  """
  Handle termination signals gracefully by setting an exit code and triggering the
  termination of all active threads. This function logs the signal received, updates
  the global exit code, and stops threads using a shared threading event.

  :param signum: Signal number indicating the type of signal that was received.
  :type signum: int
  :param _stackframe: Current stack frame at the time the signal was received.
  :type _stackframe: FrameType
  :return: None
  """

  logger.debug(f"Signal {signum} {signal.Signals(signum).name}: >>")

  # status=0/SUCCESS
  global __exit_code
  __exit_code = CONSTANTS.EXIT_SUCCESS

  t_threads_stopper.set()
  logger.info("<<")

  return None


# ----------------------------------------------------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------------------------------------------------
def main() -> None:
  """
  The main function initializes and manages the SMA inverter components, MQTT client,
  and processes the Sunnyboy modbus configuration data. It ensures a seamless setup,
  operation, and graceful shutdown of the overall system.

  This function starts by setting up required managers and extracting register map
  descriptions for the inverter. Through a series of component initialization steps, it
  establishes the necessary resources and configuration, facilitates component execution,
  and handles termination procedures. Errors during this workflow are logged for
  debugging and tracking purposes.

  :raises Exception: If an error occurs during any part of the workflow.
  :return: This function does not return any value.
  """
  logger.debug(">>")

  manager = SMAInverterManager(stopper=t_threads_stopper)

  try:
    # Create instance to read Sunnyboy modbus configuration file
    sunnyboy = smamodbusconfig.SunnyboyGetRegisterMap(stopper=t_threads_stopper,
                                                      definition_file=cfg.SMA_REGISTER_DEFINITION_FILE)

    register_description = sunnyboy.get_register_description()

    manager.mqtt_client = manager.initialize_mqtt()
    manager.setup_inverters(register_description)
    manager.start_components()
    manager.wait_till_done()
    manager.shutdown()

  except Exception as e:
    logger.error(f"Error in main execution: {e}")
    manager.shutdown()

  logger.debug("<<")
  return


# ----------------------------------------------------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------------------------------------------------
"""
Sets exitcode to SUCCESS when gracefuilly exited (CTRL-C, systemd stop, kill process)
Sets exitcode to FAILURE when main() returns (on error) 
"""
if __name__ == '__main__':
  logger.debug("__main__: >>")

  # CTRL-C
  signal.signal(signal.SIGINT, exit_gracefully)

  # Kill process / systemd stop / reload
  signal.signal(signal.SIGTERM, exit_gracefully)

  if platform.system() == 'Linux':
    signal.signal(signal.SIGHUP, exit_gracefully)

  # Check if another instance is not running and start main program
  if check_single_instance():
    main()

  # Exit code is default FAILURE.
  # Will be SUCCESS if stopped via CTRL-C, systemd stop, kill process.
  logger.debug(f"__main__: exit_code = {__exit_code} <<")
  sys.exit(__exit_code)

# ----------------------------------------------------------------------------------------------------------------------
# EOF
# ----------------------------------------------------------------------------------------------------------------------

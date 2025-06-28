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

from typing import Any, Dict, List
import threading
import time
from socket import error as socket_error
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException
import tenacity
import config as cfg

# Logging
import __main__
import logging
import os
logger = logging.getLogger(f"{os.path.splitext(os.path.basename(__main__.__file__))[0]}.{__name__}")


# ----------------------------------------------------------------------------------------------------------------------
# class SMA_ModBus
# ----------------------------------------------------------------------------------------------------------------------
class SMA_ModBus(threading.Thread):
  """
  A class to handle Modbus communication with an SMA inverter.
  It runs as a separate thread to continuously read data.
  """

  DEFAULT_UNIT_ID = 3
  DEFAULT_RETRIES = 3

  def __init__(self, trigger: threading.Event,
               stopper: threading.Event,
               inverter: Dict[str, Any],
               telegram: List[Any],
               list_of_registers: List[Dict[str, Any]]) -> None:
    """
    :param threading.Event() trigger: signals that new telegram is available
    :param threading.Event() stopper: stops thread
    :param dict() inverter: dictionary of inverter attributes (e.g., ip, port, name)
    :param list() telegram: list to store sma key:value pairs, shared with other components
    :param list() list_of_registers: list of register definitions to be read
    """
    logger.debug(f"{inverter['name']}: >>")

    super().__init__()
    self.__trigger = trigger    # Event to signal data availability
    self.__stopper = stopper    # Event to signal thread termination
    self.__inverter = inverter  # Inverter configuration details
    self.__telegram = telegram  # Shared list for storing read data
    self.__timelastread = 0     # Timestamp of the last successful read

    # Calculate interval based on configured frequency
    self.__intervaltime = 3600/cfg.SMAMQTTFREQUENCY
    self.__counter = 0          # Counter for telegrams
    self.__client = None        # Modbus TCP client instance
    self.__invertername = inverter["name"]  # Cache inverter name for logging
    self.__register_description = list_of_registers  # List of registers to query

    self.__unitid = self.DEFAULT_UNIT_ID  # Default Modbus unit ID, SMA will be queried for actual

    logger.debug(f"{self.__invertername}: <<")

  # --------------------------------------------------------------------------------------------------------------------
  # __del__
  # --------------------------------------------------------------------------------------------------------------------
  def __del__(self) -> None:
    """ Destructor to ensure the Modbus client connection is closed. """
    logger.debug(f"{self.__invertername}: >>")
    if self.__client and self.__client.is_socket_open():  # Check if client exists and is open
      self.__client.close()
    logger.debug(f"{self.__invertername}: <<")

  # --------------------------------------------------------------------------------------------------------------------
  # __create_modbus_tcp_client
  # --------------------------------------------------------------------------------------------------------------------
  @tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=1, max=120),
                  stop=tenacity.stop_after_attempt(cfg.RETRY))
  def __create_modbus_tcp_client(self) -> None:
    """
    Creates and initializes the Modbus TCP client.
    Uses tenacity to retry on failure.
    Propagates exceptions if retries are exhausted.
    :return: None
    """
    logger.debug(f"{self.__invertername}: >>")

    try:
      # Initialize the Modbus TCP client with configuration from the inverter dict
      self.__client = ModbusTcpClient(self.__inverter["ip"],
                                      timeout=self.__inverter["smatimeout"],
                                      retries=3,  # pymodbus internal retries
                                      port=self.__inverter["smaport"])
    except ModbusException as e:
      logger.warning(f"{self.__invertername}: {type(e).__name__}: {str(e)}")
      raise ModbusException(e)  # Re-raise to be caught by tenacity or caller
    except Exception as e:
      logger.error(f"{self.__invertername}: Unspecified Exception {e}")
      raise Exception(e)  # Re-raise to be aught by tenacity or caller

    logger.debug(f"{self.__invertername}: ModbusTcpClient is created = {self.__client}: <<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # __get_modbus_unitid
  # --------------------------------------------------------------------------------------------------------------------
  @tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=1, max=120),
                  stop=tenacity.stop_after_attempt(cfg.RETRY))
  def __get_modbus_unitid(self) -> None:
    """
    Connects to the inverter and reads modbus register 42109 to determine the Modbus Unit ID.
    Uses tenacity to retry on failure.
    Propagates exceptions if retries are exhausted.
    :return: None
    """
    logger.debug(f"{self.__invertername}: >>")

    try:
      if not self.__client.is_socket_open():  # Ensure client is connected
        self.__client.connect()

      # Read holding registers related to device identification
      __unit_id_raw = self.__client.read_holding_registers(address=self.__inverter["smaunitidregister"],
                                                           count=4, slave=1)

      # Check if the read was successful
      if __unit_id_raw.isError():
        logger.warning(f"{self.__invertername}: Error reading unit ID registers: {__unit_id_raw}")
        raise ModbusException("Failed to read unit ID registers")

      # Extract and combine parts of the serial number and unit ID
      self.__loid = __unit_id_raw.registers[0]
      self.__hiid = __unit_id_raw.registers[1]
      self.__hiid = self.__hiid << 16  # Shift high part to combine for full serial
      self.__physical_serial = self.__loid + self.__hiid
      self.__susyid = __unit_id_raw.registers[2]  # Susy ID (System ID)
      self.__unitid = __unit_id_raw.registers[3]  # Actual Modbus Unit ID to use for subsequent requests
    except ModbusException as e:
      logger.warning(f"{self.__invertername}: {type(e).__name__}: {str(e)}")
      raise ModbusException(e)  # Re-raise to be caught by tenacity or caller
    except Exception as e:
      logger.error(f"{self.__invertername}: Unspecified Exception {e}")
      raise Exception(e)  # Re-raise to be caught by tenacity or caller

    logger.debug(f"{self.__invertername}: ModBus Unit ID = {self.__unitid}: <<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # __read_raw_modbus_register
  # --------------------------------------------------------------------------------------------------------------------
  @tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=1, max=120),
                  stop=tenacity.stop_after_attempt(cfg.RETRY))
  def __read_raw_modbus_register(self,
                                 modbus_address_start: int,
                                 modbus_address_size: int,
                                 datatype: ModbusTcpClient.DATATYPE,
                                 encoding: str) -> None | int | List:
    """
    Read RAW modbus register(s) and convert them to the specified datatype.
    Uses tenacity to retry on failure.

    :param modbus_address_start: Starting address of the Modbus register.
    :param modbus_address_size: Number of registers to read.
    :param datatype:  Pymodbus DATATYPE constant for conversion.
    :param encoding:  String encoding (e.g., "utf-8") if datatype is STRING.
    :return: Converted modbus register value(s) or None on specific errors.
    """

    logger.debug(f"{self.__invertername}: >>")

    # Ensure the client is connected before attempting to read
    if (not self.__client.is_socket_open()) or (not self.__client.connected):
      logger.debug(f"{self.__invertername}: Client is not connected....connect")
      self.__client.connect()

    try:
      # Read input registers from the inverter using the determined unit ID
      modbus_result_raw = self.__client.read_input_registers(address=modbus_address_start,
                                                             count=modbus_address_size,
                                                             slave=self.__unitid)

      # Check for Modbus errors in the response
      if modbus_result_raw.isError():
        logger.debug(f"{self.__invertername}: Modbus error on read: {modbus_result_raw}")
        # This can happen when the inverter is off (e.g., at night)
        return None

      # Convert the raw register values to the desired Python datatype
      modbus_result = self.__client.convert_from_registers(registers=modbus_result_raw.registers,
                                                           data_type=datatype,
                                                           word_order="big",
                                                           string_encoding=encoding)

    except ModbusException as e:
      # This exception can happen when it is dark and DC side is off, or other Modbus issues.
      logger.debug(f"{self.__invertername}: Read/Convert registers: "
                   f"Address={modbus_address_start}; {e}")
      return None  # Return None to indicate a non-critical read failure (e.g. inverter off)

    except socket_error as e:
      logger.warning(f"{self.__invertername}:SocketError {e}; close connection and retry")
      # For socket errors, it's often good to close and let tenacity handle reconnection.
      self.__client.close()

      # self.__client.connect() # Let tenacity handle the reconnect on retry
      self.__client.connect()
      raise socket_error(e)  # Re-raise to trigger tenacity retry

    except Exception as e:
      logger.error(f"{self.__invertername}: Unspecified exception: "
                   f"Address={modbus_address_start}; {e}")
      return None

    return modbus_result

  # --------------------------------------------------------------------------------------------------------------------
  # __read_modbus_register
  # --------------------------------------------------------------------------------------------------------------------
  def __read_modbus_register(self, register_dict: Dict[str, Any]) -> None | int | str:
    """
    Reads a specific Modbus register based on its definition dictionary.
    Handles datatype mapping and NaN value checks.

    :param register_dict: dict containing SMA register definition (MODBUS_ADDRESS, REGISTER_SIZE, etc.)
    :return:  Formatted register value or None if read fails or value is NaN.
              No formatting based on datatype (eg decimal point) is done here, raw converted value.
    """
    logger.debug(f"{self.__invertername}: REGISTER={register_dict}: >>")

    modbus_address_start = register_dict['MODBUS_ADDRESS']
    modbus_address_size = register_dict['REGISTER_SIZE']

    logger.debug(f"{self.__invertername}: modbus_address_start={modbus_address_start}")
    logger.debug(f"{self.__invertername}: modbus_address_size={modbus_address_size}")

    encoding = None  # Default encoding
    # Map string representation of datatype to pymodbus DATATYPE constants and define NaN values
    # Consider using a dictionary for this mapping for cleaner code if more types are added.
    match register_dict['MODBUS_DATATYPE']:
      case "S16":
        datatype = self.__client.DATATYPE.INT16
        nanvalue = 0x8000  # Standard NaN for signed 16-bit
      case "U16":
        datatype = self.__client.DATATYPE.UINT16
        nanvalue = 0xFFFF  # Standard NaN for unsigned 16-bit
      case "S32":
        datatype = self.__client.DATATYPE.INT32
        nanvalue = 0x80000000  # Standard NaN for signed 32-bit
      case "U32":
        datatype = self.__client.DATATYPE.UINT32
        if register_dict['MODBUS_DATAFORMAT'] == "TAGLIST":
          nanvalue = 0xFFFFFD  # Specific NaN for U32 TAGLIST
        else:
          nanvalue = 0xFFFFFFFF  # Standard NaN for unsigned 32-bit
      case "U64":
        datatype = self.__client.DATATYPE.UINT64
        nanvalue = 0xFFFFFFFFFFFFFFFF  # Standard NaN for unsigned 64-bit
      case "STR32" | "STR16":  # Handle different string lengths
        datatype = self.__client.DATATYPE.STRING
        encoding = "utf-8"  # Assume UTF-8 for strings
        nanvalue = '\x00'  # Null character often indicates empty/NaN for strings from SMA
                           # Could also be an empty string "" depending on device.
      case _:
        logger.error(f"{self.__invertername}: Illegal datatype {register_dict['MODBUS_DATATYPE']}")
        register_dict["NAN"] = None  # Set NAN to None if datatype is unknown
        return None

    register_dict["NAN"] = nanvalue  # Store the determined NaN value in the register dict for reference

    try:
      # Call the raw reading function
      modbus_result = self.__read_raw_modbus_register(modbus_address_start=modbus_address_start,
                                                      modbus_address_size=modbus_address_size,
                                                      datatype=datatype,
                                                      encoding=encoding)

    except tenacity.RetryError as e:
      # If tenacity retries are exhausted for __read_raw_modbus_register
      logger.error(f"{self.__invertername}: Max retries exceeded for register {register_dict['CHANNEL']}: {e}")
      self.__stopper.set()  # Signal the main loop to stop as communication is persistently failing
      return None

    logger.debug(f"{self.__invertername}: Registers: {register_dict['CHANNEL']}={modbus_result}; "
                 f"TYPE = {type(modbus_result)}, "
                 f"NaN = {register_dict['NAN']}")

    # Post-processing of the modbus_result
    # Pymodbus convert_from_registers might return a list or a scalar.
    # This behavior might depend on the count and datatype.
    if modbus_result is None:  # If __read_raw_modbus_register returned None (e.g. Modbus error, inverter off)
      return None

    if isinstance(modbus_result, list):
       # This case might occur if `count` in read_input_registers > 1 and datatype isn't string.
       # Or if convert_from_registers behaves unexpectedly for certain types/counts.
       # Typically, for single values, a scalar is expected. If a list is returned for a single register,
       # it might indicate an issue or a multi-register value that wasn't fully handled by convert_from_registers
       # as a single entity (e.g. a custom struct not directly supported).
       # For now, assuming the first element is the relevant one if it's a list of one.
       logger.warning(f"{self.__invertername}: List returned, scalar expected for ModBus register: "
                      f"{register_dict['CHANNEL']}={modbus_result}. Taking first element if available.")
       return modbus_result[0] if modbus_result else None  # Return first element, or None if list is empty

    elif isinstance(modbus_result, int):  # Check for numeric types
      # Check if returned values are NaN (not a number)
      # This can happen when it is dark, DC side of SMA is off
      if modbus_result == register_dict["NAN"] or abs(modbus_result) == register_dict["NAN"]:
        logger.debug(f"{self.__invertername}: NaN value {modbus_result} detected for {register_dict['CHANNEL']}. "
                     f"Returning None.")
        return None
      return modbus_result

    elif isinstance(modbus_result, str):
      # Check for string NaN value or for all nulls.
      if modbus_result == register_dict["NAN"] or not str(modbus_result).strip('\x00'):
        logger.debug(f"{self.__invertername}: NaN string value detected for {register_dict['CHANNEL']} Returning None.")
        return None
      return str(modbus_result)  # Ensure it's a string

    else:
      logger.error(f"{self.__invertername}: Unknown TYPE: {type(modbus_result)}; "
                   f"Register: {register_dict['CHANNEL']}={modbus_result}")
      return None

  # --------------------------------------------------------------------------------------------------------------------
  # __read_telegram
  # --------------------------------------------------------------------------------------------------------------------
  def __read_telegram(self) -> None:
    """
      Continuously reads SMA telegrams (a set of registers) at a defined interval.
      Stores the telegram in the shared `self.__telegram` list.
      Sets the `self.__trigger` event to signal that a new telegram is available.

      This method runs in a loop until `self.__stopper` event is set.
      Handles retries for individual register reads via __read_modbus_register.
      If __read_modbus_register signals a persistent failure (by setting __stopper), this loop will terminate.
    """
    logger.debug(f"{self.__invertername}: >>")

    while not self.__stopper.is_set():

      # Wait until the consumer (e.g., sma_parser) has processed the current telegram
      # and cleared the trigger.
      while self.__trigger.is_set() and not self.__stopper.is_set():  # Also check stopper here
        time.sleep(0.2)  # Short sleep to avoid busy-waiting

      if self.__stopper.is_set(): break  # Exit if stopped during wait

      # Wait for the specified interval before reading the next telegram.
      # This loop ensures that reads happen at most once per __intervaltime.
      while (time.time() < (self.__timelastread + self.__intervaltime)) and not self.__stopper.is_set():
        time.sleep(0.5)

      if self.__stopper.is_set(): break  # Exit if stopped

      # Clear the shared telegram list to prepare for new data
      self.__telegram.clear()

      # Increment and add a counter as the first field to the telegram (useful for tracking)
      self.__counter += 1
      self.__telegram.append(f"{self.__counter}")

      # Iterate through the list of registers to be read
      for register in self.__register_description:
        if self.__stopper.is_set(): break  # Check stopper before each read

        # Append register value to telegram
        value = self.__read_modbus_register(register)
        self.__telegram.append((register, value))

      # Signal that a new telegram is available for processing
      self.__trigger.set()

      # For calculating the waiting period in next read sequence
      self.__timelastread = time.time()

      logger.debug(f"{self.__invertername}: TELEGRAM={self.__telegram}: <<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # run
  # --------------------------------------------------------------------------------------------------------------------
  def run(self) -> None:
    """
    Main method for the thread.
    Initializes the Modbus client and unit ID, then starts the telegram reading loop.
    Handles initial connection errors and ensures cleanup.
    """
    logger.debug(f"{self.__invertername}:>>")

    try:
      # Attempt to create the Modbus client and get the unit ID.
      # These methods have their own retry logic.
      self.__create_modbus_tcp_client()
      self.__get_modbus_unitid()

    except tenacity.RetryError as e:
      # If initial setup (client creation or unit ID fetch) fails after all retries
      logger.warning(f"{self.__invertername}: Failed to initialize Modbus client after multiple retries: {e}")
      self.__stopper.set()  # Signal that this thread cannot proceed
      if self.__client:  # Ensure client is closed if it was partially created
        self.__client.close()
      logger.debug(f"{self.__invertername}: Thread stopping due to initialization failure <<")
      return  # Exit the run method, stopping the thread

    except Exception as e:  # Catch any other unexpected exception during init
      logger.error(f"{self.__invertername}: Unexpected error during initialization: {e}")
      self.__stopper.set()
      if self.__client and self.__client.is_socket_open():
        self.__client.close()
      logger.debug(f"{self.__invertername}: Thread stopping due to unexpected initialization error <<")
      return

    # Start the main loop for reading telegrams.
    # This loop will run until self.__stopper.is_set() is true.
    try:
      logger.info(f"{self.__invertername}: Starting telegram reading loop.")
      self.__read_telegram()
    except Exception as e:
      # Catch any unexpected exceptions during the __read_telegram loop
      # This is a fallback; ideally, __read_telegram should handle its exceptions
      # or propagate them in a way that __read_modbus_register's tenacity can catch them.
      logger.error(f"{self.__invertername}: Unspecified exception in __read_telegram loop: {e}. Thread will stop.")
      # No explicit restart here, as per original comment "Restarting" was there but no code.
      # For robustness, a restart mechanism for the thread itself might be managed by a higher-level component.

    finally:
        # Cleanup actions to be performed when the thread is exiting,
        # either normally (stopper set) or due to an exception.
        logger.info(f"{self.__invertername}: Exiting run method. Cleaning up.")
        self.__stopper.set()  # Ensure stopper is set so other parts know this thread is stopping/stopped

        if self.__client and self.__client.is_socket_open():  # Check if client exists and is open
            logger.debug(f"{self.__invertername}: Closing Modbus client connection.")
            self.__client.close()
        logger.debug(f"{self.__invertername}: Thread finished <<")

    logger.debug(f"{self.__invertername}: <<")
    return

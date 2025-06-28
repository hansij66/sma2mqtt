"""
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

"""
DESIGN
1. Read SMA Modbus definition file and store in  self.__full_register_map (SMA definition table)

  [["Channel", "Object Type", "Name (SMA Speedwire)", ................],
   ["DcMs.Watt", "Measured value", "DC power input", .................],
   ["DcMs.Vol", "Measured value", "DC voltage input", ................],
   [..................................................................]]

2. Extract headers from SMA definition table and store in self.__headers and remove first row (headers)
3. Convert verbose headers (e.g. Object Type) labels to short capitalized headers, e.g. OBJECT_TYPE=1
3. Extract selected register descriptions which are specified in config.py
4. Convert selected registers to a dict
5. Return dict

"""

import config as cfg
from typing import List, Dict, Any
from html_table_parser import HTMLTableParser
import threading

# Logging
import __main__
import logging
import os
logger = logging.getLogger(f"{os.path.splitext(os.path.basename(__main__.__file__))[0]}.{__name__}")


# Table header definition as of specification V16,
# Mapping of 'verbose' headers fields to dict keys used in this program
MODBUS_HEADERS_MAP = {
  'Channel': 'CHANNEL',
  'Object Type': 'OBJECT_TYPE',
  'Name (SMA Speedwire)': 'REGISTER_DESCRIPTION',
  'Read Level': 'READ_LEVEL',
  'Group': 'GROUP',
  'Value Range / Unit / Set values': 'UNIT',
  'Default value': 'DEFAULT_VALUE',
  'Step Size': 'MODBUS_STEPSIZE',
  'Write Level': 'WRITE_LEVEL',
  'Grid Guard': 'GRIDGUARD',
  'SMA Modbus Register Adress': 'MODBUS_ADDRESS',
  'Number of contiguous SMA Modbus Registers': 'REGISTER_SIZE',
  'SMA Modbus Data Type': 'MODBUS_DATATYPE',
  'SMA Modbus Data Format': 'MODBUS_DATAFORMAT',
  'SMA Modbus Access': 'MODBUS_ACCESS',
  # 'Not a Number': 'NAN'  # Not a Number; not part of definition file, but required for this program.
}

# These are the fields used (and can be overriden in config.py)
REQUIRED_REGISTER_FIELDS = [
  'CHANNEL',
  'MODBUS_ADDRESS',
  'REGISTER_SIZE',
  'MODBUS_DATATYPE',
  'MODBUS_DATAFORMAT',
  # 'NAN'  # Not a Number; not part of definition file, but required for this program. Will be calculated in sma_modbus.py
]


# ----------------------------------------------------------------------------------------------------------------------
# class Sunnyboy_Get_Register_Map
# ----------------------------------------------------------------------------------------------------------------------
class SunnyboyGetRegisterMap:
  """
  Read SMA Modbus definition file
  Extract register descriptions which are specified in config.py
  """

# ----------------------------------------------------------------------------------------------------------------------
# __init__
# ----------------------------------------------------------------------------------------------------------------------
  def __init__(self, stopper: threading.Event(), definition_file: str) -> None:
    """
    Not fully happy with design....can it be more simple & elegant?

    Initialize the register map parser.
    :param threading.Event() stopper: stops thread
    :param string definition_file: name of SMA modbus definition file
    """
    logger.debug(f">>")

    self.__stopper = stopper
    self.__definition_file = definition_file

    # Table of all registers (list of lists)
    # aka "SMA definition table"
    self.__full_register_map: List[List[str]] = []

    # List of headers of SMA definition table
    self.__headers: List[str] = []

    # Dict: Mapping of Header LABELS to an INDEX; eg CHANNEL=0, OBJECT_TYPE=1
    # This map is used to extract required attributes from SMA definition table.
    self.__header_index_map: Dict[str, int] = {}

    # Table of requested registers (list of lists) - per MODBUSREGISTERS in config.py
    # Includes overrides per MODBUSREGISTERS applied to specific fields.
    self.__requested_registers: List[List[str]] = []

    # list of dicts; dict describes modbus register
    self.__register_map_dict: List[Dict[str, Any]] = []

    # Read SMA ModBus definition file; Exit if file is not found
    try:
      self.__full_register_map = self.__read_definition_file()

      # Get headers of SMA definition table
      self.__headers = self.__full_register_map.pop(0)
      # self.__headers.append('Not a Number')

      # Check if headers of definition file meets expected format (version V16)
      self.__validate_headers()

      # Create mapping of header labels to an index, eg CHANNEL=0, OBJECT_TYPE=1
      self.__create_header_index_mapping()

      # Create list of requested reqisters and required attributes
      self.__process_registers()

      # Remove duplicate register definitions
      self.__remove_duplicates()

      # Convert list of lists (registers) to list of dicts (registers)
      self.__convert_to_dict()
    except FileNotFoundError as error:
      logger.error(f"{error}...Exiting program. Check config.py and SMA ModBus definition file.")
      self.__stopper.set()
    except Exception as e:
      logger.error(f"Unexpected Exception: {e}")
      self.__stopper.set()

    logger.debug(f"<<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # __create_header_index_mapping
  # --------------------------------------------------------------------------------------------------------------------
  def __create_header_index_mapping(self) -> None:
    """Create mapping between header labels and their indices."""

    # According to Clause 3.5. this is same code as below
    # self.__header_index_map = {value: idx for idx, value in enumerate(MODBUS_HEADERS_MAP.values())}

    # Create mapping of labels to list index, eg CHANNEL=0, OBJECT_TYPE=1
    i = 0
    for value in MODBUS_HEADERS_MAP.values():
      self.__header_index_map[value] = i
      i += 1

    return

  # --------------------------------------------------------------------------------------------------------------------
  # __process_registers
  # --------------------------------------------------------------------------------------------------------------------
  def __process_registers(self) -> None:
    """Process registers according to configuration."""
    for row in self.__full_register_map:
      # Add NAN element to row; not part of definition file, but required for this program.
      # row.append(None)

      # Ensure that int's & float's are stored as int's and not as strings
      row[self.__header_index_map['MODBUS_ADDRESS']] = int(row[self.__header_index_map['MODBUS_ADDRESS']])
      row[self.__header_index_map['REGISTER_SIZE']] = int(row[self.__header_index_map['REGISTER_SIZE']])

      # Ensure that MODBUS_STEPSIZE is stored as float and not as string
      # If MODBUS_STEPSIZE is not specified (typically '-'), set to 0.0
      try:
        row[self.__header_index_map['MODBUS_STEPSIZE']] = float(row[self.__header_index_map['MODBUS_STEPSIZE']])
      except ValueError:
        row[self.__header_index_map['MODBUS_STEPSIZE']] = None

      # Get MODBUS_ADDRESS of register referred to in row
      modbus_address = row[self.__header_index_map['MODBUS_ADDRESS']]

      # Check if MODBUS_ADDRESS is in config.py
      for register in cfg.MODBUSREGISTERS:
        # Check if mandatory attribute MODBUS_ADDRESS is present
        if 'MODBUS_ADDRESS' not in register:
          logger.error(f"Missing MODBUS_ADDRESS in config: {register}")
          continue

        # If row matches a MODBUSREGISTERS in config.py:
        # Modify optional attributes if specified in MODBUSREGISTERS
        # Add register to list of requested registers
        if register['MODBUS_ADDRESS'] == modbus_address:
          for key in REQUIRED_REGISTER_FIELDS:
            if key in register.keys():
              # Modify optional attribute
              row[self.__header_index_map[key]] = register[key]

          self.__requested_registers.append(row)

    return

  # --------------------------------------------------------------------------------------------------------------------
  # __read_definition_file
  # --------------------------------------------------------------------------------------------------------------------
  def __read_definition_file(self) -> List[List[str]]:
    """
    Read and parse the ModBus definition file.

    Returns: List of registers (as list), including headers.
    Raises: FileNotFoundError
    """
    file_path = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.__definition_file))
    logger.debug(f"Reading ModBus definition file: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as html_file:
      parser = HTMLTableParser()
      parser.feed(html_file.read())
      parser.close()
      return parser.tables[0]

  # --------------------------------------------------------------------------------------------------------------------
  # __convert_to_dict
  # --------------------------------------------------------------------------------------------------------------------
  def __convert_to_dict(self) -> None:
    """Convert register data to dictionary format."""
    logger.debug(f">>")

    for row in self.__requested_registers:
      __register_dict = {}
      for fields in REQUIRED_REGISTER_FIELDS:
        __register_dict[fields] = row[self.__header_index_map[fields]]

      self.__register_map_dict.append(__register_dict)

    logger.debug(f"<<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # __remove_duplicates
  # --------------------------------------------------------------------------------------------------------------------
  def __remove_duplicates(self) -> None:
    logger.debug(f">>")
    self.__requested_registers.sort()

   # This is same? (Claude 3.5 Sonnet) as code below
   # self.__requested_registers = list(dict.fromkeys(map(tuple, self.__requested_registers)))

    temp_list = []
    for element in self.__requested_registers:

      # Only add element if it is not already in the list.
      if element not in temp_list:
        temp_list.append(element)
    self.__requested_registers = temp_list

    logger.debug(f"<<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # __check_version_consistency
  # --------------------------------------------------------------------------------------------------------------------
  def __validate_headers(self) -> None:
    """
    Check if SMA Modbus definition file is consistent with
    definitions in this parser.
    Based on V16

    Verify whether there are no changes in headers
    You can ignore spelling, but not order / extra columns / ....
    """
    # Get SMA Headers without mapping to LABELS, e.g. only keys
    logger.debug(f">>")
    expected_headers = list(MODBUS_HEADERS_MAP.keys())

    if self.__headers != expected_headers:
      logger.error(f"{self.__definition_file} Headers do not match expected V16 format")
      logger.error(f"DIFF = {list(set(self.__headers) - set(expected_headers))} vs "
                   f"{list(set(expected_headers) - set(self.__headers))}")

      for i in range(len(expected_headers)):
        # Compare corresponding elements of both lists
        if self.__headers[i] != expected_headers[i]:
          logger.debug(f"Mismatch between element {i}: {self.__headers[i]} vs {expected_headers[i]}")

    logger.debug(f"<<")
    return

  # --------------------------------------------------------------------------------------------------------------------
  # get_register_description
  # --------------------------------------------------------------------------------------------------------------------
  def get_register_description(self) -> List[Dict[str, Any]]:
    return self.__register_map_dict

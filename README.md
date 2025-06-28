# SMA SunnyBoy MQTT
MQTT client for SMA Sunnyboy solar inverter (Tested with SBx-AV41), with TCP modbus access.. 
Supports multiple Sunnyboys simultaneously
Written in Python 3.x
The inverter(s) has to be connected to the LAN network where this script is running.

## Install
* Install requirements
* For Debian, create a Python VENV environment before using pip3
* Adapt paths in systemd files
* Optionally: check for updated modbus/paramterlist_en.html; see modbus/README.txt

## Usage:
* Copy `systemd/sma-mqtt.service` to `/etc/systemd/system`
* Adapt path in `sma-mqtt.service` to your install location (default: `/opt/iot/sma`)
* Copy `config.rename.py` to `config.py` and adapt for your configuration.
* `sudo systemctl enable sma-mqtt`
* `sudo systemctl start sma-mqtt`

Use http://mqtt-explorer.com/ to test & inspect MQTT messages

## Requirements
See requirements.txt for version requirements
* paho-mqtt
* pymodbus
* pymodbustcp
* python 3.x
* html-table-parser-python3
* tenacity

Tested under Linux and Windows11 (except for the systemd part)

## Licence
GPL v3

## Versions
2.0.0
* Support for read out of multiple SMA Sunnyboy's
* Register description based on official SMA modbus parameter list
* Removed Homeassistant auto discovery, as HA has native SMA client support under devices

1.3.0
* Adapted systemd for venv (PIP3 libraries in Debian)

1.1.2:
* Fix exit code (SUCCESS vs FAILURE)

1.1.0:
* Initial version on github

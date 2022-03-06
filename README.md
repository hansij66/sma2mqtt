# SMA SunnyBoy MQTT
MQTT client for SMA solar inverter (SB5-AV41). Written in Python 3.x
The inverter has to be connected to the LAN network where this script is running.

Includes Home Assistant MQTT Auto Discovery. (But HA has its own native SMA integration)
## Usage:
* Copy `systemd/sma-mqtt.service` to `/etc/systemd/system`
* Adapt path in `sma-mqtt.service` to your install location (default: `/opt/iot/sma`)
* Copy `config.rename.py` to `config.py` and adapt for your configuration (minimal: mqtt ip, username, password)
* `sudo systemctl enable sma-mqtt`
* `sudo systemctl start sma-mqtt`

Use
http://mqtt-explorer.com/
to test & inspect MQTT messages

## Requirements
* paho-mqtt
* pymodbus
* python 3.x

Tested under Linux; there is no reason why it does not work under Windows.

## Licence
GPL v3

## Versions
1.1.0:
* Initial version on github

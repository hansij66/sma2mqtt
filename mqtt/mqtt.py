"""
  MQTT class using paho-mqtt

  https://github.com/eclipse/paho.mqtt.python/blob/master/src/paho/mqtt/client.py
  https://www.eclipse.org/paho/index.php?page=clients/python/index.php
  http://www.steves-internet-guide.com/mqtt-clean-sessions-example/
  http://www.steves-internet-guide.com/python-mqtt-client-changes/
  http://www.steves-internet-guide.com/mqttv5/
  https://www.hivemq.com/blog/mqtt5-essentials-part6-user-properties/
  https://cedalo.com/blog/configuring-paho-mqtt-python-client-with-examples/

  v1.0.0: initial version
  v1.0.1: add last will
  V1.1.0: 31-7-2022: Add subscribing to MQTT server
  v1.1.3: on_disconnect rc=1 (out of memory) stop program
  v1.1.4: Add test for subscribing; whether message queue is set
  V1.1.5: Fix MQTT_ERR_NOMEM
  v1.1.6: Add clean session
  v2.0.0: Parameterize clean session; remove mqtt-rate

  LIMITATIONS
  * Only transport = TCP supported; websockets is not supported
  * Clean_session and clean-start partially implemented (not relevant for publishing clients)

  Class ReasonCodes:
  MQTT version 5.0 reason codes class.
  See ReasonCodes Names for a list of possible numeric values along with their
  names and the packets to which they apply.


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

import time
import threading
import random
import string
import socket
import paho.mqtt.client as mqtt_client
import paho.mqtt as paho_mqtt
from pkg_resources import parse_version

# Logging
import __main__
import logging
import os
script = os.path.basename(__main__.__file__)
script = os.path.splitext(script)[0]
logger = logging.getLogger(script + "." + __name__)

# TODO
# MQTT_ERR_NOMEM is not very accurate; is often similar to MQTT_ERR_CONN_LOST
# eg stopping Mosquitto server generates a MQTT_ERR_NOMEM
# However, there are cases that client freezes for ethernity after a MQTT_ERR_NOMEM
# Implement a recover? With timeout? Try to reconnect?


class MQTTClient(threading.Thread):
  """
  Manages an MQTT client as a separate thread, allowing asynchronous MQTT operations.

  This class extends `threading.Thread` and provides an implementation for managing
  an MQTT client for publish and subscribe functionalities. It supports configurable
  parameters such as broker, port, QoS, protocol versions, client ID, clean session flag,
  username-password authentication, and more. It includes functionality to handle
  reconnections, manage various events (connect, disconnect, subscribe, unsubscribe, etc.),
  and maintain thread-safe communication with the broker.

  :ivar __mqtt_broker: The hostname or IP address of the MQTT broker
  :type __mqtt_broker: str
  :ivar __mqtt_stopper: A `threading.Event` to signal stopping the MQTT thread
  :type __mqtt_stopper: threading.Event
  :ivar __mqtt_port: The port to connect to the MQTT broker
  :type __mqtt_port: int
  :ivar __mqtt_client_id: Unique client ID for identifying this MQTT client
  :type __mqtt_client_id: str
  :ivar __qos: The QoS level for MQTT messages (0, 1, or 2)
  :type __qos: int
  :ivar __mqtt_cleansession: Determines whether to start a clean session at broker
  :type __mqtt_cleansession: bool
  :ivar __mqtt_protocol: The MQTT protocol version to use
  :type __mqtt_protocol: int
  :ivar __worker_threads_stopper: A `threading.Event` to signal stopping related worker threads
  :type __worker_threads_stopper: threading.Event
  :ivar __mqtt: The underlying `paho-mqtt` client instance
  :type __mqtt: paho.mqtt.client.Client
  :ivar __run: Indicates whether the MQTT thread is running
  :type __run: bool
  :ivar __keepalive: Maximum period in seconds to wait for messages from the broker
  :type __keepalive: int
  :ivar __MQTT_CONNECTION_TIMEOUT: Maximum time in seconds to wait for reconnection
  :type __MQTT_CONNECTION_TIMEOUT: int
  :ivar __connected_flag: Indicates the current connection status to the broker
  :type __connected_flag: bool
  :ivar __disconnect_start_time: Timestamp of the last disconnect event
  :type __disconnect_start_time: int
  :ivar __mqtt_counter: Counter for the total number of MQTT messages published
  :type __mqtt_counter: int
  :ivar __status_topic: Topic used to publish client status messages
  :type __status_topic: str
  :ivar __status_payload: Payload for the client status messages
  :type __status_payload: str
  :ivar __status_retain: Retain flag for status topic messages
  :type __status_retain: bool
  :ivar __message_trigger: Event triggered when a message is received and stored in the queue
  :type __message_trigger: threading.Event
  :ivar __subscribed_queue: Queue for storing received subscribed messages
  :type __subscribed_queue: queue.Queue
  :ivar __list_of_subscribed_topics: List of topics this client is subscribed to
  :type __list_of_subscribed_topics: list
  """
  def __init__(self,
               mqtt_broker,
               mqtt_stopper,
               mqtt_port=1883,
               mqtt_client_id=None,
               mqtt_qos=1,
               mqtt_cleansession=True,
               mqtt_protocol=mqtt_client.MQTTv311,
               username="",
               password="",
               worker_threads_stopper=None):

    """
    Args:
      :param str mqtt_broker: ip or dns
      :param threading.Event() mqtt_stopper: indicate to stop the mqtt thread; typically as last thread
      in main loop to flush out all mqtt messages
      :param int mqtt_port:
      :param str mqtt_client_id:
      :param int mqtt_qos: MQTT QoS 0,1,2 for publish
      :param bool mqtt_cleansession:
      :param int mqtt_protocol: MQTT protocol version
      :param str username:
      :param str password:
      :param threading.Event() worker_threads_stopper: stopper event for other worker threads;
      typically the worker threads are
             stopped in the main loop before the mqtt thread;but mqtt thread can also set this in case of failure

    Returns:
      None
    """

    logger.info(f">> paho-mqtt version = {paho_mqtt.__version__}")
    super().__init__()

    self.__mqtt_broker = mqtt_broker
    self.__mqtt_stopper = mqtt_stopper
    self.__mqtt_port = mqtt_port

    # Generate random client id if not specified;
    # Basename ('script', from log module) and extended with 10 random characters
    if mqtt_client_id is None:
      self.__mqtt_client_id = script + '_' + ''.join(random.choice(string.ascii_lowercase) for _i in range(10))
    else:
      self.__mqtt_client_id = mqtt_client_id

    logger.info(f"MQTT Client ID = {self.__mqtt_client_id}")

    if mqtt_qos not in [0, 1, 2]:
      logger.error(f"Invalid QoS level = {mqtt_qos}; reset to qos=1")
      mqtt_qos = 1

    self.__qos = mqtt_qos
    self.__mqtt_cleansession = mqtt_cleansession
    self.__mqtt_protocol = mqtt_protocol

    if worker_threads_stopper is None:
      self.__worker_threads_stopper = self.__mqtt_stopper
    else:
      self.__worker_threads_stopper = worker_threads_stopper

    # Check if installed paho-mqtt version supports MQTT v5
    # Demote to v311 if wrong version is installed
    if self.__mqtt_protocol == mqtt_client.MQTTv5:
      if parse_version(f"{paho_mqtt.__version__}") < parse_version("1.5.1"):
        logger.warning(f"Incorrect paho-mqtt version ({paho_mqtt.__version__}) to support MQTT v5, "
                       f"reverting to MQTT v311")
        self.__mqtt_protocol = mqtt_client.MQTTv311

    # clean_session is only implemented for MQTT v3
    if self.__mqtt_protocol == mqtt_client.MQTTv311 or self.__mqtt_protocol == mqtt_client.MQTTv31:
      self.__mqtt = mqtt_client.Client(self.__mqtt_client_id,
                                       clean_session=mqtt_cleansession,
                                       protocol=self.__mqtt_protocol)
    elif self.__mqtt_protocol == mqtt_client.MQTTv5:
      self.__mqtt = mqtt_client.Client(self.__mqtt_client_id,
                                       protocol=self.__mqtt_protocol)
    else:
      logger.error(f"Unknown MQTT protocol version {mqtt_protocol}....exit")
      self.__worker_threads_stopper.set()
      self.__mqtt_stopper.set()
      return

    # Indicate whether thread has started - run() has been called
    self.__run = False

    # Todo parameterize
    self.__keepalive = 600

    # MQTT client tries to force a reconnection if
    # Client remains disconnected for more than MQTT_CONNECTION_TIMEOUT seconds
    self.__MQTT_CONNECTION_TIMEOUT = 60

    # Call back functions
    self.__mqtt.on_connect = self.__on_connect
    self.__mqtt.on_disconnect = self.__on_disconnect
    self.__mqtt.on_message = self.__on_message

    # Uncomment if needed for debugging
#    self.__mqtt.on_publish = self.__on_publish
#    self.__mqtt.on_log = self.__on_log

    if self.__mqtt_protocol == mqtt_client.MQTTv311 or self.__mqtt_protocol == mqtt_client.MQTTv31:
      self.__mqtt.on_subscribe = self.__on_subscribe_v31
    elif self.__mqtt_protocol == mqtt_client.MQTTv5:
      self.__mqtt.on_subscribe = self.__on_subscribe_v5
    else:
      self.__mqtt.on_subscribe = None

    self.__mqtt.on_unsubscribe = self.__on_unsubscribe

    # Not yet implemented
    # self.__mqtt.on_unsubscribe = self.__on_unsubscribe

    # Managed via __set_connected_flag()
    # Keeps track of connected status
    self.__connected_flag = False

    # Keep track how long client is disconnected
    # When threshold is exceeded, try to recover
    # In some cases, a MQTT_ERR_NOMEM is not recovered automatically
    self.__disconnect_start_time = int(time.time())

    # Maintain a mqtt message count
    self.__mqtt_counter = 0

    self.__mqtt.username_pw_set(username, password)

    # status topic & message
    self.__status_topic = None
    self.__status_payload = None
    self.__status_retain = None

    # Maintain last return code MQTT publish
    # Can be used to print update message if successful after error
    self.__last_rc = 0

    #######
    # Trigger to clients/threads to indicate that message is received and stored in queue
    self.__message_trigger = None

    # Queue to store received subscribed messages
    self.__subscribed_queue = None

    # list of subscribed topics
    self.__list_of_subscribed_topics = []

  def __del__(self):
    """
    Destructor to clean up when the instance of the object is being destroyed.

    This method logs the shutdown of the MQTT client and provides information about
    the number of MQTT messages that have been published during the lifecycle of the
    object. It is automatically invoked when the object's reference count reaches zero.

    :return: None
    """
    logger.debug(f">>")
    logger.info(f"Shutting down MQTT Client... {self.__mqtt_counter} MQTT messages have been published")

  def __internet_on(self):
    """
    Checks internet connectivity to the MQTT broker.

    This method attempts to establish a socket connection to the MQTT broker using
    the provided broker address and port. It is used to determine if the device
    has functional internet connectivity to communicate with the specified MQTT
    broker. If the connection is successful, the socket is shut down and closed,
    and the method returns True. If any exception occurs during the connection
    process, it logs the exception details and returns False.

    :return: True if internet connectivity to the MQTT broker is available,
        otherwise False.
    :rtype: bool
    """
    logger.debug(f">>")

    socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      # Format: s.connect((HOST, PORT))
      socket_connection.connect((f"{self.__mqtt_broker}", int(self.__mqtt_port)))
      socket_connection.shutdown(socket.SHUT_RDWR)
      logger.debug(f"Internet connectivity to MQTT broker {self.__mqtt_broker} at port {self.__mqtt_port} available")
      return True
    except Exception as e:
      logger.info(f"Internet connectivity to MQTT broker {self.__mqtt_broker} at port {self.__mqtt_port} "
                  f"NOT yet available; Exception {e}")
      return False
    finally:
      socket_connection.close()

  def __set_connected_flag(self, flag=True):
    """
    Sets the connected flag and handles the disconnect timing logic.

    This method updates the internal state of the connected flag and triggers a
    disconnect timer in case the connected flag is transitioning from True to False.

    :param flag: A boolean value that represents the desired state of the connected
                 flag. Defaults to True.
    :type flag: bool

    :return: None
    """
    logger.debug(f">> flag={flag}; current __connected_flag={self.__connected_flag}")

    # if flag == False and __connected_flag == True; start trigger
    if not flag and self.__connected_flag:
      self.__disconnect_start_time = int(time.time())
      logger.debug("Disconnect TIMER started")

    self.__connected_flag = flag
    return

  def __on_connect(self, _client, userdata, flags, rc, _properties=None):
    """
    Handles the MQTT client's connection event and performs actions based on the connection
    status. This method is triggered when the MQTT client successfully connects to or fails
    to connect with the broker.

    If the connection is successful, it sets the connected flag, updates the status, and re-subscribes
    to any previously subscribed topics. If the connection fails, it logs the error and sets the
    connected flag accordingly.

    :param _client: The MQTT client instance that triggered the connection event
    :param userdata: User-defined data provided during client initialization
    :param flags: A dictionary of response flags sent by the broker
    :param rc: The result code of the connection, used to determine the success or failure of the connection
    :param _properties: Optional properties of the connection, typically used for MQTT v5.0

    :return: None
    """
    logger.debug(f">>")
    if rc == mqtt_client.CONNACK_ACCEPTED:
      logger.debug(f"Connected: userdata={userdata}; flags={flags}; rc={rc}: {mqtt_client.connack_string(rc)}")
      self.__set_connected_flag(True)
      self.__set_status()

      # Re-subscribe, in case connection was lost
      for topic in self.__list_of_subscribed_topics:
        logger.debug(f"Resubscribe topic: {topic}")
        self.__mqtt.subscribe(topic, self.__qos)

    else:
      logger.error(f"userdata={userdata}; flags={flags}; rc={rc}: {mqtt_client.connack_string(rc)}")
      self.__set_connected_flag(False)

  def __on_disconnect(self, _client, userdata, rc, _properties=None):
    """
    Handles the MQTT disconnection event and logs the appropriate message based on the
    disconnect reason (unexpected or expected). Updates the connection flag in the client.

    :param _client: The MQTT client instance that was disconnected.
    :type _client: Any
    :param userdata: The private user data provided when the MQTT client was created.
    :type userdata: Any
    :param rc: The disconnect reason code.
    :type rc: int
    :param _properties: Optional MQTT properties associated with the disconnection.
    :type _properties: Optional[Any]
    :return: None
    :rtype: None
    """
    if rc != mqtt_client.MQTT_ERR_SUCCESS:
      logger.warning(f"Unexpected disconnect, userdata = {userdata}; rc = {rc}: {mqtt_client.error_string(rc)}")
    else:
      logger.info(f"Expected disconnect, userdata = {userdata}; rc = {rc}: {mqtt_client.error_string(rc)}")

    self.__set_connected_flag(False)
    return

  def __on_message(self, _client, _userdata, message):
    """
    Handles the 'on_message' client callback when a message is received.

    This method is triggered when a message is received on a subscribed topic.
    The received message is placed into the subscribed queue for processing.
    If a message trigger event is defined, it is set to signal that a message
    has been received.

    :param _client: The MQTT client instance that received the message.
    :param _userdata: The private user data passed to the client.
    :param message: The received message instance, containing topic and payload.
    :type message: MQTTMessage
    :return: None
    """
    logger.debug(f">> message = {message.topic}  {message.payload}")

    self.__subscribed_queue.put(message)

    # set event that message has been received
    if self.__message_trigger is not None:
      self.__message_trigger.set()

  def __on_publish(self, _client, userdata, mid):
    """
    Handles the successful publishing of a message.

    This method is a callback invoked automatically by the MQTT client library
    when a message has been successfully published to a topic. It logs the provided
    userdata and message identifier (mid) for debugging purposes.

    :param _client: The MQTT client instance invoking the callback.
    :param userdata: Application-defined data passed to the callback (can be None).
    :param mid: Message identifier assigned by the MQTT library.
    :return: None
    """
    logger.debug(f"userdata={userdata}; mid={mid}")
    return None

  def __on_subscribe_v5(self, _client, _userdata, mid, reasoncodes, _properties=None):
    """
    Handles the MQTT v5 subscription acknowledgement callback.

    This method is invoked when a subscription request to a topic is acknowledged by
    the broker. It processes and logs the message identifier and reason codes
    associated with the subscription. The optional properties parameter provides
    additional metadata for the subscription when applicable.

    :param _client: The client instance that initiated the subscription.
    :type _client: Client
    :param _userdata: Custom user data passed to the callback by the user.
    :type _userdata: Any
    :param mid: The message identifier of the subscription request.
    :type mid: int
    :param reasoncodes: A list of reason codes returned by the broker to indicate the
        result of each topic filter subscription.
    :type reasoncodes: list[int]
    :param _properties: Optional properties returned by the broker in MQTT v5.
        May include metadata or additional subscription-related information.
    :type _properties: dict, optional
    :return: None
    """
    logger.debug(f"Subscribed mid variable: {mid}")

    for rc in reasoncodes:
      logger.debug(f"reasonCode = {rc}")

  def __on_subscribe_v31(self, _client, _userdata, mid, granted_qos):
    """
    Handles the subscription callback for MQTT protocol version 3.1.

    This method is triggered when the client successfully subscribes to a topic,
    logging information related to the subscription mid and granted QoS levels.

    :param _client: The MQTT client instance invoking the callback.
    :param _userdata: The private user data, if set when the client was created.
    :param mid: The message ID of the subscription request.
    :param granted_qos: A list containing the QoS levels for each requested subscription.
    :return: None
    """
    logger.debug(f"Subscribed mid variable: {mid}")

    for qos in granted_qos:
      logger.debug(f"Granted QoS = {qos}")

  def __on_unsubscribe(self, _client, _userdata, mid, _properties=None, _reasoncode=None):
    """
    Handles the unsubscribe event triggered in an MQTT client.

    This method is a callback function which is automatically invoked when the
    client successfully unsubscribes from a topic. The corresponding parameters
    are provided by the MQTT client through this callback. Additionally, this
    method logs the identifier of the associated unsubscribe action for debugging
    purposes.

    :param _client: The MQTT client instance associated with the callback.
    :param _userdata: Any user-defined data passed to the callback function.
    :param mid: Message identifier for the unsubscribe request.
    :param _properties: Optional. A dictionary containing MQTT V5 properties,
        if the MQTT version supports it.
    :param _reasoncode: Optional. Unsubscribe reason code, if available.
    :return: None
    """
    logger.debug(f">> Unsubscribed: {mid}")

  def __on_log(self, client, _userdata, level, buf):
    """
    Handles logging for the client by capturing messages logged at a specific level.

    This private method is responsible for logging debug information based on
    parameters received, including the client object, log level, and buffer content.

    :param client: The client object associated with the log message.
    :param _userdata: User-defined data, generally unspecified or unused in this context.
    :param level: The logging level indicating the severity of the message.
    :type level: int
    :param buf: The actual log message or buffer content.
    :type buf: str
    :return: None
    """
    logger.debug(f"obj={client}; level={level}; buf={buf}")

  def __set_status(self):
    """
    Updates the status by publishing the status payload to the specified status topic.

    The method checks if the `__status_topic` attribute is not `None`. If it is set,
    it publishes the `__status_payload` to the given `__status_topic` with the
    specified retain flag. The function makes no direct output but calls
    `do_publish` for the publishing process and utilizes logging for debugging.

    :return: None
    """
    logger.debug(">>")

    if self.__status_topic is not None:
      self.do_publish(self.__status_topic, self.__status_payload, self.__status_retain)

    return

  def set_status(self, topic, payload=None, retain=False):
    """
    Sets the status configuration for the current instance and triggers the
    status update operation. It defines the topic, payload, and retain
    flag for publishing the status.

    The method is used to configure status-related details, which are
    subsequently handled by the internal __set_status method to manage
    publishing the status to the specified topic with the given payload
    and retain flag.

    :param topic: The MQTT topic to which the status will be published.
    :type topic: str
    :param payload: The payload to be sent as the status message. Defaults to None.
    :type payload: Optional[Any]
    :param retain: Specifies if the message should be retained by the broker.
                   Defaults to False.
    :type retain: bool
    :return: None
    """
    logger.debug(">>")
    self.__status_topic = topic
    self.__status_payload = payload
    self.__status_retain = retain
    self.__set_status()

  def will_set(self, topic, payload=None, qos=1, retain=False):
    """
    Sets the Last Will and Testament (LWT) for the MQTT client instance. The LWT
    is a message that will be published automatically by the broker if the client
    disconnects unexpectedly. This method allows users to specify the topic, message
    payload, Quality of Service level, and retain flag for the LWT.

    .. note::
       It is recommended to set the LWT before calling the run() method as per
       MQTT documentation. Setting the LWT after the client is running may lead
       to unexpected behavior.

    :param topic: The topic on which the Last Will message will be published.
    :param payload: The message payload for the Last Will (default is None).
    :param qos: Quality of Service level for the Last Will message (default is 1).
    :param retain: Flag to specify if the Last Will message should be retained by the broker
                   (default is False).
    :return: None
    """
    logger.debug(f">>")

    if self.__run:
      logger.warning(f"Last Will/testament is set after run() is called. Not advised per documentation")

    self.__mqtt.will_set(topic, payload, qos, retain)

  def do_publish(self, topic, message, retain=False):
    """
    Publishes a message to a specified MQTT topic.

    This method sends a message to the provided MQTT topic with an option to
    retain the message on the broker. It logs the publishing status and updates
    a counter upon successful message delivery. If the publishing fails, a
    warning is logged.

    :param topic: The topic to which the message is published.
    :type topic: str
    :param message: The payload message to be published to the topic.
    :type message: str
    :param retain: Specifies whether to retain the message on the broker. Defaults to False.
    :type retain: bool
    :return: MQTT message information object of the publish operation. May
        include details such as delivery acknowledgment or success status.
    :rtype: mqtt.MQTTMessageInfo
    :raises ValueError: Raised if provided message or topic parameters are invalid or unsupported.
    """
    logger.debug(f">> TOPIC={topic}; MESSAGE={message}")

    try:
      mqttmessageinfo = self.__mqtt.publish(topic=topic, payload=message, qos=self.__qos, retain=retain)
      self.__mqtt_counter += 1

      if mqttmessageinfo.rc != mqtt_client.MQTT_ERR_SUCCESS:
        logger.warning(f"MQTT publish was not successful, rc = {mqttmessageinfo.rc}:{mqtt_client.error_string(mqttmessageinfo.rc)}")
        self.__last_rc = mqttmessageinfo.rc
      else:
        # Print only successful if previous publish was not successful
        # To prevent flooding of messages
        if self.__last_rc != 0:
          logger.info(f"MQTT publish was successful, rc = {mqttmessageinfo.rc}:{mqtt_client.error_string(mqttmessageinfo.rc)}")
          self.__last_rc = 0

    except ValueError:
      logger.warning("")

  def set_message_trigger(self, subscribed_queue, trigger=None):
    """
    Sets a trigger for messages and subscribes to the given queue.

    This method assigns a message trigger and queues the subscription to
    a specified queue. If there are topics stored from a previous state,
    it attempts to re-subscribe to those topics in case a connection was
    lost. The subscribed topics utilize the provided Quality of Service (QoS)
    level to maintain the desired connection reliability.

    :param subscribed_queue: A queue to which the subscription will be applied
                             for processing incoming messages.
    :type subscribed_queue: Any
    :param trigger: A callable or functional trigger for processing incoming
                    messages. If `None`, no trigger is set.
    :type trigger: Optional[Callable]
    :return: None
    :rtype: None
    """

    self.__message_trigger = trigger
    self.__subscribed_queue = subscribed_queue

    # Re-subscribe, in case connection was lost
    for topic in self.__list_of_subscribed_topics:
      logger.debug(f"Resubscribe topic: {topic}")
      self.__mqtt.subscribe(topic, self.__qos)

    return

  def subscribe(self, topic):
    """
    Subscribes to a given topic and manages the subscription queue.

    This method is responsible for subscribing to the specified MQTT topic.
    It ensures that the given topic is added to the internal list of subscribed topics
    for potential resubscription during reconnection. The method will not perform the
    subscription if the client is not connected or if the subscription message queue
    has not been properly set.

    :param topic: The MQTT topic to subscribe to.
    :type topic: str
    :return: None
    """
    logger.debug(f">> topic = {topic}")

    # Add to subscribed topic to queue (for resubscribing when reconnecting)
    self.__list_of_subscribed_topics.append(topic)

    if self.__subscribed_queue is None:
      logger.error(f"Subscription message queue has not been set --> call set_message_trigger")
      return

    # Subscribing will not work if client is not connected
    # Wait till there is a connection
    # Todo....not required....just store in list, will be subscribe in on_connect()
    # while not self.__connected_flag and not self.__mqtt_stopper.is_set():
    #  logger.warning(f"No connection with MQTT Broker; cannot subscribe...wait for connection")
    #  time.sleep(0.1)

    self.__mqtt.subscribe(topic, self.__qos)
    return

  def unsubscribe(self, topic):
    """
    Unsubscribes from the given MQTT topic. The method removes the topic
    from the internal list of subscribed topics and performs the actual
    unsubscription via the MQTT client. If the topic is not in the list
    of subscribed topics, a warning is logged.

    :param topic: The topic to unsubscribe from.
    :type topic: str
    :return: None
    """
    logger.debug(f">> topic = {topic}")
    self.__mqtt.unsubscribe(topic)

    try:
      self.__list_of_subscribed_topics.remove(topic)
    except ValueError:
      logger.warning(f"MQTT client was not subscribed to topic '{topic}'; "
                     f"did you use exact same topic as when subscribing?")
    return

  def run(self):
    logger.info(f"Broker = {self.__mqtt_broker}>>")
    self.__run = True

    # Wait till there is network connectivity to mqtt broker
    # Start with a small delay and incrementally (+20%) make larger
    delay = 0.1
    while not self.__internet_on():
      time.sleep(delay)
      delay = delay * 1.2

      # Timeout after 60min
      if delay > 3600:
        logger.error(f"No internet connection - EXIT")
        self.__mqtt_stopper.set()
        self.__worker_threads_stopper.set()
        return

    try:
      # Options functions to be called before connecting
      # Set queue to unlimitted (=65535) when qos>0
      self.__mqtt.max_queued_messages_set(0)
      self.__mqtt.reconnect_delay_set(min_delay=1, max_delay=360)

      # clean_session is only implemented for MQTT v3
      if self.__mqtt_protocol == mqtt_client.MQTTv311 or self.__mqtt_protocol == mqtt_client.MQTTv31:
        self.__mqtt.connect_async(host=self.__mqtt_broker,
                                  port=self.__mqtt_port,
                                  keepalive=self.__keepalive)
      elif self.__mqtt_protocol == mqtt_client.MQTTv5:
        if self.__mqtt_cleansession:
          # TODO
          # For clean_start set to True or Start_first_Only, a session expiry interval
          # has to be set via properties object
          # This is not yet implemented
          self.__mqtt.connect_async(host=self.__mqtt_broker,
                                    port=self.__mqtt_port,
                                    keepalive=self.__keepalive,
                                    clean_start=mqtt_client.MQTT_CLEAN_START_FIRST_ONLY,
                                    properties=None)
        else:
          self.__mqtt.connect_async(host=self.__mqtt_broker,
                                    port=self.__mqtt_port,
                                    keepalive=self.__keepalive,
                                    clean_start=False,
                                    properties=None)
      else:
        logger.error(f"Unknown MQTT protocol version {self.__mqtt_protocol}....exit")
        self.__worker_threads_stopper.set()
        self.__mqtt_stopper.set()

    except Exception as e:
      # logger.exception(f"Exception {format(e)}")
      logger.warning(f"Exception {format(e)}")
      self.__mqtt.disconnect()
      self.__mqtt_stopper.set()
      self.__worker_threads_stopper.set()
      return

    else:
      logger.info(f"Start mqtt loop...")
      self.__mqtt.loop_start()

    # Start infinite loop which sends queued messages every second
    while not self.__mqtt_stopper.is_set():
      # Todo: reconnect stuff needed?

      # Check connection status
      # If disconnected time exceeds threshold
      # then reconnect
      if not self.__connected_flag:
        disconnect_time = int(time.time()) - self.__disconnect_start_time
        logger.debug(f"Disconnect TIMER = {disconnect_time}")
        if disconnect_time > self.__MQTT_CONNECTION_TIMEOUT:
          try:
            self.__mqtt.reconnect()
          except Exception as e:
            # logger.exception(f"Exception {format(e)}")
            logger.warning(f"Exception {format(e)}")

            # reconnect failed....reset disconnect time, and retry after self.__MQTT_CONNECTION_TIMEOUT
            self.__disconnect_start_time = int(time.time())

      time.sleep(0.1)

    # Close mqtt broker
    logger.debug(f"Close down MQTT client & connection to broker")
    self.__mqtt.loop_stop()
    self.__mqtt.disconnect()
    self.__mqtt_stopper.set()
    self.__worker_threads_stopper.set()

    logger.info(f"Shutting down MQTT Client... {self.__mqtt_counter} MQTT messages have been published")

    logger.info(f"<<")

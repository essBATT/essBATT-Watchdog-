# This is free and unencumbered software released into the public domain.

# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.

# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# For more information, please refer to <http://unlicense.org/>

import paho.mqtt.client as mqtt
import logging
from logging.handlers import RotatingFileHandler
import time
import json
import threading
from datetime import datetime, timedelta
import numbers


DEBUGGING_ON = False # Switch for debugging

###### Script internal parameters ########################################
CERBO_KEEPALIVE_LENGTH = (30.0) # [s] Alle CERBO_KEEPALIVE_LENGTH Sekunden wird eine Keepalive Nachricht an den Cerbo gesendet
MQTT_SERVER_TIMEOUT_TIMESPAN = 60 # [s] After this timeout timespan with no from the MQTT server some error handling will be triggered
##########################################################################

LOGLEVEL_NAME_TO_NUMBER = {'CRITICAL': 50, 'FATAL': 50, 'ERROR': 40, 'WARNING': 30, 'WARN': 30, 'INFO': 20, 'DEBUG': 10, 'NOTSET': 0}
MULTIS_SWITCH_NUMBER_STRING_MAPPING = {'1':"CHARGER ON INVERTER OFF", '2':"INVERTER ON CHARGER OFF", '3':"INVERTER AND CHARGER ON", '4':"INVERTER AND CHARGER OFF"}

################### Timer Class ##############################
# Class from MestreLion: https://stackoverflow.com/questions/474528/how-to-repeatedly-execute-a-function-every-x-seconds
class RepeatedTimer(object):
  def __init__(self, interval, function, *args, **kwargs):
    self._timer = None
    self.interval = interval
    self.function = function
    self.args = args
    self.kwargs = kwargs
    self.is_running = False
    self.next_call = time.time()
    self.start()

  def _run(self):
    self.is_running = False
    self.start()
    self.function(*self.args, **self.kwargs)

  def start(self):
    if not self.is_running:
      self.next_call += self.interval
      self._timer = threading.Timer(self.next_call - time.time(), self._run)
      self._timer.start()
      self.is_running = True

  def stop(self):
    self._timer.cancel()
    self.is_running = False
###############################################################



##################### essBATT Watchdog Class ##############
class essBATT_watchdog:  
    def __init__(self, logger):
        self.logger = logger
        self.mqtt_client = None
        self.mqtt_connection_ok = False
        self.mqtt_disconnected = True
        self.rt_keep_alive_obj = RepeatedTimer(CERBO_KEEPALIVE_LENGTH, self.send_keepalive_to_cerbo)
        self.watchdog_config_data = {}
        self.watchdog_config_data_loaded_correctly = False
        self.CCGX_data = {'grid':{},'battery':{}, 'solarcharger':{}, 'settings':{}, 'system':{}}
        self.read_config_json()
        self.write_base_path = 'W/'+ self.watchdog_config_data['vrm_id'] + '/'
        self.logger.setLevel(LOGLEVEL_NAME_TO_NUMBER[self.watchdog_config_data['debug_level']])
        self.logger.info('Effective logger level: ' + str(self.logger.getEffectiveLevel()))
        self.rt_essBATT_watchdog_update_obj = RepeatedTimer(self.watchdog_config_data['watchdog_update_rate'], self.watchdog_cycle_update)
        self.rt_print_status_obj = RepeatedTimer(self.watchdog_config_data['script_alive_logging_interval'], self.print_alive_status_to_logger)

###############################################################################################################################################################################################################################
###############################################################################################################################################################################################################################
###############################################################################################################################################################################################################################       
    def run(self):        
        # Configuration of the MQTT Client object
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.username_pw_set(username=self.ess_config_data['mqtt_username'], password=self.ess_config_data['mqtt_password'])
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_subscribe = self.on_subscribe
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_disconnect = self.on_disconnect
                
        try:
            self.mqtt_client.connect('localhost', port=self.ess_config_data['mqtt_server_COM_port'], keepalive=MQTT_SERVER_TIMEOUT_TIMESPAN, bind_address="")
            self.logger.info("essBATT watchdog: Try to connect to MQTT Server: localhost on port " + str(self.ess_config_data['mqtt_server_COM_port']) + " with timeout of " + str(MQTT_SERVER_TIMEOUT_TIMESPAN) + "s")
            self.mqtt_client.loop_start()
            while not self.mqtt_connection_ok: #wait in loop until connected by on_connect callback function
                self.logger.info("Waiting for MQTT server connection...")
                time.sleep(1)
            # Subscribe to all needed <service_types> of the Venus OS system and all their messages.
            # Subscribtion to selected service_types is done to reduce the "on_message callback" load in case of ess_config "keepalive_get_all_topics":1
            # Selection of messages is done in the send_keepalive_to_cerbo() function
            base_path_str = "N/" + self.ess_config_data['vrm_id']
            subscription_list_tmp = self.create_subscribtion_list(base_path_str)
            (result, mid) = self.mqtt_client.subscribe(subscription_list_tmp)
            self.logger.info("MQTT subscribtion function return value: " + str(result))
            self.add_topic_specific_callbacks(base_path_str)
            
            ###### MAIN LOOP - essBATT watchdog loop while the MQTT connection is active ##########################
            while self.mqtt_connection_ok is True:
                # This is the loop for the watchdog cycle update functionality (watchdog_cycle_update()). It is empty because this function is
                # called with the help of the "repeated timer" in a periodical manner (see _init_ function)
                pass
        except:
            self.logger.error('Could not establish connection to MQTT Server. Critical error.')
            self.mqtt_client.loop_stop()
            self.mqtt_connection_ok = False
                
    def watchdog_cycle_update(self):
        # Only update if MQTT connection is active 
        if(self.mqtt_connection_ok is True):   
            ############# Read data from Victron System ###################################
            # This is done with the MQTT callback functions. The "up to date" data is stored in self.CCGX_data dictionary. Data fields in self.CCGX_data
            # are created when the first message for a data field is received and it can be DELETED if the device that delivers this data is removed from the bus (e.g. solarcharger at night).
            # Always make sure that the data field exists before using it!
                    
            # Reading most often needed values to local variables for convenience (while checking if they are available)
            local_values = {}
            self.read_values_to_local_dict(local_values)
                    
            ############# State machine ###################################################
            # The statemachine handles all tasks associated with switching from "normal_operation" to "charge to SOC" or "balancing" and back.
            try:
                self.statemachine_update(local_values)
            except Exception as e:
                self.logger.exception('Unhandled Exception!')
                raise


        
            
###############################################################################################################################################################################################################################
###############################################################################################################################################################################################################################
###############################################################################################################################################################################################################################
    
    def get_scheduled_starttime_datetime_obj(self, mode_string):
        format_string = self.ess_config_data['external_control_settings']['date_format'] + ' ' + self.ess_config_data['external_control_settings']['time_format']
        try:
            ret_val = datetime.strptime(self.ess_controller_state[mode_string]['scheduled_start_time'], format_string)
        except:
            ret_val = None
        return ret_val
    
            
    def datetime_obj_from_input_timestamp(self, timestring, datestring):
        # Dealing with the case, that no value is given
        
        if((timestring == "-") and (datestring == "-")):
            self.logger.debug('Datetime object creator was called without a valid timestring. So nothing.')
            return
        # Default case for time is the beginning of the day
        if(timestring == "-"):
            timestring = "00:00"
            self.logger.debug('Using default time 00:00 for datetime object because only date was given.')
        # Default case for date is the date of today or (if this time is in the past then take the date of tomorrow)
        if(datestring == "-"):
            date_obj_now = datetime.now(tz=None)
            datestring_today = date_obj_now.strftime(self.ess_config_data['external_control_settings']['date_format'])           
            format_string = self.ess_config_data['external_control_settings']['date_format'] + ' ' + self.ess_config_data['external_control_settings']['time_format']
            temp_combined_datetime_str = datestring_today + ' '  + timestring
            temp_test_obj = datetime.strptime(temp_combined_datetime_str, format_string)  
            # Selection on the datestring based on the resulting timestamp being in the past or not
            if(temp_test_obj < date_obj_now):
                timedelta_obj = timedelta(days=1)
                datestring = (datetime.now(tz=None) + timedelta_obj).strftime(self.ess_config_data['external_control_settings']['date_format'])
                self.logger.debug('Only time was given. Assuming date of "TOMORROW" because time today already passed.')
            else:
                datestring = datestring_today
                self.logger.debug('Only time was given. Assuming date of today to create datetime object.')
        
        combined_datetime_str = datestring + ' ' + timestring
        format_string = self.ess_config_data['external_control_settings']['date_format'] + ' ' + self.ess_config_data['external_control_settings']['time_format']
        try:
            temp_obj = datetime.strptime(combined_datetime_str, format_string)
            return temp_obj
        except:
            self.logger.error('Datetime Object could not be created. Input String: ' + combined_datetime_str)
            return -1
    
 
            
    def create_subscribtion_list(self, base_path_str):
        # Basic subscriptions
        subscription_list = [(base_path_str + "/battery/#", 1),
                             (base_path_str + "/grid/#", 1),
                             (base_path_str + "/solarcharger/#", 1),
                             (base_path_str + "/system/+/Ac/Consumption/#", 1),
                             (base_path_str + "/settings/#", 1),
                             (base_path_str + "/vebus/+/Mode", 1)]
        # (optional) Subscriptions for external MQTT control    
        if(self.ess_config_data['external_control_settings']['allow_external_control_over_mqtt'] == 1):
            if(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['charge_battery_to_SOC'] != "none"):
                subscription_list.append((self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['charge_battery_to_SOC'], 1))
            if(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['activate_top_balancing_mode'] != "none"):
                subscription_list.append((self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['activate_top_balancing_mode'], 1))
            if(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['deactivate_discharge'] != "none"):
                subscription_list.append((self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['deactivate_discharge'], 1))
            if(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['deactivate_charge'] != "none"):
                subscription_list.append((self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['deactivate_charge'], 1))  
            if(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['reboot_ess_controller'] != "none"):
                subscription_list.append((self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['reboot_ess_controller'], 1))                
        return subscription_list
                                       

    def read_values_to_local_dict(self, local_values):
        local_values['all_CCGX_values_available'] = True
        if('grid_power_sum' in self.CCGX_data['grid']):
            local_values['grid_power_sum'] = self.CCGX_data['grid']['grid_power_sum']
        else:
            local_values['all_CCGX_values_available'] = False
        if('soc' in self.CCGX_data['battery']):
            local_values['battery_soc'] = self.CCGX_data['battery']['soc']
        else:
            local_values['all_CCGX_values_available'] = False
        if('max_cell_voltage' in self.CCGX_data['battery']):
            local_values['battery_max_cell_voltage'] = self.CCGX_data['battery']['max_cell_voltage']
        else:
            local_values['all_CCGX_values_available'] = False
        if('min_cell_voltage' in self.CCGX_data['battery']):
            local_values['battery_min_cell_voltage'] = self.CCGX_data['battery']['min_cell_voltage']
        else:
            local_values['all_CCGX_values_available'] = False
        if('current' in self.CCGX_data['battery']):
            local_values['battery_current'] = self.CCGX_data['battery']['current']
        else:
            local_values['all_CCGX_values_available'] = False
        if('power' in self.CCGX_data['battery']):
            local_values['battery_power'] = self.CCGX_data['battery']['power']
        else:
            local_values['all_CCGX_values_available'] = False
        if('voltage' in self.CCGX_data['battery']):
            local_values['battery_voltage'] = self.CCGX_data['battery']['voltage']
        else:
            local_values['all_CCGX_values_available'] = False
        if('L1_loads_power_consumption' in self.CCGX_data['system']):
            local_values['l1_loads_power_consumtpion'] = self.CCGX_data['system']['L1_loads_power_consumption']
        else:
            local_values['all_CCGX_values_available'] = False
        if('L2_loads_power_consumption' in self.CCGX_data['system']):
            local_values['l2_loads_power_consumtpion'] = self.CCGX_data['system']['L2_loads_power_consumption']
        else:
            local_values['all_CCGX_values_available'] = False
        if('L3_loads_power_consumption' in self.CCGX_data['system']):
            local_values['l3_loads_power_consumtpion'] = self.CCGX_data['system']['L3_loads_power_consumption']
        else:
            local_values['all_CCGX_values_available'] = False
                
        local_values['solarcharger_power_sum'] = 0    
        for element in self.CCGX_data['solarcharger']:
            if('Power' in self.CCGX_data['solarcharger'][element]):
                local_values['solarcharger_power_sum'] = local_values['solarcharger_power_sum'] + self.CCGX_data['solarcharger'][element]['Power']
            else:
                local_values['all_CCGX_values_available'] = False
        local_values['solarcharger_current_sum'] = 0    
        for element in self.CCGX_data['solarcharger']:
            if('Current' in self.CCGX_data['solarcharger'][element]):
                local_values['solarcharger_current_sum'] = local_values['solarcharger_current_sum'] + self.CCGX_data['solarcharger'][element]['Current']
            else:
                local_values['all_CCGX_values_available'] = False
        
        if(local_values['all_CCGX_values_available']):        
            # Total loads power consumption
            local_values['loads_total_power'] = local_values['l1_loads_power_consumtpion'] + local_values['l2_loads_power_consumtpion'] + local_values['l3_loads_power_consumtpion']
            self.logger.debug('Loads total power: ' + str(local_values['loads_total_power']) + ', Loads L1 power: ' + str(local_values['l1_loads_power_consumtpion']) + ', Loads L2 power: ' + str(local_values['l2_loads_power_consumtpion']) + ', Loads L3 power: ' + str(local_values['l3_loads_power_consumtpion'])) 
                        
            # Estimation of the power losses from battery/solarcharger to AC loads. It might help the system to better respect the
            # battery discharge/charge limits.
            local_values['losses_dc2ac_est'] = (local_values['grid_power_sum'] - local_values['battery_power'] + local_values['solarcharger_power_sum']) - local_values['loads_total_power']
            self.logger.debug('Estimated losses DC to AC: ' + str(local_values['losses_dc2ac_est']) + 'W')
                    
        self.logger.debug('Solarcharger power sum: ' + str(local_values['solarcharger_power_sum']) + ' Solarcharger current sum: ' + str(local_values['solarcharger_current_sum']))
        if(not local_values['all_CCGX_values_available']):
            self.logger.info('all_CCGX_values_available: "' + str(local_values['all_CCGX_values_available']) + '"')    # TODO: log level back to debug
        
           
    def set_CCGX_value(self, set_val_name_str=None, set_val=0, only_set_if_deviation_to_current_setting=True):
        """Function description: Sets the corresponding value in CCGX over MQTT.
        Arguments:
        set_val_name_str: [string] Victron name of the parameter found in ess_setvalue_list.json
        set_val: [number] value to send to CCGX
        only_set_if_deviation_to_current_setting: [True/False] If set to True it checks what the current setting in CCGX is and only if the new set value is different it sends the set command. False always sends the command.
        return: 0: everything ok but no value send, 1: everything ok and value send, -1: error while sending"""
        retval = 0
        if(set_val_name_str is not None):
            if(set_val_name_str in self.CCGX_data['settings']):
                if(((only_set_if_deviation_to_current_setting is True) and (set_val != self.CCGX_data['settings'][set_val_name_str]))
                or (only_set_if_deviation_to_current_setting is False)):
                    try:
                        topic_str = self.write_base_path + self.CCGX_data['settings_base_path'] + self.ess_setvalue_list[set_val_name_str]
                        payload_str = json.dumps({"value": set_val})
                        self.mqtt_client.publish(topic=topic_str, payload=payload_str, qos=1, retain=0)
                        self.logger.debug(set_val_name_str + ': Published ' + payload_str + ' on ' + topic_str + '. self.CCGX_data["settings"]["' + set_val_name_str + '"]: ' + str(self.CCGX_data['settings'][set_val_name_str]))
                        retval = 1
                    except:
                        self.logger.error(set_val_name_str + 'setpoint sending failed!')
                        retval = -1
        else: 
            self.logger.error('No set value name given!')
            retval = -1
        return retval
    
    def set_multis_switch_mode(self, switch_position):
        """
        Possible values for "switch_position": 1=Charger Only; 2=Inverter Only; 3=On; 4=Off
        See modbus tcp register list 3.10:
        https://www.victronenergy.com/support-and-downloads/technical-information
        com.victronenergy.vebus	Switch Position	33	uint16	1	0 to 65536	/Mode	yes	1	See Venus-OS manual for limitations, for example when VE.Bus BMS or DMC is installed.
        """
        if('vebus' in self.CCGX_data):
            counter = 0
            current_instance_id = ''
            for key in self.CCGX_data['vebus']:
                current_instance_id = str(key)
                counter = counter + 1
            # If the switch has a different position than the set value switch it to the new value
            if(self.CCGX_data['vebus'][current_instance_id]['Mode'] != switch_position):
                topic_str = self.write_base_path + 'vebus/' + current_instance_id + '/Mode'
                payload_str = json.dumps({"value": switch_position})
                self.mqtt_client.publish(topic=topic_str, payload=payload_str, qos=1, retain=0)
                self.logger.info('"Multis SWITCH" switched to ' + MULTIS_SWITCH_NUMBER_STRING_MAPPING[str(switch_position)] + '(value: ' + str(switch_position) + ')')
            if(counter > 1):
                self.logger.error('It seems that there is more than one instance of "vebus" available. This was not considered during development of the script and needs to be investigated!!!')


    def send_keepalive_to_cerbo(self):
        # Documentation needed to know which values to send how is here:
        # https://github.com/victronenergy/dbus-mqtt
        # https://www.victronenergy.com/live/ess:ess_mode_2_and_3
        try:
            # Sends keepalive to Victron OS in a way, that all available topics are returned (good for debugging but high network and system load)
            if(self.watchdog_config_data['keepalive_get_all_topics'] == 1):
                payload_string = ""
                topic_string = "R/" + self.ess_config_data['vrm_id'] + "/system/0/Serial"
                # Publish Topic
                errcode = self.mqtt_client.publish(topic_string, payload=payload_string, qos=0, retain=False)
                # Logging
                log_string = "Keepalive (all topics) message send! Errorcode: " + str(errcode) + ". Published topic: \'" + topic_string
                self.logger.debug(log_string)
        except:
            # Logging
            log_string = "FAILED to publish Keep Alive message to Cerbo!"
            self.logger.warning(log_string)
            
    def print_alive_status_to_logger(self):
        self.logger.info('essBATT watchdog is up and running!')
    
    def read_config_json(self):
        try:
            if(DEBUGGING_ON):
                f = open('./smarthome_projects/essBATT-Controller-/ess_config.json') # Path for debug mode
            else:
                f = open('ess_config.json') # Path for productive mode
        except:
            self.logger.error("ess_config.json file could not be opened!")
            return
        # returns JSON object as a dictionary
        try:           
            self.ess_config_data = json.load(f)
        except:
            self.logger.error("ess_config.json was opened but could not be loaded. Check for json file syntax errors!")
            f.close()
            return
        self.ess_config_data_loaded_correctly = True
        self.logger.debug('ess_config.json file loaded.')
        f.close()
    
    def add_topic_specific_callbacks(self, base_path_str):
        # Grid
        topic_str = base_path_str + "/grid/+/Ac/Power"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_grid_power)
        topic_str = base_path_str + "/grid/+/Ac/L1/Power"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_L1_power)
        topic_str = base_path_str + "/grid/+/Ac/L1/Current"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_L1_current)
        topic_str = base_path_str + "/grid/+/Ac/L2/Power"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_L2_power)
        topic_str = base_path_str + "/grid/+/Ac/L2/Current"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_L2_current)
        topic_str = base_path_str + "/grid/+/Ac/L3/Power"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_L3_power)
        topic_str = base_path_str + "/grid/+/Ac/L3/Current"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_L3_current)
        # Battery
        topic_str = base_path_str + "/battery/+/Soc"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_battery_soc)
        topic_str = base_path_str + "/battery/+/System/MaxCellVoltage"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_battery_maxcellvoltage)
        topic_str = base_path_str + "/battery/+/System/MinCellVoltage"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_battery_mincellvoltage)
        topic_str = base_path_str + "/battery/+/Dc/0/Temperature"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_battery_temp)
        topic_str = base_path_str + "/battery/+/Dc/0/Current"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_battery_current)
        topic_str = base_path_str + "/battery/+/Dc/0/Power"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_battery_power)
        topic_str = base_path_str + "/battery/+/Dc/0/Voltage"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_battery_voltage)
        # Solarcharger
        topic_str = base_path_str + "/solarcharger/+/Yield/Power"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_solarcharger_power)
        topic_str = base_path_str + "/solarcharger/+/Dc/0/#"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_solarcharger_dc_values)
        # System
        topic_str = base_path_str + "/system/+/Ac/Consumption/#"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_system_AC_consumption)
        # Misc
        topic_str = base_path_str + "/settings/+/Settings/CGwacs/#"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_settings_Cgwacs)
        topic_str = base_path_str + "/settings/+/Settings/SystemSetup/#"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_settings_SystemSetup)
        topic_str = base_path_str + "/vebus/+/Mode"
        self.mqtt_client.message_callback_add(topic_str, self.on_msg_multis_switch_mode)
        
    def device_removed_from_bus_detected(self, topic):
        split_topic = topic.split('/')
        device_type = split_topic[2]
        device_instance = split_topic[3]
        try:
            # If the device is a solarcharger the internal structure has an entry for each solarcharger.
            if(device_type != 'solarcharger'):
                self.CCGX_data[device_type] = {}
            else:
                self.CCGX_data[device_type][device_instance] = {}
            self.logger.info('Received json string without "value". Probably device removed from bus. Topic: ' + topic)
        except:
            self.logger.error('Deleting device instance due to empty payload failed for unknown reason.') 
        
                         
    ##################### MQTT Callback Functions ##############
    # The callback when this client recieves A CONNACK from the broker    
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            self.logger.info("Success: Connected to MQTT Server with result code "+str(rc))
            self.mqtt_connection_ok = True
            self.mqtt_disconnected = False
        else:
            self.logger.error("Failed to connected to MQTT Server with result code "+str(rc))
            self.mqtt_connection_ok = False
        
    # The callback when the MQTT broker disconnects
    def on_disconnect(self, client, userdata, rc):
        self.logger.warning("MQTT server disconnected. Reason: "  + str(rc))
        self.mqtt_connection_ok = False
        self.mqtt_disconnected  = True
            
    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        self.logger.debug("New unused (!!!) MQTT message: " + msg.topic +" "+ str(msg.payload))
        #self.store_received_mqtt_message(msg.topic, msg.payload)
        
    def on_subscribe(self, client, userdata, mid, granted_qos):  # subscribe to mqtt broker
        self.logger.debug("MQTT on_subscribe function called!")
    ######### Topic specific callbacks ############################
    # Grid
    def on_msg_grid_power(self, client, userdata, msg):
        try:
            self.CCGX_data['grid']['grid_power_sum'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_L1_power(self, client, userdata, msg):
        try:
            
            self.CCGX_data['grid']['L1_power'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_L1_current(self, client, userdata, msg):
        try:
            self.CCGX_data['grid']['L1_current'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_L2_power(self, client, userdata, msg):
        try:
            self.CCGX_data['grid']['L2_power'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_L2_current(self, client, userdata, msg):
        try:
            self.CCGX_data['grid']['L2_current'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_L3_power(self, client, userdata, msg):
        try:
            self.CCGX_data['grid']['L3_power'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_L3_current(self, client, userdata, msg):
        try:    
            self.CCGX_data['grid']['L3_current'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    # Battery
    def on_msg_battery_soc(self, client, userdata, msg):
        try:
            self.CCGX_data['battery']['soc'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_battery_maxcellvoltage(self, client, userdata, msg):
        try:
            self.CCGX_data['battery']['max_cell_voltage'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_battery_mincellvoltage(self, client, userdata, msg):
        try:
            self.CCGX_data['battery']['min_cell_voltage'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_battery_temp(self, client, userdata, msg):
        try:
            self.CCGX_data['battery']['temperature'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_battery_current(self, client, userdata, msg):
        try:
            self.CCGX_data['battery']['current'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_battery_power(self, client, userdata, msg):
        try:
            self.CCGX_data['battery']['power'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
            
    def on_msg_battery_voltage(self, client, userdata, msg):
        try:
            self.CCGX_data['battery']['voltage'] = json.loads(msg.payload)['value']
        except:
            self.device_removed_from_bus_detected(msg.topic)
    # Solarcharger
    # With "grid" and "battery" callbacks the callback functions only handle one value. With the solarchargers the message handling is 
    # done a bit different, because the chargers are added and removed dynamically and you could also permantly add/remove a solarcharger and 
    # this script should still work. That´s why these solarcharger callback functions are more complex and involve "topic parsing" and handle multiple values. 
    # As long as a solarcharger is active on the bus values are stored. If it vanishes from the bus the whole data structure for this solarcharger
    # is deleted.
    def on_msg_solarcharger_power(self, client, userdata, msg):
        split_topic = msg.topic.split('/')
        try:
            payload_value = json.loads(msg.payload)['value']
            solar_charger_topic_id_str = split_topic[3]
            # If this solarcharger is not known add it to the dictionary
            if(solar_charger_topic_id_str not in self.CCGX_data['solarcharger']):
                self.CCGX_data['solarcharger'][solar_charger_topic_id_str] = {}
            # Store the power value
            self.CCGX_data['solarcharger'][solar_charger_topic_id_str]['Power'] = payload_value
        except:
            self.device_removed_from_bus_detected(msg.topic)
       
    def on_msg_solarcharger_dc_values(self, client, userdata, msg):
        split_topic = msg.topic.split('/')
        try:
            payload_value = json.loads(msg.payload)['value']
            solar_charger_topic_id_str = split_topic[3]
            value_name_str = split_topic[6]
            # If this solarcharger is not known add it to the dictionary
            if(solar_charger_topic_id_str not in self.CCGX_data['solarcharger']):
                self.CCGX_data['solarcharger'][solar_charger_topic_id_str] = {}
            # Store the value in the corresponding data field
            self.CCGX_data['solarcharger'][solar_charger_topic_id_str][value_name_str] = payload_value
        except:
            self.device_removed_from_bus_detected(msg.topic)
    
    # System
    def on_msg_system_AC_consumption(self, client, userdata, msg):
        split_topic = msg.topic.split('/')
        phase_number = split_topic[6]
        if(phase_number != 'NumberOfPhases'):
            measurement_name = split_topic[7]
            value_name = phase_number + '_loads_power_consumption'
            try:
                if(measurement_name == 'Power'):
                    payload_value = json.loads(msg.payload)['value']
                    self.CCGX_data['system'][value_name] = payload_value
            except:
                self.logger.error('Unexpected payload received. Should not happen.')
            
    # Misc
    def on_msg_settings_Cgwacs(self, client, userdata, msg):
        split_topic = msg.topic.split('/')
        value_name = split_topic[6]
        try:
            payload_value = json.loads(msg.payload)['value']
            self.CCGX_data['settings'][value_name] = payload_value
            # Store required path to the settings for write requests
            if('settings_base_path' not in self.CCGX_data):
                self.CCGX_data['settings_base_path'] = 'settings/' + split_topic[3] + '/Settings/'
        except:
            self.logger.error('Unexpected payload received. Should not happen with settings.')
        
    def on_msg_settings_SystemSetup(self, client, userdata, msg):
        split_topic = msg.topic.split('/')
        value_name = split_topic[6]
        try:
            payload_value = json.loads(msg.payload)['value']
            self.CCGX_data['settings'][value_name] = payload_value
        except:
            self.logger.error('Unexpected payload received. Should not happen with settings.')
            
    def on_msg_multis_switch_mode(self, client, userdata, msg):
        split_topic = msg.topic.split('/')
        instance_id = split_topic[3]
        value_name = split_topic[4]
        try:
            if('vebus' not in self.CCGX_data):
                self.CCGX_data['vebus'] = {}
            if(instance_id not in self.CCGX_data['vebus']):
                self.CCGX_data['vebus'][instance_id] = {}
            payload_value = json.loads(msg.payload)['value']
            self.CCGX_data['vebus'][instance_id][value_name] = payload_value
        except:
            self.logger.error('Unexpected payload received. Should not happen with vebus.')
            
################################################################################################################################################################################

  
if __name__ == "__main__":
    ######## Logger Config ###############
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
    my_handler = RotatingFileHandler('essBATT_watchdog.log', mode='a', maxBytes=50*1024*1024, backupCount=1, encoding=None, delay=0)
    my_handler.setFormatter(log_formatter)
    my_handler.setLevel(logging.DEBUG)
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.INFO) # Default is "info" but this setting is later overwritten by ess_config.json setting
    app_log.addHandler(my_handler)
    
    ####### Create essBATT watchdog object ###############################
    essBATT_watchdog_obj = essBATT_watchdog(app_log)

    ######### Start the application ############################
    if(essBATT_watchdog_obj.ess_config_data_loaded_correctly is True):
        try:
            essBATT_watchdog_obj.run()
        finally:
            essBATT_watchdog_obj.logger.warning("essBATT watchdog: Shutdown. Main loop exited and needs restart.")
            # Timer Objects have to be stopped
            essBATT_watchdog_obj.rt_keep_alive_obj.stop()
            essBATT_watchdog_obj.rt_essBATT_watchdog_update_obj.stop()
            essBATT_watchdog_obj.rt_print_status_obj.stop()
            # MQTT Loop needs to be stopped
            essBATT_watchdog_obj.mqtt_client.loop_stop()
    else:
        essBATT_watchdog_obj.logger.warning("essBATT watchdog not running and needs restart!")
        # Timer Object has to be stopped
        essBATT_watchdog_obj.rt_keep_alive_obj.stop()
        essBATT_watchdog_obj.rt_essBATT_watchdog_update_obj.stop()
        essBATT_watchdog_obj.rt_print_status_obj.stop()
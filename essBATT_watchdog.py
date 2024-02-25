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
CHARGE_LIMIT_RESET_DISCHARGE_CURRENT = -4.0
DISCHARGE_LIMIT_RESET_CHARGE_CURRENT = 4.0
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
        self.ess_internal_state = {}
        self.ess_config_data = {}
        self.ess_setvalue_list = {}
        self.ess_controller_state = {}
        self.ess_external_input = {}
        self.ess_config_data_loaded_correctly = False
        self.ess_setvalue_list_loaded_correctly = False
        self.ess_controller_state_loaded_correctly = False
        self.CCGX_data = {'grid':{},'battery':{}, 'solarcharger':{}, 'settings':{}, 'system':{}}
        self.read_config_json()
        self.read_setvalue_list_json()
        self.ess_controller_state = self.read_ess_controller_state_json()
        self.write_base_path = 'W/'+ self.ess_config_data['vrm_id'] + '/'
        self.logger.setLevel(LOGLEVEL_NAME_TO_NUMBER[self.ess_config_data['debug_level']])
        self.logger.info('Effective logger level: ' + str(self.logger.getEffectiveLevel()))
        self.rt_ess_control_update_obj = RepeatedTimer(self.ess_config_data['control_update_rate'], self.ess_control_cycle_update)
        self.rt_print_status_obj = RepeatedTimer(self.ess_config_data['script_alive_logging_interval'], self.print_alive_status_to_logger)
        self.temporary_script_states = self.create_temporary_script_states_dict()
    
    # Not all states off the script need to be stored in "ess_controller_state" file, because it is not really relevant if the state is lost due to script restart etc.
    # All less relevant states shall be stored in this dict.    
    def create_temporary_script_states_dict(self):
        ret_dict = {
            "multi_switch_min_soc_debounce_time": None,
            "winter_mode_multis_switch_off_time": None,
            "winter_mode_inactive_charge_begin_time": None,
            "emergency_(dis)charge_begin_time": None,
            "winter_mode_charge_begin_time": None,
            "discharge_current_limit_state": self.ess_config_data['ess_mode_2_settings']['max_battery_discharge_current'],
            "discharge_current_limit_hit_zero": False,
            "charge_current_limit_state": self.ess_config_data['ess_mode_2_settings']['max_battery_charge_current_2705'],
            "charge_current_limit_hit_zero": False
        }
        return ret_dict
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
            self.logger.info("essBATT controller: Try to connect to MQTT Server: localhost on port " + str(self.ess_config_data['mqtt_server_COM_port']) + " with timeout of " + str(MQTT_SERVER_TIMEOUT_TIMESPAN) + "s")
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
            # Send a keepalive directly after connected to MQTT server
            self.send_keepalive_to_cerbo()
            
            ###### MAIN LOOP - ESS Control Loop while the MQTT connection is active ##########################
            while self.mqtt_connection_ok is True:
                # This is the loop for the ess control cycle update functionality (ess_control_cycle_update()). It is empty because this function is
                # called with the help of the "repeated timer" in a periodical manner (see _init_ function)
                pass
        except:
            self.logger.error('Could not establish connection to MQTT Server. Critical error.')
            self.mqtt_client.loop_stop()
            self.mqtt_connection_ok = False
                
    def ess_control_cycle_update(self):
        # Only update if MQTT connection is active 
        if(self.mqtt_connection_ok is True):   
            # If feature is activated, read the ess_config.json file in each run - helpful if you try out different values while the script is running
            if(self.ess_config_data['check_ess_config_changes_while_running'] == 1):
                self.read_config_json()
                # Some additional settings that otherwise would not change due to online changes (changes while script is running) in ess_config_json.
                self.logger.setLevel(LOGLEVEL_NAME_TO_NUMBER[self.ess_config_data['debug_level']])
                self.rt_ess_control_update_obj.interval = self.ess_config_data['control_update_rate']

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
            ############ Calculate and write output values ################################
            # "Static" settings
            self.set_CCGX_value(set_val_name_str='MaxFeedInPower', set_val=self.ess_config_data['ess_mode_2_settings']['max_system_grid_feed_in_power_2706'], only_set_if_deviation_to_current_setting=True)
            self.set_CCGX_value(set_val_name_str='OvervoltageFeedIn', set_val=self.ess_config_data['ess_mode_2_settings']['feed_excess_dc_coupled_pv_into_grid_2707'], only_set_if_deviation_to_current_setting=True)
            self.set_CCGX_value(set_val_name_str='PreventFeedback', set_val=self.ess_config_data['ess_mode_2_settings']['feed_excess_ac_coupled_pv_into_grid_2708'], only_set_if_deviation_to_current_setting=True)
            
            # Only continue the calculation of the output values if all input values are available
            if(local_values['all_CCGX_values_available']):
                # To keep the battery safe in all situations we need to look continuosly at many variables like min- and max cell voltages, SOC, balancing state etc. and adapt the charge- or discharge limits accordingly
                try:
                    self.calculate_dis_charge_limits(local_values)
                except Exception as e:
                    self.logger.exception('Unhandled Exception!')
                    raise
                # To save some power we want to switch off only the charger, the inverter or the complete multi if it is possible/makes sense
                # e.g. in winter mode if the battery SOC goes below a threshold we can deactivate the inverter AND charger and activate them again e.g. when balancing occurs etc.
                try:
                    self.multis_switch_handling(local_values)
                except Exception as e:
                    self.logger.exception('Unhandled Exception!')
                    raise
                
                # Now we calculate the AcPowerSetPoint - usually this is around 0 for normal operation (default value specified in ess_config.json). This can be changed by internal and external input that requests:
                # 1. Charge/Discharge to SOC x% with maximum current y starting at timestamp z
                # 2. Starting balancing at timestamp z with maximum current y.
                # 3. Scheduled balancing was startet by the script
                # 4. Emergency or winter charging/discharging might be required
                try:
                    self.AcPowerSetPoint_calculation(local_values)
                except Exception as e:
                    self.logger.exception('Unhandled Exception!')
                    raise
                              
                # Publish only if the current settings value does not match the setpoint and if the setpoint was already transmitted over MQTT to save network bandwidth                      
                # Values that change more often
                self.set_CCGX_value(set_val_name_str='AcPowerSetPoint', set_val=local_values['AcPowerSetPoint'], only_set_if_deviation_to_current_setting=True)
                self.set_CCGX_value(set_val_name_str='MaxChargeCurrent', set_val=local_values['charge_current_limit_final'], only_set_if_deviation_to_current_setting=True)
                self.set_CCGX_value(set_val_name_str='MaxDischargePower', set_val=local_values['discharge_power_limit_final'], only_set_if_deviation_to_current_setting=True)
                
            # All tasks done at the end of each control loop run
            self.cleanup_after_control_loop()

        
            
###############################################################################################################################################################################################################################
###############################################################################################################################################################################################################################
###############################################################################################################################################################################################################################
    
    def multis_switch_handling(self, local_values):
        highest_priority_switch_val = 3
        current_time = datetime.now(tz=None)
        # if the current charge limit is zero than we do not need the charger
        if(local_values['charge_current_limit_final'] <= 0.00001):
            #deactivate charger (inverter only)
            highest_priority_switch_val = 2
            self.logger.debug('DEACTIVATE charger because of charge limit final being 0.')
        if(local_values['discharge_power_limit_final'] <= (local_values['solarcharger_power_sum'] * 1.2)):
            #deactivate inverter (charger only)
            # If no charger limitation exists just switch off the inverter
            if(highest_priority_switch_val == 3):
                highest_priority_switch_val = 1
                self.logger.debug('DEACTIVATE inverter because of charge limit final being below solarcharger power sum + 20%: ' + str((local_values['solarcharger_power_sum'] * 1.2)))
            # if we already have a charger limitation and now additionally an inverter limitation switch off both
            elif(highest_priority_switch_val == 2):
                highest_priority_switch_val = 4
                self.logger.debug('DEACTIVATE inverter AND charger because of charge- and discharge limit final being 0.')
            else:
                self.logger.error('Should not occur!')
        
        # Now we check if we are in minimum SOC condition for some time - this is indicated by discharge_current_limit_regular which only looks at SOC or min cell voltage (empty cell indicators)
        # Set the debounce timer if we encounter a "low SOC event"      
        if(local_values['discharge_current_limit_regular'] <= 0.0001 and self.temporary_script_states['multi_switch_min_soc_debounce_time'] is None):
            self.temporary_script_states['multi_switch_min_soc_debounce_time'] = current_time
            self.logger.debug('Low SOC event debounce timer set!')
            #highest_priority_switch_val = 4
        # If the discharge current limit does not indicate a "low SOC event" anymore reset the timer
        if(local_values['discharge_current_limit_regular'] > 0.0001 and self.temporary_script_states['multi_switch_min_soc_debounce_time'] is not None):
            self.temporary_script_states['multi_switch_min_soc_debounce_time'] = None
            self.logger.debug('Low SOC event debounce timer reset!')
        # If the discharge current limit does indicate a "low SOC event" and the timer was already set, check if the difference time passes a threshold
        if(local_values['discharge_current_limit_regular'] <= 0.0001 and self.temporary_script_states['multi_switch_min_soc_debounce_time'] is not None):
            diff_time = current_time - self.temporary_script_states['multi_switch_min_soc_debounce_time']
            self.logger.debug('Low SOC event debounce timer difference (threshold 300): ' + str(diff_time.total_seconds()))
            # Using hardcoded debounce time
            if(diff_time.total_seconds() > 300):
                # if we have detected a steady "low SOC event" we want to switch off inverter and charger
                highest_priority_switch_val = 4
                self.logger.debug('Stable low SOC after debounce time detected!')
        
        # Now we check if we have a winter limitation
        if('winter_mode' in self.ess_controller_state and self.ess_controller_state['winter_mode'] == 'activated'):
            if('winter_SOC_discharge_limit' in self.ess_controller_state and self.ess_controller_state['winter_SOC_discharge_limit'] == "activated"):
                highest_priority_switch_val = 4
                self.logger.debug('Winter mode inverter and charger switch off done!')
                
        #################################################################################################
        # In some operating states it might be necessary to activate the charger/inverter. This handling is as follows:
        # Note: Priority of charge to SOC or balancing is higher than settings from normal operation (like winter mode)
        if(self.ess_controller_state['current_state'] == 'charge_to_SOC'):
            #now we need to determine if its charging or discharging to switch on either charger or inverter
            if(self.ess_controller_state['charge_to_SOC']['requested_current_direction'] == 'discharge'):
                highest_priority_switch_val = 3 # Wanted to put value 2 (inverter only) but it did not work and was fixed to only 30W of discharge - for now it needs to stay on value 'on' to work. TODO: low priority problem
                self.logger.debug('Inverter only multis switch setting due to "charge to SOC state" discharging.')
            elif(self.ess_controller_state['charge_to_SOC']['requested_current_direction'] == 'charge'):
                highest_priority_switch_val = 1
                self.logger.debug('Charger only multis switch setting due to "charge to SOC state" charging.')
        if(self.ess_controller_state['current_state'] == 'balancing'):
            highest_priority_switch_val = 1
            self.logger.debug('Charger only multis switch setting due to "balancing state" charging.')
        
        # Finally set the value in CCGX to a different value
        self.set_multis_switch_mode(switch_position=highest_priority_switch_val)
     
    def cleanup_after_control_loop(self):
        controller_state_from_file = self.read_ess_controller_state_json()
        # if the dict from the file is not equal to the saved file, save it.
        # Rational: Reading the file often does not degrade the SD card as much as writing. So writing is limited to one time per control cycle if a change occured.
        if(controller_state_from_file != self.ess_controller_state):
            self.store_ess_controller_state_in_file()
            self.logger.debug('Change in the controller state dict detected! Storing dict to file ess_controller_state!')
            
    def statemachine_update(self, local_values):
        #### First check the external input ############################################################
        if(self.ess_config_data['external_control_settings']['allow_external_control_over_mqtt'] == 1):
            # First we need to find out if there was an update of the input data since the last statemachine update
            latest_external_input_timestamp_obj = None
            target_state = "none"
            if((('balancing' in self.ess_external_input) and ('receive_time' in self.ess_external_input['balancing'])) and (('charge_to_SOC' in self.ess_external_input) and ('receive_time' in self.ess_external_input['charge_to_SOC']))):
                if(self.ess_external_input['balancing']['receive_time'] > self.ess_external_input['charge_to_SOC']['receive_time']):
                    latest_external_input_timestamp_obj = self.ess_external_input['balancing']['receive_time']
                    target_state = "balancing"
                else:
                    latest_external_input_timestamp_obj = self.ess_external_input['charge_to_SOC']['receive_time']
                    target_state = "charge_to_SOC"
            elif(('charge_to_SOC' in self.ess_external_input) and ('receive_time' in self.ess_external_input['charge_to_SOC'])):
                latest_external_input_timestamp_obj = self.ess_external_input['charge_to_SOC']['receive_time']
                target_state = "charge_to_SOC"
            elif(('balancing' in self.ess_external_input) and ('receive_time' in self.ess_external_input['balancing'])):
                latest_external_input_timestamp_obj = self.ess_external_input['balancing']['receive_time']
                target_state = "balancing"
            else:
                self.logger.debug('Checked external input: Neither "balancing" nor "charge_to_SOC" external command received yet. No state changes required.')
            # If at least one command was received
            if(latest_external_input_timestamp_obj is not None):
                if(self.ess_external_input['new_data_received'] is True):
                    self.copy_external_data_to_internal_state_dict(target_state, local_values)
            #### Now check state changes due to scheduled events from external starttimes  #####################################################
            current_time_obj = datetime.now(tz=None)
            # Case: Immediate state changes because no scheduled charge time is set
            if('external_receive_info' in local_values):
                # Switch on
                target_state_str = local_values['external_receive_info']['target_state']
                if(     ('activated' in local_values['external_receive_info']) and
                        (local_values['external_receive_info']['activated'] is True) and
                        (self.ess_controller_state[target_state_str]['activation_time'] == 'none') and 
                        (self.ess_controller_state[target_state_str]['scheduled_start_time'] == 'none')):
                    self.do_state_update(target_state_str)
                # Switch off
                if(('activated' in local_values['external_receive_info']) and (local_values['external_receive_info']['activated'] is False)):
                    self.do_state_update('normal_operation')
            
            # Case: Checking if scheduled time is passed to switch to the new state   
            target_state_list = ['balancing', 'charge_to_SOC']
            for target_state_str in target_state_list:             
                if( (self.ess_controller_state[target_state_str]['activation_time'] == 'none') and
                    (self.ess_controller_state[target_state_str]['scheduled_start_time'] != 'none')):
                    scheduled_time_obj = self.get_scheduled_starttime_datetime_obj(target_state_str)
                    if(scheduled_time_obj is not None):
                        if(scheduled_time_obj > current_time_obj):
                            self.logger.debug('Switch to "' + target_state_str + '" waiting for scheduled time: ' + self.ess_controller_state[target_state_str]['scheduled_start_time'])
                        else:
                            self.do_state_update(target_state_str)
                            self.logger.info('Switching to "' + target_state_str + '" because scheduled time reached:' + self.ess_controller_state[target_state_str]['scheduled_start_time'])
                    else:
                        self.logger.error('scheduled_time_obj could not be created due to wrong formatted scheduled_start_time string in ess_controller_state!')
                    
        #### Now check state changes due to scheduled events in the configuration file #####################################################
        # AUTO BALANCING #
        # only evaluate if auto balancing is activated
        if(self.ess_config_data['balancing_settings']['auto_balancing_settings']['activate_auto_balancing'] == 1):
            if(self.ess_controller_state['current_state'] != 'balancing'):
                if(self.ess_controller_state['winter_mode'] == 'activated' and self.ess_config_data['winter_mode']['use_winter_mode'] == 1):
                    time_string = self.ess_config_data['winter_mode']['auto_balancing_settings']['weekday'] + ' ' + self.ess_config_data['winter_mode']['auto_balancing_settings']['time']
                    target_weekday_str = self.ess_config_data['winter_mode']['auto_balancing_settings']['weekday']
                    target_diff_days   = self.ess_config_data['winter_mode']['auto_balancing_settings']['days_to_next_autobalancing']
                else:
                    time_string = self.ess_config_data['balancing_settings']['auto_balancing_settings']['weekday'] + ' ' + self.ess_config_data['balancing_settings']['auto_balancing_settings']['time']
                    target_weekday_str = self.ess_config_data['balancing_settings']['auto_balancing_settings']['weekday'] 
                    target_diff_days   = self.ess_config_data['balancing_settings']['auto_balancing_settings']['days_to_next_autobalancing']
                try:                   
                    activate_time_obj = datetime.strptime(time_string, '%A %H:%M')
                    current_time_obj = datetime.now(tz=None)
                    target_time_passed = (current_time_obj.time() > activate_time_obj.time())
                    if(self.ess_controller_state['time_of_last_completed_balancing'] != "none"):
                        try:
                            last_balance_time_obj = datetime.strptime(self.ess_controller_state['time_of_last_completed_balancing'], '%d-%b-%Y (%H:%M:%S.%f)')
                            diff_time = current_time_obj - last_balance_time_obj
                            nof_diff_days = round(diff_time.total_seconds() / (60*60*24))
                            self.logger.debug('Checking autobalancing. Number of days since last balance: ' + str(nof_diff_days))
                            self.logger.debug('Checking autobalancing. Target diff days: ' + str(target_diff_days) + ', Target weekday: ' + str(target_weekday_str) + ' with current weekday: ' + current_time_obj.strftime('%A') + ', target time passed: ' + str(target_time_passed))
                            # Check if condition to activate the balancing is met
                            if((nof_diff_days >= target_diff_days) and (current_time_obj.strftime('%A') == target_weekday_str) and target_time_passed):
                                self.logger.info('Autobalancing condition met!')
                                self.do_state_update('balancing') 
                        except:
                            self.logger.error('Should not occur because value only set internally with no user interaction! time of last balancing in ess_controller_state file has wrong format. See strftime documentation for correct time syntax (%d-%b-%Y (%H:%M:%S.%f)).')
                    else:
                        if((current_time_obj.strftime('%A') == target_weekday_str) and target_time_passed):
                            self.logger.info('First activation autobalancing condition met!')
                            self.do_state_update('balancing') 
                except:
                    self.logger.error('Auto balancing timeinformation (weekday, time or both) are not set correctly. See strftime documentation for correct time syntax (%A,%H:%M).')
        # WINTER MODE #
        if(self.ess_config_data['winter_mode']['use_winter_mode'] == 1):
            # First need to test some hypotheses to find the correct year for winter mode start and end date
            current_time_obj = datetime.now(tz=None)
            current_time_year_str = current_time_obj.strftime('%Y')
            current_year = int(current_time_year_str)
            next_year = current_year + 1
            next_year_str = str(next_year)
            try:
                winter_mode_start_this_year_obj = datetime.strptime(self.ess_config_data['winter_mode']['winter_mode_start_date'] + current_time_year_str, '%d.%m.%Y')
                winter_mode_end_this_year_obj   = datetime.strptime(self.ess_config_data['winter_mode']['winter_mode_end_date'] + current_time_year_str, '%d.%m.%Y')
                winter_mode_end_next_year_obj   = datetime.strptime(self.ess_config_data['winter_mode']['winter_mode_end_date'] + next_year_str, '%d.%m.%Y')
            except:
                self.logger.error('Error in ess_config.json file regarding winter mode start or end dates. Probably wrong format. Use e.g. 12.03. for twelve march.')
            final_start_obj = winter_mode_start_this_year_obj
            # if winter end is after winter start the order is ok and nothing needs to be adapted
            if(winter_mode_end_this_year_obj > winter_mode_start_this_year_obj):
                final_end_obj = winter_mode_end_this_year_obj
            # if winter end is before winter start we need to distinguish: either it is still winter and the transition to summer shall not take place yet, then the end date is still this year
            # but if we already are after the winter end then the end date is next year....argh....this code made me headache. But should work now :-)
            elif((winter_mode_end_this_year_obj < winter_mode_start_this_year_obj) and (current_time_obj < winter_mode_end_this_year_obj)):
                final_end_obj = winter_mode_end_this_year_obj
            elif((winter_mode_end_this_year_obj < winter_mode_start_this_year_obj) and (current_time_obj > winter_mode_end_this_year_obj)):
                final_end_obj = winter_mode_end_next_year_obj
            else:
                self.logger.error('Should not occur. Something is wrong with the winter mode time code.')
            # After start and end date are evaluated we can check the switch conditions
            if((current_time_obj < final_start_obj) and (final_start_obj < final_end_obj)):
                # Only change if a change is necessary
                if(self.ess_controller_state['winter_mode'] != 'not_activated'):
                    self.logger.info('Winter is gone. Go into summer mode!')
                    self.ess_controller_state['winter_mode'] = 'not_activated'
            elif((current_time_obj > final_start_obj) and (current_time_obj < final_end_obj)):
                # Only change if a change is necessary
                if(self.ess_controller_state['winter_mode'] != 'activated'):
                    self.logger.info('Winter is coming! Go into winter mode!')
                    self.ess_controller_state['winter_mode'] = 'activated'
            elif((final_start_obj > final_end_obj) and (current_time_obj < final_end_obj)):
                # Only change if a change is necessary
                if(self.ess_controller_state['winter_mode'] != 'activated'):
                    self.logger.info('Winter is coming! Go into winter mode!')
                    self.ess_controller_state['winter_mode'] = 'activated'
            elif((current_time_obj > final_start_obj) and (final_start_obj < final_end_obj)):
                # Only change if a change is necessary
                if(self.ess_controller_state['winter_mode'] != 'not_activated'):
                    self.logger.info('Winter is gone. Go into summer mode!')
                    self.ess_controller_state['winter_mode'] = 'not_activated'
            else:         
                self.logger.error('Should not occur. Another thing is wrong with the winter/summer decision.')
                self.logger.error('current_time_obj: ' + str(current_time_obj) + ', final_start_obj: ' + str(final_start_obj) + ', final_end_obj: ' + str(final_end_obj))
                
            # Now check the mode SOC condition and set state accordingly
            if('battery_soc' in local_values):
                if(     self.ess_controller_state['winter_mode'] == 'activated' 
                   and  ((local_values['battery_soc'] <= self.ess_config_data['winter_mode']['winter_min_SOC'] and  self.ess_controller_state['winter_SOC_discharge_limit'] == "not_activated") # "normal" condition for winter_SOC_discharge_limit activation
                   or    (self.ess_controller_state['winter_SOC_discharge_limit'] == "activated" and self.temporary_script_states['winter_mode_multis_switch_off_time'] is None))): # condition if a skript restart occurs and we need to recover the temporary state
                    self.ess_controller_state['winter_SOC_discharge_limit'] = "activated"
                    self.temporary_script_states['winter_mode_multis_switch_off_time'] = datetime.now(tz=None)
                    self.logger.info('Winter SOC discharge limit ACTIVATED because its winter and SOC is/was equal or below ' + str(self.ess_config_data['winter_mode']['winter_min_SOC']))
                if(self.ess_controller_state['winter_mode'] == 'activated' and local_values['battery_soc'] >= self.ess_config_data['winter_mode']['winter_restart_multis_SOC'] and self.ess_controller_state['winter_SOC_discharge_limit'] == "activated"):
                    self.ess_controller_state['winter_SOC_discharge_limit'] = "not_activated"
                    self.temporary_script_states['winter_mode_multis_switch_off_time'] = None
                    self.logger.info('Winter SOC discharge limit DEACTIVATED because its winter and SOC is equal/above ' + str(self.ess_config_data['winter_mode']['winter_restart_multis_SOC']))
            
            
            # Now check if charging due to low cell voltage in winter mode is required to protect the cells
            if(self.temporary_script_states['winter_mode_multis_switch_off_time'] is not None and local_values['all_CCGX_values_available']):
                # if multis are already switched off due to winter mode for more than 10 Minutes (settling time hardcoded)
                # and the min cell voltage is equal/below the configured threshold
                diff_time_multis_switch_off_obj = current_time_obj - self.temporary_script_states['winter_mode_multis_switch_off_time']
                if(local_values['battery_min_cell_voltage'] <= self.ess_config_data['winter_mode']['winter_inactive_charge_min_voltage'] 
                   and self.temporary_script_states['winter_mode_inactive_charge_begin_time'] is None 
                   and diff_time_multis_switch_off_obj.total_seconds() > 600):
                    self.temporary_script_states['winter_mode_inactive_charge_begin_time'] = current_time_obj
                    self.activate_charge_to_SOC_from_script(target_soc = 80, max_current = 20, current_direction='charge')
                    self.logger.info('"STARTING" charging due to "WINTER MODE INACTIVE CHARGE" begin. Minimum cell voltage: ' + str(local_values['battery_min_cell_voltage']))
                # If the charging is ongoing check the condition to stop the charging
                if(self.temporary_script_states['winter_mode_inactive_charge_begin_time'] is not None):
                    diff_time_charge_started_obj = current_time_obj - self.temporary_script_states['winter_mode_inactive_charge_begin_time']
                    if(diff_time_charge_started_obj.total_seconds() > (self.ess_config_data['winter_mode']['winter_inactive_charge_time_minutes'] * 60)):
                        self.temporary_script_states['winter_mode_inactive_charge_begin_time'] = None
                        self.do_state_update('normal_operation')
                        self.logger.info('"ENDING" charging due to "WINTER MODE INACTIVE CHARGE" time minutes passed. Number of minutes passed: ' + str(diff_time_charge_started_obj.total_seconds()/60))
            
            
            # # TODO: Keep minimum voltage - Wie umsetzen????
            # if(local_values['all_CCGX_values_available']):
            #     if((self.temporary_script_states['winter_mode_charge_begin_time'] is None) and (local_values['battery_min_cell_voltage'] <= self.ess_config_data['winter_mode']['winter_inactive_charge_min_voltage'])):
            #         self.temporary_script_states['winter_mode_charge_begin_time'] = current_time_obj
            #         self.activate_charge_to_SOC_from_script(target_soc = 80, max_current = 20, current_direction='charge')
            #         self.logger.info('"STARTING" charging due to "WINTER MODE KEEP MINIMUM VOLTAGE" reached. Minimum cell voltage: ' + str(local_values['battery_min_cell_voltage']))
            #     if(self.temporary_script_states['winter_mode_charge_begin_time'] is not None):
            #         diff_time = current_time_obj - self.temporary_script_states['winter_mode_charge_begin_time']
            #         if(diff_time.total_seconds() > 600):
            #             self.temporary_script_states['winter_mode_charge_begin_time'] = None
            #             self.do_state_update('normal_operation')
            #             self.logger.info('"ENDING" charging due to "WINTER MODE KEEP MINIMUM VOLTAGE" time minutes passed. Number of minutes passed: ' + str(diff_time.total_seconds()/60))
                    
        elif(self.ess_config_data['winter_mode']['use_winter_mode'] == 0):
            pass # do nothing - just checking wrong values
        else:
            self.logger.error('Wrong use_winter_mode value in ess_config.json!')
        
        ############## Emergency (dis)charge MODE #######################
        if(self.ess_config_data['battery_settings']['emergency_(dis)charge']['use_emergency_(dis)charging'] == 1):
            if(local_values['all_CCGX_values_available']):
                current_time_obj = datetime.now(tz=None)
                # CHARGING
                if(local_values['battery_min_cell_voltage'] <= self.ess_config_data['battery_settings']['emergency_(dis)charge']['min_cell_voltage_for_emergency_charge']):
                    # If the emergency charge condition is met and is not activated yet --> activate charging
                    if(self.temporary_script_states['emergency_(dis)charge_begin_time'] is None):
                        self.temporary_script_states['emergency_(dis)charge_begin_time'] = current_time_obj
                        self.activate_charge_to_SOC_from_script(target_soc = 80, max_current = 10, current_direction='charge')
                        self.logger.info('"STARTING" charging due to "EMERGENCY"!!! Minimum cell voltage: ' + str(local_values['battery_min_cell_voltage']))
                # DISCHARGING
                if(local_values['battery_max_cell_voltage'] >= self.ess_config_data['battery_settings']['emergency_(dis)charge']['max_cell_voltage_for_emergency_discharge']):
                    # If the emergency discharge condition is met and is not activated yet --> activate discharging
                    if(self.temporary_script_states['emergency_(dis)charge_begin_time'] is None):
                        self.temporary_script_states['emergency_(dis)charge_begin_time'] = current_time_obj
                        self.activate_charge_to_SOC_from_script(target_soc = 10, max_current = 10, current_direction='discharge')
                        self.logger.info('"STARTING" discharging due to "EMERGENCY"!!! Maximum cell voltage: ' + str(local_values['battery_max_cell_voltage']))       
                # FINISHED: If emergency (dis)charging is ongoing check the abort condition         
                if(self.temporary_script_states['emergency_(dis)charge_begin_time'] is not None):
                    diff_time_emergency_started = current_time_obj - self.temporary_script_states['emergency_(dis)charge_begin_time']
                    if(diff_time_emergency_started.total_seconds() > (self.ess_config_data['battery_settings']['emergency_(dis)charge']['emergency_(dis)charge_duration_minutes']*60)):
                        self.temporary_script_states['emergency_(dis)charge_begin_time'] = None
                        self.do_state_update('normal_operation')
                        self.logger.info('"ENDING" (dis)charging due to "EMERGENCY". Number of minutes passed: ' + str(diff_time_emergency_started.total_seconds()/60) + '. Max cell voltage: ' + str(local_values['battery_max_cell_voltage']) + '. Min cell voltage: ' + str(local_values['battery_min_cell_voltage']))
                
        #### Now check if the balancing or charge_to_SOC conditions are met so that the system can go back to normal state #################
        if(local_values['all_CCGX_values_available']):
            if(self.ess_controller_state['current_state'] == 'balancing'):
                # If the min cell voltage is above the defined threshold and if min and max cell voltage are within a defined limit
                if((local_values['battery_min_cell_voltage'] >= self.ess_config_data['balancing_settings']['balancing_complete_condition']['min_cell_voltage_threshold'])
                    and (abs(local_values['battery_min_cell_voltage'] - local_values['battery_max_cell_voltage']) < self.ess_config_data['balancing_settings']['balancing_complete_condition']['max_diff_voltage_between_min_and_max_cell'])):
                    self.logger.info('Balancing complete condition met! Switching to normal operation. min_cell_voltage_threshold: ' + str(self.ess_config_data['balancing_settings']['balancing_complete_condition']['min_cell_voltage_threshold']) + ' und max_diff_voltage_between_min_and_max_cell: ' + str(self.ess_config_data['balancing_settings']['balancing_complete_condition']['max_diff_voltage_between_min_and_max_cell']))
                    self.do_state_update('normal_operation')
                    self.ess_controller_state['time_of_last_completed_balancing'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")                   
            elif((self.ess_controller_state['current_state'] == 'charge_to_SOC') and (self.ess_controller_state['charge_to_SOC']['requested_current_direction'] != 'none')):
                if(self.ess_controller_state['charge_to_SOC']['requested_current_direction'] == 'discharge'):
                    if(local_values['battery_soc'] <= self.ess_controller_state['charge_to_SOC']['target_SOC']):
                        self.logger.info('Charge to SOC completed! Switching to normal operation. Current direction: ' + self.ess_controller_state['charge_to_SOC']['requested_current_direction'] + ', battery SOC: ' + str(local_values['battery_soc']) + ', target SOC: ' + str(self.ess_controller_state['charge_to_SOC']['target_SOC']))
                        self.do_state_update('normal_operation')
                elif(self.ess_controller_state['charge_to_SOC']['requested_current_direction'] == 'charge'):
                    if(local_values['battery_soc'] >= self.ess_controller_state['charge_to_SOC']['target_SOC']):
                        self.logger.info('Charge to SOC completed! Switching to normal operation. Current direction: ' + self.ess_controller_state['charge_to_SOC']['requested_current_direction'] + ', battery SOC: ' + str(local_values['battery_soc']) + ', target SOC: ' + str(self.ess_controller_state['charge_to_SOC']['target_SOC']))
                        self.do_state_update('normal_operation')                       
                elif(self.ess_controller_state['charge_to_SOC']['requested_current_direction'] == 'SOC_reached'):
                    self.logger.info('Charge to SOC completed! Switching to normal operation. Current direction: ' + self.ess_controller_state['charge_to_SOC']['requested_current_direction'] + ', battery SOC: ' + str(local_values['battery_soc']) + ', target SOC: ' + str(self.ess_controller_state['charge_to_SOC']['target_SOC']))
                    self.do_state_update('normal_operation')                   
                else:
                    self.logger.error('requested_current_direction has unknown state: ' + self.ess_controller_state['charge_to_SOC']['requested_current_direction'])
            elif(self.ess_controller_state['current_state'] == 'normal_operation'):
                # No actions required - just here to check for unknown states 
                pass
            else:
                self.logger.error('Unknown string in "current_state": ' + self.ess_controller_state['current_state'])
                
    def activate_charge_to_SOC_from_script(self, target_soc, max_current="none", current_direction='charge'):
        """
        This function activates the charging/discharging immediately and has no scheduled start time functionality.
        """
        # Clipping to min and max values
        if(target_soc > 99.9):
            target_soc = 99.9
            self.logger.debug('Charge to SOC target SOC limited to 99.9')
        if(target_soc < 0.1):
            target_soc = 0.1
            self.logger.debug('Charge to SOC target SOC limited to 0.1')
        self.ess_controller_state['charge_to_SOC']['target_SOC'] = target_soc
        self.ess_controller_state['charge_to_SOC']['max_current'] = max_current
        self.ess_controller_state['charge_to_SOC']['requested_current_direction'] = current_direction
        self.do_state_update('charge_to_SOC')
        
    
    def get_scheduled_starttime_datetime_obj(self, mode_string):
        format_string = self.ess_config_data['external_control_settings']['date_format'] + ' ' + self.ess_config_data['external_control_settings']['time_format']
        try:
            ret_val = datetime.strptime(self.ess_controller_state[mode_string]['scheduled_start_time'], format_string)
        except:
            ret_val = None
        return ret_val
       
    def do_state_update(self, target_state_str):
        now_string = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
        self.logger.info('State change from ' + self.ess_controller_state['current_state'] + ' to "' + target_state_str.upper() + '"!')
        self.ess_controller_state['current_state'] = target_state_str
        self.ess_controller_state['time_of_last_change'] = now_string
        if((target_state_str == 'balancing') or (target_state_str == 'charge_to_SOC')):
            self.ess_controller_state[target_state_str]['activation_time'] = now_string
        if(target_state_str == 'normal_operation'):
            self.reset_single_state_data('balancing')
            self.reset_single_state_data('charge_to_SOC')

            
    def copy_external_data_to_internal_state_dict(self, target_state_str, local_values):
        local_values['external_receive_info'] = {}
        # More complex handling when target state is balancing or charge_to_SOC...
        if((target_state_str == "balancing") or (target_state_str == "charge_to_SOC")):
            # A state change should only be performed if the new state is different to the current state
            # or if a state was switched off (which brings it automatically to state "normal_operation")
            # First: different state
            #if(self.ess_controller_state['current_state'] != target_state_str):
            if((target_state_str == "balancing") and (self.ess_external_input['balancing']['activated'] == '1')):
                # When receiving a new activation command the other state data is profilactically deleted
                self.reset_single_state_data('charge_to_SOC')
                if('current_limit_input' in self.ess_external_input['balancing']):
                    self.ess_controller_state['balancing']['max_current'] = self.ess_external_input['balancing']['current_limit_input']
                self.update_state_scheduledtime_from_external_input(target_state_str)
                local_values['external_receive_info']['target_state'] = target_state_str
                local_values['external_receive_info']['activated'] = True
                self.ess_external_input['new_data_received'] = False
                # After everything is read and copy we need to flush the external input data
                self.ess_external_input['balancing'] = {}
                self.logger.debug('Copied balancing external input data to ess controller state because of switch on command.')
            elif((target_state_str == "charge_to_SOC") and (self.ess_external_input['charge_to_SOC']['activated'] == '1')):
                # When receiving a new activation command the other state data is profilactically deleted
                #TODO Send switch off command to balancing topic to switch it off on sender site
                self.reset_single_state_data('balancing')
                # First evaluate if the goal is to discharge or to charge
                if('target_SOC' in self.ess_external_input['charge_to_SOC']):
                    if(self.ess_external_input['charge_to_SOC']['target_SOC'] > local_values['battery_soc']):
                        self.ess_controller_state['charge_to_SOC']['requested_current_direction'] = 'charge'
                    elif(self.ess_external_input['charge_to_SOC']['target_SOC'] == local_values['battery_soc']):
                        self.ess_controller_state['charge_to_SOC']['requested_current_direction'] = 'SOC_reached'
                        self.logger.info('Target SOC for charge_to_SOC already "REACHED"!')
                    else:
                        self.ess_controller_state['charge_to_SOC']['requested_current_direction'] = 'discharge'
                else:
                    self.logger.error('target_SOC was not in self.ess_external_input["charge_to_SOC"] but is mandatory for this command!')
                self.logger.debug('(Dis-)charge? --> ' + self.ess_controller_state['charge_to_SOC']['requested_current_direction'])
                # Now set the rest of the values
                if('target_SOC' in self.ess_external_input['charge_to_SOC']):
                    # Clipping to min and max values
                    if(self.ess_external_input['charge_to_SOC']['target_SOC'] > 99.9):
                        self.ess_controller_state['charge_to_SOC']['target_SOC'] = 99.9
                        self.logger.debug('Charge to SOC target SOC limited to 99.9')
                    elif(self.ess_external_input['charge_to_SOC']['target_SOC'] < 0.1):
                        self.ess_controller_state['charge_to_SOC']['target_SOC'] = 0.1
                        self.logger.debug('Charge to SOC target SOC limited to 0.1')
                    else:
                        self.ess_controller_state['charge_to_SOC']['target_SOC'] = self.ess_external_input['charge_to_SOC']['target_SOC']
                if('current_limit_input' in self.ess_external_input['charge_to_SOC']):
                    self.ess_controller_state['charge_to_SOC']['max_current'] = self.ess_external_input['charge_to_SOC']['current_limit_input']
                self.update_state_scheduledtime_from_external_input(target_state_str)
                local_values['external_receive_info']['target_state'] = target_state_str
                local_values['external_receive_info']['activated'] = True
                self.ess_external_input['new_data_received'] = False
                # After everything is read and copy we need to flush the external input data
                self.ess_external_input['charge_to_SOC'] = {}
                self.logger.debug('Copied charge_to_SOC external input data to ess controller state because of switch on command.')
            # Second: same state but switched off
            #else:
            elif((target_state_str == "balancing") and (self.ess_external_input['balancing']['activated'] == '0')):
                self.reset_single_state_data('balancing')
                self.logger.info('Balancing switch off command registered!')
                local_values['external_receive_info']['target_state'] = target_state_str
                local_values['external_receive_info']['activated'] = False
                self.ess_external_input['new_data_received'] = False
                # After everything is read and copy we need to flush the external input data
                self.ess_external_input['balancing'] = {}

            elif((target_state_str == "charge_to_SOC") and (self.ess_external_input['charge_to_SOC']['activated'] == '0')):
                self.reset_single_state_data('charge_to_SOC')
                self.logger.info('Charge to SOC switch off command registered!')
                local_values['external_receive_info']['target_state'] = target_state_str
                local_values['external_receive_info']['activated'] = False
                self.ess_external_input['new_data_received'] = False
                # After everything is read and copy we need to flush the external input data
                self.ess_external_input['charge_to_SOC'] = {}
            else:
                self.logger.error('Unknown state: ' + target_state_str)
                return
     
    
    def update_state_scheduledtime_from_external_input(self, mode_string):
        if('date_input' not in self.ess_external_input[mode_string]):
            date_input_local = '-'
        else:
            date_input_local = self.ess_external_input[mode_string]['date_input']
        if('time_input' not in self.ess_external_input[mode_string]):
            time_input_local = '-'
        else:
            time_input_local = self.ess_external_input[mode_string]['time_input']
        scheduled_start_time = self.datetime_obj_from_input_timestamp(time_input_local, date_input_local)
        # If the return value scheduled_start_time is not a number (datetime_obj_from_input_timestamp() returns -1 if no starttime can be set)
        if((not isinstance(scheduled_start_time, numbers.Number)) and (scheduled_start_time is not None)):
            format_string = self.ess_config_data['external_control_settings']['date_format'] + ' ' + self.ess_config_data['external_control_settings']['time_format']
            self.ess_controller_state[mode_string]['scheduled_start_time'] = scheduled_start_time.strftime(format_string)
        else:
            self.ess_controller_state[mode_string]['scheduled_start_time'] = 'none'
        self.logger.debug('Updated ' + mode_string + 'scheduled start time to ' + self.ess_controller_state[mode_string]['scheduled_start_time']) 
                
    def reset_single_state_data(self, reset_state):
        # Update the potentially changed settings in all cases
        if(reset_state == "balancing"):
            self.ess_controller_state['balancing']['activation_time'] = 'none'
            self.ess_controller_state['balancing']['max_current'] = 'none'
            self.ess_controller_state['balancing']['scheduled_start_time'] = 'none'
            self.logger.debug('Reset state: ' + reset_state)
        elif(reset_state == "charge_to_SOC"):
            self.ess_controller_state['charge_to_SOC']['activation_time'] = 'none'
            self.ess_controller_state['charge_to_SOC']['target_SOC'] = 'none'
            self.ess_controller_state['charge_to_SOC']['max_current'] = 'none'
            self.ess_controller_state['charge_to_SOC']['scheduled_start_time'] = 'none'
            self.ess_controller_state['charge_to_SOC']['requested_current_direction'] = 'none'
            self.logger.debug('Reset state: ' + reset_state)
        else:
            self.logger.error('Unknown state: ' + reset_state)
            
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
                                       
    
    def AcPowerSetPoint_calculation(self, local_values):
        local_values['AcPowerSetPoint'] = 0 # default case if something fails
        if(self.ess_controller_state['current_state'] == "normal_operation"):
            # In normal operation the AcPowerSetPoint is the user defined value taken from the ess_config file
            local_values['AcPowerSetPoint'] = int(self.ess_config_data['ess_mode_2_settings']['grid_power_setpoint_2700'])
            self.logger.debug('AcPowerSetPoint from NORMAL OPERATION set to:' + str(local_values['AcPowerSetPoint']))  
        elif(self.ess_controller_state['current_state'] == 'charge_to_SOC'):
            # First check if (dis)charge is limited by time
            
            # Calc AcPowerSetPoint based on the given or set (dis)charge limits. Calculated Setpoint is 10% higher given through the limits, loads and solarpower input because the maximum (dis)charge
            # current should be reached and is of course limited by the separate CCGX limits. To put it differently: the current limit is NOT realized with the AcPowerSetPoint
            # but with the current/power limits. AcPowerSetPoint is only set to 10% above the limit (+loads and solarpower), to have some fallback safety. Didn´t feel right to put an more or less unlimited power value out
            # that might be much above the safety limit for the battery
            if(self.ess_controller_state['charge_to_SOC']['requested_current_direction'] == 'charge'):
                max_charge_power_final = ((local_values['charge_current_limit_final'] * local_values['battery_voltage']) + local_values['loads_total_power'] - local_values['solarcharger_power_sum']) * 1.1
                local_values['AcPowerSetPoint'] = int(max_charge_power_final)        
            elif(self.ess_controller_state['charge_to_SOC']['requested_current_direction'] == 'discharge'):
                local_values['AcPowerSetPoint'] = int((-local_values['discharge_power_limit_final'] - local_values['solarcharger_power_sum'] + local_values['loads_total_power']) * 1.1)
            elif(self.ess_controller_state['charge_to_SOC']['requested_current_direction'] == 'SOC_reached'):
                local_values['AcPowerSetPoint'] = int(self.ess_config_data['ess_mode_2_settings']['grid_power_setpoint_2700'])
            else:
                self.logger.error('Unknwon requested discharge direction: ' + self.ess_controller_state['charge_to_SOC']['requested_current_direction'])
                return
            self.logger.debug('AcPowerSetPoint from CHARGE TO SOC set to:' + str(local_values['AcPowerSetPoint']))
        elif(self.ess_controller_state['current_state'] == "balancing"):
            # Calc AcPowerSetPoint based on the given or set (dis)charge limits and corrected with the power consumed by the loads
            max_charge_power_final = ((local_values['charge_current_limit_final'] * local_values['battery_voltage']) + local_values['loads_total_power'] - local_values['solarcharger_power_sum']) * 1.1
            local_values['AcPowerSetPoint'] = int(max_charge_power_final)  
            self.logger.debug('AcPowerSetPoint from BALANCING set to:' + str(local_values['AcPowerSetPoint']))
            self.logger.debug('AcPowerSetPoint Calculation:' + str(local_values['AcPowerSetPoint']) + '= (Current limit final:' + str(local_values['charge_current_limit_final']) + '* battery voltage: ' + str(local_values['battery_voltage']) + ') + total loads: ' + str(local_values['loads_total_power']) + ' - solarcharger input: ' + str(local_values['solarcharger_power_sum']))
            pass
        else:
            self.logger.error('Unknown "current state". Check ess_controller_state file.')
        pass
    
    def calculate_dis_charge_limits(self, local_values):
        local_values['charge_current_limit_regular'] = self.get_charge_current_limit_with_battery_protection()
        local_values['discharge_current_limit_regular'] = self.get_discharge_current_limit_with_battery_protection()                           
                      
        # If a current limit is limiting the battery output power there sometimes is a more or less constant violation of this limit. First we need to calculate the deviation
        # and if it is configured (ess_config_data['battery_settings']['compensate_current_limit_violations'] == 1) output current is further reduced by this amount
                    
        local_values['charge_current_limit_violation'] = False
        local_values['discharge_current_limit_violation'] = False
        # Case discharge limit violation
        if((local_values['battery_current'] < 0.0) and (abs(local_values['battery_current']) > local_values['discharge_current_limit_regular'])):
            local_values['violation_current'] = abs(local_values['battery_current']) - local_values['discharge_current_limit_regular']
            local_values['charge_current_limit_violation'] = True
        # Case charge limit violation
        elif((local_values['battery_current'] > 0.0) and (abs(local_values['battery_current']) > local_values['charge_current_limit_regular'])):
            local_values['violation_current'] = abs(local_values['battery_current']) - local_values['charge_current_limit_regular']
            local_values['charge_current_limit_violation'] = True
        else:
            pass
                    
        # For charging the MaxChargeCurrent register 2705 takes all input and outputs into account and can be set directly. For discharging
        # we can only set the power in register 2704 (MaxDischargePower). To be able to stay within the configurated current bounds we need to calculate
        # how the current limit maps to the discharge output power limit taking into account also the solarcharger input.       
        # Max output power Multi                      = (Maximum power the battery is allowed to deliver) + (Current solar input power) 
        local_values['discharge_power_limit_regular'] = self.calc_discharge_power_limit_from_current(local_values, local_values['discharge_current_limit_regular'])
        self.logger.debug('Discharge power limit:' + str(local_values['discharge_power_limit_regular']) + 'W.')
        # If configurated compensate for DC to AC losses (not sure if this is always helpful)
        if(self.ess_config_data['battery_settings']['compensate_current_limit_violations'] == 1):
            if(local_values['discharge_current_limit_violation'] is True):
                local_values['discharge_power_limit_regular'] = local_values['discharge_power_limit_regular'] - (local_values['violation_current'] * local_values['battery_voltage'])
                self.logger.debug('Discharge power limit violated and compensated by: ' + str(local_values['violation_current']) + 'A. Discharge power limit now: ' + str(local_values['discharge_power_limit_regular']))
            elif(local_values['charge_current_limit_violation'] is True):
                local_values['charge_current_limit_regular'] = local_values['charge_current_limit_regular'] - local_values['violation_current']
                self.logger.debug('Charge current limit violated and compensated by: ' + str(local_values['violation_current']) + 'W. Charge current limit now: ' + str(local_values['charge_current_limit_regular']) + 'A.')
            else:
                pass                    
        self.logger.debug('Max output power multi (regular): ' + str(local_values['discharge_power_limit_regular']))
        ########## Now we take the special winter limits from ess_config into account #######
        if('winter_mode' in self.ess_controller_state and self.ess_controller_state['winter_mode'] == 'activated'):
            if('winter_SOC_discharge_limit' in self.ess_controller_state and self.ess_controller_state['winter_SOC_discharge_limit'] == "activated"):
                local_values['winter_discharge_limit'] = 0.0
        ########## Now we take the current ess_controller state given through external commands into account #######
        # Note that the user can only restrict the (dis)charge limits further and NOT weaken the limits in any way. The limits calculated until here
        # are the basic battery and system protection. But if a user wants to have stricter limits for balancing or (dis)charging they can be set
        local_values['external_current_limit_set'] = False
        if(self.ess_controller_state['current_state'] == 'balancing'):
            if(self.ess_controller_state['balancing']['max_current'] != 'none'):
                local_values['external_current_limit'] = self.ess_controller_state['balancing']['max_current']
                local_values['external_current_limit_set'] = True
        elif(self.ess_controller_state['current_state'] == 'charge_to_SOC'):
            if(self.ess_controller_state['charge_to_SOC']['max_current'] != 'none'):
                local_values['external_current_limit'] = self.ess_controller_state['charge_to_SOC']['max_current']
                local_values['external_current_limit_set'] = True
        elif(self.ess_controller_state['current_state'] == 'normal_operation'):
            # Just here for the 'current_state' string check
            pass
        else:
            self.logger.error('Unknown ess controller state: ' + self.ess_controller_state['current_state'])
            
        ####### Now we check if charging or discharging is completely forbidden by external commands #################################
        if('deactivate_charge' in self.ess_external_input):
            if(self.ess_external_input['deactivate_charge']['activated']):
                local_values['deactivate_charge_limit'] = 0.0
        if('deactivate_discharge' in self.ess_external_input):
            if(self.ess_external_input['deactivate_discharge']['activated']):
                local_values['deactivate_discharge_limit'] = 0.0     
              
        ####### Now we introduce a new limit based on the (dis)charge_limit_regular, that lets the discharge limit "snap" to the lowest/highest limit the min/max cell voltage has triggered in that cycle to smooth the allowed current
        if(self.ess_config_data['battery_settings']['smooth_voltage_based_(dis)charge_limits'] == 1):
            # New limit for discharging
            # If the regular current limit is smaller than the max battery discharge current we start to hit the discharge limits which we want to control/filter 
            discharge_regular_limit_diff = local_values['discharge_current_limit_regular'] - self.temporary_script_states['discharge_current_limit_state']
            # if the current limit is smaller than the limit from the last cycle the difference will be negative --> we hit a new discharge current threshold
            if(discharge_regular_limit_diff < 0):
                self.temporary_script_states['discharge_current_limit_state'] = local_values['discharge_current_limit_regular']
                self.logger.debug('Discharge current limit got stricter. Now is: ' + str(self.temporary_script_states['discharge_current_limit_state']))
            # To have a better reset condition we need to store if we have hit the "zero current" limit
            if(local_values['discharge_current_limit_regular'] < 0.1):
                self.temporary_script_states['discharge_current_limit_hit_zero'] = True
            # Now that we have the set condition for the 'discharge_current_limit_state' we now need the reset condition:
            # If the minimum cell voltage gets above the "min_cell_voltage_discharging_resume" from ess_config.json "the discharge limit fixing" done here will be reset
            if(   (local_values['battery_min_cell_voltage'] > self.ess_config_data['battery_settings']['min_cell_voltage_discharging_resume'] and self.temporary_script_states['discharge_current_limit_hit_zero'] is True)
                or ((local_values['battery_current'] > DISCHARGE_LIMIT_RESET_CHARGE_CURRENT) and (self.temporary_script_states['discharge_current_limit_state'] < self.ess_config_data['ess_mode_2_settings']['max_battery_discharge_current']))):
                self.temporary_script_states['discharge_current_limit_state'] = self.ess_config_data['ess_mode_2_settings']['max_battery_discharge_current']
                self.temporary_script_states['discharge_current_limit_hit_zero'] = False
                self.logger.info('Discharge current limit reset to default value! Now: ' + str(self.temporary_script_states['discharge_current_limit_state']))
                if((local_values['battery_min_cell_voltage'] > self.ess_config_data['battery_settings']['min_cell_voltage_discharging_resume'] and self.temporary_script_states['discharge_current_limit_hit_zero'] is True)):
                    self.logger.info('Condition that triggered the reset: Min cell voltage above discharge resume voltage and discharge current limit had hit zero before.')
                if((local_values['battery_min_cell_voltage'] > self.ess_config_data['battery_settings']['min_cell_voltage_discharging_resume'] and self.temporary_script_states['discharge_current_limit_hit_zero'] is True)):
                    self.logger.info('Condition that triggered the reset: Battery current above "reset charge current" (internal script parameter) and the stored discharge limit was not yet reset.')
            # New limit for charging
            # If the regular current limit is smaller than the max battery discharge current we start to hit the discharge limits which we want to control/filter 
            charge_regular_limit_diff = local_values['charge_current_limit_regular'] - self.temporary_script_states['charge_current_limit_state']
            # if the current limit is smaller than the limit from the last cycle the difference will be negative --> we hit a new charge current threshold
            if(charge_regular_limit_diff < 0):
                self.temporary_script_states['charge_current_limit_state'] = local_values['charge_current_limit_regular']
                self.logger.debug('Charge current limit got stricter. Now is: ' + str(self.temporary_script_states['charge_current_limit_state']))
            # To have a better reset condition we need to store if we have hit the "zero current" limit
            if(local_values['charge_current_limit_regular'] < 0.1):
                self.temporary_script_states['charge_current_limit_hit_zero'] = True
            # Now that we have the set condition for the 'charge_current_limit_state' we now need the reset condition:
            # If the maximum cell voltage gets below the "max_cell_voltage_charging_resume" from ess_config.json and previously has hit the charge limit 0 
            # OR we have a strong discharge signal "the charge limit fixing" done here will be reset
            if(   (local_values['battery_max_cell_voltage'] <= self.ess_config_data['battery_settings']['max_cell_voltage_charging_resume'] and self.temporary_script_states['charge_current_limit_hit_zero'] is True)
                or ((local_values['battery_current'] < CHARGE_LIMIT_RESET_DISCHARGE_CURRENT) and (self.temporary_script_states['charge_current_limit_state'] < self.ess_config_data['ess_mode_2_settings']['max_battery_charge_current_2705']))):
                self.temporary_script_states['charge_current_limit_state'] = self.ess_config_data['ess_mode_2_settings']['max_battery_charge_current_2705']
                self.temporary_script_states['charge_current_limit_hit_zero'] = False
                self.logger.info('Charge current limit reset to default value! Now: ' + str(self.temporary_script_states['charge_current_limit_state']))
                if((local_values['battery_max_cell_voltage'] <= self.ess_config_data['battery_settings']['max_cell_voltage_charging_resume'] and self.temporary_script_states['charge_current_limit_hit_zero'] is True)):
                    self.logger.info('Condition that triggered the reset: Max cell voltage below charge resume voltage and charge current limit had hit zero before.')
                if(((local_values['battery_current'] < CHARGE_LIMIT_RESET_DISCHARGE_CURRENT) and (self.temporary_script_states['charge_current_limit_state'] < self.ess_config_data['ess_mode_2_settings']['max_battery_charge_current_2705']))):
                    self.logger.info('Condition that triggered the reset: Battery current above "reset discharge current" (internal script parameter) and the stored charge limit was not yet reset.')
        
        ###### Now bring all limits together and aggregate the final limits ##########################################################
        
        # Append all valid charge current limits
        charge_current_limits_list = []
        info_str = ''
        if('charge_current_limit_regular' in local_values):
            charge_current_limits_list.append(local_values['charge_current_limit_regular'])
            info_str = info_str + 'charge_current_limit_regular, '
        if('external_current_limit' in local_values):
            charge_current_limits_list.append(local_values['external_current_limit'])
            info_str = info_str + 'external_current_limit, '
        if('deactivate_charge_limit' in local_values):
            charge_current_limits_list.append(local_values['deactivate_charge_limit'])
            info_str = info_str + 'deactivate_charge_limit, '
        if('charge_current_limit_state' in self.temporary_script_states):
            charge_current_limits_list.append(self.temporary_script_states['charge_current_limit_state'])
            info_str = info_str + 'charge_current_limit_state'
        try:
            local_values['charge_current_limit_final'] = min(charge_current_limits_list)
            self.logger.debug('charge_current_limit_final: ' + str(local_values['charge_current_limit_final']) + ' from list: ' + ','.join(map(str, charge_current_limits_list)) + ' (' + info_str + ').')
        except:
            self.logger.error('Determination of charge current limits failed!')
        # Append all valid discharge current limits
        discharge_current_limits_list = []
        info_str = ''
        if('discharge_current_limit_regular' in local_values):
            discharge_current_limits_list.append(local_values['discharge_current_limit_regular'])    
        if('external_current_limit' in local_values):
            discharge_current_limits_list.append(local_values['external_current_limit'])
        if('winter_discharge_limit' in local_values):
            discharge_current_limits_list.append(local_values['winter_discharge_limit'])
        if('discharge_current_limit_state' in self.temporary_script_states):
            discharge_current_limits_list.append(self.temporary_script_states['discharge_current_limit_state'])
        try:
            local_values['discharge_current_limit_final'] = min(discharge_current_limits_list)
        except:
            self.logger.error('Determination of discharge current limits failed!')
        # Append all valid discharge power limits
        discharge_power_limits_list = []
        if('discharge_power_limit_regular' in local_values):
            discharge_power_limits_list.append(local_values['discharge_power_limit_regular'])
            info_str = info_str + 'discharge_power_limit_regular, '
        if('external_current_limit' in local_values):
            discharge_power_limit_external = self.calc_discharge_power_limit_from_current(local_values, local_values['external_current_limit'])
            discharge_power_limits_list.append(discharge_power_limit_external)
            info_str = info_str + 'external_current_limit, '
        if('deactivate_discharge_limit' in local_values):
            discharge_power_limit_deactivated = self.calc_discharge_power_limit_from_current(local_values, local_values['deactivate_discharge_limit'])
            discharge_power_limits_list.append(discharge_power_limit_deactivated)
            info_str = info_str + 'deactivate_discharge_limit, '
        if('winter_discharge_limit' in local_values):
            discharge_power_limit_winter = self.calc_discharge_power_limit_from_current(local_values, local_values['winter_discharge_limit'])
            discharge_power_limits_list.append(discharge_power_limit_winter)
            info_str = info_str + 'winter_discharge_limit, '
        if('discharge_current_limit_state' in self.temporary_script_states):
            discharge_power_limit_regular_state = self.calc_discharge_power_limit_from_current(local_values, self.temporary_script_states['discharge_current_limit_state'])
            discharge_power_limits_list.append(discharge_power_limit_regular_state)
            info_str = info_str + 'discharge_current_limit_state'
        try:
            local_values['discharge_power_limit_final'] = int(min(discharge_power_limits_list))
            self.logger.debug('discharge_power_limit_final: ' + str(local_values['discharge_power_limit_final']) + ' from list: ' + ','.join(map(str, discharge_power_limits_list)) + '(' + info_str + ').')
        except:
            self.logger.error('Determination of discharge power limits failed!')
            
        # Store values from this cycle as information for next cycle
        self.temporary_script_states['discharge_regular_current_limit_last_cycle'] = local_values['discharge_current_limit_regular']

    def calc_discharge_power_limit_from_current(self, local_values, input_current):
        # Max output power Multi = (Maximum power the battery is allowed to deliver      ) + (Current solar input power) 
        return                     ((abs(input_current * local_values['battery_voltage'])) + local_values['solarcharger_power_sum'])
    
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
        
    def get_charge_current_limit_with_battery_protection(self):
        current_charge_limit = 0.0
        soc_or_max_cell_limit_set = False
        if(('max_cell_voltage' in self.CCGX_data['battery']) and ('soc' in self.CCGX_data['battery'])):
            battery_max_cell_voltage = self.CCGX_data['battery']['max_cell_voltage']
            battery_soc = self.CCGX_data['battery']['soc']
            
            # Stop charging if max voltage is reached for one cell
            if(battery_max_cell_voltage >= self.ess_config_data['battery_settings']['max_cell_voltage_charging']):
                current_charge_limit = 0.0
            else:
                # Check the other conditions
                try:
                    # SOC based limits
                    soc_based_current_charge_limit = -1.0
                    for list_index, soc_limit in enumerate(self.ess_config_data['battery_settings']['soc_based_charge_limit_soc_array']):
                        if(battery_soc >= soc_limit):
                            soc_based_current_charge_limit = self.ess_config_data['battery_settings']['soc_based_charge_limit_current_array'][list_index]
                    # Max cell voltage based limits
                    max_cell_based_current_charge_limit = -1.0
                    for list_index, max_cell_limit in enumerate(self.ess_config_data['battery_settings']['max_cell_based_charge_limit_voltage_array']):
                        if(battery_max_cell_voltage >= max_cell_limit):
                            max_cell_based_current_charge_limit = self.ess_config_data['battery_settings']['max_cell_based_charge_limit_current_array'][list_index]
                    # Now lets do the final limit aggregation
                    if(self.ess_config_data['battery_settings']['charge_limit_mode'] == 'soc_and_max_cell'):
                        if(soc_based_current_charge_limit >= 0.0 and max_cell_based_current_charge_limit >= 0.0):
                            current_charge_limit = min(soc_based_current_charge_limit, max_cell_based_current_charge_limit)
                            soc_or_max_cell_limit_set = True
                            self.logger.debug('Charge current limit SOC and Max Cell (both limits active) (' + self.ess_config_data['battery_settings']['charge_limit_mode'] + ') with current_charge_limit ' + str(current_charge_limit))
                        elif(soc_based_current_charge_limit >= 0.0):
                            current_charge_limit = soc_based_current_charge_limit
                            soc_or_max_cell_limit_set = True
                            self.logger.debug('Charge current limit SOC and Max Cell (only SOC limit active) (' + self.ess_config_data['battery_settings']['charge_limit_mode'] + ') with current_charge_limit ' + str(current_charge_limit))
                        elif(max_cell_based_current_charge_limit >= 0.0):
                            current_charge_limit = max_cell_based_current_charge_limit
                            soc_or_max_cell_limit_set = True
                            self.logger.debug('Charge current limit SOC and Max Cell (only Max Cell limit active) (' + self.ess_config_data['battery_settings']['charge_limit_mode'] + ') with current_charge_limit ' + str(current_charge_limit))
                        else:
                            pass #can not occur
                    elif(self.ess_config_data['battery_settings']['charge_limit_mode'] == 'soc_only'):
                        if(soc_based_current_charge_limit >= 0.0):
                            current_charge_limit = soc_based_current_charge_limit
                            soc_or_max_cell_limit_set = True
                            self.logger.debug('Charge current limit SOC only (' + self.ess_config_data['battery_settings']['charge_limit_mode'] + ') with current_charge_limit ' + str(current_charge_limit))
                    elif(self.ess_config_data['battery_settings']['charge_limit_mode'] == 'max_cell_only'):
                        if(max_cell_based_current_charge_limit >= 0.0):
                            current_charge_limit = max_cell_based_current_charge_limit
                            soc_or_max_cell_limit_set = True
                            self.logger.debug('Charge current limit Max Cell Only (' + self.ess_config_data['battery_settings']['charge_limit_mode'] + ') with current_charge_limit ' + str(current_charge_limit))
                    else:
                        self.logger.error('Charge limit mode not unknown in ess_config.json. You entered value: ' + self.ess_config_data['battery_settings']['charge_limit_mode'])
                except:
                    self.logger.error('Error in soc_based_charge_limit or max_cell_based_charge_limit config in ess_config.json. 1. Limits need to be in ascending order 2. the two corresponding arrays need to have the same number of elements.')
                    return current_charge_limit  
                # The last step is to take the configured maximum charge limit into account
                if(soc_or_max_cell_limit_set is True):
                    current_charge_limit = min(self.ess_config_data['ess_mode_2_settings']['max_battery_charge_current_2705'], current_charge_limit)
                else:
                    current_charge_limit = self.ess_config_data['ess_mode_2_settings']['max_battery_charge_current_2705']            
        else:
            self.logger.warning('Either max_cell_voltage or soc was not available!')
        self.logger.debug('Current charge limit: ' + str(current_charge_limit) + 'A')
        return current_charge_limit
    
    
    def get_discharge_current_limit_with_battery_protection(self):
        current_discharge_limit = 0.0
        soc_or_min_cell_limit_set = False
        if(('min_cell_voltage' in self.CCGX_data['battery']) and ('soc' in self.CCGX_data['battery'])):
            battery_min_cell_voltage = self.CCGX_data['battery']['min_cell_voltage']
            battery_soc = self.CCGX_data['battery']['soc']
            
            # Stop discharging if min voltage is reached for one cell
            if(battery_min_cell_voltage <= self.ess_config_data['battery_settings']['min_cell_voltage_discharging']):
                current_discharge_limit = 0.0
            else:
                # Check the other conditions
                try:
                    # SOC based limits
                    soc_based_current_discharge_limit = -1.0
                    for list_index, soc_limit in enumerate(self.ess_config_data['battery_settings']['soc_based_discharge_limit_soc_array']):
                        if(battery_soc <= soc_limit):
                            soc_based_current_discharge_limit = self.ess_config_data['battery_settings']['soc_based_discharge_limit_current_array'][list_index]
                    # Min cell voltage based limits
                    min_cell_based_current_discharge_limit = -1.0
                    for list_index, min_cell_limit in enumerate(self.ess_config_data['battery_settings']['min_cell_based_discharge_limit_voltage_array']):
                        if(battery_min_cell_voltage <= min_cell_limit):
                            min_cell_based_current_discharge_limit = self.ess_config_data['battery_settings']['min_cell_based_discharge_limit_current_array'][list_index]
                    # Now lets do the final limit aggregation
                    if(self.ess_config_data['battery_settings']['discharge_limit_mode'] == 'soc_and_min_cell'):
                        if(soc_based_current_discharge_limit >= 0.0 and min_cell_based_current_discharge_limit >= 0.0):
                            current_discharge_limit = min(soc_based_current_discharge_limit, min_cell_based_current_discharge_limit)
                            soc_or_min_cell_limit_set = True
                            self.logger.debug('Discharge current limit SOC and Min Cell (both limits active) (' + self.ess_config_data['battery_settings']['discharge_limit_mode'] + ') with current_discharge_limit ' + str(current_discharge_limit))
                        elif(soc_based_current_discharge_limit >= 0.0):
                            current_discharge_limit = soc_based_current_discharge_limit
                            soc_or_min_cell_limit_set = True
                            self.logger.debug('Discharge current limit SOC and Min Cell (only SOC limit active) (' + self.ess_config_data['battery_settings']['discharge_limit_mode'] + ') with current_discharge_limit ' + str(current_discharge_limit))
                        elif(min_cell_based_current_discharge_limit >= 0.0):
                            current_discharge_limit = min_cell_based_current_discharge_limit
                            soc_or_min_cell_limit_set = True
                            self.logger.debug('Discharge current limit SOC and Min Cell (only Min Cell limit active) (' + self.ess_config_data['battery_settings']['discharge_limit_mode'] + ') with current_discharge_limit ' + str(current_discharge_limit))
                        else:
                            pass #can not occur                            
                    elif(self.ess_config_data['battery_settings']['discharge_limit_mode'] == 'soc_only'):
                        if(soc_based_current_discharge_limit >= 0.0):
                            current_discharge_limit = soc_based_current_discharge_limit
                            soc_or_min_cell_limit_set = True
                            self.logger.debug('Discharge current limit SOC only. (' + self.ess_config_data['battery_settings']['discharge_limit_mode'] + ') with current_discharge_limit ' + str(current_discharge_limit))
                    elif(self.ess_config_data['battery_settings']['discharge_limit_mode'] == 'min_cell_only'):
                        if(min_cell_based_current_discharge_limit >= 0.0):
                            current_discharge_limit = min_cell_based_current_discharge_limit
                            soc_or_min_cell_limit_set = True
                            self.logger.debug('Discharge current limit Min Cell only. (' + self.ess_config_data['battery_settings']['discharge_limit_mode'] + ') with current_discharge_limit ' + str(current_discharge_limit))
                    else:
                        self.logger.error('Discharge limit mode not unknown in ess_config.json. You entered value: ' + self.ess_config_data['battery_settings']['discharge_limit_mode'])
                except:
                    self.logger.error('Error in soc_based_discharge_limit or min_cell_based_discharge_limit config in ess_config.json. 1. Limits need to be in descending order 2. the two corresponding arrays need to have the same number of elements.')
                    return current_discharge_limit  
                # The last step is to take the configured maximum charge limit into account
                if(soc_or_min_cell_limit_set is True):
                    current_discharge_limit = min(self.ess_config_data['ess_mode_2_settings']['max_battery_discharge_current'], current_discharge_limit)
                else:
                    current_discharge_limit = self.ess_config_data['ess_mode_2_settings']['max_battery_discharge_current']           
        else:
            self.logger.warning('Either min_cell_voltage or soc was not available!')
        self.logger.debug('Current discharge limit: ' + str(current_discharge_limit) + 'A')
        return current_discharge_limit
        
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

    def reboot_ess_controller_script(self):
        # TODO
        self.logger.error('Reboot function not yet implemented!')
    
    def send_keepalive_to_cerbo(self):
        # Documentation needed to know which values to send how is here:
        # https://github.com/victronenergy/dbus-mqtt
        # https://www.victronenergy.com/live/ess:ess_mode_2_and_3
        try:
            # Sends keepalive to Victron OS in a way, that all available topics are returned (good for debugging but high network and system load)
            if(self.ess_config_data['keepalive_get_all_topics'] == 1):
                payload_string = ""
                topic_string = "R/" + self.ess_config_data['vrm_id'] + "/system/0/Serial"
                # Publish Topic
                errcode = self.mqtt_client.publish(topic_string, payload=payload_string, qos=0, retain=False)
                # Logging
                log_string = "Keepalive (all topics) message send! Errorcode: " + str(errcode) + ". Published topic: \'" + topic_string
                self.logger.debug(log_string)
            # Sends keepalive to Vecus OS in a way, that only the required topics are returned
            elif(self.ess_config_data['keepalive_get_all_topics'] == 0):
                topic_string = "R/" + self.ess_config_data['vrm_id'] + "/keepalive"
                topics_list = []                               
                topics_list.append("battery/+/Dc/0/#")
                topics_list.append("battery/+/Soc")
                topics_list.append("battery/+/System/MaxCellVoltage")
                topics_list.append("battery/+/System/MinCellVoltage")
                topics_list.append("grid/+/Ac/Power")
                topics_list.append("grid/+/Ac/L1/Power")
                topics_list.append("grid/+/Ac/L1/Current")
                topics_list.append("grid/+/Ac/L2/Power")
                topics_list.append("grid/+/Ac/L2/Current")
                topics_list.append("grid/+/Ac/L3/Power")
                topics_list.append("grid/+/Ac/L3/Current")
                topics_list.append("system/+/Ac/Consumption/#")
                topics_list.append("solarcharger/+/Yield/Power")
                topics_list.append("solarcharger/+/Dc/0/#")
                topics_list.append("+/+/ProductId")
                topics_list.append("settings/+/Settings/CGwacs/#")
                topics_list.append("settings/+/Settings/SystemSetup/#")
                topics_list.append("vebus/+/Mode")
                payload = json.dumps(topics_list)
                errcode = self.mqtt_client.publish(topic_string, payload)
                self.logger.debug("Keepalive (selected topics) message send! Errorcode: " + str(errcode) + ". Published topic: \'" + topic_string + '. Payload: ' + payload)
            else:
                self.logger.warning(log_string)
        except:
            # Logging
            log_string = "FAILED to publish Keep Alive message to Cerbo!"
            self.logger.warning(log_string)
            
    def print_alive_status_to_logger(self):
        self.logger.info('ESS Controller script is up and running!')
    
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
        
    def read_setvalue_list_json(self):
        try:
            if(DEBUGGING_ON):
                f = open('./smarthome_projects/essBATT-Controller-/ess_setvalue_list.json') # Path for debug mode
            else:
                f = open('ess_setvalue_list.json') # Path for productive mode
        except:
            self.logger.error("ess_setvalue_list.json file could not be opened!")
            return
        # returns JSON object as a dictionary
        try:           
            self.ess_setvalue_list = json.load(f)
        except:
            self.logger.error("ess_setvalue_list.json was opened but could not be loaded. Check for json file syntax errors!")
            f.close()
            return
        self.ess_setvalue_list_loaded_correctly = True
        self.logger.debug('ess_setvalue_list.json file loaded.')
        f.close()
        
    def read_ess_controller_state_json(self):
        local_dict = {}
        try:
            if(DEBUGGING_ON):
                f = open('./smarthome_projects/essBATT-Controller-/ess_controller_state') # Path for debug mode
            else:
                f = open('./ess_controller_state') # Path for productive mode
        except:
            self.logger.error("ess_controller_state file could not be opened!")
            return
        # returns JSON object as a dictionary
        try:           
            local_dict = json.load(f)
        except:
            self.logger.error("ess_controller_state was opened but could not be loaded. Check for json file syntax errors!")
            f.close()
            return
        self.ess_controller_state_loaded_correctly = True
        self.logger.debug('ess_controller_state.json file loaded.')
        f.close()
        return local_dict
        
    def store_ess_controller_state_in_file(self):
        self.ess_controller_state_loaded_correctly = False
        try:
            if(DEBUGGING_ON):
                f = open('./smarthome_projects/essBATT-Controller-/ess_controller_state', 'w', encoding='utf-8') # Path for debug mode
            else:
                f = open('./ess_controller_state', 'w', encoding='utf-8') # Path for productive mode
        except:
            self.logger.error("ess_controller_state file could not be opened!")
            return
        # returns JSON object as a dictionary
        try:           
            json.dump(self.ess_controller_state, f, ensure_ascii=False, indent=4)
        except:
            self.logger.error("ess_controller_state was opened but could not be stored. Check for json file syntax errors!")
            f.close()
            return
        self.ess_controller_state_loaded_correctly = True
        self.logger.debug('ess_controller_state.json file loaded.')
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
        # External Control
        if(self.ess_config_data['external_control_settings']['allow_external_control_over_mqtt'] == 1):
            if(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['charge_battery_to_SOC'] != "none"):
                self.mqtt_client.message_callback_add(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['charge_battery_to_SOC'], self.on_msg_ext_charge_to_SOC)
            if(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['activate_top_balancing_mode'] != "none"):
                self.mqtt_client.message_callback_add(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['activate_top_balancing_mode'], self.on_msg_ext_balancing)
            if(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['deactivate_discharge'] != "none"):
                self.mqtt_client.message_callback_add(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['deactivate_discharge'], self.on_msg_ext_deactivate_discharge)
            if(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['deactivate_charge'] != "none"):
                self.mqtt_client.message_callback_add(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['deactivate_charge'], self.on_msg_ext_deactivate_charge)
            if(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['reboot_ess_controller'] != "none"):
                self.mqtt_client.message_callback_add(self.ess_config_data['external_control_settings']['mqtt_external_control_topics']['reboot_ess_controller'], self.on_msg_ext_reboot_ess_controller)
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

    # External
    def on_msg_ext_charge_to_SOC(self, client, userdata, msg):
        try:
            payload = msg.payload.decode('utf-8')
            split_payload = payload.split('/')
            current_limit_used = False
            starttime_used = False
            startdate_used = False

            activated = split_payload[0]
            target_soc = int(split_payload[1])
            if(split_payload[2] != '-'):
                current_limit = int(split_payload[2])
                current_limit_used = True
            if(split_payload[3] != '-'):
                time_input = split_payload[3]
                starttime_used = True
            if(split_payload[4] != '-'):
                date_input = split_payload[4]
                startdate_used = True
        except:
            self.logger.error('Payload could not be decoded.')
            return
        try:
            if('charge_to_SOC' not in self.ess_external_input):
                self.ess_external_input['charge_to_SOC'] = {}
            self.ess_external_input['charge_to_SOC']['target_SOC'] = target_soc
            self.ess_external_input['charge_to_SOC']['activated'] = activated
            if(current_limit_used):
                self.ess_external_input['charge_to_SOC']['current_limit_input'] = current_limit
            if(starttime_used):
                self.ess_external_input['charge_to_SOC']['time_input'] = time_input
            if(startdate_used):
                self.ess_external_input['charge_to_SOC']['date_input'] = date_input
            self.ess_external_input['charge_to_SOC']['receive_time'] = datetime.now(tz=None)
            self.ess_external_input['new_data_received'] = True
        except:
            self.logger.error('Should not occur. Function failed.')
            
    def on_msg_ext_balancing(self, client, userdata, msg):
        try:
            payload = msg.payload.decode('utf-8')
            split_payload = payload.split('/')
            current_limit_used = False
            starttime_used = False
            startdate_used = False
            
            activated = split_payload[0]
            if(split_payload[1] != '-'):
                current_limit = int(split_payload[1])
                current_limit_used = True
            if(split_payload[2] != '-'):
                time_input = split_payload[2]
                starttime_used = True
            if(split_payload[3] != '-'):
                date_input = split_payload[3]
                startdate_used = True
        except:
            self.logger.error('Payload could not be decoded.')
            return
        try:
            if('balancing' not in self.ess_external_input):
                self.ess_external_input['balancing'] = {}
            self.ess_external_input['balancing']['activated'] = activated
            if(current_limit_used):
                self.ess_external_input['balancing']['current_limit_input'] = current_limit
            if(starttime_used):
                self.ess_external_input['balancing']['time_input'] = time_input
            if(startdate_used):
                self.ess_external_input['balancing']['date_input'] = date_input
            self.ess_external_input['balancing']['receive_time'] = datetime.now(tz=None)
            self.ess_external_input['new_data_received'] = True
        except:
            self.logger.error('Should not occur. Function failed.')
    
    def on_msg_ext_deactivate_discharge(self, client, userdata, msg):
        try:
            payload = msg.payload.decode('utf-8')
            if((payload == 'False') or (payload == 'false')):
                activation_state = False
            elif((payload == 'True') or (payload == 'true')):
                activation_state = True
            else:
                self.logger.error('Unknown payload: ' + payload)
                return
            if('deactivate_discharge' not in self.ess_external_input):
                self.ess_external_input['deactivate_discharge'] = {}
            self.ess_external_input['deactivate_discharge']['activated'] = activation_state
            self.ess_external_input['deactivate_discharge']['receive_time'] = datetime.now(tz=None)
            if(activation_state):
                self.logger.info('"DISCHARGING" is now "DEACTIVATED"!')
            else:
                self.logger.info('"DISCHARGING" is now "ALLOWED"!')
        except:
            self.logger.error('Payload could not be decoded.')
            return
        
    def on_msg_ext_deactivate_charge(self, client, userdata, msg):
        try:
            payload = msg.payload.decode('utf-8')
            if((payload == 'False') or (payload == 'false')):
                activation_state = False
            elif((payload == 'True') or (payload == 'true')):
                activation_state = True
            else:
                self.logger.error('Unknown payload: ' + payload)
                return
            if('deactivate_charge' not in self.ess_external_input):
                self.ess_external_input['deactivate_charge'] = {}
            self.ess_external_input['deactivate_charge']['activated'] = activation_state
            self.ess_external_input['deactivate_charge']['receive_time'] = datetime.now(tz=None)
            if(activation_state):
                self.logger.info('"CHARGING" is now "DEACTIVATED"!')
            else:
                self.logger.info('"CHARGING" is now "ALLOWED"!')
        except:
            self.logger.error('Payload could not be decoded.')
            return
        
    def on_msg_ext_reboot_ess_controller(self, client, userdata, msg):
        try:
            payload = msg.payload.decode('utf-8')
            if((payload == 'False') or (payload == 'false')):
                reboot = False
            elif((payload == 'True') or (payload == 'true')):
                reboot = True
            else:
                self.logger.error('Unknown payload: ' + payload)
                return
            if(reboot is True):
                self.reboot_ess_controller_script()
        except:
            self.logger.error('Payload could not be decoded.')
            return

    ###############################################################
            
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
    
    ####### Create ESS Controller Object ###############################
    ess_controller_obj = essBATT_watchdog(app_log)

    ######### Start the application ############################
    if(ess_controller_obj.ess_config_data_loaded_correctly is True):
        try:
            ess_controller_obj.run()
        finally:
            ess_controller_obj.logger.warning("essBATT watchdog: Shutdown. Control loop exited and needs restart.")
            # Timer Objects have to be stopped
            ess_controller_obj.rt_keep_alive_obj.stop()
            ess_controller_obj.rt_ess_control_update_obj.stop()
            ess_controller_obj.rt_print_status_obj.stop()
            # MQTT Loop needs to be stopped
            ess_controller_obj.mqtt_client.loop_stop()
    else:
        ess_controller_obj.logger.warning("essBATT watchdog not running and needs restart!")
        # Timer Object has to be stopped
        ess_controller_obj.rt_keep_alive_obj.stop()
        ess_controller_obj.rt_ess_control_update_obj.stop()
        ess_controller_obj.rt_print_status_obj.stop()
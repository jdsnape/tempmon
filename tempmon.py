import paho.mqtt.client as mqtt
import datetime
import time
import json
import ConfigParser as configparser
import argparse
from collections import defaultdict
import logging
import time
import sqlite3


class room:
	def __init__(self, name, floor, hysteresis):
		self.name=name
		self.floor=floor
		self.hysteresis=float(hysteresis)

		current_time = int(time.time())
		
		self.threshold_time = (15*60) 
		self.last_reported_temp=0
		self.relay_state=-1 #we init this to -1 because we don't know on startup - will be set by first status call
		self.setting_confirmed=False
		self.confirmed_timestamp=0 #Time that the device confirmed it had processed a change to relay state
		self.set_timestamp=current_time - self.threshold_time - 60 #Time that we sent a command to change the relay state

	def expected_temp(self,current_temp):
		logging.debug("Getting expected temperature, based on knowledge of current temp: %f", current_temp)	
		#Initialise DB
		logging.debug("Initialising DB %s",Config.get('app','db'))
		conn = sqlite3.connect(Config.get('app','db'))
		c = conn.cursor()

		self.last_reported_temp=current_temp

		#We want a single time, so we're consistent accorss all queries
		current_time = int(time.time())
	
		#Get seconds since midnight
		today=datetime.date.fromtimestamp(current_time)
		seconds_since_midnight = int(time.time() - time.mktime(today.timetuple()))
		day_of_week = datetime.datetime.today().weekday()
		logging.debug("We think the date/time is: current_time: %d, seconds_since_midnight: %d, day_of_week: %d",current_time, seconds_since_midnight, day_of_week)
		#Check for any overrides
		logging.debug("Checking to see if we have any overrides programmed for current time %d",current_time)
		c.execute('SELECT overrides.temperature,overrides.start_time, overrides.end_time FROM overrides \
				JOIN rooms ON overrides.room_id = rooms.id \
				WHERE rooms.room_name="{room_name}" and rooms.floor="{floor}" and overrides.start_time<{current_time} and overrides.end_time>{current_time} ORDER BY overrides.end_time DESC LIMIT 1'.\
		format(room_name=self.name,floor=self.floor,current_time=seconds_since_midnight))
		all_rows = c.fetchall()
		if len(all_rows)==1:
			logging.debug("Override found: %f deg from %s to %s",all_rows[0][0],all_rows[0][1], all_rows[0][2])
			msg={"reason":"override","setpoint":float(all_rows[0][0])}
			client.publish("myhome/"+self.floor+"/"+self.name+"/temperature_setpoint",payload=json.dumps(msg))
			conn.close()
			return float(all_rows[0][0])
		logging.debug("Deleting all expired overrides")
		#Do some override houskeeping (delete all overrides that ended before seconds_since_midnight)
		c.execute('DELETE from overrides where end_time<{seconds_since_midnight}'.format(seconds_since_midnight=seconds_since_midnight))


		#Check the programmed temperature
		logging.debug("No overrides found. Checking programmed temperature")		
		c.execute('WITH previous_setpoint as (SELECT temperature,seconds_of_day from scheduling JOIN rooms on scheduling.room_id=rooms.id where rooms.room_name="{room_name}" and rooms.floor="{floor}" and day_of_week={day} and scheduling.seconds_of_day < {seconds_of_day} ORDER BY seconds_of_day DESC LIMIT 1), next_setpoint as (SELECT temperature,seconds_of_day from scheduling JOIN rooms on scheduling.room_id=rooms.id where rooms.room_name="{room_name}" and rooms.floor="{floor}" and day_of_week={day} and scheduling.seconds_of_day > {seconds_of_day} ORDER BY seconds_of_day ASC LIMIT 1) SELECT * FROM previous_setpoint UNION ALL select * from next_setpoint order by seconds_of_day desc;'.format(room_name=self.name,floor=self.floor,day=day_of_week,seconds_of_day = seconds_since_midnight))
		all_rows = c.fetchall()
		if len(all_rows)==0:
			logging.error("Unable to get any setpoint information! Returning comfort temp")
			msg={"reason":"single_row","setpoint":19.0}
			client.publish("myhome/"+self.floor+"/"+self.name+"/temperature_setpoint",payload=json.dumps(msg))
			return 19
		if len(all_rows)==1:
			logging.error("Unable to get two setpoints - aiming towards future one")
			next_temp = all_rows[0][0]
			next_time = all_rows[0][1]
			previous_temp = next_temp
			previous_time = next_time - 3600
		else:
			previous_temp = all_rows[1][0]
			next_temp = all_rows[0][0]
			previous_time = all_rows[1][1]
			next_time = all_rows[0][1]	
		# Results in two rows, the first one is the next setpoint, the second is the previous setpoint

		logging.debug("Previous setpoint temp: %f, Next setpoint temp: %f",previous_temp, next_temp)

		conn.close()


		#We have current temp, a previous temp and a next temp
		#
		# We want to see if we need to turn on to meet the next temp
		# Otherwise, we want to maintain the previous temp
		temperature_difference = previous_temp - current_temp
		time_difference = next_time - seconds_since_midnight
	
		logging.debug("Temperature difference is: %f",temperature_difference)
	
		logging.debug("Checking if temperature needs to rise to meet next temp")
		#Return what we'd expect the temperature to be based on current_temp, future_temp and time between the two. 
	
		msg={"reason":"prev_temp","setpoint":previous_temp}
		client.publish("myhome/"+self.floor+"/"+self.name+"/temperature_setpoint",payload=json.dumps(msg))
		
		
		ramp_coefficient = 0.00018 #degrees per second, manually calculated from grafana - TODO: generate this from this application and update in DB
		#if ((time_difference * ramp_coefficient) + current_temp) >= next_temp:
		seconds_to_future_temp = (next_temp - current_temp) / ramp_coefficient
		if (seconds_to_future_temp >= time_difference):
			logging.debug("It's time for the heating to come on so it can heat up in time to meet the target")
			#msg={"reason":"next_temp","setpoint":next_temp}
			#client.publish("myhome/"+self.floor+"/"+self.name+"/temperature_setpoint",payload=json.dumps(msg))
			return next_temp
		elif  temperature_difference < 0:
			logging.debug("Temperature needs to fall")
			#Returning the prev temp, this will cause the device to turn off
			#msg={"reason":"prev_temp","setpoint":previous_temp}
			#client.publish("myhome/"+self.floor+"/"+self.name+"/temperature_setpoint",payload=json.dumps(msg))
			return previous_temp
		else:
			logging.debug("Return previous scheduled temp")
			#msg={"reason":"prev_temp","setpoint":previous_temp}
			#client.publish("myhome/"+self.floor+"/"+self.name+"/temperature_setpoint",payload=json.dumps(msg))
			return previous_temp

		#get temperature steps before and after current time
		logging.error("Couldn't work out what temp to return - returning default temp")
		msg={"reason":"unknown_temp","setpoint":18.0}
		client.publish("myhome/"+self.floor+"/"+self.name+"/temperature_setpoint",payload=json.dumps(msg))
		return 18
	# target temperature is made up of --
	# Next temperature set-point, plus a time to get to it (e.g. 2 hours to make heating come on 2 hours before target temp/time

	def turn_on(self):
		logging.debug("Turning on")		
		#Check when last change was made so we're not flipping on and off - 15 minute pause hard-coded
		current_time = int(time.time())
		logging.debug("We changed state previously at %d, currently %d, threshold %d",self.set_timestamp, current_time, self.threshold_time)
		if current_time - self.set_timestamp >= self.threshold_time:
			client.publish("myhome/"+self.floor+"/"+self.name+"/ctrl",payload=1, retain=True)
			self.set_timestamp=time.time()
			self.relay_state=1
		else:
			logging.warning("We've changed state in last %d minutes, cowardly refusing to change again",self.threshold_time/60)

	def turn_off(self):
		logging.debug("turning off")
		current_time = int(time.time())
		logging.debug("We changed state previously at %d, currently %d",self.set_timestamp, current_time)
		if current_time - self.set_timestamp >= self.threshold_time:
			client.publish("myhome/"+self.floor+"/"+self.name+"/ctrl",payload=0, retain=True)
			self.set_timestamp=time.time()
			self.relay_state=0
		else:
			logging.warning("We've changed state in last 15 minutes, cowardly refusing to change again")

	def create_override(self,setpoint):
		logging.debug("Initialising DB %s",Config.get('app','db'))
		conn = sqlite3.connect(Config.get('app','db'))
		c = conn.cursor()
		current_time = int(time.time())

		today=datetime.date.fromtimestamp(current_time)
		seconds_since_midnight = int(time.time() - time.mktime(today.timetuple()))
		day_of_week = datetime.datetime.today().weekday()
		
		logging.debug("We think the date/time is: current_time: %d, seconds_since_midnight: %d, day_of_week: %d",current_time, seconds_since_midnight, day_of_week)

		c.execute('SELECT id from rooms where room_name="{room_name}"'.format(room_name=self.name))
		room_id=c.fetchall()[0][0]	
		logging.debug("Room ID %s", room_id) 
		logging.debug("Removing overrides for current time")
		c.execute('DELETE from overrides where id in (SELECT overrides.id from overrides \
				JOIN rooms on overrides.room_id = rooms.id \
				WHERE rooms.room_name="{room_name}" and rooms.floor="{floor}" and overrides.start_time<={seconds_since_midnight} and end_time>={seconds_since_midnight})'\
				.format(room_name=self.name, floor=self.floor,seconds_since_midnight=seconds_since_midnight ))

		logging.debug("Getting time of next programmed setpoint")

		c.execute('SELECT seconds_of_day from scheduling \
			JOIN rooms on scheduling.room_id=rooms.id \
			WHERE rooms.room_name="{room_name}" and rooms.floor="{floor}" \
			and day_of_week={day} and scheduling.seconds_of_day > {seconds_of_day} \
			ORDER BY seconds_of_day ASC LIMIT 1;'.format(room_name=self.name,floor=self.floor,day=day_of_week,\
			seconds_of_day = seconds_since_midnight))

		all_rows = c.fetchall()
		logging.debug("No. results %d",len(all_rows))
		if len(all_rows)==1:
			next_setpoint_time=int(all_rows[0][0])-1
		else:
			next_setpoint_time=86400
			
		logging.debug("Creating new override: %f deg from %s to %s, room_id %s", setpoint, seconds_since_midnight, next_setpoint_time, room_id)
		c.execute('INSERT INTO overrides (temperature,start_time, end_time, room_id) VALUES (?,?,?,?)',(setpoint, seconds_since_midnight, next_setpoint_time, room_id))
		conn.commit()
		conn.close()


	def clear_override(self):
		conn = sqlite3.connect(Config.get('app','db'))
		c = conn.cursor()
		current_time = int(time.time())

		today=datetime.date.fromtimestamp(current_time)
		seconds_since_midnight = int(time.time() - time.mktime(today.timetuple()))
		day_of_week = datetime.datetime.today().weekday()
		
		logging.debug("We think the date/time is: current_time: %d, seconds_since_midnight: %d, day_of_week: %d",current_time, seconds_since_midnight, day_of_week)

		c.execute('SELECT id from rooms where room_name="{room_name}"'.format(room_name=self.name))
		room_id=c.fetchall()[0][0]	
		logging.debug("Room ID %s", room_id) 
		logging.debug("Removing overrides for current time")
		c.execute('DELETE from overrides where id in (SELECT overrides.id from overrides \
				JOIN rooms on overrides.room_id = rooms.id \
				WHERE rooms.room_name="{room_name}" and rooms.floor="{floor}" and overrides.start_time<={seconds_since_midnight} and end_time>={seconds_since_midnight})'\
				.format(room_name=self.name, floor=self.floor,seconds_since_midnight=seconds_since_midnight ))

		#TODO: It would be good here to trigger check_device_temp with the last known temp from teh device so we don't have to wait for it to check in

		conn.commit()
		conn.close()
	


def check_device_status(room,msg):
	logging.debug("Received status message from controller: %s, relay_stat is %s",msg.payload,room.relay_state)
	if msg.payload=="0" or msg.payload=="1":
		if(int(msg.payload)==room.relay_state):
			logging.debug("Confirmed status and updated timestamp and flag")
			room.confirmed_timestamp=time.time()
			room.setting_confirmed=True
		else:
			if room.relay_state==0:
				logging.debug("The device has said it is on, but we think it should be off - turning off")
				room.turn_off()
			elif room.relay_state==1:
				logging.debug("The device has said it is off, but we think it should be on - turning on")
				room.turn_on()
			else:
				room.relay_state = int(msg.payload)
				logging.debug("We don't know the state, so setting the state to the state reported by device, state now: %s", room.relay_state)
	else:
		logging.error("The device has reported an invalid state %s",msg.payload)	


def check_device_temp(room,msg):
	data=json.loads(msg.payload)
	logging.debug("Received temperature reading from probe: %s",data['temp'])
	expected_temp = room.expected_temp(data['temp'])
	logging.debug("Received expected temp: %s", expected_temp)
	logging.debug("Current relay state: %d", room.relay_state)
	#Check temp + relay state to see if need to turn heat on or off
	#TODO: what happens if we've sent an on/off command already, but not been processed? we don't care, as handled by the check_device_status function
	if (float(data['temp']) < (expected_temp-room.hysteresis)) and room.relay_state!=1:
		logging.debug("Temp is lower than expected temp (%s deg)",expected_temp)
		room.turn_on()
	elif (float(data['temp'])>=(expected_temp+room.hysteresis)) and room.relay_state!=0:
		logging.debug("Temp is higher than expected temp (%s deg)",expected_temp)
		room.turn_off()
	else:
		logging.debug("No state change needed")

	# Look at current temperature. Compare against target temperature, and turn on if below. 
def check_setpoint(room, msg):
	data=json.loads(msg.payload)
	if data['reason']=='homekit_override':
		logging.debug("We've received a temp from homekit: %f",data['setpoint'])
		room.create_override(data['setpoint'])

def check_mode(room, msg):
	data=json.loads(msg.payload)
	if data['state']=='auto':
		logging.debug("We've received an auto message from homekit - clearing override")
		room.clear_override()


def unhandled_topic(room,msg):
	logging.warning("Received message on unhandled topic %s",msg.topic)

mqtt_message_map = defaultdict(lambda: unhandled_topic)
mqtt_message_map['status'] = check_device_status
mqtt_message_map['temperature'] = check_device_temp
mqtt_message_map['temperature_setpoint'] = check_setpoint
mqtt_message_map['thermostat_ctrl'] = check_mode

#TODO: Add functionality to handle setpoint changes coming in over MQTT

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logging.debug("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    logging.debug("Subscribing to topic: %s","myhome/"+Config.get('room','floor')+"/"+Config.get('room','room_name')+"/#")
    client.subscribe("myhome/"+Config.get('room','floor')+"/"+Config.get('room','room_name')+"/#")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	logging.debug("Received message on topic %s, payload %s",msg.topic, msg.payload)
	mqtt_message_map[msg.topic.split("/")[-1]](room,msg)



parser = argparse.ArgumentParser(description='Temperature control for a room in the house.')
parser.add_argument("-c", "--config", dest='config_file',required=True, type=str, help="Config file (required)")
args = parser.parse_args()

Config = configparser.ConfigParser(defaults={'host':'127.0.0.1','port':1883,'log_level': 'WARNING','db':'/home/jdsnape/workers/tempmon/tempmon.db'})
if len(Config.read(args.config_file))==0:
	logging.error("Unable to read configuration file")
	exit(1)

#Set logging level
numeric_level = getattr(logging, Config.get('app','log_level').upper(), None)
if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % Config.get('app','log_level').upper())
fmt="%(levelname)s\t %(funcName)s():%(lineno)i: \t%(message)s"
logging.basicConfig(level=numeric_level,format=fmt)



client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

logging.debug("Connecting to broker %s:%p",Config.get('mqtt','host'), Config.get('mqtt','port'))
client.connect(Config.get('mqtt','host'), Config.get('mqtt','port'), 60)

room = room(Config.get('room','room_name'),Config.get('room','floor'),Config.get('room','hysteresis'))

#Standard threshold to prevent hysterese - in seconds

#Threaded loop for handling MQTT messages
client.loop_start()

while True:
	#We can use this loop for general housekeeping (e.g. checking that we have received confirmation messages)
	print("starting loop")
	time.sleep(1000)

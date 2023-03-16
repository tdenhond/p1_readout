import sys
import serial
import datetime 
import time 
import pandas
import re 
import pickle
import matplotlib.pyplot as plt


#COM port config
ser           =  serial.Serial()
ser.baudrate  =  115200
ser.bytesize  =  serial.EIGHTBITS
ser.parity    =  serial.PARITY_NONE
ser.stopbits  =  serial.STOPBITS_ONE
ser.xonxoff   =  0
ser.rtscts    =  0
ser.timeout   =  20
ser.port      =  "/dev/ttyUSB0"

#Open COM port
try:
    ser.open()
except:
    sys.exit(f"Error opening serial port {ser.port}")

regex_energy    = re.compile("\((\d+\.\d+)\*kWh\)")
regex_power     = re.compile("\((\d+\.\d+)\*kW\)")
regex_voltage   = re.compile("\((\d+\.\d+)\*V\)")
regex_current   = re.compile("\((\d+\.\d+)\*A\)")

data_objects = {"0-0:96.1.4":   ("version",                               lambda x: ".".join(re.match("\((\d{3})(\d{2})\)", x).groups() )),
                "0-0:96.1.1":   ("equipment_id",                          None),
                "0-0:1.0.0":    ("timestamp",                             lambda x: time.mktime(datetime.datetime.strptime(x[1:-2], "%y%m%d%H%M%S").timetuple())),
                "1-0:1.8.1":    ("readout_electricty_consumed_tariff_1",  lambda x: float(regex_energy.match(x).groups()[0])),
                "1-0:1.8.2":    ("readout_electricty_consumed_tariff_2",  lambda x: float(regex_energy.match(x).groups()[0])),
                "1-0:2.8.1":    ("readout_electricty_produced_tariff_1",  lambda x: float(regex_energy.match(x).groups()[0])),
                "1-0:2.8.2":    ("readout_electricty_produced_tariff_2",  lambda x: float(regex_energy.match(x).groups()[0])),
                "0-0:96.14.0":  ("tariff_indicator",                      None),
                "1-0:1.4.0":    ("current_avg_demand",                    lambda x: float(regex_power.match(x).groups()[0])),
                "1-0:1.6.0":    ("max_demand_month",                      lambda x: float(regex_power.match(x).groups()[0])),
                "0-0:98.1.0":   ("max_demand_13month",                    None),
                "1-0:1.7.0":    ("actual_power_consumed",                 lambda x: float(regex_power.match(x).groups()[0])),
                "1-0:2.7.0":    ("actual_power_produced",                 lambda x: float(regex_power.match(x).groups()[0])),
                "1-0:21.7.0":   ("instantaneous_power_consumed_l1",       lambda x: float(regex_power.match(x).groups()[0])),
                "1-0:41.7.0":   ("instantaneous_power_consumed_l2",       lambda x: float(regex_power.match(x).groups()[0])),
                "1-0:61.7.0":   ("instantaneous_power_consumed_l3",       lambda x: float(regex_power.match(x).groups()[0])),
                "1-0:22.7.0":   ("instantaneous_power_produced_l1",       lambda x: float(regex_power.match(x).groups()[0])),
                "1-0:42.7.0":   ("instantaneous_power_produced_l2",       lambda x: float(regex_power.match(x).groups()[0])),
                "1-0:62.7.0":   ("instantaneous_power_produced_l3",       lambda x: float(regex_power.match(x).groups()[0])),
                "1-0:32.7.0":   ("instantaneous_voltage_produced_l1",     lambda x: float(regex_voltage.match(x).groups()[0])),
                "1-0:52.7.0":   ("instantaneous_voltage_produced_l2",     lambda x: float(regex_voltage.match(x).groups()[0])),
                "1-0:72.7.0":   ("instantaneous_voltage_produced_l3",     lambda x: float(regex_voltage.match(x).groups()[0])),
                "1-0:31.7.0":   ("instantaneous_current_produced_l1",     lambda x: float(regex_current.match(x).groups()[0])),
                "1-0:51.7.0":   ("instantaneous_current_produced_l2",     lambda x: float(regex_current.match(x).groups()[0])),
                "1-0:71.7.0":   ("instantaneous_current_produced_l3",     lambda x: float(regex_current.match(x).groups()[0])),
                "0-0:96.3.10":  ("breaker_state",                         None),
                "0-0:17.0.0":   ("limiter_threshold",                     None),
                "1-0:31.4.0":   ("fuse_supervision_threshold",            None),
                "0-0:96.13.0":  ("text_message",                          None),
                }

                
first_sample_done = False
regex = re.compile('(\d-\d:\d+\.\d+\.\d+)(\([\w*.]+\))+')
samples = []
sample = {}

while(len(samples)<1024):
    msg = ser.readline().decode("UTF-8").strip()
    m = regex.match(msg)
    if len(msg) == 0:
        continue
    if msg[0] == "!":
        if first_sample_done:
            samples.append(sample)
        if len(samples) >= 1024:
            df = pandas.DataFrame(samples[1:])
            last_timestamp = max(df["timestamp"])
            with open(f"/home/tristan/dev/readout_data_{last_timestamp}.pkl", "wb") as outfile:
                pickle.dump(df, outfile)
            samples = []
        first_sample_done = True
        sample = {}
    if m is None:
        continue
    if not m.groups()[0] in data_objects:
        continue

    key, fcn = data_objects[m.groups()[0]]
    if fcn is None:
        sample[key] = m.groups()[1:]
    else:
        sample[key] = fcn(m.groups()[1])
    




        


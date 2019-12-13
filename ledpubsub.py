
'''
/*
 * Started from the AWS IOT pubsub examples and doco plus alot of help from stackoverflow.com to figure out paho + x-amzn-mqtt-ca
 * https://github.com/aws/aws-iot-device-sdk-python/blob/master/samples/basicPubSub/basicPubSub.py
 * https://docs.aws.amazon.com/iot/latest/developerguide/protocols.html
 * https://stackoverflow.com/questions/48998420/aws-iot-use-mqtt-on-port-443
 *
 * user@raspberry-pi:~/demo $ cat ledpubsub.py
 *
 * FYI: This example would be better off using the on connect callback instead of the os execl based command restart
 *      Thus, the send_test function ended up being needed. When layer 3 network device failover or path changes occur
 *      the Paho client "loop" would in fact reconnect the client to the endpoint and be able to publish but subscribtions
 *      would no longer work (see my note above about using on connect callbak instead :p)
 *      That said this had run for over a month and survived a number of topology changes by restarting itself
 * 
 * combined AWS IOT Pub Sub Client using MQTT over SSL (tcp port 443 only) to control an attached LED with an attched switch
 * MQTT over SSL was chosen to avoid the need for firewall or proxy changes at our test location
 * this also supported using an AWS IOT Button with associated AWS IOT Rule and Lambda python function
 * http://docs.aws.amazon.com/iot/latest/developerguide/iot-lambda-rule.html
 *
 * myThingName is used for the name of the thing in this example
 * us-east-1 is used for the region name in this example
 *
 *    import boto3
 *    import json
 *    client = boto3.client('iot-data', region_name='us-east-1')
 *    def lambda_handler(event, context):
 *        response = client.get_thing_shadow(thingName='myThingName')
 *        streamingBody = response["payload"]
 *        jsonState = json.loads(streamingBody.read())
 *        led_reported = "OFF"
 *        led_reported = jsonState["state"]["reported"]["LED"]
 *        if led_reported == 'ON':
 *            response = client.publish(
 *                topic='$aws/things/myThingName/shadow/update',
 *                qos=1,
 *                payload=json.dumps({"state": {"desired": {"LED": "OFF"}}})
 *            )
 *        else:
 *            response = client.publish(
 *                topic='$aws/things/myThingName/shadow/update',
 *                qos=1,
 *                payload=json.dumps({"state": {"desired": {"LED": "ON"}}})
 *            )
 *
 */
 '''

import os, sys, ssl, time, datetime, json, logging, traceback
import paho.mqtt.client as mqtt
import RPi.GPIO as GPIO
from random import randint
from apscheduler.schedulers.background import BackgroundScheduler
## extend main class to include flags etc
mqtt.Client.reported_led_status = "ON"
mqtt.Client.test_time_text = "0000.0000"
mqtt.Client.test_time_flag = "match"

## on a new line put the program name with version
print ("\n  Starting AWS IOT MQTT over SSL test client (2019JUN19)")

## Make sure these are correct
##
aws_iot_endpoint = "abcdefgh123456.iot.us-east-1.amazonaws.com"   ## <random>.iot.<region>.amazonaws.com
thingname = "myThingName"
led_pin = 12   ## GPIO pin used for the led
switch_pin = 19   ## GPIO pin used for the switch
cert = "./demo-cert/" + thingname + "-cert.pem"   ## relative location of your thing certificate - mine was in a subirectory called demo-cert and named myThingName-cert.pem
private = "./demo-cert/" + thingname + "-privKey.pem"   ## relative location of your thing private key - mine was in a subirectory called demo-cert and named myThingName-privKey.pem
ca = "./demo-cert/AmazonRootCA1.pem"   ## yip you guessed it - the relative path to the Amazon Root CA1
log_level = "INFO"   ## Default is INFO, logging levels are DEBUG, INFO, WARNING, ERROR, CRITICAL
##
## these should not need to be changed
IoT_protocol_name = "x-amzn-mqtt-ca"
url = "https://{}".format(aws_iot_endpoint)
topicSU = "$aws/things/" + thingname + "/shadow/update"   ## define the shadow topics for our thing
topicSUA = "$aws/things/" + thingname + "/shadow/update/accepted"
topicSUR = "$aws/things/" + thingname + "/shadow/update/rejected"
topicSUD = "$aws/things/" + thingname + "/shadow/update/delta"
topicSUX = "$aws/things/" + thingname + "/shadow/update/document"
topicSG = "$aws/things/" + thingname + "/shadow/get"
topicSGA = "$aws/things/" + thingname + "/shadow/get/accepted"
topicSGR = "$aws/things/" + thingname + "/shadow/get/rejected"
topicSD = "$aws/things/" + thingname + "/shadow/delete"
topicSDA = "$aws/things/" + thingname + "/shadow/delete/accepted"
topicSDR = "$aws/things/" + thingname + "/shadow/delete/rejected"
topicSH = "$aws/things/" + thingname + "/shadow/#"
topicTT = thingname + "/test"   ## define the test topic for our thing
led = "ON"
shadow_state_reported_on = json.dumps({"state": {"reported": {"LED": "ON"}}})
shadow_state_reported_off = json.dumps({"state": {"reported": {"LED": "OFF"}}})
restart_name = os.path.abspath(__file__)
## setup logging
logger = logging.getLogger()
logger.setLevel(log_level)
handler = logging.StreamHandler(sys.stdout)
log_format = logging.Formatter('%(asctime)s - %(levelname)s - (%(threadName)s) - [%(name)s] - %(message)s')
handler.setFormatter(log_format)
logger.addHandler(handler)
## Start the scheduler and set the logging level lower as it was overly verbose and not helpful
scheduler = BackgroundScheduler()
scheduler.start()
logging.getLogger('apscheduler').setLevel(logging.WARNING)
## Initiate GPIO for LED and Switch
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)
GPIO.setup(led_pin, GPIO.OUT)
GPIO.setup(switch_pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
## Print Debug messages for cert and key
logger.debug("   cert: " + cert)   ## debug cert and path
logger.debug("private: " + private)   ## debug privKey and path
logger.debug("     ca: " + ca)   ## debug Amazon Root CA and path

def ssl_alpn():
    try:
        logger.info("open ssl version:{}".format(ssl.OPENSSL_VERSION))   ## display the opnessl version
        ssl_context = ssl.create_default_context()
        ssl_context.set_alpn_protocols([IoT_protocol_name])
        ssl_context.load_verify_locations(cafile=ca)
        ssl_context.load_cert_chain(certfile=cert, keyfile=private)
        return  ssl_context
    except Exception as e:
        logger.error("exception ssl_alpn()")
        logger.error("Make sure your path and file names for your cert and key are correct")
        logger.error("    cert = " + cert)
        logger.error(" private = " + private)
        logger.error("      ca = " + ca)
        raise e

def on_log(client, userdata, level, string):
    logger.debug("LOG: " + string)

def on_message(client, userdata, message):
    if str(message.topic) == topicTT:
        if mqtt.Client.test_time_text == str(message.payload.decode("utf-8")):
            mqtt.Client.test_time_flag = "match"
        else:
            logger.info("AWS Response Topic: " + str(message.topic))
            logger.info("QoS: " + str(message.qos))
            logger.info("Payload: " + str(message.payload.decode("utf-8")))
    elif str(message.topic) == topicSUD:
        logger.debug("AWS Response Topic: " + str(message.topic))
        logger.debug("QoS: " + str(message.qos))
        logger.debug("Payload: " + str(message.payload.decode("utf-8")))
        shadow_state_delta = ""
        shadow_state_delta = json.loads(message.payload.decode("utf-8"))
        LED_Status_Change(shadow_state_delta, "delta")
    elif str(message.topic) == topicSGA:
        logger.debug("AWS Response Topic: " + str(message.topic))
        logger.debug("QoS: " + str(message.qos))
        logger.debug("Payload: " + str(message.payload.decode("utf-8")))
        shadow_state_doc = ""
        shadow_state_doc = json.loads(message.payload.decode("utf-8"))
        mqtt1.unsubscribe(topicSGA)   ## unsubscribe to topic
        logger.info(topicSGA + " unsubscribed")
        mqtt1.unsubscribe(topicSGR)   ## unsubscribe to topic
        logger.info(topicSGR + " unsubscribed")
        LED_Status_Change(shadow_state_doc, "get_req")
    elif str(message.topic) == topicSGR:
        logger.debug("AWS Response Topic: " + str(message.topic))
        logger.debug("QoS: " + str(message.qos))
        logger.debug("Payload: " + str(message.payload.decode("utf-8")))
        shadow_state_init_txt = json.dumps({"state": {"desired": {"LED": "OFF"}}})
        mqtt1.publish(topicSU, shadow_state_init_txt, qos=1)
        mqtt1.unsubscribe(topicSGR)   ## unsubscribe to topic
        logger.info(topicSGA + " unsubscribed")
        mqtt1.unsubscribe(topicSGA)   ## unsubscribe to topic
        logger.info(topicSGR + " unsubscribed")
    else:
        logger.warning("Received Unexpected Message")
        logger.warning("AWS Response Topic: " + str(message.topic))
        logger.warning("QoS: " + str(message.qos))
        logger.warning("Payload: " + str(message.payload.decode("utf-8")))

def LED_Status_Change(shadow_doc, req_type):
    desired_led_status = ""   ## get the desired led status
    if req_type == "delta":
        desired_led_status = str(shadow_doc['state']['LED'])
    elif req_type == "get_req":
        desired_led_status = str(shadow_doc['state']['desired']['LED'])
    if desired_led_status == "ON":   ## set the LED to the desired state
        mqtt.Client.reported_led_status = "ON"
        GPIO.output(led_pin, GPIO.HIGH)   ## turn LED ON
        mqtt1.publish(topicSU,shadow_state_reported_on,qos=1)   ## Report LED ON Status back to Shadow
        logger.info("\rReceived Request. \"LED: ON\"    Reporting Status to Shadow State \"LED: ON\"")
    elif desired_led_status == "OFF":
        mqtt.Client.reported_led_status = "OFF"
        GPIO.output(led_pin, GPIO.LOW)   ## turn LED OFF
        mqtt1.publish(topicSU,shadow_state_reported_off,qos=1)   ## Report LED OFF Status back to Shadow
        logger.info("\rReceived Request. \"LED: OFF\"   Reporting Status to Shadow State \"LED: OFF\"")
    else:   ## invalid LED status requested
        logger.warning("\r---WARNING--- Invalid Request attempting to set LED to \"" + str(desired_led_status) + "\"")
        logger.warning("              Request should set LED to either \"ON\" or \"OFF\"")

def send_test():   ## we usually run this no more than once a minute but we enforce at least 30 seconds of cooldown inside of this function
    testtime = time.time()   ## see notes at top of file about this function and replacing it with a callback
    if (float(mqtt.Client.test_time_text) + 30) < testtime:
        mqtt.Client.test_time_text = str(testtime)
        mqtt.Client.test_time_flag = "nomatch"
        mqtt1.publish(topicTT,mqtt.Client.test_time_text,qos=1)
    else:
        logger.warning("\r---WARNING--- send_test is being called while still on cooldown (30 second minimum cooldown)")

if __name__ == '__main__':
    try:
        currdatetime = datetime.datetime.now()
        curr_min = currdatetime.minute
        rand_min = curr_min % 10   ## if you use this with something that could be a negative integer be sure and wrap abs() around curr_min
        min_list = str(rand_min) + "," + str((rand_min + 10)) + "," + str((rand_min + 20)) + "," + str((rand_min + 30)) + "," + str((rand_min + 40)) + "," + str((rand_min + 50))
        rand_sec = str(randint(0, 59))
        disp_sec = rand_sec.rjust(2, '0')   ## pad the seconds with zeros so that it always appears as two digits
        logger.debug("Scheduled MQTT Communication Test Times - every hour at (min:sec):   0" + str(rand_min) + ":" + disp_sec + ",   " + str((rand_min + 10)) + ":" + disp_sec + ",   " + str((rand_min + 20)) + ":" + disp_sec + ",   " + str((rand_min + 30)) + ":" + disp_sec + ",   " + str((rand_min + 40)) + ":" + disp_sec + ",   " + str((rand_min + 50)) + ":" + disp_sec)
        # mqtt1 = mqtt.Client()   ## create mqtt client instance with random client id and default optional settings (clean_session=True, userdata=None, protocol=MQTTv311, transport="tcp")
        mqtt1 = mqtt.Client(client_id=thingname)   ## create mqtt client instance with specified client id = to thing name
        logger.info("start aws mqtt over https connection")
        ssl_context= ssl_alpn()   ## setup to use mqtt over https
        mqtt1.tls_set_context(context=ssl_context)   ## start mqtt over https
        if log_level == "DEBUG":
            mqtt1.on_log=on_log   ## Paho MQTT function logging - displays the details of connecting, keep alives, subscribing, and publishing
        mqtt1.on_message=on_message   ## attach function to callback
        mqtt1.connect(aws_iot_endpoint, port=443, keepalive=45)   ## connect to broker on tcp 443
        time.sleep(3)   ## make sure we get connected before subscribing
        logger.info("aws mqtt connection successful")
        mqtt1.subscribe(topicSUD, 1)   ## subscribe to topic
        logger.info(topicSUD + " subscribed")
        mqtt1.subscribe(topicSGA, 1)   ## subscribe to topic
        logger.info(topicSGA + " subscribed")
        mqtt1.subscribe(topicSGR, 1)   ## subscribe to topic
        logger.info(topicSGR + " subscribed")
        mqtt1.subscribe(topicTT, 1)   ## subscribe to topic
        logger.info(topicTT + " subscribed")
        mqtt1.loop_start()   ## start loop to process callbacks
        time.sleep(3)   ## make sure the loop is started before we publish
        mqtt1.publish(topicSG,"",qos=1)   ## if exist get the current shadow if not create it
        time.sleep(1)
        scheduler.add_job(send_test, trigger='cron', minute=min_list, second=rand_sec) # Schedules a re-occuring non-blocking test message once for each minute requested in the list
        while True:
            if mqtt.Client.test_time_flag != "match":   ## check if we have run the send_test logic
                testtime = time.time()
                if (float(mqtt.Client.test_time_text) + 5) < testtime:   ## if we have gone more than 5 seconds without seeing our send_test come back then perform a restart
                    scheduler.shutdown()   ## stop the scheduler
                    mqtt1.loop_stop()   ## stops mqtt processing loop
                    mqtt1.disconnect()   ## mqtt disconnect gracefully
                    time.sleep(1)
                    os.execl(sys.executable, 'python3', restart_name, *sys.argv[1:])   ## restart this python script
            input_state = GPIO.input(switch_pin)
            if input_state == False:   ## check if the local button has been pushed and if so toggle the led requested status based on current reported status
                time.sleep(0.25)
                if mqtt.Client.reported_led_status == "ON":
                    led = "OFF"
                else:
                    led = "ON"
                shadow_state_desired_status = json.dumps({"state": {"desired": {"LED": led}}})
                logger.debug("Local switch pressed")
                logger.debug("local led desired value: " + led)
                logger.debug("Publishing to " + topicSU + ":{}".format(shadow_state_desired_status))
                mqtt1.publish(topicSU, shadow_state_desired_status, qos=1)
            time.sleep(0.25)
    except KeyboardInterrupt:
        logger.error("\rKeyboardInterrupt received                                              \nThis may take a few seconds, please wait...")
        scheduler.shutdown()   ## stop the scheduler
        mqtt1.loop_stop()   ## stops mqtt processing loop
        mqtt1.disconnect()   ## mqtt disconnect gracefully
        time.sleep(1)
        logger.error("Client exited normally by Keyboard Interrupt")
    except Exception as e:   ## if possible we should exit cleanly
        scheduler.shutdown()   ## stop the scheduler
        mqtt1.loop_stop()   ## stops mqtt processing loop
        mqtt1.disconnect()   ## mqtt disconnect gracefully
        time.sleep(1)
        logger.error("exception main()")
        logger.error("e obj:{}".format(vars(e)))
        logger.error("message:{}".format(e.message))
        traceback.print_exc(file=sys.stdout)

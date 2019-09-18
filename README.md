# ledpubsub
My first attempt at Python or MQTT

AWS IOT Raspberry Pi LED subscriber and switch publisher test python3 client

Using MQTT over HTTPS with ALPN (x-amzn-mqtt-ca) so all communication is over TCP 443

These links were a big help as we *had* to have this working over HTTPS only

https://github.com/aws/aws-iot-device-sdk-python

https://docs.aws.amazon.com/iot/latest/developerguide/protocols.html

https://stackoverflow.com/questions/48998420/aws-iot-use-mqtt-on-port-443


# iotpolicy
A strict but large IOT policy allowing the client to connect, (read, write, and delete the shadow), and (read and write to the test topic)

Only one client can be connected at any given time

The client name must match the thingname

const ddbGeo = require('dynamodb-geo');
const AWS = require('aws-sdk');

AWS
    .config
    .update({accessKeyId: process.env.AWS_KEY, secretAccessKey: process.env.AWS_SECRET});

AWS
    .config
    .update({region: "us-west-1"});

async function publishSNSMessage(message, event_type, flow_sid) {

    var params = {
        Message: JSON.stringify(message),
        /* required */
        TopicArn: process.env.SNS_TOPIC,
        MessageAttributes: {
            "event.type": {
                DataType: "String",
                StringValue: event_type
            },
            "event.flow": {
                DataType: "String",
                StringValue: flow_sid
            }
        }
    };

    // Create promise and SNS service object
    try {
        var publishTextPromise = new AWS
            .SNS({apiVersion: '2010-03-31'})
            .publish(params)
            .promise();

        await publishTextPromise.then(function (data) {
            console.log(`Message ${params.Message} send sent to the topic ${params.TopicArn}`);
            console.log("Message " + data);
            var jsonData = JSON.parse(params.Message);

            console.log("json data:");
            console.log(jsonData);
            return;
        })
            .catch(function (err) {
                console.error(err, err.stack);
                return err;
            });

    } catch (err) {
        return err;
    }

}

async function asyncForEach(array, callback) {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
    }
}

exports.handler = async(event) => {

    // in this function we need ot do a geo search based on the incoming
    // latitude/longitudes of the donor.  then we need to either send out no donors
    // found message or a donors found along with a list of the donor phone numbers
    // so we can alert them later.  i believe we need to save that list of donors
    // to the request record so we know who we contacted later.  we may need that
    // list in order to send updates and cancellation notifications let's parse this
    // incoming record let's parse this incoming record
    await asyncForEach(event.Records, async(record) => {
        console.log("INCOMING RECORD.MESSAGE: ");
        console.log(record);
        let message = JSON.parse(record.body);
        let event_type = record.messageAttributes["event.type"].stringValue;
        let flow_sid = record.messageAttributes["event.flow"].stringValue;

        console.log("EVENT TYPE: " + event_type);
        //need to parse twice since the json gets escaped twice

        console.log("message: ");
        console.log(message);

        // let entity = event.entity; let entityKey = event.key; let entityLocation =
        // JSON.parse(event.location); let params = JSON.parse(event.params);
        const ddb = new AWS.DynamoDB({endpoint: "dynamodb.us-west-1.amazonaws.com"});

        // Configuration for a new instance of a GeoDataManager. Each GeoDataManager
        // instance represents a table
        const config = new ddbGeo.GeoDataManagerConfiguration(ddb, message.entity);
        const myGeoTableManager = new ddbGeo.GeoDataManager(config);
        console.log("Toggling " + message.params.alerts.BOOL + " to " + !message.params.alerts.BOOL);
        // Instantiate the table manager
        await myGeoTableManager.updatePoint({
            RangeKeyValue: {
                S: message.to
            }, // Use this to ensure uniqueness of the hash/range pairs.
            GeoPoint: { // An object specifying latitutde and longitude as plain numbers. Used to build the geohash, the hashkey and geojson data
                latitude: message.params.lat,
                longitude: message.params.lon
            },
            UpdateItemInput: { // Passed through to the underlying DynamoDB.putItem request. TableName is filled in for you.
                UpdateExpression: 'SET alerts = :alerts',
                ExpressionAttributeValues: {
                    ':alerts': {
                        BOOL: !message.params.alerts.BOOL
                    }
                }
                // ... Anything else to pass through to `putItem`, eg ConditionExpression
                }
            })
            .promise()
            .then(async() => {
                let toggleStatus = (!message.params.alerts.BOOL)
                    ? "on"
                    : "off";
                await publishSNSMessage({
                    "from": message.from,
                    "to": message.to,
                    "params": {
                        "type": "donor.alerts.toggled." + toggleStatus
                    }
                }, "donor.alerts.toggled." + toggleStatus, flow_sid).then((data) => {
                    return;
                });
            });

    });

};
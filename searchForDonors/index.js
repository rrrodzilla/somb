const ddbGeo = require('dynamodb-geo');
const AWS = require('aws-sdk');

AWS
    .config
    .update({accessKeyId: process.env.AWS_KEY, secretAccessKey: process.env.AWS_SECRET});

AWS
    .config
    .update({region: "us-west-1"});

async function asyncForEach(array, callback) {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
    }
}

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

exports.handler = async(event) => {

    // in this function we need ot do a geo search based on the incoming
    // latitude/longitudes of the donor.  then we need to either send out no donors
    // found message or a donors found along with a list of the donor phone numbers
    // so we can alert them later.  i believe we need to save that list of donors to
    // the request record so we know who we contacted later.  we may need that list
    // in order to send updates and cancellation notifications let's parse this
    // incoming record let's parse this incoming record

    await asyncForEach(event.Records, async(record) => {
        let message = JSON.parse(record.body);
        let event_type = record.messageAttributes["event.type"].stringValue;
        let flow_sid = record.messageAttributes["event.flow"].stringValue;

        console.log("INCOMING RECORD.MESSAGE: ");
        console.log("EVENT TYPE: " + event_type);
        //need to parse twice since the json gets escaped twice

        console.log("message: ");
        console.log(message);

        // let entity = event.entity; let entityKey = event.key; let entityLocation =
        // JSON.parse(event.location); let params = JSON.parse(event.params);
        const ddb = new AWS.DynamoDB({endpoint: "dynamodb.us-west-1.amazonaws.com"});

        // Configuration for a new instance of a GeoDataManager. Each GeoDataManager
        // instance represents a table
        const config = new ddbGeo.GeoDataManagerConfiguration(ddb, 'volunteers');
        const myGeoTableManager = new ddbGeo.GeoDataManager(config);

        // Instantiate the table manager

        await myGeoTableManager.queryRadius({
            RadiusInMeter: 5000,
            CenterPoint: {
                latitude: message.params.request.lat,
                longitude: message.params.request.lon
            }
        })
        // Print the results, an array of DynamoDB.AttributeMaps
            .then(async(volunteers) => {

            let results = [];
            volunteers.map((volunteer) => {
                if (volunteer.alerts.BOOL) 
                    results.push(volunteer);
                }
            );

            await publishSNSMessage({
                "from": message.from,
                "to": message.to,
                "params": (results.length === 0)
                    ? {
                        "type": "no.donors.in.area"
                    }
                    : {
                        "type": "donors.in.area",
                        "request": results
                    }
            }, (results.length === 0)
                ? "no.donors.in.area"
                : "donors.in.area", flow_sid).then(async() => {
                //now let's notify the donors we need an asynch foreach
                if (results.length > 0) {

                    await asyncForEach(results, async(donor) => {
                        await publishSNSMessage({
                            "from": "+12062029844",
                            "to": donor.rangeKey.S,
                            "params": {
                                "type": "notify.donors.new.request",
                                "donor": donor.rangeKey.S,
                                "request": message.params.request
                            }
                        }, "notify.donors.new.request", "FW74b64bf4d64b130de3f5857b20bfe6e8").then(() => {
                            return;
                        }).catch(error => {
                            console.log(error);
                        });
                    });

                }

                return;
            });

            console.log(results);
            return results;
        });

    });

};
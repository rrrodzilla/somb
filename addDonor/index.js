const ddbGeo = require('dynamodb-geo');
const AWS = require('aws-sdk');

AWS
    .config
    .update({accessKeyId: process.env.AWS_KEY, secretAccessKey: process.env.AWS_SECRET});

AWS
    .config
    .update({region: "us-west-1"});

async function publishSNSMessage(message, event_type) {

    var params = {
        Message: JSON.stringify(message),
        /* required */
        TopicArn: process.env.SNS_TOPIC,
        MessageAttributes: {
            "event.type": {
                DataType: "String",
                StringValue: event_type
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
    // incoming record
    var record = JSON.parse(event.Records[0].body);
    console.log("INCOMING RECORD: ");
    console.log(record);
    console.log('event.type');
    //event type should be open.new.request
    let event_type = record.MessageAttributes["event.type"].Value;
    console.log("INCOMING RECORD.MESSAGE: ");
    console.log("EVENT TYPE: " + event_type);

    //need to parse twice since the json gets escaped twice
    let msg_org = JSON.parse(record.Message);
    console.log("SUCCESS PARSE 1");
    console.log(msg_org);
    let msg_obj = JSON.parse(msg_org);
    console.log("SUCCESS PARSE 2");

    console.log(msg_obj);

    let lat = parseFloat(msg_obj.params.lat);
    let lon = parseFloat(msg_obj.params.lon);

    let msg = event.message;
    console.log(msg);

    // let entity = event.entity; let entityKey = event.key; let entityLocation =
    // JSON.parse(event.location); let params = JSON.parse(event.params);
    const ddb = new AWS.DynamoDB({endpoint: "dynamodb.us-west-1.amazonaws.com"});

    // Configuration for a new instance of a GeoDataManager. Each GeoDataManager
    // instance represents a table
    const config = new ddbGeo.GeoDataManagerConfiguration(ddb, 'volunteers');
    const myGeoTableManager = new ddbGeo.GeoDataManager(config);

    // Instantiate the table manager
    await myGeoTableManager.putPoint({
        RangeKeyValue: {
            S: msg_obj.to
        }, // Use this to ensure uniqueness of the hash/range pairs.
        GeoPoint: { // An object specifying latitutde and longitude as plain numbers. Used to build the geohash, the hashkey and geojson data
            latitude: lat,
            longitude: lon
        },
        PutItemInput: { // Passed through to the underlying DynamoDB.putItem request. TableName is filled in for you.
            Item: { // The primary key, geohash and geojson data is filled in for you
                country: {
                    S: 'UK'
                }, // Specify attribute values using { type: value } objects, like the DynamoDB API.
                capital: {
                    S: 'London'
                }
            },
                // ... Anything else to pass through to `putItem`, eg ConditionExpression
            }
        })
        .promise()
        .then(async() => {
            await publishSNSMessage({
                "from": msg_obj.from,
                "to": msg_obj.to,
                "params": {
                    "type": "new.donor.added"
                }
            }, "new.donor.added").then((data) => {
                return;
            });
        });

};